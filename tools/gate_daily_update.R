# Daily-update wave: golden-triple gate (2015/2020/2024-06-30).
# Pattern: tools/oldcik_gate_tier2.R. Assembles the three dates into
# cache/gate_daily/pass<N>/ (NEVER cache/snapshots) and verifies against
# production snapshots:
#   A. no production ticker dropped, no unexpected additions
#   B. strengthened validate_snapshot passes on every gate output
#      (zero-Unknown + mask-fires included; daily-layer check excluded
#      here -- production timeseries layers are rebuilt only in the
#      terminal rebuild, where the runner enforces it)
#   C. common tickers: raw values identical EXCEPT (i) cross-section-
#      stage columns (ms_*, ch_inv_ia, herf, ww_index -- sector medians
#      shift when the 24 recovered names join their true sectors),
#      (ii) multiclass-REGISTRY tickers (tier2 exemption), (iii) the 24
#      D2/A sector-fixed tickers (their masked indicators flip to NA and
#      their grouping changes -- the documented intended delta). D2/B is
#      value-neutral by construction, so everything else must be
#      byte-identical.
#   D. truth anchors (tier2 set), market cap priced at the ANNUAL FILING
#      date
#   E. D1 daily-layer anchors: AAPL/NVDA layers rebuilt into a session
#      temp dir; split-window market caps + pe continuity + the
#      validator's split-consistency check
#
# Two consecutive clean passes required before the terminal rebuild:
#   Rscript tools/gate_daily_update.R 1
#   Rscript tools/gate_daily_update.R 2   (also diffs pass2 vs pass1)
# After pass 2: repoint the rebuild runner's determinism baseline at
# cache/gate_daily/pass2 and bump its GATED_COMMIT.

args <- commandArgs(trailingOnly = TRUE)
PASS_N <- if (length(args)) as.integer(args[1]) else 1L
CHECK_ONLY <- "--check-only" %in% args

suppressPackageStartupMessages({ library(data.table); library(arrow) })
suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/sector_classifier.R")
  source("R/ttm_eps.R")
  source("R/indicator_compute.R")
  source("R/pit_assembler.R")
  source("R/timeseries_builder.R")
  source("R/pipeline_runner.R")
})

msg <- function(...) message(sprintf("[gateD:%d] %s", PASS_N, sprintf(...)))
fails <- character(0)
fail <- function(...) { m <- sprintf(...); fails <<- c(fails, m); msg("FAIL %s", m) }

GOLD <- c("2015-06-30", "2020-06-30", "2024-06-30")
OUT  <- file.path("cache/gate_daily", paste0("pass", PASS_N))
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)

XS_OK <- function(cols) grepl("^ms_", cols) | cols %in%
  c("ch_inv_ia", "herf", "ww_index")

# -- phase 0: preflight ------------------------------------------------------
stopifnot(length(.MULTICLASS_REGISTRY) == 16)
stopifnot(identical(sort(names(.PROVIDER_SYMBOL_MAP)),
                    sort(names(.SPLITS_SYMBOL_MAP))))
sec_tbl <- as.data.table(read_parquet("cache/lookups/sector_industry.parquet"))
stopifnot("valid_from" %in% names(sec_tbl))            # D2/B migrated
SECTOR_FIXED <- fread("data/sector_overrides_delisted.csv")$ticker
stopifnot(length(SECTOR_FIXED) == 24)
stopifnot(all(SECTOR_FIXED %in% sec_tbl$ticker))       # D2/A applied
msg("preflight ok: dated sector table, %d sector-fixed names", length(SECTOR_FIXED))

REG_TKS <- vapply(.MULTICLASS_REGISTRY, `[[`, character(1), "ticker")
EXEMPT  <- union(REG_TKS, SECTOR_FIXED)

# -- phase 1: assemble golden dates ------------------------------------------
if (!CHECK_ONLY) {
  for (d in GOLD) {
    msg("assembling %s", d)
    r <- tryCatch(assemble_snapshot(d, output_dir = OUT, prefetch_prices = FALSE),
                  error = function(e) { fail("assemble %s: %s", d, conditionMessage(e)); NULL })
  }
}

# -- phase 2: checks A-C ------------------------------------------------------
for (d in GOLD) {
  gf <- file.path(OUT, sprintf("pit_%s_raw.parquet", d))
  pf <- sprintf("cache/snapshots/pit_%s_raw.parquet", d)
  if (!file.exists(gf)) { fail("missing gate output %s", d); next }
  g <- as.data.table(read_parquet(gf)); p <- as.data.table(read_parquet(pf))

  dropped <- setdiff(p$ticker, g$ticker)
  if (length(dropped)) fail("%s dropped tickers: %s", d, paste(dropped, collapse = ","))
  new_tks <- setdiff(g$ticker, p$ticker)
  if (length(new_tks)) fail("%s unexpected new tickers: %s", d,
                            paste(new_tks, collapse = ","))

  # B: strengthened validator on the gate output (daily layer excluded;
  # it is rebuilt in the terminal rebuild, where the runner enforces it)
  v <- suppressMessages(validate_snapshot(d, output_dir = OUT,
                                          check_daily = FALSE))
  if (!isTRUE(v$pass)) {
    fail("%s validate_snapshot: %s", d,
         paste(names(v$checks)[!v$checks], collapse = ","))
  } else msg("%s: strengthened validate_snapshot PASS (%d checks)",
             d, length(v$checks))

  # C: common tickers byte-stable outside XS columns and the exempt set
  common <- setdiff(intersect(p$ticker, g$ticker), EXEMPT)
  cols <- setdiff(names(p), c("date", "ticker", "industry", "sector"))
  cols_strict <- cols[!XS_OK(cols)]
  setkey(g, ticker); setkey(p, ticker)
  n_drift <- 0L; drift_ex <- character(0)
  for (tk in common) {
    a <- unlist(p[tk, ..cols_strict]); b <- unlist(g[tk, ..cols_strict])
    same <- (is.na(a) & is.na(b)) |
      (!is.na(a) & !is.na(b) & abs(a - b) <= pmax(1e-9, abs(a) * 1e-9))
    if (!all(same)) {
      n_drift <- n_drift + 1L
      if (length(drift_ex) < 8)
        drift_ex <- c(drift_ex, sprintf("%s:%s", tk,
          paste(head(names(a)[!same], 3), collapse = "+")))
    }
  }
  if (n_drift) fail("%s: %d common tickers drift outside XS columns (%s)",
                    d, n_drift, paste(drift_ex, collapse = " "))
  else msg("%s: common tickers stable outside XS columns (D2/B value-neutral)", d)

  # the 24 sector-fixed names: masked indicators must now be NA per sector
  fixed_here <- intersect(SECTOR_FIXED, g$ticker)
  ov <- fread("data/sector_overrides_delisted.csv")
  for (tk in fixed_here) {
    sec <- ov[ticker == tk, sector]
    if (!sec %in% names(.SECTOR_NA_INDICATORS)) next
    mcols <- intersect(.SECTOR_NA_INDICATORS[[sec]], names(g))
    leak <- mcols[vapply(mcols, function(cn) !is.na(g[tk][[cn]]), logical(1))]
    if (length(leak)) fail("%s: %s (%s) mask leak: %s", d, tk, sec,
                           paste(leak, collapse = ","))
  }
  msg("%s: sector-fixed present=%d, Unknown bucket=%d", d,
      length(fixed_here), g[sector == "Unknown", .N])
}

# -- phase 3: truth anchors (D) ----------------------------------------------
anchor <- function(dt, d, tk, col, lo, hi) {
  v <- dt[ticker == tk][[col]]
  ok <- length(v) == 1 && !is.na(v) && v >= lo && v <= hi
  msg("anchor %-5s %s %-12s = %-12s [%g, %g] %s", tk, d, col,
      if (length(v) && !is.na(v)) signif(v, 4) else "NA/ABSENT", lo, hi,
      if (ok) "PASS" else "FAIL")
  if (!ok) fail("anchor %s %s %s", tk, d, col)
  ok
}

g15 <- as.data.table(read_parquet(file.path(OUT, "pit_2015-06-30_raw.parquet")))
g20 <- as.data.table(read_parquet(file.path(OUT, "pit_2020-06-30_raw.parquet")))
g24 <- as.data.table(read_parquet(file.path(OUT, "pit_2024-06-30_raw.parquet")))
anchor(g15, "2015", "AAPL",  "market_cap", 5.5e11, 7.0e11)
anchor(g15, "2015", "NVDA",  "pe_trailing", 12, 35)
anchor(g15, "2015", "BRK.B", "market_cap", 2.8e11, 3.9e11)
anchor(g24, "2024", "BRK.B", "market_cap", 8.0e11, 10.0e11)
anchor(g24, "2024", "V",     "market_cap", 4.3e11, 6.0e11)
anchor(g24, "2024", "GDDY",  "pe_trailing", 8, 20)
anchor(g15, "2015", "AET",  "market_cap", 2.5e10, 5.5e10)
anchor(g15, "2015", "ESRX", "market_cap", 3.5e10, 9.0e10)
anchor(g15, "2015", "TWX",  "market_cap", 5.0e10, 9.5e10)
anchor(g15, "2015", "JWN",  "market_cap", 0.8e10, 2.2e10)
anchor(g15, "2015", "TIF",  "market_cap", 0.7e10, 1.6e10)
anchor(g20, "2020", "FLIR", "market_cap", 3.0e9, 9.0e9)
anchor(g20, "2020", "TIF",  "market_cap", 0.9e10, 2.2e10)
# sector-fixed names now carry a sector (D3 dead) -- spot anchors
anchor(g20, "2020", "TWTR", "market_cap", 1.5e10, 4.5e10)
anchor(g15, "2015", "XLNX", "market_cap", 0.8e10, 1.6e10)

# -- phase 4: D1 daily-layer anchors (E) --------------------------------------
msg("D1 daily-layer anchors (AAPL/NVDA into temp ts_dir)")
tmp_ts <- file.path(tempdir(), "gate_d1_ts")
dir.create(tmp_ts, showWarnings = FALSE)
m <- as.data.table(read_parquet("cache/lookups/constituent_master.parquet"))
for (tk in c("AAPL", "NVDA")) {
  cik <- m[ticker == tk, cik][1]
  sec <- .sector_asof(sec_tbl, Sys.Date())[ticker == tk, sector]
  invisible(build_ticker_fundamentals(tk, cik, sec, ts_dir = tmp_ts, force = TRUE))
  invisible(update_ticker_daily(tk, through_date = as.Date("2026-06-30"),
                                ts_dir = tmp_ts))
}
aapl <- as.data.table(read_parquet(file.path(tmp_ts, "AAPL_daily.parquet")))
nvda <- as.data.table(read_parquet(file.path(tmp_ts, "NVDA_daily.parquet")))
aapl[, date := as.Date(date)]; nvda[, date := as.Date(date)]
aapl[, ticker := "AAPL"]; nvda[, ticker := "NVDA"]
invisible(anchor(aapl[date == as.Date("2020-09-15")], "d1", "AAPL", "market_cap", 1.7e12, 2.2e12))
invisible(anchor(aapl[date == as.Date("2014-07-15")], "d1", "AAPL", "market_cap", 5.0e11, 6.5e11))
invisible(anchor(nvda[date == as.Date("2024-06-28")], "d1", "NVDA", "market_cap", 2.7e12, 3.4e12))
invisible(anchor(nvda[date == as.Date("2024-06-28")], "d1", "NVDA", "pe_trailing", 80, 130))
invisible(anchor(nvda[date == as.Date("2024-06-28")], "d1", "NVDA", "peg", 0.05, 1))
dl <- suppressMessages(validate_daily_layer(ts_dir = tmp_ts))
if (!isTRUE(dl$checks["split_consistent_market_cap"])) {
  fail("D1 split consistency on temp layers")
} else msg("D1 split consistency: %d events, all consistent",
           dl$details$n_split_events_checked)

# -- verdict ------------------------------------------------------------------
msg("==================================================")
if (length(fails)) {
  msg("VERDICT: FAIL (%d)", length(fails))
  for (f in fails) msg("  - %s", f)
  quit(status = 1)
} else {
  msg("VERDICT: CLEAN PASS %d", PASS_N)
  if (PASS_N >= 2) {
    for (d in GOLD) {
      a <- as.data.table(read_parquet(file.path("cache/gate_daily/pass1",
                                                sprintf("pit_%s_raw.parquet", d))))
      b <- as.data.table(read_parquet(file.path("cache/gate_daily/pass2",
                                                sprintf("pit_%s_raw.parquet", d))))
      if (!isTRUE(all.equal(a, b, tolerance = 0))) {
        msg("PASS-STABILITY FAIL at %s", d); quit(status = 1)
      }
    }
    msg("pass1 == pass2 byte-stable: gate COMPLETE")
  }
}
