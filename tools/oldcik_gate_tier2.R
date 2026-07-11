# Old-CIK wave Tier 2: golden-triple gate (2015/2020/2024-06-30).
# Assembles the three dates into cache/gate_tier2/pass<N>/ (NEVER
# cache/snapshots) and verifies against production snapshots:
#   A. no production ticker dropped
#   B. recovered names light up (marquee minimum set per date)
#   C. common tickers: raw values identical EXCEPT cross-section-stage
#      columns (ms_*, ch_inv_ia, herf, ww_index) -- the Wave M contract
#   D. truth anchors incl. recovered names, priced at ANNUAL FILING date
#   E. market-cap sanity sweep on every recovered name present
# Phase 0 warms the splits caches for every recovered name (the snapshot
# loop loads with fetch=FALSE -- the UAA half-market-cap lesson).
#
# Two consecutive clean passes required before the terminal rebuild:
#   Rscript tools/oldcik_gate_tier2.R 1
#   Rscript tools/oldcik_gate_tier2.R 2   (also diffs pass2 vs pass1)
# Known allowance: PRU revenue_surprise at 2015 (cache-vintage drift,
# reconciled by the terminal rebuild).

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
})

msg <- function(...) message(sprintf("[gate2:%d] %s", PASS_N, sprintf(...)))
fails <- character(0)
fail <- function(...) { m <- sprintf(...); fails <<- c(fails, m); msg("FAIL %s", m) }

GOLD <- c("2015-06-30", "2020-06-30", "2024-06-30")
OUT  <- file.path("cache/gate_tier2", paste0("pass", PASS_N))
dir.create(OUT, recursive = TRUE, showWarnings = FALSE)

# cross-section-stage columns allowed to drift on non-target tickers
XS_OK <- function(cols) grepl("^ms_", cols) | cols %in%
  c("ch_inv_ia", "herf", "ww_index")

# -- phase 0: preflight ------------------------------------------------------
stopifnot(identical(sort(names(.PROVIDER_SYMBOL_MAP)),
                    sort(names(.SPLITS_SYMBOL_MAP))))
stopifnot(all(.SPLITS_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)] ==
                .PROVIDER_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)]))
stopifnot(length(.MULTICLASS_REGISTRY) == 16)
m <- as.data.table(read_parquet("cache/lookups/constituent_master.parquet"))
stopifnot(nrow(m) == 850)
res <- as.data.table(read_parquet("cache/lookups/oldcik_cik_resolution.parquet"))[verdict == "RESOLVED"]
recovered <- sort(unique(c(res$ticker, "NWSA", "FOXA", "FOX", "CCE", "ESRX")))
msg("preflight ok: %d recovered names", length(recovered))

# -- phase 1: warm splits (idempotent; cache-hit is a no-op) -----------------
if (!CHECK_ONLY) {
  n_warm <- 0L
  for (tk in recovered) {
    s <- tryCatch(load_ticker_splits(tk, fetch = TRUE), error = function(e) NULL)
    if (!is.null(s)) n_warm <- n_warm + 1L
  }
  msg("splits warm: %d/%d have a cache", n_warm, length(recovered))

  # -- phase 2: assemble golden dates ----------------------------------------
  for (d in GOLD) {
    msg("assembling %s", d)
    r <- tryCatch(assemble_snapshot(d, output_dir = OUT, prefetch_prices = FALSE),
                  error = function(e) { fail("assemble %s: %s", d, conditionMessage(e)); NULL })
  }
}

# Multiclass-registry tickers are WAVE TARGETS: their caches were
# re-patched after the last production rebuild (Tier 1 added NWS/FOX;
# the 2026-07-09 re-patch shifted DISCA/PRU vintages), so their drift vs
# production is the documented intended delta. Anchors still bind them.
REG_TKS <- vapply(.MULTICLASS_REGISTRY, `[[`, character(1), "ticker")

# -- phase 3: checks ---------------------------------------------------------
anchor <- function(dt, d, tk, col, lo, hi) {
  v <- dt[ticker == tk][[col]]
  ok <- length(v) == 1 && !is.na(v) && v >= lo && v <= hi
  msg("anchor %-5s %s %-12s = %-12s [%g, %g] %s", tk, d, col,
      if (length(v) && !is.na(v)) signif(v, 4) else "NA/ABSENT", lo, hi,
      if (ok) "PASS" else "FAIL")
  if (!ok) fail("anchor %s %s %s", tk, d, col)
  ok
}

MARQUEE <- list(
  "2015-06-30" = c("AET", "ESRX", "JWN", "TIF", "ACE", "TYC", "JEC",
                   "HRS", "CELG", "ALXN", "CCE"),
  "2020-06-30" = c("TIF", "NBL", "FLIR", "VAR", "ETFC"),
  "2024-06-30" = character(0))

for (d in GOLD) {
  gf <- file.path(OUT, sprintf("pit_%s_raw.parquet", d))
  pf <- sprintf("cache/snapshots/pit_%s_raw.parquet", d)
  if (!file.exists(gf)) { fail("missing gate output %s", d); next }
  g <- as.data.table(read_parquet(gf)); p <- as.data.table(read_parquet(pf))

  dropped <- setdiff(p$ticker, g$ticker)
  if (length(dropped)) fail("%s dropped tickers: %s", d, paste(dropped, collapse = ","))
  new_tks <- setdiff(g$ticker, p$ticker)
  msg("%s: +%d new tickers (%s)", d, length(new_tks),
      paste(head(sort(new_tks), 30), collapse = ","))

  miss <- setdiff(MARQUEE[[d]], g$ticker)
  if (length(miss)) fail("%s marquee names absent: %s", d, paste(miss, collapse = ","))

  # C: common tickers byte-stable outside cross-section-stage columns
  # (registry tickers exempt: wave targets, see REG_TKS above)
  common <- setdiff(intersect(p$ticker, g$ticker), REG_TKS)
  cols <- setdiff(names(p), c("date", "ticker", "industry", "sector"))
  cols_strict <- cols[!XS_OK(cols)]
  setkey(g, ticker); setkey(p, ticker)
  n_drift <- 0L; drift_ex <- character(0)
  for (tk in common) {
    a <- unlist(p[tk, ..cols_strict]); b <- unlist(g[tk, ..cols_strict])
    same <- (is.na(a) & is.na(b)) |
      (!is.na(a) & !is.na(b) & abs(a - b) <= pmax(1e-9, abs(a) * 1e-9))
    if (!all(same)) {
      bad <- names(a)[!same]
      # known allowance: PRU revenue_surprise 2015 cache-vintage drift
      bad <- setdiff(bad, if (tk == "PRU" && d == "2015-06-30") "revenue_surprise" else character(0))
      if (length(bad)) {
        n_drift <- n_drift + 1L
        if (length(drift_ex) < 8)
          drift_ex <- c(drift_ex, sprintf("%s:%s", tk, paste(head(bad, 3), collapse = "+")))
      }
    }
  }
  if (n_drift) fail("%s: %d common tickers drift outside XS columns (%s)",
                    d, n_drift, paste(drift_ex, collapse = " "))
  else msg("%s: common tickers stable outside XS columns", d)

  # E: mcap sanity on recovered names present
  rec_here <- intersect(recovered, g$ticker)
  mc <- g[ticker %in% rec_here, .(ticker, market_cap)]
  bad_mc <- mc[!is.na(market_cap) & (market_cap < 2e8 | market_cap > 3e12)]
  if (nrow(bad_mc)) fail("%s absurd market caps: %s", d,
    paste(sprintf("%s=%.3g", bad_mc$ticker, bad_mc$market_cap), collapse = ","))
  msg("%s: recovered present=%d, with mcap=%d, mcap-NA=%d", d,
      nrow(mc), mc[!is.na(market_cap), .N], mc[is.na(market_cap), .N])
}

# D: truth anchors (bands generous; filing-date price basis)
g15 <- as.data.table(read_parquet(file.path(OUT, "pit_2015-06-30_raw.parquet")))
g20 <- as.data.table(read_parquet(file.path(OUT, "pit_2020-06-30_raw.parquet")))
g24 <- as.data.table(read_parquet(file.path(OUT, "pit_2024-06-30_raw.parquet")))
# legacy anchors (must keep passing)
anchor(g15, "2015", "AAPL",  "market_cap", 5.5e11, 7.0e11)
anchor(g15, "2015", "NVDA",  "pe_trailing", 12, 35)
anchor(g15, "2015", "BRK.B", "market_cap", 2.8e11, 3.9e11)
anchor(g24, "2024", "BRK.B", "market_cap", 8.0e11, 10.0e11)
anchor(g24, "2024", "V",     "market_cap", 4.3e11, 6.0e11)
anchor(g24, "2024", "GDDY",  "pe_trailing", 8, 20)
# recovered-name anchors
anchor(g15, "2015", "AET",  "market_cap", 2.5e10, 5.5e10)
anchor(g15, "2015", "ESRX", "market_cap", 3.5e10, 9.0e10)
anchor(g15, "2015", "TWX",  "market_cap", 5.0e10, 9.5e10)
anchor(g15, "2015", "JWN",  "market_cap", 0.8e10, 2.2e10)
anchor(g15, "2015", "TIF",  "market_cap", 0.7e10, 1.6e10)
anchor(g20, "2020", "FLIR", "market_cap", 3.0e9, 9.0e9)
anchor(g20, "2020", "TIF",  "market_cap", 0.9e10, 2.2e10)

msg("==================================================")
if (length(fails)) {
  msg("VERDICT: FAIL (%d)", length(fails))
  for (f in fails) msg("  - %s", f)
  quit(status = 1)
} else {
  msg("VERDICT: CLEAN PASS %d", PASS_N)
  if (PASS_N >= 2) {
    # stability across passes: byte-identical outputs
    for (d in GOLD) {
      a <- as.data.table(read_parquet(file.path("cache/gate_tier2/pass1",
                                                sprintf("pit_%s_raw.parquet", d))))
      b <- as.data.table(read_parquet(file.path("cache/gate_tier2/pass2",
                                                sprintf("pit_%s_raw.parquet", d))))
      if (!isTRUE(all.equal(a, b, tolerance = 0))) {
        msg("PASS-STABILITY FAIL at %s", d); quit(status = 1)
      }
    }
    msg("pass1 == pass2 byte-stable: gate COMPLETE")
  }
}
