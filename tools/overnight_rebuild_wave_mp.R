# Overnight terminal rebuild after Waves M (multi-class shares/EPS) and
# P (Tiingo price fallback). Designed against every failure mode in
# docs/THE_DISASTER_REBUILD_LESSON.md:
#   - hard preflight (no exists() degradation, no stale module trees)
#   - golden-date determinism check before touching anything
#   - per-date state file: resumable without stale-date ambiguity
#   - never stop on a single date/ticker failure; collect and report
#   - built-in phase-4 verification with a final verdict block
#
# Usage:
#   caffeinate -i Rscript tools/overnight_rebuild_wave_mp.R [--preflight]
# Resume after interruption: rerun the same command; completed dates are
# skipped via the state file (delete tools/../cache/.rebuild_state to
# force a fresh pass).

t_run0 <- Sys.time()
args <- commandArgs(trailingOnly = TRUE)
preflight_only <- "--preflight" %in% args

STATE_FILE <- "cache/.rebuild_state_wave_mp"
GATED_COMMIT <- "015a512"

msg <- function(...) message(sprintf("[%s] %s",
  format(Sys.time(), "%H:%M:%S"), sprintf(...)))

# =============================================================================
# Phase 0: preflight (fail fast, loud)
# =============================================================================
msg("phase 0: preflight")

if (!file.exists("CLAUDE.md") || !dir.exists("R")) {
  stop("run from the repo root")
}
git_hash <- tryCatch(system("git rev-parse --short HEAD", intern = TRUE),
                     error = function(e) "unknown")
msg("git HEAD: %s (gated modules commit: %s)", git_hash, GATED_COMMIT)
# The R/ modules must be IDENTICAL to the gated commit (tools/docs may
# move); a dirty or drifted module tree is exactly how the postmortem's
# clobbered-output failure happened.
r_drift <- suppressWarnings(system(
  sprintf("git diff --quiet %s -- R/ && git diff --quiet -- R/", GATED_COMMIT)))
if (r_drift != 0) {
  stop(sprintf("R/ modules differ from gated commit %s -- refusing to rebuild on unverified code",
               GATED_COMMIT))
}

suppressWarnings(suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/sector_classifier.R")
  source("R/ttm_eps.R")
  source("R/indicator_compute.R")
  source("R/pit_assembler.R")
  source("R/timeseries_builder.R")
  source("R/pipeline_runner.R")
}))

# Rule 3 of the postmortem: never let a missing module degrade silently.
required_fns <- c("load_ticker_splits", ".split_factor", ".merge_multiclass",
                  ".fetch_prices_tiingo", ".tiingo_convert",
                  ".provider_symbol", ".price_cache_path",
                  "compute_ticker_indicators", "assemble_snapshot",
                  "build_timeseries", "augment_daily_ttm",
                  "validate_snapshot", "pit_dedup")
missing <- required_fns[!vapply(required_fns, exists, logical(1))]
if (length(missing)) stop("missing functions: ", paste(missing, collapse = ", "))

stopifnot(length(.MULTICLASS_REGISTRY) == 16)
stopifnot(identical(sort(names(.PROVIDER_SYMBOL_MAP)),
                    sort(names(.SPLITS_SYMBOL_MAP))))
stopifnot(all(.SPLITS_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)] ==
                .PROVIDER_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)]))
if (.tiingo_token() == "") {
  msg("WARNING: no Tiingo token -- OK only because all price caches are warm")
}

# canonical date grid = the existing snapshot files (never invent a grid)
dates <- sort(sub("pit_(.*)_raw\\.parquet", "\\1",
                  list.files("cache/snapshots", pattern = "_raw\\.parquet$")))
stopifnot(length(dates) == 66)
msg("date grid: %d dates, %s .. %s", length(dates), dates[1], dates[length(dates)])

# determinism check: re-assemble the most recent GATED golden date into a
# temp dir and compare against the gated file. Any drift -> stop.
# Tier 2: the baseline is the GATE OUTPUT (cache/gate_tier2/pass2), not
# the production snapshot -- production predates the Tier 1/2 additions
# (FOXA/NWSA/WBD at 2024 etc.; see the f876382 note), so comparing
# against it would flag the gated additions as drift.
msg("determinism check: 2024-06-30 (Tier 2 gate baseline)")
tmp_out <- file.path(tempdir(), "det_check")
dir.create(tmp_out, showWarnings = FALSE)
det <- assemble_snapshot("2024-06-30", output_dir = tmp_out,
                         prefetch_prices = FALSE)
BASELINE_2024 <- "cache/gate_tier2/pass2/pit_2024-06-30_raw.parquet"
if (!file.exists(BASELINE_2024)) stop(
  "Tier 2 gate baseline missing -- run tools/oldcik_gate_tier2.R (2 passes) first")
gold <- as.data.table(arrow::read_parquet(BASELINE_2024))
newf <- as.data.table(arrow::read_parquet(file.path(tmp_out, "pit_2024-06-30_raw.parquet")))
setkey(gold, ticker); setkey(newf, ticker)
stopifnot(identical(gold$ticker, newf$ticker))
cols <- setdiff(names(gold), c("date", "ticker", "industry", "sector"))
n_drift <- 0L
for (tk in gold$ticker) {
  a <- unlist(gold[tk, ..cols]); b <- unlist(newf[tk, ..cols])
  same <- (is.na(a) & is.na(b)) |
    (!is.na(a) & !is.na(b) & abs(a - b) <= pmax(1e-9, abs(a) * 1e-9))
  if (!all(same)) n_drift <- n_drift + 1L
}
if (n_drift > 0) stop(sprintf(
  "determinism check FAILED: %d tickers drifted vs the gated golden file", n_drift))
msg("determinism check PASSED (0 drift)")

if (preflight_only) { msg("preflight only -- exiting clean"); quit(status = 0) }

# =============================================================================
# Phase 1: snapshots (per-date, resumable, never stop on one failure)
# =============================================================================
msg("phase 1: snapshots")
done <- if (file.exists(STATE_FILE)) readLines(STATE_FILE) else character(0)
if (length(done)) msg("resuming: %d dates already completed this run-series", length(done))

failed_dates <- character(0)
for (d in dates) {
  if (d %in% done) next
  ok <- tryCatch({
    r <- assemble_snapshot(d, prefetch_prices = TRUE)
    !is.null(r)
  }, error = function(e) {
    msg("DATE FAILED %s: %s", d, conditionMessage(e)); FALSE
  })
  if (ok) {
    cat(d, "\n", file = STATE_FILE, append = TRUE)
    msg("date done: %s", d)
  } else {
    failed_dates <- c(failed_dates, d)
  }
}
msg("phase 1 complete: %d ok, %d failed (%s)",
    length(dates) - length(failed_dates), length(failed_dates),
    paste(failed_dates, collapse = ","))

# =============================================================================
# Phase 2: timeseries layers for AFFECTED tickers only
# =============================================================================
msg("phase 2: affected timeseries layers")
registry_tks <- vapply(.MULTICLASS_REGISTRY, `[[`, character(1), "ticker")
tiingo_tks <- unique(sub("_tiingo_.*", "", list.files("cache/prices",
                                                      pattern = "_tiingo_")))
mapped_tks <- names(.PROVIDER_SYMBOL_MAP)
# bonus: any price cache written since the waves started (2026-07-08)
fresh <- list.files("cache/prices", pattern = "_yahoo_", full.names = TRUE)
fresh_tks <- sub("_yahoo_.*", "", basename(
  fresh[file.mtime(fresh) >= as.POSIXct("2026-07-08 00:00:00")]))
affected <- sort(unique(c(registry_tks, "MKC", tiingo_tks, mapped_tks,
                          fresh_tks)))
# only tickers that actually have a fundamentals cache get layers
have_fund <- gsub("^\\d+_(.+)\\.parquet$", "\\1",
                  list.files("cache/fundamentals", pattern = "\\.parquet$"))
affected <- intersect(affected, have_fund)
msg("affected tickers: %d (%s...)", length(affected),
    paste(head(affected, 8), collapse = ","))

for (tk in affected) {
  for (sfx in c("_fund.parquet", "_daily.parquet")) {
    f <- file.path("cache/timeseries", paste0(tk, sfx))
    if (file.exists(f)) file.remove(f)
  }
}
invisible(build_timeseries(tickers = affected, end_date = as.Date("2026-06-30")))
msg("phase 2 complete")

# =============================================================================
# Phase 3: TTM augment (canonical, idempotent, all daily layers)
# =============================================================================
msg("phase 3: augment_daily_ttm")
invisible(augment_daily_ttm())
msg("phase 3 complete")

# =============================================================================
# Phase 4: verification (validate all dates + audit + anchors + pin)
# =============================================================================
msg("phase 4: verification")
v_fail <- character(0)
for (d in dates) {
  v <- tryCatch(suppressMessages(validate_snapshot(d)),
                error = function(e) NULL)
  if (is.null(v) || !isTRUE(v$pass)) v_fail <- c(v_fail, d)
}
msg("validate_snapshot: %d/%d pass%s", length(dates) - length(v_fail),
    length(dates),
    if (length(v_fail)) paste0(" FAIL: ", paste(v_fail, collapse = ",")) else "")

# census audit (rule 4): fundamentals-present-but-mcap-NA across all dates
n_gap <- 0L; gap_2024 <- character(0)
for (d in dates) {
  dt <- as.data.table(arrow::read_parquet(
    sprintf("cache/snapshots/pit_%s_raw.parquet", d)))
  g <- dt[is.na(market_cap) & !is.na(revenue_raw), ticker]
  n_gap <- n_gap + length(g)
  if (d == "2024-06-30") gap_2024 <- g
}
# Tier 2 note: the gap census INCREASES vs the post-Wave-M/P ledger BY
# DESIGN -- ~147 recovered names now carry fundamentals, and the subset
# with price-unrecoverable windows (KNOWN_LIMITATIONS L4) lands in this
# audit as fundamentals-present-but-mcap-NA. Those rows previously had
# no data at all; a larger census here is recovery, not regression.
msg("census: %d gap ticker-dates total (77 documented pre-Tier-2; L4 adds the price-dark windows); 2024-06-30 residuals: %s",
    n_gap, paste(sort(gap_2024), collapse = ",") )

# truth anchors (rule 2: external facts, band checks)
anchor <- function(d, tk, col, lo, hi) {
  dt <- as.data.table(arrow::read_parquet(
    sprintf("cache/snapshots/pit_%s_raw.parquet", d)))
  v <- dt[ticker == tk][[col]]
  ok <- length(v) == 1 && !is.na(v) && v >= lo && v <= hi
  msg("anchor %-6s %s %s = %s [%g, %g] %s", tk, d, col,
      if (length(v)) signif(v, 4) else "ABSENT", lo, hi,
      if (ok) "PASS" else "FAIL")
  ok
}
a <- c(
  # AAPL band is on the FILING-DATE price basis: the previously verified
  # anchor is $616B (see memory/postmortem), NOT the ~$725B calendar
  # mid-2015 peak -- the first terminal-rebuild run flagged a false FAIL
  # here with the value at exactly 6.165e11.
  anchor("2015-06-30", "AAPL",  "market_cap", 5.5e11, 7.0e11),
  anchor("2015-06-30", "NVDA",  "pe_trailing", 12, 35),
  anchor("2015-06-30", "BRK.B", "market_cap", 2.8e11, 3.9e11),
  anchor("2018-06-30", "ATVI",  "market_cap", 4.5e10, 6.5e10),
  anchor("2024-06-30", "BRK.B", "market_cap", 8.0e11, 10.0e11),
  anchor("2024-06-30", "V",     "market_cap", 4.3e11, 6.0e11),
  anchor("2012-06-30", "K",     "market_cap", 1.5e10, 2.4e10),
  anchor("2024-06-30", "GDDY",  "pe_trailing", 8, 20),
  # old-CIK wave Tier 2 recovered names (filing-date price basis)
  anchor("2015-06-30", "AET",   "market_cap", 2.5e10, 5.5e10),
  anchor("2015-06-30", "ESRX",  "market_cap", 3.5e10, 9.0e10),
  anchor("2015-06-30", "TWX",   "market_cap", 5.0e10, 9.5e10),
  anchor("2020-06-30", "FLIR",  "market_cap", 3.0e9,  9.0e9),
  anchor("2012-06-30", "NWSA",  "market_cap", 3.0e10, 8.0e10)  # old News Corp via TFCFA stitch
)

# indicator-count pin: exactly 130 indicators in every file
pin_fail <- character(0)
for (d in dates) {
  dt <- arrow::read_parquet(sprintf("cache/snapshots/pit_%s_raw.parquet", d))
  if (ncol(dt) - 4L != 130L) pin_fail <- c(pin_fail, d)
}
msg("indicator pin (130): %s",
    if (length(pin_fail)) paste("FAIL:", paste(pin_fail, collapse = ","))
    else "all 66 pass")

# =============================================================================
# Verdict
# =============================================================================
el <- round(as.numeric(difftime(Sys.time(), t_run0, units = "hours")), 2)
verdict <- length(failed_dates) == 0 && length(v_fail) == 0 &&
  all(a) && length(pin_fail) == 0
msg("==================================================")
msg("VERDICT: %s  (%.2f h)", if (verdict) "CLEAN" else "ATTENTION NEEDED", el)
msg("  snapshot failures: %s",
    if (length(failed_dates)) paste(failed_dates, collapse = ",") else "none")
msg("  validate failures: %s",
    if (length(v_fail)) paste(v_fail, collapse = ",") else "none")
msg("  anchors: %d/%d", sum(a), length(a))
msg("  pin: %s", if (length(pin_fail)) "FAIL" else "ok")
msg("==================================================")
