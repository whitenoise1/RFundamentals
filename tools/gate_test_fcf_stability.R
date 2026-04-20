#!/usr/bin/env Rscript
# -- Gate test: before-vs-after fcf_stability on known cases --
# Compares the legacy (YTD-contaminated) SD against the new de-cumulated
# SD for a curated set of tickers covering each class identified in
# docs/research/07_cfo_cumulation_issue.md.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")

classes <- list(
  clean       = c("AAPL", "MSFT", "WMT", "JNJ", "JPM"),
  ttm_leak    = c("AMZN"),                 # Class 1
  sparse_leak = c("QCOM", "SJM", "LKQ"),   # Class 2
  fy_realign  = c("NVDA", "HRB")           # Class 3
)

all_tickers <- unname(unlist(classes))

ticker_class <- function(t) {
  for (cls in names(classes))
    if (t %in% classes[[cls]]) return(cls)
  NA_character_
}

one_ticker <- function(ticker) {
  files <- list.files("cache/fundamentals",
                      pattern = paste0("_", ticker, "\\.parquet$"),
                      full.names = TRUE)
  if (length(files) == 0) return(NULL)
  dt <- as.data.table(read_parquet(files[1]))
  wide <- pivot_fundamentals(dt)
  if (is.null(wide)) return(NULL)
  q <- wide[period_type %in% c("Q1","Q2","Q3","Q4")]
  if (nrow(q) < 8) return(NULL)

  # -- Legacy SD: raw operating_cashflow / total_assets on tail(16) --
  setorder(q, fiscal_year, period_type)
  q_tail_legacy <- tail(q[!is.na(operating_cashflow) &
                            !is.na(total_assets) & total_assets > 0], 16)
  sd_legacy <- if (nrow(q_tail_legacy) >= 8)
    sd(q_tail_legacy$operating_cashflow / q_tail_legacy$total_assets) else NA_real_

  # -- New SD: standalone CFO / total_assets on tail(16) of valid rows --
  decum <- .decumulate_cfo(q)
  setorder(decum, fiscal_year, period_type)
  valid <- !is.na(decum$cfo_std) &
           !is.na(decum$total_assets) & decum$total_assets > 0
  q_tail_new <- tail(decum[valid], 16)
  sd_new <- if (nrow(q_tail_new) >= 8)
    sd(q_tail_new$cfo_std / q_tail_new$total_assets) else NA_real_

  data.table(
    ticker       = ticker,
    class        = ticker_class(ticker),
    n_rows_legacy = nrow(q_tail_legacy),
    n_rows_new   = nrow(q_tail_new),
    sd_legacy    = sd_legacy,
    sd_new       = sd_new,
    ratio_new_to_legacy = sd_new / sd_legacy
  )
}

results <- rbindlist(lapply(all_tickers, one_ticker))

cat("\n=== Gate test: fcf_stability legacy vs new ===\n\n")
print(results[, .(ticker, class,
                   n_old = n_rows_legacy, n_new = n_rows_new,
                   sd_old = round(sd_legacy, 5),
                   sd_new = round(sd_new, 5),
                   ratio  = round(ratio_new_to_legacy, 3))],
      row.names = FALSE)

cat("\n\n=== Expectations ===\n")
cat("CLEAN tickers:       sd_new should be materially < sd_old (sawtooth gone).\n")
cat("TTM_LEAK (AMZN):     sd_new must be finite and < sd_old post-rebuild.\n")
cat("SPARSE_LEAK:         sd_new < sd_old; a few rows may drop.\n")
cat("FY_REALIGN:          sd_new should be defensible; stub rows dropped.\n")

# -- Hard assertions (will stop() on failure for CI) --
cat("\n=== Assertions ===\n")
stopifnot("AMZN" %in% results$ticker)
amzn <- results[ticker == "AMZN"]
if (is.na(amzn$sd_new) || !is.finite(amzn$sd_new)) {
  stop("GATE FAIL: AMZN sd_new should be finite post-rebuild, got ",
       amzn$sd_new)
}
if (!(amzn$sd_new < amzn$sd_legacy)) {
  stop(sprintf(
    "GATE FAIL: AMZN sd_new (%.5f) should be < sd_legacy (%.5f) after TTM removal",
    amzn$sd_new, amzn$sd_legacy))
}
cat(sprintf("  PASS  AMZN sd_new = %.5f, ratio %.3f (TTM leak removed, sawtooth gone)\n",
            amzn$sd_new, amzn$ratio_new_to_legacy))

clean_res <- results[class == "clean" & !is.na(sd_legacy) & !is.na(sd_new)]
if (nrow(clean_res) == 0) stop("GATE FAIL: no CLEAN tickers produced both SDs")

# We expect the sawtooth-removal to reduce SD for most industrial-profile
# clean tickers, but not all. Banks with sign-alternating quarterly CFO
# (e.g. JPM) and extremely seasonal firms (e.g. HRB) can show higher SD
# after de-cumulation -- this is the correct behavior, not a regression.
# Require a majority to drop.
n_reduced <- sum(clean_res$sd_new < clean_res$sd_legacy)
if (n_reduced < ceiling(nrow(clean_res) / 2)) {
  stop(sprintf("GATE FAIL: only %d/%d CLEAN tickers reduced SD; expected majority",
               n_reduced, nrow(clean_res)))
}
cat(sprintf("  PASS  %d/%d CLEAN tickers: sd_new < sd_legacy (majority drop)\n",
            n_reduced, nrow(clean_res)))

# Additionally: every ticker (including sparse_leak and fy_realign) must
# produce a non-degenerate SD (either finite or explicitly NA), never Inf / NaN.
bad <- results[!is.na(sd_new) & (!is.finite(sd_new) | sd_new < 0)]
if (nrow(bad) > 0) {
  stop(sprintf("GATE FAIL: %d ticker(s) produced non-finite or negative sd_new",
               nrow(bad)))
}
cat("  PASS  no non-finite or negative sd_new values\n")

cat("\nGate test complete.\n")
