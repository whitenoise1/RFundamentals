#!/usr/bin/env Rscript
# Smoke test for .decumulate_cfo: show standalone series vs YTD input
# for representative tickers.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})
source("R/indicator_compute.R")

show_one <- function(ticker) {
  files <- list.files("cache/fundamentals",
                      pattern = paste0("_", ticker, "\\.parquet$"),
                      full.names = TRUE)
  if (length(files) == 0) { cat(sprintf("No file for %s\n", ticker)); return() }
  dt <- as.data.table(read_parquet(files[1]))
  wide <- pivot_fundamentals(dt)
  q <- wide[period_type %in% c("Q1","Q2","Q3","Q4")]
  if (nrow(q) == 0) { cat(sprintf("%s: no quarterly rows\n", ticker)); return() }
  decum <- .decumulate_cfo(q)
  setorder(decum, fiscal_year, period_type)
  cat(sprintf("\n=== %s: last 16 quarters ===\n", ticker))
  tail_rows <- tail(decum, 16)
  print(tail_rows[, .(fiscal_year, period_type,
                       days = cfo_period_days,
                       kind = cfo_kind, exp = expected_kind,
                       cfo_ytd = round(operating_cashflow / 1e9, 2),
                       cfo_std = round(cfo_std / 1e9, 2),
                       assets  = round(total_assets / 1e9, 1))],
        row.names = FALSE)

  # Compute fcf_stability: old (raw YTD) vs new (standalone)
  if (nrow(decum) >= 8) {
    valid_old <- !is.na(decum$operating_cashflow) &
                 !is.na(decum$total_assets) & decum$total_assets > 0
    old_tail <- tail(decum[valid_old], 16)
    old_sd <- if (nrow(old_tail) >= 8)
      sd(old_tail$operating_cashflow / old_tail$total_assets) else NA_real_

    valid_new <- !is.na(decum$cfo_std) &
                 !is.na(decum$total_assets) & decum$total_assets > 0
    new_tail <- tail(decum[valid_new], 16)
    new_sd <- if (nrow(new_tail) >= 8)
      sd(new_tail$cfo_std / new_tail$total_assets) else NA_real_

    cat(sprintf("%s fcf_stability: OLD (YTD-contaminated) = %s  |  NEW (standalone) = %s\n",
                ticker,
                formatC(old_sd, digits = 5, format = "f"),
                formatC(new_sd, digits = 5, format = "f")))
    cat(sprintf("  valid rows old=%d, new=%d\n", nrow(old_tail), nrow(new_tail)))
  }
}

for (t in c("AAPL", "MSFT", "WMT", "AMZN", "NVDA", "HRB", "QCOM")) {
  show_one(t)
}
