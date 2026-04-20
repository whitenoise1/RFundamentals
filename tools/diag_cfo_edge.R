#!/usr/bin/env Rscript
# -- Edge-case diagnostic: AMZN and NVDA quarterly CFO rows --
# From the main diagnostic, AMZN showed all Q2/Q3 rows as "other"
# (period_days outside the expected 90/180/270/360 bands), and NVDA
# showed a small number of 3mo rows mixed in with YTD rows.
# This script dumps the raw rows so we can see what's going on.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

cache_dir <- "cache/fundamentals"

dump_one <- function(ticker) {
  file <- list.files(cache_dir, pattern = paste0("_", ticker, "\\.parquet$"),
                     full.names = TRUE)
  stopifnot(length(file) == 1)
  dt <- as.data.table(read_parquet(file))
  dt <- dt[concept == "operating_cashflow"]
  dt[, period_days := as.integer(period_end - period_start)]
  setorder(dt, period_end, fiscal_qtr)
  dt[, .(fiscal_year, fiscal_qtr, period_start, period_end,
         period_days, form, tag, value_bn = round(value / 1e9, 2))]
}

cat("\n=== AMZN operating_cashflow rows (full dump) ===\n")
amzn <- dump_one("AMZN")
cat("Unique period_days values:\n")
print(amzn[, .N, by = period_days][order(period_days)], row.names = FALSE)
cat("\nFirst 30 rows:\n")
print(head(amzn, 30), row.names = FALSE)
cat("\nLast 20 rows:\n")
print(tail(amzn, 20), row.names = FALSE)

cat("\n=== NVDA operating_cashflow rows (full dump) ===\n")
nvda <- dump_one("NVDA")
cat("Unique period_days values:\n")
print(nvda[, .N, by = period_days][order(period_days)], row.names = FALSE)
cat("\nRows with period_days < 120 (the '3mo' outliers):\n")
print(nvda[period_days < 120], row.names = FALSE)
cat("\nLast 25 rows:\n")
print(tail(nvda, 25), row.names = FALSE)

cat("\n=== AMZN: does any row have NA period_start? ===\n")
amzn_raw <- as.data.table(read_parquet(
  list.files(cache_dir, pattern = "_AMZN\\.parquet$", full.names = TRUE)
))
amzn_cfo_raw <- amzn_raw[concept == "operating_cashflow"]
cat(sprintf("NA period_start: %d / %d rows\n",
            sum(is.na(amzn_cfo_raw$period_start)), nrow(amzn_cfo_raw)))

invisible(NULL)
