#!/usr/bin/env Rscript
# -- Rebuild cache for 7 CFO cumulation non-conformers --
# Tickers identified in the 60-ticker diagnostic (docs/research/07_cfo_cumulation_issue.md):
#   TTM-leak:    AMZN
#   Sparse-leak: QCOM, SJM, LKQ
#   FY-realign:  NVDA, HRB
#   Extra:      ANF
# All 7 files are force-refreshed under the current dedup_fundamentals
# (duration-match tie-break active). Post-refresh, tools/gate_test_fcf_stability.R
# should flip AMZN's sd_new from NA to a finite value.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})
source("R/fundamental_fetcher.R")

if (Sys.getenv("EDGAR_UA") == "") {
  # SEC EDGAR requires a real User-Agent. Set EDGAR_UA in your environment
  # (e.g. ~/.Renviron) to something like: "YourOrg/1.0 you@example.com".
  # The fetcher falls back to "BSTAR/1.0 contact@email.com" otherwise.
  message("  EDGAR_UA not set; using fetcher default. ",
          "Set EDGAR_UA in your environment for SEC compliance.")
}

targets <- data.table(
  ticker = c("AMZN",       "NVDA",       "QCOM",       "SJM",
             "LKQ",        "ANF",        "HRB"),
  cik    = c("0001018724", "0001045810", "0000804328", "0000091419",
             "0001065696", "0001018840", "0000012659")
)

message(sprintf("Rebuilding %d non-conformer caches...", nrow(targets)))

results <- list()
for (i in seq_len(nrow(targets))) {
  tk  <- targets$ticker[i]
  cik <- targets$cik[i]
  path <- file.path("cache/fundamentals", sprintf("%s_%s.parquet", cik, tk))
  old_rows <- if (file.exists(path))
    nrow(as.data.table(read_parquet(path))) else NA_integer_
  old_cfo  <- if (file.exists(path)) {
    d <- as.data.table(read_parquet(path))
    sum(d$concept == "operating_cashflow")
  } else NA_integer_

  t0 <- Sys.time()
  dt <- tryCatch(
    fetch_and_cache_ticker(tk, cik, force_refresh = TRUE),
    error = function(e) {
      message(sprintf("  [%d/%d] %s FAILED: %s", i, nrow(targets), tk, e$message))
      NULL
    })
  dt_s <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 2)

  if (is.null(dt)) {
    results[[tk]] <- data.table(ticker = tk, cik = cik,
                                 old_rows = old_rows, new_rows = NA_integer_,
                                 old_cfo = old_cfo,  new_cfo = NA_integer_,
                                 sec = dt_s, status = "FAILED")
    next
  }

  new_cfo <- sum(dt$concept == "operating_cashflow")
  results[[tk]] <- data.table(
    ticker = tk, cik = cik,
    old_rows = old_rows, new_rows = nrow(dt),
    old_cfo  = old_cfo,  new_cfo  = new_cfo,
    sec = dt_s, status = "OK"
  )
  message(sprintf("  [%d/%d] %s rows %d -> %d, cfo %d -> %d (%.1fs)",
                  i, nrow(targets), tk, old_rows, nrow(dt),
                  old_cfo, new_cfo, dt_s))
}

summary_dt <- rbindlist(results)
cat("\n=== Rebuild summary ===\n")
print(summary_dt, row.names = FALSE)

if (any(summary_dt$status != "OK")) {
  stop("One or more fetches failed; see summary above")
}

cat("\nDone. Next: Rscript tools/gate_test_fcf_stability.R\n")
