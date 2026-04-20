#!/usr/bin/env Rscript
# -- Rebuild cache for tickers surfaced by the universe sweep --
# From tools/diag_cfo_universe.R output on 2026-04-20:
#   NO_CFO_ROWS (needs new alias + rebuild):
#     FMC, TFX, ITT, DRI   -- all file NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
#     (SII is IFRS-only, not addressed here)
#   ANOMALOUS_TICKER / TTM-leak (needs rebuild under current dedup tie-break):
#     KR, AAP
# Post-rebuild: re-run tools/diag_cfo_universe.R and confirm NO_CFO_ROWS count
# drops to 1 (SII only) and ANOMALOUS_TICKER count drops to 0.

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
  ticker = c("FMC",        "TFX",        "ITT",        "DRI",
             "KR",         "AAP"),
  cik    = c("0000037785", "0000096943", "0000216228", "0000940944",
             "0000056873", "0001158449")
)

message(sprintf("Rebuilding %d tickers (alias expansion + TTM-leak fix)...",
                nrow(targets)))

results <- list()
for (i in seq_len(nrow(targets))) {
  tk  <- targets$ticker[i]
  cik <- targets$cik[i]
  path <- file.path("cache/fundamentals", sprintf("%s_%s.parquet", cik, tk))

  old <- if (file.exists(path))
    as.data.table(read_parquet(path)) else data.table()
  old_rows <- nrow(old)
  old_cfo  <- if (old_rows > 0) sum(old$concept == "operating_cashflow") else 0L

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

cat("\nDone. Next: Rscript tools/diag_cfo_universe.R\n")
