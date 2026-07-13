# run_timeseries.R -- CLI runner for LOW-LEVEL daily time series operations
#
# For staying current, the canonical entry point is the manifest-driven
# incremental updater (split guard, price appends, EDGAR freshness,
# roster/sector maintenance):
#   Rscript tools/run_incremental_update.R
# The `update` mode below is the raw layer-append path: no split guard
# and no manifest -- a split between runs mis-bases that ticker until
# its splits cache refreshes. Fine for ad-hoc/single-ticker work.
#
# Usage:
#   Rscript run_timeseries.R build                     # historical build
#   Rscript run_timeseries.R update [YYYY-MM-DD]       # low-level daily append
#   Rscript run_timeseries.R ticker AAPL [YYYY-MM-DD]  # single ticker
#
# For interactive use and documentation, see rfundamentals_guide.R

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")
source("R/pit_assembler.R")
source("R/pipeline_runner.R")
source("R/timeseries_builder.R")
source("R/ttm_eps.R")   # enables the eps_ttm/pe_ttm augment hook in build/update

args <- commandArgs(trailingOnly = TRUE)
cmd  <- if (length(args) >= 1) args[1] else ""

if (cmd == "build") {

  build_timeseries()

} else if (cmd == "update") {

  through <- if (length(args) >= 2) as.Date(args[2]) else Sys.Date()
  update_all_daily(through_date = through, refresh_prices = TRUE)

} else if (cmd == "ticker") {

  if (length(args) < 2) stop("Usage: Rscript run_timeseries.R ticker AAPL")
  tk      <- args[2]
  through <- if (length(args) >= 3) as.Date(args[3]) else Sys.Date()

  master  <- as.data.table(read_parquet("cache/lookups/constituent_master.parquet"))
  # current view of the dated sector table (SCD since D2/B)
  sectors <- .sector_asof(
    as.data.table(read_parquet("cache/lookups/sector_industry.parquet")),
    Sys.Date())
  info    <- merge(data.table(ticker = tk), master[, .(ticker, cik)], by = "ticker")
  info    <- merge(info, sectors[, .(ticker, sector)], by = "ticker", all.x = TRUE)

  build_ticker_fundamentals(tk, info$cik[1],
                            ifelse(is.na(info$sector[1]), "Unknown", info$sector[1]))
  update_ticker_daily(tk, through_date = through, refresh_price = TRUE)

} else {
  message("Usage:")
  message("  Rscript run_timeseries.R build                     # historical build")
  message("  Rscript run_timeseries.R update [YYYY-MM-DD]       # daily update")
  message("  Rscript run_timeseries.R ticker AAPL [YYYY-MM-DD]  # single ticker")
  message("\nFor interactive use, see rfundamentals_guide.R")
  if (!interactive()) quit(status = 0)
}
