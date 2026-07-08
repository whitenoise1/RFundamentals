# ============================================================================
# tools/regen_price_basis.R -- as-traded price basis regeneration
# ============================================================================
# Usage:
#   Rscript tools/regen_price_basis.R timeseries  # rebuild daily layers
#   Rscript tools/regen_price_basis.R snapshots   # re-assemble all PIT dates
#
# Rationale: get_price_on_date and the daily-layer price column moved from
# Yahoo Adjusted close (today's split+dividend basis) to the as-traded
# close (Close / post-date split factor). Every historical price level and
# the 25 price-sensitive indicators change; the daily updater only appends,
# so daily layers must be rebuilt from scratch. Fund layers are price-free
# and are left alone. Snapshot phase re-assembles every cached PIT date.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(quantmod)
  library(xts)
  library(zoo)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")
source("R/pit_assembler.R")
source("R/pipeline_runner.R")
source("R/timeseries_builder.R")
source("R/ttm_eps.R")

mode <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(mode) || !mode %in% c("snapshots", "timeseries")) {
  stop("usage: Rscript tools/regen_price_basis.R snapshots|timeseries")
}

if (mode == "timeseries") {

  # Daily layers: delete, then rebuild from the existing fund layers so the
  # full price history is re-extracted on the as-traded basis (the updater
  # only appends -- old rows would keep the Adjusted-basis price otherwise).
  # build_timeseries skips existing fund layers and re-runs the eps_ttm
  # augment at the end.
  daily <- list.files(.TS_DIR, pattern = "_daily\\.parquet$",
                      full.names = TRUE)
  message(sprintf("regen_price_basis: removing %d daily layers", length(daily)))
  unlink(daily)
  build_timeseries()
  message("regen_price_basis: timeseries phase done")

} else {

  files <- list.files("cache/snapshots", pattern = "^pit_.*_raw\\.parquet$")
  dates <- sort(as.Date(gsub("^pit_(.*)_raw\\.parquet$", "\\1", files)))
  message(sprintf("regen_price_basis: %d snapshot dates", length(dates)))

  for (d in as.character(dates)) {
    message(sprintf("  %s: regenerating...", d))
    res <- tryCatch(
      assemble_snapshot(snapshot_date = as.Date(d), prefetch_prices = FALSE),
      error = function(e) {
        warning(sprintf("  %s FAILED: %s", d, e$message), call. = FALSE)
        NULL
      }
    )
    if (!is.null(res)) {
      val <- tryCatch(validate_snapshot(as.Date(d)), error = function(e) NULL)
    }
  }
  message("regen_price_basis: snapshot phase done")
}
