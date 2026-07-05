# ============================================================================
# tools/regen_wave4.R -- Wave 4 full regeneration (116 indicators)
# ============================================================================
# Usage:
#   Rscript tools/regen_wave4.R snapshots   # re-assemble all cached PIT dates
#   Rscript tools/regen_wave4.R timeseries  # force fund layers + fresh daily
#
# Rationale: pre-Wave-4 caches lack the 8 new stubs and 29 indicator
# columns. build_timeseries skips existing fund layers and the daily
# updater only appends, so an incremental run would leave the new
# columns NA for all history (Wave 4 review, migration findings).
# Snapshot phase is resumable: dates whose raw parquet already has the
# full indicator set are skipped.
# ============================================================================

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
source("R/ttm_eps.R")

mode <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(mode) || !mode %in% c("snapshots", "timeseries")) {
  stop("usage: Rscript tools/regen_wave4.R snapshots|timeseries")
}

n_ind <- length(get_indicator_names())

if (mode == "snapshots") {

  files <- list.files("cache/snapshots", pattern = "^pit_.*_raw\\.parquet$")
  dates <- sort(as.Date(gsub("^pit_(.*)_raw\\.parquet$", "\\1", files)))
  message(sprintf("regen_wave4: %d snapshot dates, target %d indicators",
                  length(dates), n_ind))

  for (d in as.character(dates)) {
    path <- sprintf("cache/snapshots/pit_%s_raw.parquet", d)
    have <- tryCatch(
      sum(get_indicator_names() %in%
            names(arrow::read_parquet(path, col_select = NULL))),
      error = function(e) 0L
    )
    if (have == n_ind) {
      message(sprintf("  %s: already at %d indicators, skip", d, n_ind))
      next
    }
    message(sprintf("  %s: regenerating...", d))
    res <- tryCatch(
      assemble_snapshot(snapshot_date = as.Date(d),
                        prefetch_prices = TRUE),
      error = function(e) {
        warning(sprintf("  %s FAILED: %s", d, e$message), call. = FALSE)
        NULL
      }
    )
    if (!is.null(res)) {
      val <- tryCatch(validate_snapshot(as.Date(d)),
                      error = function(e) NULL)
    }
  }
  message("regen_wave4: snapshot phase done")

} else {

  master  <- as.data.table(arrow::read_parquet(
    "cache/lookups/constituent_master.parquet"))
  sectors <- as.data.table(arrow::read_parquet(
    "cache/lookups/sector_industry.parquet"))

  fund_files <- list.files("cache/fundamentals", pattern = "\\.parquet$")
  tickers <- gsub("^\\d+_(.+)\\.parquet$", "\\1", fund_files)

  tk_dt <- data.table(ticker = tickers)
  tk_dt <- merge(tk_dt, master[, .(ticker, cik)], by = "ticker", all.x = TRUE)
  tk_dt <- merge(tk_dt, sectors[, .(ticker, sector)], by = "ticker",
                 all.x = TRUE)
  tk_dt[is.na(sector), sector := "Unknown"]

  message(sprintf("regen_wave4: forcing %d fund layers", nrow(tk_dt)))
  n_ok <- 0L
  for (i in seq_len(nrow(tk_dt))) {
    tk <- tk_dt$ticker[i]
    f <- tryCatch(
      build_ticker_fundamentals(tk, tk_dt$cik[i], tk_dt$sector[i],
                                force = TRUE),
      error = function(e) {
        warning(sprintf("  fund %s FAILED: %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )
    if (!is.null(f)) n_ok <- n_ok + 1L
    if (i %% 50 == 0) message(sprintf("  fund layers: %d / %d", i, nrow(tk_dt)))
  }
  message(sprintf("regen_wave4: fund layers done (%d ok)", n_ok))

  # Daily layers: delete, then rebuild from the new fund layers so the
  # full history carries all 23 price-sensitive columns (the updater
  # only appends -- pre-upgrade rows would stay NA otherwise).
  daily <- list.files(.TS_DIR, pattern = "_daily\\.parquet$",
                      full.names = TRUE)
  message(sprintf("regen_wave4: removing %d daily layers", length(daily)))
  unlink(daily)
  build_timeseries()
  message("regen_wave4: timeseries phase done")
}
