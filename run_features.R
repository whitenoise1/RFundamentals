# run_features.R -- CLI runner for multi-dimensional feature standardization
#
# Usage:
#   Rscript run_features.R build [from] [to]        # historical build
#   Rscript run_features.R update [YYYY-MM-DD]       # daily update
#
# Requires: run_timeseries.R build must have completed first.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(zoo)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")
source("R/pit_assembler.R")
source("R/pipeline_runner.R")
source("R/timeseries_builder.R")
source("R/feature_standardizer.R")

args <- commandArgs(trailingOnly = TRUE)
cmd  <- if (length(args) >= 1) args[1] else ""

if (cmd == "build") {

  from <- if (length(args) >= 2) args[2] else "2010-01-04"
  to   <- if (length(args) >= 3) as.Date(args[3]) else Sys.Date()
  build_all_features(from_date = from, to_date = to)

} else if (cmd == "update") {

  through <- if (length(args) >= 2) as.Date(args[2]) else Sys.Date()
  update_features(through_date = through)

} else {
  message("Usage:")
  message("  Rscript run_features.R build [from] [to]        # historical build")
  message("  Rscript run_features.R update [YYYY-MM-DD]       # daily update")
  message("\nRequires: timeseries data must be built first (run_timeseries.R build)")
  if (!interactive()) quit(status = 0)
}
