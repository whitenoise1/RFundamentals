# Canonical entry point for the incremental update (daily-update wave,
# docs/DESIGN_DAILY_UPDATE.md sections 4-6). Sources the module stack
# and runs run_incremental_update(): Tier D always, Tiers W/M when due.
# First-ever run bootstraps cache/lookups/update_manifest.parquet from
# cache state. Suitable for cron / scheduled runs.
#
# Usage:
#   Rscript tools/run_incremental_update.R [--force-weekly] [--force-monthly]
#
# feature_standardizer.R is NOT sourced here by design: the
# update_features hook is existence-gated and the feature layer is
# outside the snapshot/daily pipeline; source it in a wrapper if that
# layer should refresh too.

args <- commandArgs(trailingOnly = TRUE)

suppressPackageStartupMessages({ library(data.table); library(arrow) })
suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/sector_classifier.R")
  source("R/ttm_eps.R")
  source("R/indicator_compute.R")
  source("R/pit_assembler.R")
  source("R/timeseries_builder.R")
  source("R/pipeline_runner.R")
  source("R/update_runner.R")
})

# postmortem rule 3: no silent degradation
required_fns <- c("run_incremental_update", "bootstrap_update_manifest",
                  "load_ticker_splits", ".split_factor",
                  "build_ticker_pershare", "update_ticker_daily",
                  "augment_daily_ttm", "merge_sector_scd", ".sector_asof")
missing <- required_fns[!vapply(required_fns, exists, logical(1))]
if (length(missing)) stop("missing functions: ", paste(missing, collapse = ", "))

stats <- run_incremental_update(
  as_of         = Sys.Date(),
  force_weekly  = "--force-weekly" %in% args,
  force_monthly = "--force-monthly" %in% args
)

message("=== run_incremental_update driver: done ===")
