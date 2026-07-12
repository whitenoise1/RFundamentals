# Daily-update wave Phase 1: prove the strengthened validate_snapshot
# FAILS on the current production snapshots (docs/DESIGN_DAILY_UPDATE.md
# section 8, Phase 1). A gate that passes on known-bad data is not a
# gate: production carries D3 (Unknown-sector bucket on 62/66 dates) and
# D1 (annual-stub daily market cap), so the expected verdict here is a
# loud, specific failure profile:
#   - no_unknown_sector fails on ~62/66 dates
#   - daily_layer_valid fails everywhere (sector coverage gap + D1
#     split-inconsistent market caps)
# Once the wave's fixes land and the terminal rebuild completes, this
# same script must report 66/66 PASS.
#
# Usage: Rscript tools/gate_phase1_validator.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })
suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/sector_classifier.R")
  source("R/indicator_compute.R")
  source("R/pit_assembler.R")
  source("R/pipeline_runner.R")
})

dates <- sort(sub("pit_(.*)_raw\\.parquet", "\\1",
                  list.files("cache/snapshots", pattern = "_raw\\.parquet$")))
message(sprintf("[phase1] validating %d snapshot dates", length(dates)))

fail_dates <- character(0)
fail_checks <- list()
for (d in dates) {
  v <- tryCatch(suppressMessages(validate_snapshot(d)),
                error = function(e) { message(sprintf("  %s ERROR: %s", d,
                                              conditionMessage(e))); NULL })
  if (is.null(v) || !isTRUE(v$pass)) {
    fail_dates <- c(fail_dates, d)
    if (!is.null(v)) {
      bad <- names(v$checks)[!v$checks]
      for (b in bad) fail_checks[[b]] <- c(fail_checks[[b]], d)
    }
  }
}

message("[phase1] ==================================================")
message(sprintf("[phase1] %d/%d dates FAIL the strengthened gate",
                length(fail_dates), length(dates)))
for (b in names(fail_checks)) {
  message(sprintf("[phase1]   %-32s fails on %d dates", b,
                  length(fail_checks[[b]])))
}
message(sprintf("[phase1] passing dates: %s",
                paste(setdiff(dates, fail_dates), collapse = ", ")))
message("[phase1] expected on pre-fix production: ~62/66 via no_unknown_sector,")
message("[phase1] 66/66 via daily_layer_valid. 0 failures here = gate is broken.")
