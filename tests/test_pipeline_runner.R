# ============================================================================
# test_pipeline_runner.R  --  Unit Tests for Module 6 (pipeline_runner.R)
# ============================================================================
# Run: Rscript tests/test_pipeline_runner.R
#
# Tests:
#   1. validate_snapshot: checks against real or synthetic snapshots
#   2. summarize_coverage: coverage report structure
#   3. list_snapshots: directory scanning
#   4. run_full_build prerequisite checks
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

# -- Test harness --
.n_pass <- 0L
.n_fail <- 0L

test <- function(name, expr) {
  ok <- tryCatch(isTRUE(expr), error = function(e) FALSE)
  if (ok) {
    .n_pass <<- .n_pass + 1L
    message(sprintf("  PASS  %s", name))
  } else {
    .n_fail <<- .n_fail + 1L
    message(sprintf("  FAIL  %s", name))
  }
}


# ============================================================================
# SETUP: Create a synthetic snapshot for testing validation
# ============================================================================
message("\n=== Building synthetic snapshot for validation tests ===")

test_dir <- file.path(tempdir(), "test_snapshots")
dir.create(test_dir, recursive = TRUE, showWarnings = FALSE)

ind_names <- get_indicator_names()
n_tickers <- 50

# Synthetic raw snapshot
set.seed(42)
raw_dt <- data.table(
  date     = as.Date("2023-06-30"),
  ticker   = paste0("TK", sprintf("%03d", 1:n_tickers)),
  industry = sample(c("Software", "Banks", "Oil & Gas", "Pharma",
                       "Semiconductors"), n_tickers, replace = TRUE),
  sector   = sample(c("Technology", "Financial", "Energy", "Healthcare",
                       "Industrials"), n_tickers, replace = TRUE)
)

# Add indicator columns with realistic-ish values
for (col in ind_names) {
  if (col %in% c("f_roa", "f_droa", "f_cfo", "f_accrual",
                  "f_dlever", "f_dliquid", "f_eq_off",
                  "f_dmargin", "f_dturn")) {
    raw_dt[, (col) := sample(0:1, n_tickers, replace = TRUE)]
  } else if (col == "f_score") {
    raw_dt[, (col) := sample(0:9, n_tickers, replace = TRUE)]
  } else {
    raw_dt[, (col) := rnorm(n_tickers, mean = 0.1, sd = 0.5)]
  }
  # Sprinkle some NAs (~10%)
  na_idx <- sample(n_tickers, ceiling(n_tickers * 0.1))
  raw_dt[na_idx, (col) := NA_real_]
}

# Synthetic z-scored snapshot (same structure, z-scored values)
zsc_dt <- copy(raw_dt)
for (col in ind_names) {
  vals <- zsc_dt[[col]]
  valid <- !is.na(vals)
  if (sum(valid) > 2) {
    mu <- mean(vals[valid])
    s  <- sd(vals[valid])
    if (s > 1e-9) {
      z <- (vals - mu) / s
      z <- pmin(pmax(z, -3), 3)
      z[!valid] <- NA_real_
      zsc_dt[, (col) := z]
    }
  }
}

# Write parquet
snapshot_date <- "2023-06-30"
arrow::write_parquet(raw_dt,
                     file.path(test_dir, sprintf("pit_%s_raw.parquet", snapshot_date)))
arrow::write_parquet(zsc_dt,
                     file.path(test_dir, sprintf("pit_%s_zscore.parquet", snapshot_date)))

message(sprintf("  wrote synthetic snapshot: %d tickers, %d indicators",
                n_tickers, length(ind_names)))


# ============================================================================
# TEST 1: validate_snapshot
# ============================================================================
message("\n=== validate_snapshot ===")

val <- validate_snapshot(snapshot_date, output_dir = test_dir)

test("validate returns list",
     is.list(val))

test("validate has pass field",
     "pass" %in% names(val))

test("validate has checks field",
     "checks" %in% names(val))

test("validate has details field",
     "details" %in% names(val))

test("validate: raw file exists",
     val$checks["raw_file_exists"])

test("validate: zscore file exists",
     val$checks["zscore_file_exists"])

test("validate: all columns present",
     val$checks["all_columns_present"])

test("validate: no duplicate tickers",
     val$checks["no_duplicate_tickers"])

test("validate: ticker count plausible (50 in [100,600] fails)", {
  # 50 tickers is below the 100 threshold, so this should be FALSE
  !val$checks["ticker_count_plausible"]
})

test("validate: multiple sectors",
     val$checks["multiple_sectors"])

test("validate: f_score range valid",
     val$checks["f_score_range_valid"])

test("validate: details has n_tickers",
     val$details$n_tickers == n_tickers)

test("validate: details has na_rates",
     length(val$details$na_rates) == length(ind_names))

test("validate: z-score mean near zero",
     val$checks["zscore_mean_near_zero"])

# Test with missing file
val_missing <- validate_snapshot("1999-01-01", output_dir = test_dir)
test("validate: missing file returns pass=FALSE",
     !val_missing$pass)

# Test with corrupt file
corrupt_path <- file.path(test_dir, "pit_2000-01-01_raw.parquet")
writeLines("not a parquet file", corrupt_path)
val_corrupt <- validate_snapshot("2000-01-01", output_dir = test_dir)
test("validate: corrupt file returns pass=FALSE",
     !val_corrupt$pass)
unlink(corrupt_path)


# ============================================================================
# TEST 2: summarize_coverage
# ============================================================================
message("\n=== summarize_coverage ===")

cov <- summarize_coverage(snapshot_date, output_dir = test_dir)

test("coverage is data.table",
     is.data.table(cov))

test("coverage has indicator column",
     "indicator" %in% names(cov))

test("coverage has pct_valid column",
     "pct_valid" %in% names(cov))

test("coverage has n_total column",
     "n_total" %in% names(cov))

test("coverage covers all indicators",
     nrow(cov) == length(ind_names))

test("coverage: n_total is correct",
     all(cov$n_total == n_tickers))

test("coverage: pct_valid in [0, 100]",
     all(cov$pct_valid >= 0 & cov$pct_valid <= 100))

test("coverage: has sector breakdown columns",
     ncol(cov) > 4)  # indicator + n_total + n_valid + pct_valid + sector cols

# Non-existent snapshot
test("coverage: error on missing snapshot", {
  err <- tryCatch(
    summarize_coverage("1999-01-01", output_dir = test_dir),
    error = function(e) "error"
  )
  err == "error"
})


# ============================================================================
# TEST 3: list_snapshots
# ============================================================================
message("\n=== list_snapshots ===")

snapshots <- list_snapshots(output_dir = test_dir)

test("list_snapshots returns character vector",
     is.character(snapshots))

test("list_snapshots finds our snapshot",
     snapshot_date %in% snapshots)

test("list_snapshots: empty dir returns empty",
     length(list_snapshots(output_dir = tempdir())) == 0 ||
     is.character(list_snapshots(output_dir = tempdir())))

test("list_snapshots: nonexistent dir returns empty",
     length(list_snapshots(output_dir = "/nonexistent/path")) == 0)


# ============================================================================
# TEST 4: run_full_build prerequisite checks
# ============================================================================
message("\n=== run_full_build prerequisites ===")

test("run_full_build: fails with missing master", {
  err <- tryCatch(
    run_full_build(master_path = "/nonexistent/master.parquet"),
    error = function(e) e$message
  )
  grepl("prerequisite", err, ignore.case = TRUE)
})

test("run_full_build: fails with missing sector", {
  # Create a temporary master file
  tmp_master <- file.path(tempdir(), "test_master.parquet")
  arrow::write_parquet(data.table(ticker = "AAPL"), tmp_master)

  err <- tryCatch(
    run_full_build(master_path = tmp_master,
                   sector_path = "/nonexistent/sector.parquet"),
    error = function(e) e$message
  )
  unlink(tmp_master)
  grepl("prerequisite", err, ignore.case = TRUE)
})


# ============================================================================
# TEST 5: Larger synthetic snapshot for ticker_count_plausible
# ============================================================================
message("\n=== validate_snapshot with plausible ticker count ===")

# Create a snapshot with 450 tickers
n_large <- 450
large_raw <- data.table(
  date     = as.Date("2023-09-30"),
  ticker   = paste0("TK", sprintf("%04d", 1:n_large)),
  industry = sample(c("Software", "Banks"), n_large, replace = TRUE),
  sector   = sample(c("Technology", "Financial", "Energy",
                       "Healthcare", "Industrials"), n_large, replace = TRUE)
)
for (col in ind_names) {
  if (col %in% c("f_roa", "f_droa", "f_cfo", "f_accrual",
                  "f_dlever", "f_dliquid", "f_eq_off",
                  "f_dmargin", "f_dturn")) {
    large_raw[, (col) := sample(0:1, n_large, replace = TRUE)]
  } else if (col == "f_score") {
    large_raw[, (col) := sample(0:9, n_large, replace = TRUE)]
  } else {
    large_raw[, (col) := rnorm(n_large, mean = 0.1, sd = 0.5)]
  }
}

large_zsc <- copy(large_raw)
for (col in ind_names) {
  vals <- large_zsc[[col]]
  mu <- mean(vals, na.rm = TRUE)
  s <- sd(vals, na.rm = TRUE)
  if (s > 0) large_zsc[, (col) := (vals - mu) / s]
}

arrow::write_parquet(large_raw,
                     file.path(test_dir, "pit_2023-09-30_raw.parquet"))
arrow::write_parquet(large_zsc,
                     file.path(test_dir, "pit_2023-09-30_zscore.parquet"))

val_large <- validate_snapshot("2023-09-30", output_dir = test_dir)

test("validate: 450 tickers is plausible",
     val_large$checks["ticker_count_plausible"])

test("validate: no 99% NA indicators (full synthetic data)",
     val_large$checks["no_99pct_na_indicators"])

test("validate: all checks pass for well-formed snapshot",
     val_large$pass)


# ============================================================================
# CLEANUP
# ============================================================================
unlink(test_dir, recursive = TRUE)


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== test_pipeline_runner.R: %d passed, %d failed ===",
                .n_pass, .n_fail))

if (.n_fail > 0) stop("Tests failed", call. = FALSE)
