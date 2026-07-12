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

# A well-formed snapshot honors the sector NA-mask: masked indicators are
# NA for every member of a masked sector (validate_snapshot asserts this
# since the daily-update wave).
for (sec in names(.SECTOR_NA_INDICATORS)) {
  cols <- intersect(.SECTOR_NA_INDICATORS[[sec]], names(raw_dt))
  idx <- which(raw_dt$sector == sec)
  if (length(idx) && length(cols)) {
    for (cn in cols) raw_dt[idx, (cn) := NA_real_]
  }
}

# Synthetic z-scored snapshot: use the pipeline's own .robust_zscore so
# the fixture satisfies the median/MAD invariants validate_snapshot asserts.
zsc_dt <- copy(raw_dt)
for (col in ind_names) {
  zsc_dt[, (col) := .robust_zscore(zsc_dt[[col]])]
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

# check_daily = FALSE throughout the fixture tests: the synthetic
# snapshots have no timeseries layer (the daily-layer gate is exercised
# separately in TEST 6).
val <- validate_snapshot(snapshot_date, output_dir = test_dir,
                         check_daily = FALSE)

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

test("validate: z-score median exactly zero (median/MAD invariant)",
     val$checks["zscore_median_zero"])

test("validate: z-score MAD exactly one (median/MAD invariant)",
     val$checks["zscore_mad_one"])

test("validate: no Unknown sectors in clean fixture",
     val$checks["no_unknown_sector"])

test("validate: mask fires for Financial in clean fixture",
     val$checks["mask_fires_financial"])

# Test with missing file
val_missing <- validate_snapshot("1999-01-01", output_dir = test_dir,
                                 check_daily = FALSE)
test("validate: missing file returns pass=FALSE",
     !val_missing$pass)

# Test with corrupt file
corrupt_path <- file.path(test_dir, "pit_2000-01-01_raw.parquet")
writeLines("not a parquet file", corrupt_path)
val_corrupt <- validate_snapshot("2000-01-01", output_dir = test_dir,
                                 check_daily = FALSE)
test("validate: corrupt file returns pass=FALSE",
     !val_corrupt$pass)
unlink(corrupt_path)

# -- D3 gate: Unknown sector bucket must fail --
unk_dt <- copy(raw_dt)
unk_dt[1:3, sector := "Unknown"]
arrow::write_parquet(unk_dt,
                     file.path(test_dir, "pit_2023-12-31_raw.parquet"))
arrow::write_parquet(zsc_dt,
                     file.path(test_dir, "pit_2023-12-31_zscore.parquet"))
val_unk <- validate_snapshot("2023-12-31", output_dir = test_dir,
                             check_daily = FALSE)
test("validate: Unknown sector bucket fails no_unknown_sector",
     !val_unk$checks["no_unknown_sector"])
test("validate: Unknown sector tickers reported in details",
     length(val_unk$details$unknown_sector_tickers) == 3)
test("validate: Unknown sector bucket fails overall",
     !val_unk$pass)

# -- Mask-fires gate: a Financial member with a masked value must fail --
leak_dt <- copy(raw_dt)
fin_idx <- which(leak_dt$sector == "Financial")[1]
leak_dt[fin_idx, gpa := 0.42]
arrow::write_parquet(leak_dt,
                     file.path(test_dir, "pit_2024-03-31_raw.parquet"))
arrow::write_parquet(zsc_dt,
                     file.path(test_dir, "pit_2024-03-31_zscore.parquet"))
val_leak <- validate_snapshot("2024-03-31", output_dir = test_dir,
                              check_daily = FALSE)
test("validate: unmasked Financial gpa fails mask_fires_financial",
     !val_leak$checks["mask_fires_financial"])
test("validate: mask leak fails overall",
     !val_leak$pass)


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

for (sec in names(.SECTOR_NA_INDICATORS)) {
  cols <- intersect(.SECTOR_NA_INDICATORS[[sec]], names(large_raw))
  idx <- which(large_raw$sector == sec)
  if (length(idx) && length(cols)) {
    for (cn in cols) large_raw[idx, (cn) := NA_real_]
  }
}

large_zsc <- copy(large_raw)
for (col in ind_names) {
  large_zsc[, (col) := .robust_zscore(large_zsc[[col]])]
}

arrow::write_parquet(large_raw,
                     file.path(test_dir, "pit_2023-09-30_raw.parquet"))
arrow::write_parquet(large_zsc,
                     file.path(test_dir, "pit_2023-09-30_zscore.parquet"))

val_large <- validate_snapshot("2023-09-30", output_dir = test_dir,
                               check_daily = FALSE)

test("validate: 450 tickers is plausible",
     val_large$checks["ticker_count_plausible"])

test("validate: no 99% NA indicators (full synthetic data)",
     val_large$checks["no_99pct_na_indicators"])

test("validate: all checks pass for well-formed snapshot",
     val_large$pass)


# ============================================================================
# TEST 6: validate_daily_layer (D1 gate + sector coverage + on-disk mask)
# ============================================================================
message("\n=== validate_daily_layer ===")

dl_ts    <- file.path(tempdir(), "test_ts");     dir.create(dl_ts, showWarnings = FALSE)
dl_split <- file.path(tempdir(), "test_splits"); dir.create(dl_split, showWarnings = FALSE)
dl_sec   <- file.path(tempdir(), "test_sector.parquet")

# Sector lookup: GOODCO/BADCO Technology, FINCO Financial; NOSEC absent.
arrow::write_parquet(data.table(
  ticker   = c("GOODCO", "BADCO", "FINCO"),
  sector   = c("Technology", "Technology", "Financial"),
  industry = c("Software", "Software", "Banks - Regional"),
  source   = "finviz"), dl_sec)

# 40 trading days around a 2:1 split at day 21 (ratio 0.5, multiple 2).
dl_dates <- as.Date("2020-06-01") + 0:39
ex_date  <- dl_dates[21]
pre      <- dl_dates <  ex_date
price    <- ifelse(pre, 100, 50)          # as-traded price halves

# GOODCO: split-consistent (shares double at the split; pe continuous)
arrow::write_parquet(data.table(
  date = dl_dates, price = price,
  market_cap  = ifelse(pre, 100 * 1e9, 50 * 2e9),
  pe_trailing = 20,
  gpa = NA_real_), file.path(dl_ts, "GOODCO_daily.parquet"))

# BADCO: the D1 defect (stub shares never adjust; pe halves at the split)
arrow::write_parquet(data.table(
  date = dl_dates, price = price,
  market_cap  = price * 1e9,
  pe_trailing = ifelse(pre, 20, 10),
  gpa = NA_real_), file.path(dl_ts, "BADCO_daily.parquet"))

# FINCO: no split, but a masked indicator carries values on disk
arrow::write_parquet(data.table(
  date = dl_dates, price = 30, market_cap = 30 * 5e8,
  pe_trailing = 12,
  gpa = 0.3, net_debt_price = 0.5),
  file.path(dl_ts, "FINCO_daily.parquet"))

# NOSEC: fund layer only, no sector row -> coverage failure
arrow::write_parquet(data.table(fiscal_year = 2020L,
                                filed_date = as.Date("2021-02-01")),
                     file.path(dl_ts, "NOSEC_fund.parquet"))

# Splits caches: same true split for GOODCO and BADCO; none for FINCO.
for (tk in c("GOODCO", "BADCO")) {
  arrow::write_parquet(data.table(ex_date = ex_date, ratio = 0.5),
                       file.path(dl_split, paste0(tk, ".parquet")))
}

dl <- validate_daily_layer(ts_dir = dl_ts, sector_path = dl_sec,
                           split_dir = dl_split)

test("daily layer: returns list with pass/checks/details",
     is.list(dl) && all(c("pass", "checks", "details") %in% names(dl)))

test("daily layer: missing sector row fails coverage",
     !dl$checks["sector_coverage_complete"])

test("daily layer: NOSEC reported in details",
     identical(dl$details$no_sector_tickers, "NOSEC"))

test("daily layer: on-disk mask leak detected (FINCO)",
     !dl$checks["mask_fires_on_disk"] &&
       grepl("^FINCO:", dl$details$mask_leaks[1]))

test("daily layer: split-inconsistent market cap detected (BADCO)",
     !dl$checks["split_consistent_market_cap"] &&
       identical(dl$details$split_jump_failures$ticker, "BADCO"))

test("daily layer: split-consistent GOODCO not flagged",
     !"GOODCO" %in% dl$details$split_jump_failures$ticker)

test("daily layer: overall fail on bad fixture",
     !dl$pass)

# Clean fixture: fix FINCO + BADCO, give NOSEC a sector
arrow::write_parquet(data.table(
  ticker   = c("GOODCO", "BADCO", "FINCO", "NOSEC"),
  sector   = c("Technology", "Technology", "Financial", "Energy"),
  industry = c("Software", "Software", "Banks - Regional", "Oil & Gas"),
  source   = "finviz"), dl_sec)
arrow::write_parquet(data.table(
  date = dl_dates, price = price,
  market_cap  = ifelse(pre, 100 * 1e9, 50 * 2e9),
  pe_trailing = 20,
  gpa = NA_real_), file.path(dl_ts, "BADCO_daily.parquet"))
arrow::write_parquet(data.table(
  date = dl_dates, price = 30, market_cap = 30 * 5e8,
  pe_trailing = 12,
  gpa = NA_real_, net_debt_price = NA_real_),
  file.path(dl_ts, "FINCO_daily.parquet"))

dl2 <- validate_daily_layer(ts_dir = dl_ts, sector_path = dl_sec,
                            split_dir = dl_split)

test("daily layer: clean fixture passes coverage",
     dl2$checks["sector_coverage_complete"])
test("daily layer: clean fixture passes mask check",
     dl2$checks["mask_fires_on_disk"])
test("daily layer: clean fixture passes split consistency",
     dl2$checks["split_consistent_market_cap"])
test("daily layer: file-count plausibility still fails on 3 files",
     !dl2$checks["daily_files_present"])

unlink(c(dl_ts, dl_split), recursive = TRUE)
unlink(dl_sec)


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
