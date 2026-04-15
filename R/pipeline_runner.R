# ============================================================================
# pipeline_runner.R  --  Pipeline Orchestration & Validation (Module 6)
# ============================================================================
# Orchestrates the full pipeline from constituent master through snapshot
# assembly. Provides validation and coverage reporting.
#
# Public API:
#   run_full_build(start_date, end_date, ...)
#   run_daily_update(date, ...)
#   validate_snapshot(snapshot_date, output_dir)
#   summarize_coverage(snapshot_date, output_dir)
#
# Dependencies: data.table, arrow
# Requires: all other modules already sourced
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})


# =============================================================================
# CONSTANTS
# =============================================================================

.DEFAULT_MASTER_PATH  <- "cache/lookups/constituent_master.parquet"
.DEFAULT_SECTOR_PATH  <- "cache/lookups/sector_industry.parquet"
.DEFAULT_FUND_DIR     <- "cache/fundamentals"
.DEFAULT_PRICE_DIR    <- "cache/prices"
.DEFAULT_SNAPSHOT_DIR <- "cache/snapshots"


# =============================================================================
# PRIVATE HELPERS
# =============================================================================

.assert_output <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}

.check_prerequisite <- function(path, label) {
  if (!file.exists(path)) {
    stop(sprintf("run_full_build: prerequisite missing: %s (%s)", label, path),
         call. = FALSE)
  }
  message(sprintf("  [ok] %s", label))
}


# =============================================================================
# 1. run_full_build()
# =============================================================================
#' Run the full end-to-end pipeline
#'
#' Verifies that all prerequisites exist (Sessions A-D outputs),
#' then builds historical snapshots at quarterly cadence.
#'
#' Does NOT re-run Sessions A-D. Those must be completed before calling this.
#'
#' @param start_date Character. First snapshot date (default "2010-03-31").
#' @param end_date Character or Date. Last snapshot date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @param force_refresh Logical. Rebuild existing snapshots.
#' @return List of snapshot stats (invisibly).
run_full_build <- function(start_date      = "2010-03-31",
                           end_date        = Sys.Date(),
                           master_path     = .DEFAULT_MASTER_PATH,
                           sector_path     = .DEFAULT_SECTOR_PATH,
                           fund_dir        = .DEFAULT_FUND_DIR,
                           price_cache_dir = .DEFAULT_PRICE_DIR,
                           output_dir      = .DEFAULT_SNAPSHOT_DIR,
                           force_refresh   = FALSE) {

  message("run_full_build: starting pipeline...")
  t0 <- Sys.time()

  # -- Check prerequisites --
  message("  checking prerequisites...")
  .check_prerequisite(master_path, "constituent_master.parquet")
  .check_prerequisite(sector_path, "sector_industry.parquet")

  # Check fundamentals cache has files
  fund_files <- list.files(fund_dir, pattern = "\\.parquet$")
  if (length(fund_files) == 0) {
    stop("run_full_build: no fundamentals cached. Run Sessions C+D first.",
         call. = FALSE)
  }
  message(sprintf("  [ok] fundamentals cache: %d files", length(fund_files)))

  # -- Build snapshots --
  results <- build_historical_snapshots(
    start_date      = start_date,
    end_date        = end_date,
    master_path     = master_path,
    sector_path     = sector_path,
    fund_dir        = fund_dir,
    price_cache_dir = price_cache_dir,
    output_dir      = output_dir,
    force_refresh   = force_refresh
  )

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("\nrun_full_build: complete in %.1f minutes", elapsed))

  # -- Summary --
  snapshot_files <- list.files(output_dir, pattern = "pit_.*_raw\\.parquet$")
  message(sprintf("  total snapshots on disk: %d", length(snapshot_files)))

  invisible(results)
}


# =============================================================================
# 2. run_daily_update()
# =============================================================================
#' Run an incremental update for a single date
#'
#' Assembles (or reassembles) the snapshot for the given date.
#' Useful for daily or weekly refreshes of the latest snapshot.
#'
#' @param date Date or character. Snapshot date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @return Snapshot result (list with $raw, $zscored, $stats), or NULL.
run_daily_update <- function(date            = Sys.Date(),
                             master_path     = .DEFAULT_MASTER_PATH,
                             sector_path     = .DEFAULT_SECTOR_PATH,
                             fund_dir        = .DEFAULT_FUND_DIR,
                             price_cache_dir = .DEFAULT_PRICE_DIR,
                             output_dir      = .DEFAULT_SNAPSHOT_DIR) {

  message(sprintf("run_daily_update: %s", as.Date(date)))
  t0 <- Sys.time()

  result <- assemble_snapshot(
    snapshot_date   = date,
    master_path     = master_path,
    sector_path     = sector_path,
    fund_dir        = fund_dir,
    price_cache_dir = price_cache_dir,
    output_dir      = output_dir,
    prefetch_prices = TRUE
  )

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("run_daily_update: done in %.1f minutes", elapsed))

  invisible(result)
}


# =============================================================================
# 3. validate_snapshot()
# =============================================================================
#' Validate a snapshot against sanity checks
#'
#' Checks:
#'   1. Ticker count is plausible (~400-505 for S&P 500)
#'   2. Z-scores have mean near 0 and SD near 1
#'   3. No indicator is >99% NA
#'   4. Key metadata columns present
#'   5. No duplicate tickers
#'   6. Piotroski F-Score is 0-9
#'
#' @param snapshot_date Date or character.
#' @param output_dir Character. Snapshot directory.
#' @return List with $pass (logical), $checks (named logical vector),
#'   $details (list of diagnostic info).
validate_snapshot <- function(snapshot_date,
                              output_dir = .DEFAULT_SNAPSHOT_DIR) {

  snapshot_date <- as.Date(snapshot_date)
  message(sprintf("validate_snapshot: %s", snapshot_date))

  raw_path    <- file.path(output_dir,
                           sprintf("pit_%s_raw.parquet", snapshot_date))
  zscore_path <- file.path(output_dir,
                           sprintf("pit_%s_zscore.parquet", snapshot_date))

  checks  <- c()
  details <- list()

  # -- File existence --
  checks["raw_file_exists"]    <- file.exists(raw_path)
  checks["zscore_file_exists"] <- file.exists(zscore_path)

  if (!checks["raw_file_exists"]) {
    message("  FAIL: raw parquet not found")
    return(list(pass = FALSE, checks = checks, details = details))
  }

  raw <- tryCatch(
    as.data.table(arrow::read_parquet(raw_path)),
    error = function(e) {
      message(sprintf("  FAIL: corrupt raw parquet: %s", e$message))
      NULL
    }
  )
  if (is.null(raw)) {
    checks["raw_readable"] <- FALSE
    return(list(pass = FALSE, checks = checks, details = details))
  }

  zsc <- if (checks["zscore_file_exists"]) {
    tryCatch(
      as.data.table(arrow::read_parquet(zscore_path)),
      error = function(e) NULL
    )
  } else NULL

  n_tickers <- nrow(raw)
  details$n_tickers <- n_tickers
  ind_names <- get_indicator_names()

  # -- Ticker count --
  checks["ticker_count_plausible"] <- n_tickers >= 100 && n_tickers <= 600
  message(sprintf("  tickers: %d %s", n_tickers,
                  if (checks["ticker_count_plausible"]) "[ok]" else "[WARN]"))

  # -- Required columns --
  required <- c("date", "ticker", "industry", "sector", ind_names)
  checks["all_columns_present"] <- all(required %in% names(raw))

  # -- No duplicate tickers --
  checks["no_duplicate_tickers"] <- !anyDuplicated(raw$ticker)

  # -- NA rates per indicator --
  na_rates <- sapply(ind_names, function(col) {
    if (col %in% names(raw)) mean(is.na(raw[[col]])) else 1
  })
  details$na_rates <- na_rates

  high_na <- na_rates[na_rates > 0.99]
  checks["no_99pct_na_indicators"] <- length(high_na) == 0
  if (length(high_na) > 0) {
    message(sprintf("  WARNING: %d indicators >99%% NA: %s",
                    length(high_na), paste(names(high_na), collapse = ", ")))
  }

  # Median NA rate
  details$median_na_rate <- median(na_rates)
  message(sprintf("  median NA rate: %.1f%%", details$median_na_rate * 100))

  # -- Piotroski F-Score range --
  if ("f_score" %in% names(raw)) {
    fs <- raw$f_score[!is.na(raw$f_score)]
    checks["f_score_range_valid"] <- length(fs) == 0 ||
      (min(fs) >= 0 && max(fs) <= 9)
    details$f_score_mean <- if (length(fs) > 0) mean(fs) else NA
  }

  # -- Z-score validation --
  if (!is.null(zsc)) {
    # Check mean near 0, SD near 1 for non-NA indicators
    z_means <- sapply(ind_names, function(col) {
      if (col %in% names(zsc)) mean(zsc[[col]], na.rm = TRUE) else NA
    })
    z_sds <- sapply(ind_names, function(col) {
      if (col %in% names(zsc)) sd(zsc[[col]], na.rm = TRUE) else NA
    })

    valid_z <- !is.na(z_means) & !is.na(z_sds)
    if (sum(valid_z) > 0) {
      checks["zscore_mean_near_zero"] <- all(abs(z_means[valid_z]) < 0.5)
      # Low-coverage indicators can have compressed SD after winsorization
      n_bad_sd <- sum(z_sds[valid_z] <= 0.1 | z_sds[valid_z] > 2.0)
      checks["zscore_sd_reasonable"]  <- n_bad_sd <= 5
      details$z_mean_range <- range(z_means[valid_z])
      details$z_sd_range   <- range(z_sds[valid_z])

      message(sprintf("  z-score means: [%.3f, %.3f]",
                      details$z_mean_range[1], details$z_mean_range[2]))
      message(sprintf("  z-score SDs:   [%.3f, %.3f]",
                      details$z_sd_range[1], details$z_sd_range[2]))
    }
  }

  # -- Sector distribution --
  if ("sector" %in% names(raw)) {
    sector_dist <- raw[, .N, by = sector][order(-N)]
    details$sector_distribution <- sector_dist
    checks["multiple_sectors"] <- nrow(sector_dist) >= 5
    message("  sectors:")
    for (j in seq_len(nrow(sector_dist))) {
      message(sprintf("    %s: %d", sector_dist$sector[j], sector_dist$N[j]))
    }
  }

  # -- Overall pass --
  all_pass <- all(checks)
  message(sprintf("  result: %d/%d checks passed -- %s",
                  sum(checks), length(checks),
                  if (all_pass) "PASS" else "FAIL"))

  list(pass = all_pass, checks = checks, details = details)
}


# =============================================================================
# 4. summarize_coverage()
# =============================================================================
#' Generate a coverage report for a snapshot
#'
#' Shows the percentage of tickers with non-NA values for each indicator,
#' broken down by sector.
#'
#' @param snapshot_date Date or character.
#' @param output_dir Character. Snapshot directory.
#' @return data.table with indicator, overall coverage, and per-sector coverage.
summarize_coverage <- function(snapshot_date,
                               output_dir = .DEFAULT_SNAPSHOT_DIR) {

  snapshot_date <- as.Date(snapshot_date)

  raw <- load_snapshot(snapshot_date, type = "raw", output_dir = output_dir)
  if (is.null(raw)) {
    stop(sprintf("summarize_coverage: no snapshot found for %s", snapshot_date),
         call. = FALSE)
  }

  ind_names <- get_indicator_names()
  sectors   <- unique(raw$sector)

  # Overall coverage per indicator
  overall <- data.table(
    indicator = ind_names,
    n_total   = nrow(raw),
    n_valid   = sapply(ind_names, function(col) {
      if (col %in% names(raw)) sum(!is.na(raw[[col]])) else 0L
    }),
    pct_valid = sapply(ind_names, function(col) {
      if (col %in% names(raw)) {
        round(mean(!is.na(raw[[col]])) * 100, 1)
      } else 0
    })
  )

  # Per-sector coverage (wide format: one column per sector)
  for (sec in sort(sectors)) {
    sec_dt <- raw[sector == sec]
    col_name <- gsub(" ", "_", tolower(sec))
    overall[, (col_name) := sapply(ind_names, function(col) {
      if (col %in% names(sec_dt) && nrow(sec_dt) > 0) {
        round(mean(!is.na(sec_dt[[col]])) * 100, 1)
      } else 0
    })]
  }

  # Print summary
  message(sprintf("summarize_coverage: %s (%d tickers, %d indicators)",
                  snapshot_date, nrow(raw), length(ind_names)))

  # Group by coverage tier
  high   <- overall[pct_valid >= 90]
  medium <- overall[pct_valid >= 50 & pct_valid < 90]
  low    <- overall[pct_valid < 50]

  message(sprintf("  high coverage (>=90%%): %d indicators", nrow(high)))
  message(sprintf("  medium coverage (50-89%%): %d indicators", nrow(medium)))
  message(sprintf("  low coverage (<50%%): %d indicators", nrow(low)))

  if (nrow(low) > 0) {
    message("  low coverage indicators:")
    for (j in seq_len(nrow(low))) {
      message(sprintf("    %s: %.1f%%", low$indicator[j], low$pct_valid[j]))
    }
  }

  .assert_output(overall, "summarize_coverage", list(
    "is data.table"        = is.data.table,
    "has indicator col"    = function(x) "indicator" %in% names(x),
    "has pct_valid col"    = function(x) "pct_valid" %in% names(x),
    "covers all indicators" = function(x) nrow(x) == length(ind_names)
  ))

  overall
}


# =============================================================================
# 5. list_snapshots()
# =============================================================================
#' List all available snapshot dates
#'
#' @param output_dir Character. Snapshot directory.
#' @return Character vector of snapshot dates (sorted), or character(0).
list_snapshots <- function(output_dir = .DEFAULT_SNAPSHOT_DIR) {

  if (!dir.exists(output_dir)) return(character(0))

  files <- list.files(output_dir, pattern = "pit_.*_raw\\.parquet$")
  dates <- gsub("pit_(.*)_raw\\.parquet", "\\1", files)
  sort(dates)
}
