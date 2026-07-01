# ============================================================================
# test_feature_standardizer.R  --  Unit + Gate Tests for Module 8
# ============================================================================
# Run: Rscript tests/test_feature_standardizer.R
#
# Structure:
#   1. Unit tests: constants, robust winsorization, rolling z-score, log-safe,
#      CS within-sector z-scoring, edge cases
#   2. Gate tests: integration with real cached data, build pipeline,
#      cross-dimensional correlation
# ============================================================================

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
source("R/timeseries_builder.R")
source("R/feature_standardizer.R")

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
# UNIT TESTS
# ============================================================================

# -- 1. Constants consistency --
message("\n=== Unit: Constants ===")

all_ind <- get_indicator_names()

test("standardizable + passthrough = all indicators",
     length(.STANDARDIZABLE_INDICATORS) + length(.PASSTHROUGH_INDICATORS) ==
       length(all_ind))

test("no overlap between standardizable and passthrough",
     length(intersect(.STANDARDIZABLE_INDICATORS, .PASSTHROUGH_INDICATORS)) == 0)

test("all standardizable are valid indicator names",
     all(.STANDARDIZABLE_INDICATORS %in% all_ind))

test("all passthrough are valid indicator names",
     all(.PASSTHROUGH_INDICATORS %in% all_ind))

test("standardizable count = all indicators minus passthrough",
     length(.STANDARDIZABLE_INDICATORS) ==
       length(get_indicator_names()) - length(.PASSTHROUGH_INDICATORS))

test("9 passthrough indicators (Piotroski binaries)",
     length(.PASSTHROUGH_INDICATORS) == 9)

test("5 log-transform indicators (size/level)",
     length(.LOG_TRANSFORM_INDICATORS) == 5)

test("all log-transform indicators are standardizable",
     all(.LOG_TRANSFORM_INDICATORS %in% .STANDARDIZABLE_INDICATORS))

test("5 financial-NA indicators",
     length(.FEAT_FINANCIAL_NA) == 5)

test("all financial-NA indicators are standardizable",
     all(.FEAT_FINANCIAL_NA %in% .STANDARDIZABLE_INDICATORS))

test("TS window = 1260 (5yr trading days)",
     .TS_WINDOW == 1260L)

test("TS min obs = 252 (1yr)",
     .TS_MIN_OBS == 252L)

test("feature names count = 3 dims x standardizable + passthrough",
     length(get_feature_names()) ==
       3L * length(.STANDARDIZABLE_INDICATORS) + length(.PASSTHROUGH_INDICATORS))


# -- 2. Robust winsorization --
message("\n=== Unit: .robust_winsorize() ===")

test("normal data: no clipping needed",
     {
       x <- rnorm(100)
       w <- .robust_winsorize(x)
       all(!is.na(w)) && length(w) == 100
     })

test("extreme outlier clipped",
     {
       set.seed(1)
       x <- c(rnorm(99, mean = 0, sd = 1), 100)
       w <- .robust_winsorize(x)
       w[100] < 100
     })

test("NAs preserved",
     {
       x <- c(1, 2, 3, NA, 5, 6, 7)
       w <- .robust_winsorize(x)
       is.na(w[4]) && !is.na(w[1])
     })

test("too few values (< 3): returns input unchanged",
     {
       x <- c(1, NA)
       identical(.robust_winsorize(x), x)
     })

test("constant vector: returns unchanged (MAD = 0)",
     {
       x <- rep(5, 10)
       identical(.robust_winsorize(x), x)
     })

test("symmetric clipping",
     {
       x <- c(-100, seq(-2, 2, length.out = 98), 100)
       w <- .robust_winsorize(x)
       abs(w[1]) == abs(w[100])
     })


# -- 3. Rolling z-score --
message("\n=== Unit: .rolling_zscore() ===")

test("all NA when fewer than min_obs",
     {
       x <- rnorm(200)
       z <- .rolling_zscore(x, window = 1260, min_obs = 252)
       all(is.na(z))
     })

test("produces values after min_obs observations",
     {
       x <- rnorm(500)
       z <- .rolling_zscore(x, window = 1260, min_obs = 100)
       # First 99 should be NA, then values should appear
       all(is.na(z[1:99])) && !all(is.na(z[100:500]))
     })

test("z-scores bounded to [-3, 3]",
     {
       set.seed(42)
       x <- c(rnorm(300), 100, -100)  # add outliers
       z <- .rolling_zscore(x, window = 300, min_obs = 50)
       valid <- z[!is.na(z)]
       length(valid) > 0 && all(valid >= -3 & valid <= 3)
     })

test("empty input returns empty",
     length(.rolling_zscore(numeric(0))) == 0)

test("all-NA input returns all-NA",
     {
       x <- rep(NA_real_, 100)
       z <- .rolling_zscore(x, min_obs = 10)
       all(is.na(z))
     })

test("constant series: z-scores are NA (sd = 0)",
     {
       x <- rep(5.0, 500)
       z <- .rolling_zscore(x, window = 300, min_obs = 100)
       all(is.na(z))
     })

test("z-score of mean value is ~0",
     {
       set.seed(123)
       x <- rnorm(1000, mean = 50, sd = 10)
       z <- .rolling_zscore(x, window = 500, min_obs = 200)
       # At the midpoint, mean of window ~ 50, so z(50) ~ 0
       valid <- z[!is.na(z)]
       length(valid) > 0 && median(abs(valid)) < 1.5
     })

# Point-in-time guarantee: appending a future observation (even an extreme
# outlier) must not change any past z-score. This is the regression test for
# the whole-series winsorization look-ahead that was fixed.
test("rolling_zscore is causal: future data cannot change past z-scores",
     {
       set.seed(7)
       n <- 800L
       x <- rnorm(n)
       z_base <- .rolling_zscore(x, window = 300L, min_obs = 100L)
       # Append an extreme future value and recompute over the longer series.
       z_ext  <- .rolling_zscore(c(x, 50), window = 300L, min_obs = 100L)
       nn <- !is.na(z_base)
       length(z_ext) == n + 1L &&
         all(abs(z_base[nn] - z_ext[seq_len(n)][nn]) < 1e-9)
     })


# -- 4. Log-safe --
message("\n=== Unit: .log_safe() ===")

test("positive values: log1p applied",
     {
       x <- c(100, 1000, 1e6)
       l <- .log_safe(x)
       all(l > 0) && all(l == log1p(x))
     })

test("zero: returns 0",
     .log_safe(0) == 0)

test("negative values: sign preserved",
     {
       x <- c(-100, -1000)
       l <- .log_safe(x)
       all(l < 0) && all(l == -log1p(abs(x)))
     })

test("NA preserved",
     is.na(.log_safe(NA_real_)))


# -- 5. CS within-sector z-scoring --
message("\n=== Unit: build_cs_sec() ===")

test("basic CS z-scoring with two sectors",
     {
       dt <- data.table(
         ticker = paste0("T", 1:20),
         sector = rep(c("Tech", "Energy"), each = 10),
         roe    = c(rnorm(10, 0.15, 0.05), rnorm(10, 0.10, 0.03)),
         pb     = c(rnorm(10, 5, 1), rnorm(10, 2, 0.5))
       )
       result <- build_cs_sec(as.Date("2024-01-02"), dt,
                              feat_dir = tempdir(), save = FALSE)
       !is.null(result) &&
         "roe_cs_sec" %in% names(result) &&
         "pb_cs_sec" %in% names(result) &&
         nrow(result) == 20
     })

test("CS z-scores bounded to [-3, 3]",
     {
       set.seed(99)
       dt <- data.table(
         ticker = paste0("T", 1:50),
         sector = rep("Tech", 50),
         roe    = c(rnorm(49, 0.15, 0.05), 10)  # extreme outlier
       )
       result <- build_cs_sec(as.Date("2024-01-02"), dt,
                              feat_dir = tempdir(), save = FALSE)
       z <- result$roe_cs_sec
       valid <- z[!is.na(z)]
       length(valid) > 0 && all(valid >= -3 & valid <= 3)
     })

test("small sector (< 5) gets NA",
     {
       dt <- data.table(
         ticker = paste0("T", 1:8),
         sector = c(rep("Tech", 5), rep("Tiny", 3)),
         roe    = rnorm(8)
       )
       result <- build_cs_sec(as.Date("2024-01-02"), dt,
                              feat_dir = tempdir(), save = FALSE)
       # Tiny sector (3 stocks) should be NA
       tiny_idx <- which(dt$sector == "Tiny")
       all(is.na(result$roe_cs_sec[tiny_idx]))
     })

test("financial-NA indicators are NA for Financial sector",
     {
       dt <- data.table(
         ticker = paste0("T", 1:20),
         sector = rep(c("Financial", "Tech"), each = 10),
         gpa    = rnorm(20),
         roe    = rnorm(20)
       )
       result <- build_cs_sec(as.Date("2024-01-02"), dt,
                              feat_dir = tempdir(), save = FALSE)
       fin_idx <- which(dt$sector == "Financial")
       # gpa (financial-NA) should be NA for financials
       all(is.na(result$gpa_cs_sec[fin_idx]))
     })

test("within-sector: mean ~0 and sd ~1 for each sector",
     {
       set.seed(42)
       dt <- data.table(
         ticker = paste0("T", 1:100),
         sector = rep(c("Tech", "Energy"), each = 50),
         roe    = c(rnorm(50, 0.15, 0.05), rnorm(50, 0.10, 0.03))
       )
       result <- build_cs_sec(as.Date("2024-01-02"), dt,
                              feat_dir = tempdir(), save = FALSE)
       tech_z <- result$roe_cs_sec[1:50]
       energy_z <- result$roe_cs_sec[51:100]
       tech_z <- tech_z[!is.na(tech_z)]
       energy_z <- energy_z[!is.na(energy_z)]
       abs(mean(tech_z)) < 0.2 && abs(mean(energy_z)) < 0.2 &&
         abs(sd(tech_z) - 1) < 0.3 && abs(sd(energy_z) - 1) < 0.3
     })


# ============================================================================
# GATE TESTS (require real cached data)
# ============================================================================

has_daily_data <- dir.exists("cache/timeseries") &&
  length(list.files("cache/timeseries", pattern = "_daily\\.parquet$")) > 10

if (!has_daily_data) {
  message("\n=== SKIP: Gate tests (no cached daily data) ===")
} else {

  message("\n=== Gate: build_ts_own (AAPL) ===")

  test("build_ts_own produces output for AAPL",
       {
         tmp_dir <- file.path(tempdir(), "feat_test")
         if (dir.exists(tmp_dir)) unlink(tmp_dir, recursive = TRUE)
         dir.create(tmp_dir, recursive = TRUE)

         result <- build_ts_own("AAPL", feat_dir = tmp_dir, force = TRUE)
         !is.null(result) && nrow(result) > 100 && "date" %in% names(result)
       })

  test("TS own has correct column naming pattern",
       {
         result <- build_ts_own("AAPL", feat_dir = tmp_dir, force = TRUE)
         ts_cols <- grep("_ts_own$", names(result), value = TRUE)
         length(ts_cols) == length(.STANDARDIZABLE_INDICATORS)
       })

  test("TS own z-scores bounded to [-3, 3]",
       {
         result <- build_ts_own("AAPL", feat_dir = tmp_dir, force = TRUE)
         ts_cols <- grep("_ts_own$", names(result), value = TRUE)
         all_vals <- unlist(result[, ..ts_cols])
         valid <- all_vals[!is.na(all_vals)]
         length(valid) > 0 && all(valid >= -3 & valid <= 3)
       })

  test("TS own first ~252 rows are NA (min_obs requirement)",
       {
         result <- build_ts_own("AAPL", feat_dir = tmp_dir, force = TRUE)
         # Check first indicator
         first_col <- grep("_ts_own$", names(result), value = TRUE)[1]
         vals <- result[[first_col]]
         non_na_idx <- which(!is.na(vals))
         length(non_na_idx) > 0 && min(non_na_idx) >= 200  # ~252, allow some slack
       })


  message("\n=== Gate: build_cs_sec (real cross-section) ===")

  test("build_cs_sec on real cross-section data",
       {
         cs <- load_daily_cross_section(as.Date("2024-01-02"), zscore = FALSE)
         if (is.null(cs)) {
           message("    (skipped: no data for 2024-01-02)")
           TRUE
         } else {
           result <- build_cs_sec(as.Date("2024-01-02"), cs,
                                  feat_dir = tmp_dir, save = TRUE)
           !is.null(result) && nrow(result) > 100 &&
             "roe_cs_sec" %in% names(result)
         }
       })


  message("\n=== Gate: Mini pipeline (20 tickers) ===")

  test("build_all_features on 20 tickers",
       {
         mini_dir <- file.path(tempdir(), "feat_mini")
         if (dir.exists(mini_dir)) unlink(mini_dir, recursive = TRUE)

         available <- list_timeseries_tickers()
         mini_tickers <- head(sort(available), 20)

         stats <- build_all_features(
           from_date = "2023-01-03",
           to_date   = "2023-06-30",
           feat_dir  = mini_dir,
           tickers   = mini_tickers
         )

         stats$n_assembled > 0
       })

  test("assembled features contain every get_feature_names() + meta column",
       {
         mini_dir <- file.path(tempdir(), "feat_mini")
         feat_files <- list.files(file.path(mini_dir, "daily"),
                                  pattern = "_features\\.parquet$",
                                  full.names = TRUE)
         if (length(feat_files) == 0) {
           FALSE
         } else {
           dt <- as.data.table(arrow::read_parquet(feat_files[1]))
           # assemble_ticker_features ensures every dimension column exists, so
           # all feature names (plus the 3 meta cols) must be present exactly.
           all(get_feature_names() %in% names(dt)) &&
             all(c("date", "ticker", "sector") %in% names(dt))
         }
       })

  test("z-scores in assembled features bounded to [-3, 3]",
       {
         mini_dir <- file.path(tempdir(), "feat_mini")
         feat_files <- list.files(file.path(mini_dir, "daily"),
                                  pattern = "_features\\.parquet$",
                                  full.names = TRUE)
         if (length(feat_files) == 0) {
           FALSE
         } else {
           dt <- as.data.table(arrow::read_parquet(feat_files[1]))
           z_cols <- grep("_(cs_sec|ts_own|ts_sec)$", names(dt), value = TRUE)
           if (length(z_cols) == 0) {
             FALSE
           } else {
             all_vals <- unlist(dt[, ..z_cols])
             valid <- all_vals[!is.na(all_vals)]
             length(valid) > 0 && all(valid >= -3 & valid <= 3)
           }
         }
       })

  test("load_feature_cross_section returns stacked data",
       {
         mini_dir <- file.path(tempdir(), "feat_mini")
         cs <- load_feature_cross_section(as.Date("2023-03-15"),
                                          feat_dir = mini_dir)
         !is.null(cs) && nrow(cs) > 5 && "ticker" %in% names(cs)
       })

  test("load_ticker_features returns per-ticker data",
       {
         mini_dir <- file.path(tempdir(), "feat_mini")
         available <- list.files(file.path(mini_dir, "daily"),
                                 pattern = "_features\\.parquet$")
         tk <- gsub("_features\\.parquet$", "", available[1])
         dt <- load_ticker_features(tk, feat_dir = mini_dir)
         !is.null(dt) && nrow(dt) > 10 && "date" %in% names(dt)
       })

  # Cleanup
  unlink(tmp_dir, recursive = TRUE)
}


# ============================================================================
# SUMMARY
# ============================================================================

message(sprintf("\n=== RESULTS: %d passed, %d failed (of %d) ===",
                .n_pass, .n_fail, .n_pass + .n_fail))
if (.n_fail > 0) message("*** FAILURES DETECTED ***")
