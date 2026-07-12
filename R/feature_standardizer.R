# ============================================================================
# feature_standardizer.R  --  Multi-Dimensional Factor Standardization (Module 8)
# ============================================================================
# Produces three standardization dimensions per indicator for downstream
# BSTAR consumption:
#   1. CS within-sector (_cs_sec): z-score within sector at each date
#   2. TS stock-level (_ts_own): 5yr rolling z-score vs own history
#   3. TS sector-level (_ts_sec): 5yr rolling z-score of sector median
#
# Academic basis: Baz et al. 2015 (CS/TS correlation ~0.43),
# MSCI 2021 "Theory of Value Relativity", GSAM 2018 "Mixed" approach.
# See docs/research/ for full literature review.
#
# Storage:
#   cache/features/sector_medians.parquet
#   cache/features/ts_sec.parquet
#   cache/features/ts_own/{ticker}_ts_own.parquet
#   cache/features/cs_sec/{YYYY-MM-DD}.parquet
#   cache/features/daily/{ticker}_features.parquet
#
# Public API:
#   build_all_features(from_date, to_date, ...)
#   update_features(through_date, ...)
#   load_feature_cross_section(target_date, ...)
#   load_ticker_features(ticker, from, to, ...)
#   get_feature_names()
#
# Dependencies: data.table, arrow, zoo
# Requires: indicator_compute.R, sector_classifier.R, timeseries_builder.R
#           (already sourced)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(zoo)
})


# =============================================================================
# SECTION 1: CONSTANTS
# =============================================================================

.FEAT_DIR <- "cache/features"

# Rolling window for TS z-scores: 5 years of trading days
.TS_WINDOW  <- 1260L

# Minimum observations before producing TS z-scores (1 year)
.TS_MIN_OBS <- 252L

# MAD multiplier for robust winsorization (Barra standard)
.MAD_MULT <- 3.0

# Z-score clipping bound
.ZSCORE_CLIP <- 3.0

# Minimum sector size for meaningful CS z-scores
.MIN_SECTOR_SIZE <- 5L

# Binary (0/1) components: passed through untransformed, since z-scoring
# a binary is meaningless. Piotroski f_* plus the Wave 5 Mohanram
# components (0/1 after cross-section binarization). The composites
# `f_score` (0-9) and `ms_score` (0-8) are NOT listed on purpose -- they
# are continuous enough to standardize like any other quality factor and
# stay in .STANDARDIZABLE_INDICATORS.
.PASSTHROUGH_INDICATORS <- c(
  "f_roa", "f_droa", "f_cfo", "f_accrual",
  "f_dlever", "f_dliquid", "f_eq_off",
  "f_dmargin", "f_dturn",
  "ms_roa", "ms_cfroa", "ms_accrual", "ms_roa_vol",
  "ms_rev_vol", "ms_rd", "ms_capex", "ms_adv"
)

# Indicators to standardize: full set minus the 17 binary components
# (derived from get_indicator_names() so it tracks the schema).
.STANDARDIZABLE_INDICATORS <- setdiff(get_indicator_names(),
                                       .PASSTHROUGH_INDICATORS)

# Size / level indicators requiring sign-preserving log-transform before
# standardizing: heavy right-skew, log-normal by construction, so raw
# percentile winsorization cannot tame the order-of-magnitude spread.
.LOG_TRANSFORM_INDICATORS <- c("market_cap", "enterprise_value", "revenue_raw",
                               "shares_outstanding", "public_float")

# Sector masking reuses the single source of truth from
# indicator_compute.R (.SECTOR_NA_INDICATORS via .sector_na_mask) so the
# lists can never drift apart. Since Wave 5 the mask covers Utilities
# and Real Estate (o_score / z_score / herf) in addition to Financial.


# =============================================================================
# SECTION 2: PRIVATE HELPERS
# =============================================================================

.assert_output_feat <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}


#' MAD-based robust winsorization (Barra approach)
#'
#' Clips values beyond mad_mult * MAD from the median. R's mad() already
#' applies the 1.4826 consistency constant for normal distributions.
#'
#' @param x Numeric vector.
#' @param mad_mult Numeric. Number of MADs from median for clip bounds.
#' @return Numeric vector with outliers clipped.
.robust_winsorize <- function(x, mad_mult = .MAD_MULT) {
  valid <- x[!is.na(x)]
  if (length(valid) < 3L) return(x)
  med <- median(valid)
  mad_val <- mad(valid)
  if (mad_val < 1e-12) return(x)
  lower <- med - mad_mult * mad_val
  upper <- med + mad_mult * mad_val
  pmin(pmax(x, lower), upper)
}


#' Causal trailing rolling mean / sd / count (fully vectorized)
#'
#' Trailing window ending at each position (align = "right"): every statistic
#' at index t uses only observations in (t - window, t], never future data.
#' Positions before the window is full use the expanding prefix. O(n), no loops.
#'
#' @param x Numeric vector (NAs allowed; excluded from the moments).
#' @param window Integer rolling window size.
#' @return List with numeric vectors mean, sd, count (all length(x)).
.causal_roll_stats <- function(x, window) {
  n       <- length(x)
  not_na  <- as.numeric(!is.na(x))
  x_zero  <- ifelse(is.na(x), 0, x)
  x_sq    <- x_zero^2

  cum_count <- cumsum(not_na)
  cum_sum   <- cumsum(x_zero)
  cum_sum2  <- cumsum(x_sq)

  roll_count <- frollsum(not_na, n = window, align = "right", algo = "exact")
  roll_sum   <- frollsum(x_zero, n = window, align = "right", algo = "exact")
  roll_sum2  <- frollsum(x_sq,   n = window, align = "right", algo = "exact")

  # Expanding prefix for positions 1:(window-1)
  expand_idx <- seq_len(min(window - 1L, n))
  roll_count[expand_idx] <- cum_count[expand_idx]
  roll_sum[expand_idx]   <- cum_sum[expand_idx]
  roll_sum2[expand_idx]  <- cum_sum2[expand_idx]

  roll_mean <- roll_sum / roll_count
  roll_var  <- roll_sum2 / roll_count - roll_mean^2
  roll_var  <- roll_var * roll_count / pmax(roll_count - 1, 1)   # Bessel
  roll_sd   <- sqrt(pmax(roll_var, 0))

  list(mean = roll_mean, sd = roll_sd, count = roll_count)
}


#' Rolling time-series z-score (fully vectorized, point-in-time)
#'
#' Two causal passes over the series so the value stamped at date t depends
#' only on data up to and including t (no look-ahead):
#'   1. Trailing mean/sd from the raw series define a mad_mult-sigma band.
#'   2. Each point is winsorized to its own trailing band, then trailing
#'      mean/sd are recomputed from the winsorized series for the z-score.
#' This replaces the earlier whole-series (future-peeking) winsorization.
#'
#' @param x Numeric vector. Full time series for one indicator.
#' @param window Integer. Rolling window size (default 1260 = 5yr).
#' @param min_obs Integer. Minimum non-NA observations (default 252 = 1yr).
#' @param mad_mult Numeric. Sigma multiplier for the trailing winsor band.
#' @param clip Numeric. Z-score clipping bound.
#' @return Numeric vector of same length as x with rolling z-scores.
.rolling_zscore <- function(x, window = .TS_WINDOW, min_obs = .TS_MIN_OBS,
                            mad_mult = .MAD_MULT, clip = .ZSCORE_CLIP) {
  n <- length(x)
  if (n == 0L) return(numeric(0))
  if (n < min_obs) return(rep(NA_real_, n))

  # Pass 1: causal trailing moments from the RAW series (no future data).
  s1 <- .causal_roll_stats(x, window)

  # Causal winsorization: clip each point to its own trailing mad_mult-sigma
  # band. Where the trailing sd is not yet estimable, keep the raw value
  # (those positions are NA'd below anyway via the min_obs guard).
  lower  <- s1$mean - mad_mult * s1$sd
  upper  <- s1$mean + mad_mult * s1$sd
  usable <- is.finite(lower) & is.finite(upper) & s1$sd > 0
  xw <- x
  xw[usable] <- pmin(pmax(x[usable], lower[usable]), upper[usable])

  # Pass 2: recompute causal trailing moments from the winsorized series.
  s2 <- .causal_roll_stats(xw, window)

  z <- (xw - s2$mean) / s2$sd

  # Constraints
  z[is.na(x)] <- NA_real_                 # preserve original NAs
  z[s2$count < min_obs] <- NA_real_       # insufficient observations
  z[s2$sd < 1e-12] <- NA_real_            # constant series
  z <- pmin(pmax(z, -clip), clip)         # clip to bounds
  z[is.na(z)] <- NA_real_                 # clean up NaN from 0/0

  z
}


#' Safe log transform for size indicators
#'
#' @param x Numeric vector.
#' @return log of absolute value, preserving sign and NAs.
.log_safe <- function(x) {
  ifelse(is.na(x), NA_real_, sign(x) * log1p(abs(x)))
}


#' Preload all daily parquet files into memory
#'
#' Reads each {ticker}_daily.parquet into a list keyed by ticker.
#' Used for efficient cross-section extraction during historical builds.
#'
#' @param ts_dir Character. Time series directory.
#' @param sector_path Character. Path to sector lookup.
#' @return Named list of data.tables (key = ticker).
.preload_all_daily <- function(ts_dir = "cache/timeseries",
                               sector_path = "cache/lookups/sector_industry.parquet") {
  daily_files <- list.files(ts_dir, pattern = "_daily\\.parquet$",
                            full.names = TRUE)
  if (length(daily_files) == 0) {
    warning(".preload_all_daily: no daily files found")
    return(list())
  }

  # Load sector lookup
  sec_dt <- NULL
  if (file.exists(sector_path)) {
    # current view of the dated sector table (SCD, D2/B)
    sec_dt <- .sector_asof(as.data.table(arrow::read_parquet(sector_path)),
                           Sys.Date())
  }

  result <- vector("list", length(daily_files))
  names_vec <- character(length(daily_files))

  for (i in seq_along(daily_files)) {
    tk <- gsub("_daily\\.parquet$", "", basename(daily_files[i]))
    dt <- tryCatch({
      d <- as.data.table(arrow::read_parquet(daily_files[i]))
      d[, date := as.Date(date)]
      d[, ticker := tk]
      # Attach sector + industry (industry feeds the herf / ww_index
      # finalization in industry_adjust_cross_section)
      if (!is.null(sec_dt) && tk %in% sec_dt$ticker) {
        d[, sector := sec_dt[ticker == tk, sector][1]]
        ind_val <- if ("industry" %in% names(sec_dt)) {
          sec_dt[ticker == tk, industry][1]
        } else NA_character_
        d[, industry := if (is.na(ind_val)) "Unknown" else ind_val]
      } else {
        d[, sector := "Unknown"]
        d[, industry := "Unknown"]
      }
      d
    }, error = function(e) NULL)

    if (!is.null(dt) && nrow(dt) > 0) {
      result[[i]] <- dt
      names_vec[i] <- tk
    }
  }

  keep <- nzchar(names_vec)
  result <- result[keep]
  names(result) <- names_vec[keep]
  result
}


#' Extract cross-section for a single date from preloaded data
#'
#' @param preloaded Named list of data.tables (from .preload_all_daily).
#' @param target_date Date. The date to extract.
#' @param ind_cols Character vector. Indicator columns to include.
#' @return data.table with one row per ticker, or NULL if empty.
.extract_cross_section <- function(preloaded, target_date, ind_cols) {
  target_date <- as.Date(target_date)
  rows <- vector("list", length(preloaded))
  n_ok <- 0L

  for (i in seq_along(preloaded)) {
    dt <- preloaded[[i]]
    row <- dt[date == target_date]
    if (nrow(row) > 0) {
      n_ok <- n_ok + 1L
      rows[[n_ok]] <- row[1]
    }
  }

  if (n_ok == 0L) return(NULL)
  cs <- rbindlist(rows[seq_len(n_ok)], fill = TRUE)

  # Finalize sector-demeaned indicators (ch_inv_ia): per-ticker daily
  # layers store the firm-level ingredient; every cross-section assembly
  # point must demean before use (same call as compute_cross_section and
  # load_daily_cross_section). Sector is attached by .preload_all_daily.
  if (exists("industry_adjust_cross_section")) {
    industry_adjust_cross_section(cs)
  }

  # Ensure required columns exist
  meta <- c("date", "ticker", "sector")
  present_meta <- intersect(meta, names(cs))
  present_ind <- intersect(ind_cols, names(cs))
  other <- setdiff(names(cs), c(present_meta, present_ind))
  setcolorder(cs, c(present_meta, present_ind))
  cs
}


#' Get all unique sorted trading dates from preloaded data
#'
#' @param preloaded Named list of data.tables.
#' @return Date vector, sorted.
.get_all_dates <- function(preloaded) {
  all_dates <- unique(unlist(lapply(preloaded, function(dt) as.character(dt$date))))
  sort(as.Date(all_dates))
}


# =============================================================================
# SECTION 3: build_sector_medians()
# =============================================================================

#' Compute sector medians for all dates
#'
#' For each trading date and sector, computes the median of each standardizable
#' indicator across all stocks in that sector. Used as input for TS sector
#' z-scores.
#'
#' @param from_date Date or character. Start date.
#' @param to_date Date or character. End date.
#' @param preloaded Named list. Output of .preload_all_daily(). If NULL, loads.
#' @param ts_dir Character. Time series directory.
#' @param sector_path Character. Sector lookup path.
#' @param feat_dir Character. Feature output directory.
#' @return data.table with columns: date, sector, {48 indicator medians}.
build_sector_medians <- function(from_date   = "2010-01-04",
                                 to_date     = Sys.Date(),
                                 preloaded   = NULL,
                                 ts_dir      = "cache/timeseries",
                                 sector_path = "cache/lookups/sector_industry.parquet",
                                 feat_dir    = .FEAT_DIR) {

  t0 <- Sys.time()
  from_date <- as.Date(from_date)
  to_date   <- as.Date(to_date)

  if (!dir.exists(feat_dir)) dir.create(feat_dir, recursive = TRUE)
  med_path <- file.path(feat_dir, "sector_medians.parquet")

  # Load existing and determine new dates
  existing <- NULL
  if (file.exists(med_path)) {
    existing <- as.data.table(arrow::read_parquet(med_path))
    existing[, date := as.Date(date)]
  }

  if (is.null(preloaded)) {
    message("build_sector_medians: preloading daily data...")
    preloaded <- .preload_all_daily(ts_dir, sector_path)
  }

  all_dates <- .get_all_dates(preloaded)
  all_dates <- all_dates[all_dates >= from_date & all_dates <= to_date]

  if (!is.null(existing) && nrow(existing) > 0) {
    done_dates <- unique(existing$date)
    all_dates <- all_dates[!all_dates %in% done_dates]
  }

  if (length(all_dates) == 0) {
    message("build_sector_medians: nothing to do (all dates covered)")
    return(existing)
  }

  message(sprintf("build_sector_medians: computing medians for %d dates...",
                  length(all_dates)))

  ind_cols <- .STANDARDIZABLE_INDICATORS
  results <- vector("list", length(all_dates))
  n_ok <- 0L

  for (i in seq_along(all_dates)) {
    d <- all_dates[i]
    if (i %% 500 == 0 || i == 1) {
      message(sprintf("  [%d/%d] %s", i, length(all_dates), d))
    }

    cs <- .extract_cross_section(preloaded, d, ind_cols)
    if (is.null(cs) || nrow(cs) < 10) next

    # Log-transform size indicators
    for (col in intersect(.LOG_TRANSFORM_INDICATORS, names(cs))) {
      set(cs, j = col, value = .log_safe(cs[[col]]))
    }

    # Compute sector medians
    present_ind <- intersect(ind_cols, names(cs))
    sector_med <- cs[, lapply(.SD, function(v) {
      valid <- v[!is.na(v)]
      if (length(valid) == 0) NA_real_ else median(valid)
    }), by = sector, .SDcols = present_ind]

    sector_med[, date := d]
    n_ok <- n_ok + 1L
    results[[n_ok]] <- sector_med
  }

  if (n_ok == 0) {
    message("build_sector_medians: no valid dates produced")
    return(existing)
  }

  new_dt <- rbindlist(results[seq_len(n_ok)], fill = TRUE)

  # Combine with existing
  if (!is.null(existing) && nrow(existing) > 0) {
    combined <- rbindlist(list(existing, new_dt), fill = TRUE)
  } else {
    combined <- new_dt
  }

  setorder(combined, date, sector)
  arrow::write_parquet(combined, med_path)

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("build_sector_medians: done in %.1f min (%d new dates, %d total rows)",
                  elapsed, n_ok, nrow(combined)))
  combined
}


# =============================================================================
# SECTION 4: build_ts_own()
# =============================================================================

#' Compute TS stock-level z-scores for a single ticker
#'
#' For each of 48 standardizable indicators, computes a 5-year rolling
#' z-score of the ticker's own history.
#'
#' @param ticker Character. Ticker symbol.
#' @param ticker_dt data.table or NULL. Pre-loaded daily data for this ticker.
#'   If NULL, loads from disk.
#' @param ts_dir Character. Time series directory.
#' @param feat_dir Character. Feature output directory.
#' @param force Logical. Rebuild even if file exists.
#' @return data.table with columns: date, {48 indicators}_ts_own.
build_ts_own <- function(ticker,
                         ticker_dt = NULL,
                         ts_dir    = "cache/timeseries",
                         feat_dir  = .FEAT_DIR,
                         force     = FALSE) {

  ts_own_dir <- file.path(feat_dir, "ts_own")
  if (!dir.exists(ts_own_dir)) dir.create(ts_own_dir, recursive = TRUE)

  out_path <- file.path(ts_own_dir, sprintf("%s_ts_own.parquet", ticker))
  if (file.exists(out_path) && !force) {
    return(as.data.table(arrow::read_parquet(out_path)))
  }

  # Load daily data
  if (is.null(ticker_dt)) {
    ticker_dt <- load_ticker_timeseries(ticker, ts_dir = ts_dir)
  }
  if (is.null(ticker_dt) || nrow(ticker_dt) == 0) return(NULL)

  ticker_dt[, date := as.Date(date)]
  setorder(ticker_dt, date)

  ind_cols <- intersect(.STANDARDIZABLE_INDICATORS, names(ticker_dt))
  if (length(ind_cols) == 0) return(NULL)

  # Build result
  result <- data.table(date = ticker_dt$date)

  for (col in ind_cols) {
    vals <- as.numeric(ticker_dt[[col]])

    # Log-transform size indicators
    if (col %in% .LOG_TRANSFORM_INDICATORS) {
      vals <- .log_safe(vals)
    }

    z <- .rolling_zscore(vals)
    ts_col <- paste0(col, "_ts_own")
    set(result, j = ts_col, value = z)
  }

  # Fill missing indicators with NA columns
  for (col in setdiff(.STANDARDIZABLE_INDICATORS, ind_cols)) {
    ts_col <- paste0(col, "_ts_own")
    set(result, j = ts_col, value = NA_real_)
  }

  arrow::write_parquet(result, out_path)
  result
}


# =============================================================================
# SECTION 5: build_ts_sec()
# =============================================================================

#' Compute TS sector-level z-scores
#'
#' Takes sector medians (from build_sector_medians) and computes a 5-year
#' rolling z-score for each indicator within each sector.
#'
#' @param sector_medians data.table. Output of build_sector_medians().
#'   If NULL, loads from disk.
#' @param feat_dir Character. Feature output directory.
#' @return data.table with columns: date, sector, {48 indicators}_ts_sec.
build_ts_sec <- function(sector_medians = NULL,
                         feat_dir       = .FEAT_DIR) {

  t0 <- Sys.time()
  ts_sec_path <- file.path(feat_dir, "ts_sec.parquet")

  if (is.null(sector_medians)) {
    med_path <- file.path(feat_dir, "sector_medians.parquet")
    if (!file.exists(med_path)) {
      stop("build_ts_sec: sector_medians.parquet not found. Run build_sector_medians() first.")
    }
    sector_medians <- as.data.table(arrow::read_parquet(med_path))
    sector_medians[, date := as.Date(date)]
  }

  ind_cols <- intersect(.STANDARDIZABLE_INDICATORS, names(sector_medians))
  sectors <- sort(unique(sector_medians$sector))

  message(sprintf("build_ts_sec: %d sectors, %d indicators, %d date-sector rows",
                  length(sectors), length(ind_cols), nrow(sector_medians)))

  results <- vector("list", length(sectors))

  for (si in seq_along(sectors)) {
    sec <- sectors[si]
    sec_dt <- sector_medians[sector == sec]
    setorder(sec_dt, date)

    sec_result <- data.table(date = sec_dt$date, sector = sec)

    for (col in ind_cols) {
      vals <- as.numeric(sec_dt[[col]])
      z <- .rolling_zscore(vals)
      ts_col <- paste0(col, "_ts_sec")
      set(sec_result, j = ts_col, value = z)
    }

    # Fill missing indicator columns
    for (col in setdiff(.STANDARDIZABLE_INDICATORS, ind_cols)) {
      ts_col <- paste0(col, "_ts_sec")
      set(sec_result, j = ts_col, value = NA_real_)
    }

    results[[si]] <- sec_result
  }

  combined <- rbindlist(results, fill = TRUE)
  setorder(combined, date, sector)
  arrow::write_parquet(combined, ts_sec_path)

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  message(sprintf("build_ts_sec: done in %.1f sec (%d rows)", elapsed, nrow(combined)))
  combined
}


# =============================================================================
# SECTION 6: build_cs_sec()
# =============================================================================

#' Compute CS within-sector z-scores for a single date
#'
#' Groups stocks by sector, applies robust winsorization within each sector,
#' then z-scores. Minimum sector size enforced. Financial-NA indicators
#' set to NA for financial sector stocks.
#'
#' @param target_date Date. The date to compute.
#' @param cross_section_dt data.table. Cross-section for that date (must have
#'   ticker, sector, and indicator columns). If NULL, loads via preloaded data.
#' @param feat_dir Character. Feature output directory.
#' @param save Logical. Write per-date parquet (default TRUE).
#' @return data.table with columns: date, ticker, {48 indicators}_cs_sec.
build_cs_sec <- function(target_date,
                         cross_section_dt,
                         feat_dir = .FEAT_DIR,
                         save     = TRUE) {

  target_date <- as.Date(target_date)

  cs_sec_dir <- file.path(feat_dir, "cs_sec")
  if (!dir.exists(cs_sec_dir)) dir.create(cs_sec_dir, recursive = TRUE)

  dt <- copy(cross_section_dt)
  if (is.null(dt) || nrow(dt) == 0) return(NULL)

  # Ensure required columns
  if (!all(c("ticker", "sector") %in% names(dt))) return(NULL)

  ind_cols <- intersect(.STANDARDIZABLE_INDICATORS, names(dt))
  if (length(ind_cols) == 0) return(NULL)

  # Log-transform size indicators
  for (col in intersect(.LOG_TRANSFORM_INDICATORS, ind_cols)) {
    set(dt, j = col, value = .log_safe(dt[[col]]))
  }

  # Initialize result
  result <- data.table(date = target_date, ticker = dt$ticker)

  for (col in ind_cols) {
    vals <- as.numeric(dt[[col]])
    cs_col <- paste0(col, "_cs_sec")

    # Sector-masked indicators (.SECTOR_NA_INDICATORS: Financial plus
    # the Wave 5 Utilities / Real Estate exclusions): exclude masked
    # rows from computation
    masked <- .sector_na_mask(col, dt$sector)
    if (any(masked)) {
      vals[masked] <- NA_real_
    }

    # Group by sector, z-score within each
    z_out <- rep(NA_real_, nrow(dt))
    sectors <- unique(dt$sector)

    for (sec in sectors) {
      idx <- which(dt$sector == sec)
      sec_vals <- vals[idx]
      valid <- !is.na(sec_vals)

      if (sum(valid) < .MIN_SECTOR_SIZE) next

      # Robust winsorize within sector
      winsorized <- sec_vals
      winsorized[valid] <- .robust_winsorize(sec_vals[valid])

      mu <- mean(winsorized[valid])
      s <- sd(winsorized[valid])
      if (is.na(s) || s < 1e-12) next

      z <- (winsorized - mu) / s
      z <- pmin(pmax(z, -.ZSCORE_CLIP), .ZSCORE_CLIP)
      z[!valid] <- NA_real_
      z_out[idx] <- z
    }

    set(result, j = cs_col, value = z_out)
  }

  # Fill missing indicator columns
  for (col in setdiff(.STANDARDIZABLE_INDICATORS, ind_cols)) {
    cs_col <- paste0(col, "_cs_sec")
    set(result, j = cs_col, value = NA_real_)
  }

  if (save) {
    out_path <- file.path(cs_sec_dir, sprintf("%s.parquet", target_date))
    arrow::write_parquet(result, out_path)
  }

  result
}


# =============================================================================
# SECTION 7: assemble_ticker_features()
# =============================================================================

#' Merge all three dimensions for a single ticker
#'
#' Joins TS own, TS sector, and CS within-sector z-scores by date.
#' Adds passthrough Piotroski binary flags from the daily data.
#'
#' @param ticker Character. Ticker symbol.
#' @param ticker_dt data.table or NULL. Pre-loaded daily data.
#' @param ts_own_dt data.table or NULL. Pre-loaded TS own z-scores.
#' @param ts_sec_dt data.table. Full TS sector z-scores (all sectors).
#' @param cs_sec_list Named list of data.tables keyed by date string,
#'   or NULL to load from disk.
#' @param sector Character. Ticker's sector.
#' @param ts_dir Character. Time series directory.
#' @param feat_dir Character. Feature output directory.
#' @return data.table with 3 meta + all get_feature_names() columns
#'   (currently 162), or NULL on failure.
assemble_ticker_features <- function(ticker,
                                     ticker_dt   = NULL,
                                     ts_own_dt   = NULL,
                                     ts_sec_dt   = NULL,
                                     cs_sec_list = NULL,
                                     sector      = "Unknown",
                                     ts_dir      = "cache/timeseries",
                                     feat_dir    = .FEAT_DIR) {

  daily_dir <- file.path(feat_dir, "daily")
  if (!dir.exists(daily_dir)) dir.create(daily_dir, recursive = TRUE)

  # Local copies for data.table scoping (avoids ..ticker/..sector issues)
  .target_ticker <- ticker
  .target_sector <- sector

  # Load daily data for passthrough indicators
  if (is.null(ticker_dt)) {
    ticker_dt <- load_ticker_timeseries(ticker, ts_dir = ts_dir)
  }
  if (is.null(ticker_dt) || nrow(ticker_dt) == 0) return(NULL)
  ticker_dt[, date := as.Date(date)]

  # Load TS own z-scores
  if (is.null(ts_own_dt)) {
    ts_own_path <- file.path(feat_dir, "ts_own",
                             sprintf("%s_ts_own.parquet", ticker))
    if (!file.exists(ts_own_path)) return(NULL)
    ts_own_dt <- as.data.table(arrow::read_parquet(ts_own_path))
  }
  ts_own_dt[, date := as.Date(date)]

  # Start with dates from TS own (defines the date range)
  result <- data.table(date = ts_own_dt$date,
                       ticker = ticker,
                       sector = sector)

  # Add TS own columns
  ts_own_cols <- grep("_ts_own$", names(ts_own_dt), value = TRUE)
  for (col in ts_own_cols) {
    set(result, j = col, value = ts_own_dt[[col]])
  }

  # Ensure all TS own columns exist (stable output shape even if the parquet
  # is missing some indicators)
  for (col in .STANDARDIZABLE_INDICATORS) {
    ts_own_col <- paste0(col, "_ts_own")
    if (!ts_own_col %in% names(result)) {
      set(result, j = ts_own_col, value = NA_real_)
    }
  }

  # Add TS sector columns (join by date + sector)
  if (!is.null(ts_sec_dt)) {
    ts_sec_dt[, date := as.Date(date)]
    sec_subset <- ts_sec_dt[sector == .target_sector]
    if (nrow(sec_subset) > 0) {
      ts_sec_cols <- grep("_ts_sec$", names(sec_subset), value = TRUE)
      result <- merge(result, sec_subset[, c("date", ts_sec_cols), with = FALSE],
                      by = "date", all.x = TRUE)
    }
  }

  # Ensure all TS sector columns exist
  for (col in .STANDARDIZABLE_INDICATORS) {
    ts_sec_col <- paste0(col, "_ts_sec")
    if (!ts_sec_col %in% names(result)) {
      set(result, j = ts_sec_col, value = NA_real_)
    }
  }

  # Add CS within-sector columns
  if (!is.null(cs_sec_list)) {
    # From in-memory list (historical build)
    cs_rows <- vector("list", nrow(result))
    n_cs <- 0L

    for (i in seq_len(nrow(result))) {
      d_str <- as.character(result$date[i])
      if (d_str %in% names(cs_sec_list)) {
        cs_dt <- cs_sec_list[[d_str]]
        row <- cs_dt[ticker == .target_ticker]
        if (nrow(row) > 0) {
          n_cs <- n_cs + 1L
          cs_rows[[n_cs]] <- data.table(idx = i, row[1])
        }
      }
    }

    if (n_cs > 0) {
      cs_combined <- rbindlist(cs_rows[seq_len(n_cs)], fill = TRUE)
      cs_cols <- grep("_cs_sec$", names(cs_combined), value = TRUE)
      for (col in cs_cols) {
        vals <- rep(NA_real_, nrow(result))
        vals[cs_combined$idx] <- cs_combined[[col]]
        set(result, j = col, value = vals)
      }
    }
  } else {
    # From disk (daily update)
    cs_sec_dir <- file.path(feat_dir, "cs_sec")
    cs_cols_needed <- paste0(.STANDARDIZABLE_INDICATORS, "_cs_sec")

    for (col in cs_cols_needed) {
      set(result, j = col, value = NA_real_)
    }

    for (i in seq_len(nrow(result))) {
      d <- result$date[i]
      cs_path <- file.path(cs_sec_dir, sprintf("%s.parquet", d))
      if (!file.exists(cs_path)) next

      cs_dt <- tryCatch(
        as.data.table(arrow::read_parquet(cs_path)),
        error = function(e) NULL
      )
      if (is.null(cs_dt)) next

      row <- cs_dt[ticker == .target_ticker]
      if (nrow(row) == 0) next

      cs_cols <- intersect(cs_cols_needed, names(row))
      for (col in cs_cols) {
        set(result, i = as.integer(i), j = col, value = row[[col]][1])
      }
    }
  }

  # Ensure all CS columns exist
  for (col in .STANDARDIZABLE_INDICATORS) {
    cs_col <- paste0(col, "_cs_sec")
    if (!cs_col %in% names(result)) {
      set(result, j = cs_col, value = NA_real_)
    }
  }

  # Add passthrough Piotroski binary flags
  pass_present <- intersect(.PASSTHROUGH_INDICATORS, names(ticker_dt))
  if (length(pass_present) > 0) {
    pass_dt <- ticker_dt[, c("date", pass_present), with = FALSE]
    result <- merge(result, pass_dt, by = "date", all.x = TRUE)
  }
  for (col in setdiff(.PASSTHROUGH_INDICATORS, names(result))) {
    set(result, j = col, value = NA_integer_)
  }

  setorder(result, date)

  .assert_output_feat(result, "assemble_ticker_features", list(
    "is data.table"      = is.data.table,
    "has date col"       = function(x) "date" %in% names(x),
    "has ticker col"     = function(x) "ticker" %in% names(x),
    "all feature cols"   = function(x) all(get_feature_names() %in% names(x)),
    "nrow > 0"           = function(x) nrow(x) > 0
  ))

  # Save
  out_path <- file.path(daily_dir, sprintf("%s_features.parquet", ticker))
  arrow::write_parquet(result, out_path)

  result
}


# =============================================================================
# SECTION 8: PUBLIC API
# =============================================================================

#' Get all feature column names
#'
#' @return Character vector of feature names: 3 dimensions x
#'   length(.STANDARDIZABLE_INDICATORS) z-scored columns plus the 9 Piotroski
#'   passthrough flags (currently 50 * 3 + 9 = 159).
get_feature_names <- function() {
  cs_names <- paste0(.STANDARDIZABLE_INDICATORS, "_cs_sec")
  ts_own_names <- paste0(.STANDARDIZABLE_INDICATORS, "_ts_own")
  ts_sec_names <- paste0(.STANDARDIZABLE_INDICATORS, "_ts_sec")
  c(cs_names, ts_own_names, ts_sec_names, .PASSTHROUGH_INDICATORS)
}


#' Build all features: historical build orchestrator
#'
#' Four-pass process:
#'   Pass 1: Sector medians + CS within-sector z-scores (date-oriented)
#'   Pass 2: TS stock-level z-scores (ticker-oriented)
#'   Pass 3: TS sector-level z-scores
#'   Pass 4: Assembly (merge all dimensions per ticker)
#'
#' @param from_date Date or character. Start date (default "2010-01-04").
#' @param to_date Date or character. End date (default today).
#' @param ts_dir Character. Time series directory.
#' @param sector_path Character. Sector lookup path.
#' @param feat_dir Character. Feature output directory.
#' @param tickers Character vector or NULL. Specific tickers (NULL = all).
#' @return Invisible list of stats.
build_all_features <- function(from_date   = "2010-01-04",
                               to_date     = Sys.Date(),
                               ts_dir      = "cache/timeseries",
                               sector_path = "cache/lookups/sector_industry.parquet",
                               feat_dir    = .FEAT_DIR,
                               tickers     = NULL) {

  t0 <- Sys.time()
  from_date <- as.Date(from_date)
  to_date   <- as.Date(to_date)

  message(paste(rep("=", 60), collapse = ""))
  message("build_all_features: starting historical build")
  message(sprintf("  range: %s to %s", from_date, to_date))

  # Create directories
  for (d in c(feat_dir, file.path(feat_dir, c("ts_own", "cs_sec", "daily")))) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }

  # ---- PRELOAD ----
  message("\n[1/4] Preloading daily data into memory...")
  preloaded <- .preload_all_daily(ts_dir, sector_path)
  n_loaded <- length(preloaded)
  message(sprintf("  loaded %d tickers", n_loaded))
  if (n_loaded == 0) stop("build_all_features: no daily data found")

  # Determine tickers
  if (is.null(tickers)) {
    tickers <- names(preloaded)
  } else {
    tickers <- intersect(tickers, names(preloaded))
  }
  message(sprintf("  processing %d tickers", length(tickers)))

  # Get sector mapping
  sec_dt <- NULL
  if (file.exists(sector_path)) {
    # current view of the dated sector table (SCD, D2/B)
    sec_dt <- .sector_asof(as.data.table(arrow::read_parquet(sector_path)),
                           Sys.Date())
  }
  get_sector <- function(tk) {
    if (!is.null(sec_dt) && tk %in% sec_dt$ticker) {
      sec_dt[ticker == tk, sector][1]
    } else {
      "Unknown"
    }
  }

  # ---- PASS 1: Sector medians + CS within-sector ----
  message("\n[2/4] Pass 1: Sector medians + CS within-sector z-scores...")
  ind_cols <- .STANDARDIZABLE_INDICATORS

  all_dates <- .get_all_dates(preloaded)
  all_dates <- all_dates[all_dates >= from_date & all_dates <= to_date]
  message(sprintf("  %d trading dates", length(all_dates)))

  med_results <- vector("list", length(all_dates))
  cs_sec_list <- vector("list", length(all_dates))
  names(cs_sec_list) <- as.character(all_dates)
  n_med <- 0L

  for (i in seq_along(all_dates)) {
    d <- all_dates[i]
    if (i %% 500 == 0 || i == 1 || i == length(all_dates)) {
      message(sprintf("  [%d/%d] %s", i, length(all_dates), d))
    }

    cs <- .extract_cross_section(preloaded, d, ind_cols)
    if (is.null(cs) || nrow(cs) < 10) next

    # Compute CS within-sector z-scores
    cs_result <- tryCatch(
      build_cs_sec(d, cs, feat_dir = feat_dir, save = TRUE),
      error = function(e) NULL
    )
    if (!is.null(cs_result)) {
      cs_sec_list[[as.character(d)]] <- cs_result
    }

    # Compute sector medians (on log-transformed size indicators)
    cs_copy <- copy(cs)
    for (col in intersect(.LOG_TRANSFORM_INDICATORS, names(cs_copy))) {
      set(cs_copy, j = col, value = .log_safe(cs_copy[[col]]))
    }

    present_ind <- intersect(ind_cols, names(cs_copy))
    sector_med <- cs_copy[, lapply(.SD, function(v) {
      valid <- v[!is.na(v)]
      if (length(valid) == 0) NA_real_ else median(valid)
    }), by = sector, .SDcols = present_ind]
    sector_med[, date := d]

    n_med <- n_med + 1L
    med_results[[n_med]] <- sector_med
  }

  # Save sector medians
  if (n_med > 0) {
    med_dt <- rbindlist(med_results[seq_len(n_med)], fill = TRUE)
    setorder(med_dt, date, sector)
    arrow::write_parquet(med_dt, file.path(feat_dir, "sector_medians.parquet"))
    message(sprintf("  sector medians: %d rows", nrow(med_dt)))
  } else {
    stop("build_all_features: no sector medians computed")
  }

  # ---- PASS 2: TS stock-level z-scores ----
  message("\n[3/4] Pass 2: TS stock-level z-scores...")
  n_ts_own <- 0L; n_ts_fail <- 0L

  for (i in seq_along(tickers)) {
    tk <- tickers[i]
    if (i %% 100 == 0 || i == 1 || i == length(tickers)) {
      message(sprintf("  [%d/%d] %s", i, length(tickers), tk))
    }

    result <- tryCatch(
      build_ts_own(tk, ticker_dt = preloaded[[tk]], feat_dir = feat_dir,
                   force = TRUE),
      error = function(e) {
        warning(sprintf("  ts_own failed: %s -- %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )

    if (!is.null(result)) {
      n_ts_own <- n_ts_own + 1L
    } else {
      n_ts_fail <- n_ts_fail + 1L
    }
  }
  message(sprintf("  TS own: %d ok, %d failed", n_ts_own, n_ts_fail))

  # ---- PASS 3: TS sector-level z-scores ----
  message("\n[3.5/4] Pass 3: TS sector-level z-scores...")
  ts_sec_dt <- tryCatch(
    build_ts_sec(sector_medians = med_dt, feat_dir = feat_dir),
    error = function(e) {
      warning(sprintf("build_ts_sec failed: %s", e$message), call. = FALSE)
      NULL
    }
  )

  # ---- PASS 4: Assembly ----
  message("\n[4/4] Pass 4: Assembling per-ticker features...")
  n_assembled <- 0L; n_asm_fail <- 0L

  for (i in seq_along(tickers)) {
    tk <- tickers[i]
    if (i %% 100 == 0 || i == 1 || i == length(tickers)) {
      message(sprintf("  [%d/%d] %s", i, length(tickers), tk))
    }

    result <- tryCatch(
      assemble_ticker_features(
        ticker      = tk,
        ticker_dt   = preloaded[[tk]],
        ts_own_dt   = NULL,  # loaded from disk
        ts_sec_dt   = ts_sec_dt,
        cs_sec_list = cs_sec_list,
        sector      = get_sector(tk),
        ts_dir      = ts_dir,
        feat_dir    = feat_dir
      ),
      error = function(e) {
        warning(sprintf("  assembly failed: %s -- %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )

    if (!is.null(result)) {
      n_assembled <- n_assembled + 1L
    } else {
      n_asm_fail <- n_asm_fail + 1L
    }
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf(paste0(
    "\nbuild_all_features: done in %.1f min\n",
    "  sector medians: %d dates\n",
    "  TS own: %d ok, %d failed\n",
    "  assembled: %d ok, %d failed"),
    elapsed, n_med, n_ts_own, n_ts_fail, n_assembled, n_asm_fail))

  invisible(list(
    n_tickers    = length(tickers),
    n_dates      = length(all_dates),
    n_ts_own     = n_ts_own,
    n_assembled  = n_assembled,
    n_fail       = n_ts_fail + n_asm_fail,
    elapsed_min  = elapsed
  ))
}


#' Update features: daily incremental
#'
#' For a single new date, updates sector medians, all three z-score dimensions,
#' and assembles updated features for each ticker.
#'
#' @param through_date Date or character. Update through this date (default today).
#' @param ts_dir Character. Time series directory.
#' @param sector_path Character. Sector lookup path.
#' @param feat_dir Character. Feature output directory.
#' @return Invisible list of stats.
update_features <- function(through_date = Sys.Date(),
                            ts_dir       = "cache/timeseries",
                            sector_path  = "cache/lookups/sector_industry.parquet",
                            feat_dir     = .FEAT_DIR) {

  t0 <- Sys.time()
  through_date <- as.Date(through_date)
  message(sprintf("update_features: updating through %s", through_date))

  # Determine last processed date
  med_path <- file.path(feat_dir, "sector_medians.parquet")
  last_date <- as.Date("2010-01-03")
  if (file.exists(med_path)) {
    med_dt <- as.data.table(arrow::read_parquet(med_path))
    med_dt[, date := as.Date(date)]
    last_date <- max(med_dt$date)
  }

  if (last_date >= through_date) {
    message("update_features: already up to date")
    return(invisible(NULL))
  }

  # Load cross-section for each new date
  ind_cols <- .STANDARDIZABLE_INDICATORS
  new_dates <- seq(last_date + 1L, through_date, by = "day")

  # Load sector lookup
  sec_dt <- NULL
  if (file.exists(sector_path)) {
    # current view of the dated sector table (SCD, D2/B)
    sec_dt <- .sector_asof(as.data.table(arrow::read_parquet(sector_path)),
                           Sys.Date())
  }

  n_updated <- 0L

  for (d in as.character(new_dates)) {
    d <- as.Date(d)

    # Load cross-section for this date
    cs <- tryCatch(
      load_daily_cross_section(d, ts_dir = "cache/timeseries",
                               sector_path = sector_path, zscore = FALSE),
      error = function(e) NULL
    )

    # Handle load_daily_cross_section returning a list with $raw
    if (is.list(cs) && "raw" %in% names(cs)) cs <- cs$raw
    if (is.null(cs) || nrow(cs) < 10) next

    # CS within-sector z-scores for this date
    build_cs_sec(d, cs, feat_dir = feat_dir, save = TRUE)

    # Sector medians for this date
    cs_copy <- copy(cs)
    for (col in intersect(.LOG_TRANSFORM_INDICATORS, names(cs_copy))) {
      set(cs_copy, j = col, value = .log_safe(cs_copy[[col]]))
    }
    present_ind <- intersect(ind_cols, names(cs_copy))
    sector_med <- cs_copy[, lapply(.SD, function(v) {
      valid <- v[!is.na(v)]
      if (length(valid) == 0) NA_real_ else median(valid)
    }), by = sector, .SDcols = present_ind]
    sector_med[, date := d]

    # Append to sector medians
    if (file.exists(med_path)) {
      existing_med <- as.data.table(arrow::read_parquet(med_path))
      combined <- rbindlist(list(existing_med, sector_med), fill = TRUE)
    } else {
      combined <- sector_med
    }
    arrow::write_parquet(combined, med_path)

    n_updated <- n_updated + 1L
  }

  if (n_updated == 0) {
    message("update_features: no trading days in range")
    return(invisible(NULL))
  }

  # Rebuild TS sector z-scores (needs full history)
  build_ts_sec(feat_dir = feat_dir)

  # Update TS own and assemble for each ticker
  ts_tickers <- list_timeseries_tickers()
  ts_sec_dt <- tryCatch(
    as.data.table(arrow::read_parquet(file.path(feat_dir, "ts_sec.parquet"))),
    error = function(e) NULL
  )

  n_tk_ok <- 0L
  for (tk in ts_tickers) {
    tryCatch({
      build_ts_own(tk, feat_dir = feat_dir, force = TRUE)

      sec <- if (!is.null(sec_dt) && tk %in% sec_dt$ticker) {
        sec_dt[ticker == tk, sector][1]
      } else {
        "Unknown"
      }

      assemble_ticker_features(
        ticker = tk, ts_sec_dt = ts_sec_dt,
        sector = sec, ts_dir = ts_dir, feat_dir = feat_dir
      )
      n_tk_ok <- n_tk_ok + 1L
    }, error = function(e) {
      warning(sprintf("update_features: %s failed -- %s", tk, e$message),
              call. = FALSE)
    })
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("update_features: done in %.1f min (%d new dates, %d tickers updated)",
                  elapsed, n_updated, n_tk_ok))
  invisible(list(n_dates = n_updated, n_tickers = n_tk_ok, elapsed_min = elapsed))
}


#' Load cross-sectional feature matrix for a specific date
#'
#' Reads individual ticker feature files and assembles a cross-section.
#'
#' @param target_date Date or character.
#' @param feat_dir Character. Feature directory.
#' @return data.table with all tickers and 155 columns, or NULL.
load_feature_cross_section <- function(target_date,
                                       feat_dir = .FEAT_DIR) {
  target_date <- as.Date(target_date)
  daily_dir <- file.path(feat_dir, "daily")

  if (!dir.exists(daily_dir)) return(NULL)

  feat_files <- list.files(daily_dir, pattern = "_features\\.parquet$",
                           full.names = TRUE)
  if (length(feat_files) == 0) return(NULL)

  rows <- vector("list", length(feat_files))
  n_ok <- 0L

  for (f in feat_files) {
    dt <- tryCatch({
      d <- as.data.table(arrow::read_parquet(f))
      d[, date := as.Date(date)]
      d[date == target_date]
    }, error = function(e) NULL)

    if (!is.null(dt) && nrow(dt) > 0) {
      n_ok <- n_ok + 1L
      rows[[n_ok]] <- dt[1]
    }
  }

  if (n_ok == 0) return(NULL)
  cs <- rbindlist(rows[seq_len(n_ok)], fill = TRUE)
  setorder(cs, ticker)

  .assert_output_feat(cs, "load_feature_cross_section", list(
    "is data.table"  = is.data.table,
    "has date col"   = function(x) "date" %in% names(x),
    "has ticker col" = function(x) "ticker" %in% names(x),
    "nrow > 0"       = function(x) nrow(x) > 0
  ))
  cs
}


#' Load feature time series for a single ticker
#'
#' @param ticker Character.
#' @param from Date or character. Start date filter (optional).
#' @param to Date or character. End date filter (optional).
#' @param feat_dir Character. Feature directory.
#' @return data.table, or NULL if not found.
load_ticker_features <- function(ticker, from = NULL, to = NULL,
                                 feat_dir = .FEAT_DIR) {
  path <- file.path(feat_dir, "daily", sprintf("%s_features.parquet", ticker))
  if (!file.exists(path)) return(NULL)

  dt <- tryCatch(
    as.data.table(arrow::read_parquet(path)),
    error = function(e) NULL
  )
  if (is.null(dt) || nrow(dt) == 0) return(NULL)

  dt[, date := as.Date(date)]
  if (!is.null(from)) dt <- dt[date >= as.Date(from)]
  if (!is.null(to))   dt <- dt[date <= as.Date(to)]

  .assert_output_feat(dt, "load_ticker_features", list(
    "is data.table"  = is.data.table,
    "has date col"   = function(x) "date" %in% names(x),
    "has ticker col" = function(x) "ticker" %in% names(x)
  ))
  dt
}
