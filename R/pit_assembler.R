# ============================================================================
# pit_assembler.R  --  Point-in-Time Cross-Sectional Assembler (Module 5)
# ============================================================================
# Assembles point-in-time fundamental snapshots for the S&P 500 universe.
# For any snapshot date d: determines universe membership, retrieves most
# recent annual filing per ticker (filed <= d), fetches closing price on
# filing date, computes all indicators, and produces cross-sectional matrices.
#
# Output (per snapshot date, two parquet files):
#   cache/snapshots/pit_{YYYY-MM-DD}_raw.parquet
#   cache/snapshots/pit_{YYYY-MM-DD}_zscore.parquet
#
# Output schema (both files):
#   date       Date    Snapshot date
#   ticker     chr     Ticker symbol
#   industry   chr     Finviz industry
#   sector     chr     Finviz sector
#   [57 indicator columns]  num  Raw or z-scored values
#
# Public API:
#   get_universe_at_date(snapshot_date, master_dt)
#   fetch_ticker_prices(ticker, from, cache_dir, retries)
#   get_price_on_date(price_xts, target_date)
#   get_latest_annual_filing(fund_dt, as_of_date)
#   assemble_snapshot(snapshot_date, ...)
#   build_historical_snapshots(start_date, end_date, ...)
#
# Dependencies: data.table, arrow, quantmod, xts, zoo
# Requires: constituent_master.R, sector_classifier.R,
#           fundamental_fetcher.R, indicator_compute.R (already sourced)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(quantmod)
  library(xts)
  library(zoo)
})


# =============================================================================
# CONSTANTS
# =============================================================================

.PRICE_CACHE_DIR <- "cache/prices"
.SNAPSHOT_DIR     <- "cache/snapshots"


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


# =============================================================================
# 1. get_universe_at_date()
# =============================================================================
#' Determine which tickers were in the S&P 500 on a given date
#'
#' A ticker is in the universe on date d if:
#'   date_added <= d (or date_added is NA, meaning original member)
#'   AND date_removed > d (or date_removed is NA, meaning still in index)
#'
#' @param snapshot_date Date or character coercible to Date.
#' @param master_dt data.table. Constituent master (from constituent_master.parquet).
#' @return data.table subset of master_dt for tickers in the universe at that date.
get_universe_at_date <- function(snapshot_date, master_dt) {

  d <- as.Date(snapshot_date)

  universe <- master_dt[
    (is.na(date_added) | as.Date(date_added) <= d) &
    (is.na(date_removed) | as.Date(date_removed) > d)
  ]

  .assert_output(universe, "get_universe_at_date", list(
    "is data.table"    = is.data.table,
    "has ticker col"   = function(x) "ticker" %in% names(x),
    "has cik col"      = function(x) "cik" %in% names(x)
  ))

  universe
}


# =============================================================================
# 2. fetch_ticker_prices()
# =============================================================================
#' Download OHLCV price data for a single ticker with parquet cache
#'
#' Adapted from download_ohlcv() in ref/data_loader.R.
#' Returns xts object. Caches to cache/prices/{ticker}_yahoo_{from}.parquet.
#'
#' @param ticker Character. Ticker symbol.
#' @param from Character. Start date (default "2009-01-01").
#' @param cache_dir Character. Cache directory.
#' @param retries Integer. Number of download attempts.
#' @return xts OHLCV object, or NULL on failure.
fetch_ticker_prices <- function(ticker, from = "2009-01-01",
                                cache_dir = .PRICE_CACHE_DIR,
                                retries = 3L) {

  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  cache_file <- file.path(cache_dir,
                          sprintf("%s_yahoo_%s.parquet", ticker, from))

  # Try cache first
  if (file.exists(cache_file)) {
    cached <- tryCatch({
      df <- as.data.frame(arrow::read_parquet(cache_file))
      xts(df[, -1, drop = FALSE], order.by = as.Date(df$date))
    }, error = function(e) NULL)
    if (!is.null(cached) && nrow(cached) > 0) return(cached)
  }

  # Download from Yahoo
  px <- NULL
  for (attempt in seq_len(retries)) {
    px <- tryCatch({
      p <- getSymbols(ticker, src = "yahoo", from = from,
                      to = Sys.Date(), auto.assign = FALSE)
      if (is.null(p) || nrow(p) == 0) stop("empty result")
      p
    }, error = function(e) {
      if (attempt < retries) Sys.sleep(2^attempt)
      NULL
    })
    if (!is.null(px)) break
  }

  if (is.null(px)) return(NULL)

  # Cache write
  df <- data.frame(date = as.character(index(px)), coredata(px),
                   check.names = FALSE)
  tryCatch(arrow::write_parquet(df, cache_file), error = function(e) NULL)

  px
}


# =============================================================================
# 3. get_price_on_date()
# =============================================================================
#' Look up adjusted closing price on a specific date
#'
#' If the target date is not a trading day, uses the most recent prior
#' trading day (up to 10 calendar days back).
#'
#' @param price_xts xts. OHLCV price data for one ticker.
#' @param target_date Date or character. The date to look up.
#' @return Numeric scalar (adjusted close), or NA_real_ if unavailable.
get_price_on_date <- function(price_xts, target_date) {

  if (is.null(price_xts) || nrow(price_xts) == 0) return(NA_real_)

  d <- as.Date(target_date)
  dates <- index(price_xts)
  valid <- dates[dates <= d]
  if (length(valid) == 0) return(NA_real_)

  closest <- max(valid)
  # Don't reach back more than 10 calendar days
  if (as.numeric(d - closest) > 10) return(NA_real_)

  row <- coredata(price_xts[closest, ])
  nc <- ncol(price_xts)

  # Adjusted close is column 6, regular close is column 4
  if (nc >= 6) {
    as.numeric(row[1, 6])
  } else if (nc >= 4) {
    as.numeric(row[1, 4])
  } else {
    NA_real_
  }
}


# =============================================================================
# 4. get_latest_annual_filing()
# =============================================================================
#' Find the most recent annual filing available as of a given date
#'
#' Scans long-format fundamentals for FY rows where filed <= as_of_date.
#' Returns the fiscal year and the latest filed date for that year.
#'
#' @param fund_dt data.table. Long-format fundamentals for one ticker.
#' @param as_of_date Date or character. Point-in-time cutoff.
#' @return List with $fiscal_year (integer) and $filed_date (Date),
#'   or NULL if no annual filing is available.
get_latest_annual_filing <- function(fund_dt, as_of_date,
                                     min_concepts = 5L) {

  if (is.null(fund_dt) || nrow(fund_dt) == 0) return(NULL)

  d <- as.Date(as_of_date)

  # Point-in-time: only data that was filed on or before the snapshot date
  available <- fund_dt[!is.na(filed) & as.Date(filed) <= d]
  if (nrow(available) == 0) return(NULL)

  # Annual filings only
  fy_rows <- available[!is.na(period_type) & period_type == "FY"]
  if (nrow(fy_rows) == 0) return(NULL)

  # Walk backwards from most recent FY until we find one with sufficient data.
  # Non-December fiscal year companies can have sparse FY entries where only
  # one or two concepts are classified as that fiscal year.
  fy_candidates <- sort(unique(fy_rows$fiscal_year), decreasing = TRUE)

  for (fy in fy_candidates) {
    fy_data <- fy_rows[fiscal_year == fy]
    n_concepts <- length(unique(fy_data$concept))

    if (n_concepts >= min_concepts) {
      filed_date <- max(fy_data$filed, na.rm = TRUE)
      return(list(fiscal_year = as.integer(fy),
                  filed_date  = as.Date(filed_date)))
    }
  }

  # No fiscal year with sufficient data
  NULL
}


# =============================================================================
# 5. .generate_snapshot_dates()  --  Quarterly end dates
# =============================================================================
.generate_snapshot_dates <- function(start_date, end_date) {

  start <- as.Date(start_date)
  end   <- as.Date(end_date)

  start_year <- as.integer(format(start, "%Y"))
  end_year   <- as.integer(format(end, "%Y"))

  qe_months <- c(3L, 6L, 9L, 12L)
  qe_days   <- c(31L, 30L, 30L, 31L)

  dates <- as.Date(character(0))
  for (y in start_year:end_year) {
    for (q in seq_along(qe_months)) {
      d <- as.Date(sprintf("%d-%02d-%02d", y, qe_months[q], qe_days[q]))
      if (d >= start && d <= end) {
        dates <- c(dates, d)
      }
    }
  }

  sort(dates)
}


# =============================================================================
# 6. .prefetch_universe_prices()  --  Batch price download
# =============================================================================
#' Download and cache prices for all tickers in a universe
#'
#' Only fetches tickers not already cached. Logs progress.
#'
#' @param tickers Character vector.
#' @param cache_dir Character. Price cache directory.
#' @return Invisible NULL. Side effect: populates cache.
.prefetch_universe_prices <- function(tickers, from = "2009-01-01",
                                      cache_dir = .PRICE_CACHE_DIR) {

  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  # Check which tickers need fetching
  need_fetch <- character(0)
  for (tk in tickers) {
    cache_file <- file.path(cache_dir,
                            sprintf("%s_yahoo_%s.parquet", tk, from))
    if (!file.exists(cache_file)) {
      need_fetch <- c(need_fetch, tk)
    }
  }

  if (length(need_fetch) == 0) {
    message("  prices: all cached")
    return(invisible(NULL))
  }

  message(sprintf("  prices: fetching %d tickers (of %d total)...",
                  length(need_fetch), length(tickers)))

  n_ok <- 0L
  n_fail <- 0L

  for (i in seq_along(need_fetch)) {
    tk <- need_fetch[i]
    if (i %% 50 == 0 || i == 1) {
      message(sprintf("    price fetch %d/%d: %s", i, length(need_fetch), tk))
    }

    px <- tryCatch(
      fetch_ticker_prices(tk, cache_dir = cache_dir),
      error = function(e) NULL
    )

    if (!is.null(px)) {
      n_ok <- n_ok + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  message(sprintf("  prices: %d fetched, %d failed", n_ok, n_fail))
  invisible(NULL)
}


# =============================================================================
# 7. assemble_snapshot()  --  Core assembler
# =============================================================================
#' Assemble a point-in-time cross-sectional snapshot for a single date
#'
#' For each ticker in the S&P 500 on the snapshot date:
#'   1. Loads cached fundamentals
#'   2. Filters to filings known as of snapshot date (point-in-time)
#'   3. Identifies the most recent annual filing
#'   4. Gets the closing price on the filing date
#'   5. Computes all 57 indicators
#'   6. Stacks into cross-section and z-scores
#'
#' @param snapshot_date Date or character. The snapshot date.
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @param prefetch_prices Logical. Pre-download prices for full universe.
#' @param .master_dt data.table or NULL. Pre-loaded master (avoids re-reading).
#' @param .sector_dt data.table or NULL. Pre-loaded sectors (avoids re-reading).
#' @return List with $raw, $zscored (data.tables), $stats, or NULL on failure.
assemble_snapshot <- function(snapshot_date,
                              master_path     = "cache/lookups/constituent_master.parquet",
                              sector_path     = "cache/lookups/sector_industry.parquet",
                              fund_dir        = "cache/fundamentals",
                              price_cache_dir = "cache/prices",
                              output_dir      = "cache/snapshots",
                              prefetch_prices = TRUE,
                              .master_dt      = NULL,
                              .sector_dt      = NULL) {

  snapshot_date <- as.Date(snapshot_date)
  message(sprintf("assemble_snapshot: %s", snapshot_date))

  # -- Load lookups (use pre-loaded if provided) --
  if (!is.null(.master_dt)) {
    master <- .master_dt
  } else {
    if (!file.exists(master_path)) {
      stop("assemble_snapshot: constituent_master.parquet not found")
    }
    master <- as.data.table(arrow::read_parquet(master_path))
  }

  if (!is.null(.sector_dt)) {
    sectors <- .sector_dt
  } else {
    if (!file.exists(sector_path)) {
      stop("assemble_snapshot: sector_industry.parquet not found")
    }
    sectors <- as.data.table(arrow::read_parquet(sector_path))
  }

  # -- Universe at snapshot date --
  universe <- get_universe_at_date(snapshot_date, master)
  message(sprintf("  universe: %d tickers", nrow(universe)))

  # Merge sector/industry
  universe <- merge(universe, sectors[, .(ticker, sector, industry)],
                    by = "ticker", all.x = TRUE)

  # -- Prefetch prices --
  if (prefetch_prices) {
    .prefetch_universe_prices(universe$ticker, cache_dir = price_cache_dir)
  }

  # -- Process each ticker --
  n_total <- nrow(universe)

  # Pre-allocate collection lists (avoid O(n^2) vector growth)
  indicator_list   <- vector("list", n_total)
  ticker_buf       <- vector("character", n_total)
  sector_buf       <- vector("character", n_total)
  industry_buf     <- vector("character", n_total)
  n_success  <- 0L
  n_no_fund  <- 0L
  n_no_filing <- 0L
  n_no_price <- 0L

  for (i in seq_len(n_total)) {
    tk  <- universe$ticker[i]
    cik <- universe$cik[i]
    sec <- universe$sector[i]
    ind <- universe$industry[i]

    if (i %% 100 == 0 || i == 1) {
      message(sprintf("  computing %d/%d: %s", i, n_total, tk))
    }

    # Load fundamentals
    fund_dt <- tryCatch(
      get_fundamentals(tk, cik, cache_dir = fund_dir),
      error = function(e) NULL
    )
    if (is.null(fund_dt) || nrow(fund_dt) == 0) {
      n_no_fund <- n_no_fund + 1L
      next
    }

    # Point-in-time filter: only data filed on or before snapshot date
    fund_dt <- fund_dt[!is.na(filed) & as.Date(filed) <= snapshot_date]
    if (nrow(fund_dt) == 0) {
      n_no_filing <- n_no_filing + 1L
      next
    }

    # Find latest annual filing
    filing <- get_latest_annual_filing(fund_dt, snapshot_date)
    if (is.null(filing)) {
      n_no_filing <- n_no_filing + 1L
      next
    }

    # Get price on filing date
    price_data <- tryCatch(
      fetch_ticker_prices(tk, cache_dir = price_cache_dir),
      error = function(e) NULL
    )
    price <- get_price_on_date(price_data, filing$filed_date)
    if (is.na(price)) n_no_price <- n_no_price + 1L

    # Compute indicators (pass point-in-time-filtered fundamentals)
    sector_label <- if (is.na(sec)) "Unknown" else sec
    indicators <- tryCatch(
      compute_ticker_indicators(fund_dt, price, sector_label,
                                target_fy = filing$fiscal_year),
      error = function(e) {
        warning(sprintf("  indicators failed for %s: %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )
    if (is.null(indicators)) next

    n_success <- n_success + 1L
    indicator_list[[n_success]] <- indicators
    ticker_buf[n_success]       <- tk
    sector_buf[n_success]       <- sector_label
    industry_buf[n_success]     <- if (is.na(ind)) "Unknown" else ind
  }

  # Trim pre-allocated buffers to actual size
  indicator_list   <- indicator_list[seq_len(n_success)]
  valid_tickers    <- ticker_buf[seq_len(n_success)]
  valid_sectors    <- sector_buf[seq_len(n_success)]
  valid_industries <- industry_buf[seq_len(n_success)]

  message(sprintf(
    "  results: %d success (%d without price), %d no fundamentals, %d no filing (of %d)",
    n_success, n_no_price, n_no_fund, n_no_filing, n_total))

  if (n_success == 0) {
    warning("assemble_snapshot: no tickers computed successfully")
    return(NULL)
  }

  # -- Build cross-section --
  cs <- compute_cross_section(indicator_list, valid_tickers, valid_sectors)

  # Add metadata columns
  raw_dt <- copy(cs$raw)
  raw_dt[, date     := snapshot_date]
  raw_dt[, industry := valid_industries]
  raw_dt[, sector   := valid_sectors]
  setcolorder(raw_dt, c("date", "ticker", "industry", "sector"))

  zscored_dt <- copy(cs$zscored)
  zscored_dt[, date     := snapshot_date]
  zscored_dt[, industry := valid_industries]
  zscored_dt[, sector   := valid_sectors]
  setcolorder(zscored_dt, c("date", "ticker", "industry", "sector"))

  # -- Validate --
  .assert_output(raw_dt, "assemble_snapshot$raw", list(
    "is data.table"     = is.data.table,
    "has date col"      = function(x) "date" %in% names(x),
    "has ticker col"    = function(x) "ticker" %in% names(x),
    "has sector col"    = function(x) "sector" %in% names(x),
    "has indicator cols" = function(x) {
      all(get_indicator_names() %in% names(x))
    }
  ))

  # -- Write parquet --
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  raw_path    <- file.path(output_dir,
                           sprintf("pit_%s_raw.parquet", snapshot_date))
  zscore_path <- file.path(output_dir,
                           sprintf("pit_%s_zscore.parquet", snapshot_date))

  arrow::write_parquet(raw_dt, raw_path)
  arrow::write_parquet(zscored_dt, zscore_path)

  message(sprintf("  wrote: %s (%d tickers x %d indicators)",
                  basename(raw_path), nrow(raw_dt),
                  length(get_indicator_names())))

  list(
    raw     = raw_dt,
    zscored = zscored_dt,
    stats   = list(
      snapshot_date = snapshot_date,
      n_universe    = n_total,
      n_success     = n_success,
      n_no_fund     = n_no_fund,
      n_no_filing   = n_no_filing,
      n_no_price    = n_no_price
    )
  )
}


# =============================================================================
# 8. build_historical_snapshots()  --  Quarterly cadence builder
# =============================================================================
#' Build point-in-time snapshots at quarterly cadence
#'
#' Generates snapshots for quarter-end dates (Mar 31, Jun 30, Sep 30, Dec 31)
#' between start_date and end_date. Resumable: skips dates with existing
#' parquet files unless force_refresh = TRUE.
#'
#' @param start_date Character. First quarter-end date (default "2010-03-31").
#' @param end_date Character or Date. Last date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @param force_refresh Logical. Rebuild existing snapshots.
#' @return List of stats per snapshot date (invisibly).
build_historical_snapshots <- function(
    start_date      = "2010-03-31",
    end_date        = Sys.Date(),
    master_path     = "cache/lookups/constituent_master.parquet",
    sector_path     = "cache/lookups/sector_industry.parquet",
    fund_dir        = "cache/fundamentals",
    price_cache_dir = "cache/prices",
    output_dir      = "cache/snapshots",
    force_refresh   = FALSE) {

  dates <- .generate_snapshot_dates(start_date, end_date)
  message(sprintf(
    "build_historical_snapshots: %d quarterly dates (%s to %s)",
    length(dates), min(dates), max(dates)))

  # Load lookups once, pass to each snapshot call
  master_dt <- as.data.table(arrow::read_parquet(master_path))
  sector_dt <- as.data.table(arrow::read_parquet(sector_path))

  results  <- list()
  n_built  <- 0L
  n_skip   <- 0L
  n_fail   <- 0L

  for (i in seq_along(dates)) {
    d <- dates[i]

    # Check if already exists
    raw_path <- file.path(output_dir,
                          sprintf("pit_%s_raw.parquet", d))
    if (file.exists(raw_path) && !force_refresh) {
      n_skip <- n_skip + 1L
      next
    }

    message(sprintf("\n[%d/%d] Snapshot: %s", i, length(dates), d))

    result <- tryCatch(
      assemble_snapshot(d, master_path, sector_path, fund_dir,
                        price_cache_dir, output_dir,
                        .master_dt = master_dt,
                        .sector_dt = sector_dt),
      error = function(e) {
        warning(sprintf("  snapshot failed for %s: %s", d, e$message),
                call. = FALSE)
        NULL
      }
    )

    if (!is.null(result)) {
      results[[as.character(d)]] <- result$stats
      n_built <- n_built + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  message(sprintf(
    "\nbuild_historical_snapshots: done -- %d built, %d skipped, %d failed (of %d)",
    n_built, n_skip, n_fail, length(dates)))

  invisible(results)
}


# =============================================================================
# 9. load_snapshot()  --  Reader convenience function
# =============================================================================
#' Load a previously assembled snapshot
#'
#' @param snapshot_date Date or character.
#' @param type Character. "raw" or "zscore".
#' @param output_dir Character.
#' @return data.table, or NULL if file does not exist.
load_snapshot <- function(snapshot_date, type = "raw",
                          output_dir = "cache/snapshots") {

  stopifnot(type %in% c("raw", "zscore"))
  path <- file.path(output_dir,
                    sprintf("pit_%s_%s.parquet", as.Date(snapshot_date), type))

  if (!file.exists(path)) return(NULL)

  dt <- tryCatch(
    as.data.table(arrow::read_parquet(path)),
    error = function(e) NULL
  )

  if (!is.null(dt)) {
    .assert_output(dt, "load_snapshot", list(
      "is data.table"  = is.data.table,
      "has ticker col" = function(x) "ticker" %in% names(x)
    ))
  }

  dt
}
