# ============================================================================
# timeseries_builder.R  --  Daily Time Series Builder (Module 7)
# ============================================================================
# Builds and maintains per-ticker daily time series of fundamental indicators.
#
# Architecture (two-layer storage per ticker):
#   1. Fundamentals layer ({ticker}_fund.parquet): sparse, one row per fiscal
#      year. Stores fundamental-only indicators and accounting stubs for
#      price-sensitive recomputation. Updates only on new filings.
#   2. Daily layer ({ticker}_daily.parquet): dense, one row per trading day.
#      Stores all 57 indicators. Price-sensitive indicators recomputed daily
#      from stubs + closing price; fundamental-only carried forward.
#
# Storage:
#   cache/timeseries/{ticker}_fund.parquet
#   cache/timeseries/{ticker}_daily.parquet
#
# Public API:
#   build_ticker_fundamentals(ticker, cik, sector, ...)
#   update_ticker_daily(ticker, through_date, ...)
#   build_timeseries(start_date, end_date, ...)   -- historical build
#   update_all_daily(through_date, ...)            -- daily incremental
#   load_daily_cross_section(target_date, ...)     -- cross-section reader
#   load_ticker_timeseries(ticker, ...)            -- per-ticker reader
#
# Dependencies: data.table, arrow, quantmod, xts, zoo
# Requires: indicator_compute.R, fundamental_fetcher.R, pit_assembler.R
#           (already sourced)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(quantmod)
  library(xts)
  library(zoo)
})


# =============================================================================
# SECTION 1: CONSTANTS
# =============================================================================

.TS_DIR <- "cache/timeseries"

# Price-sensitive indicators: change with market price
.PRICE_SENSITIVE_INDICATORS <- c(
  "pe_trailing", "peg", "pb", "ps", "pfcf",
  "ev_ebitda", "ev_revenue", "earnings_yield",
  "dividend_yield", "buyback_yield",
  "market_cap", "enterprise_value"
)

# Accounting stubs: raw items stored in the fundamentals layer that enable
# recomputation of price-sensitive indicators without re-reading XBRL data.
# Prefixed with "stub_" to avoid column name collisions with indicators.
.STUB_NAMES <- c(
  "stub_shares", "stub_eps", "stub_equity",
  "stub_revenue", "stub_fcf", "stub_ebitda",
  "stub_total_debt", "stub_net_debt", "stub_cash",
  "stub_dividends", "stub_buybacks"
)

# Fundamental-only indicators: change only when a new filing appears
.FUNDAMENTAL_INDICATORS <- setdiff(get_indicator_names(), .PRICE_SENSITIVE_INDICATORS)


# =============================================================================
# SECTION 2: PRIVATE HELPERS
# =============================================================================

.assert_output_ts <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}


#' Extract accounting stubs from pivoted wide-format data for a fiscal year
#'
#' @param wide_dt data.table. Output of pivot_fundamentals + .derive_quantities.
#' @param target_fy Integer. Target fiscal year.
#' @return Named list of stub values, or NULL.
.extract_stubs <- function(wide_dt, target_fy) {

  fy_rows <- wide_dt[period_type == "FY"]
  curr_idx <- which(fy_rows$fiscal_year == target_fy)
  if (length(curr_idx) == 0) return(NULL)
  curr <- fy_rows[curr_idx[1]]

  list(
    stub_shares     = .col(curr, "shares_outstanding"),
    stub_eps        = .col(curr, "eps_diluted"),
    stub_equity     = .col(curr, "stockholders_equity"),
    stub_revenue    = .col(curr, "revenue"),
    stub_fcf        = .col(curr, "fcf"),
    stub_ebitda     = .col(curr, "ebitda"),
    stub_total_debt = .col(curr, "total_debt"),
    stub_net_debt   = .col(curr, "net_debt"),
    stub_cash       = .col(curr, "cash"),
    stub_dividends  = .col(curr, "dividends_paid"),
    stub_buybacks   = .col(curr, "buybacks")
  )
}


#' Vectorized computation of price-sensitive indicators
#'
#' Given vectors of accounting stubs and prices (one element per trading day),
#' computes all 12 price-sensitive indicators. Replicates the logic from
#' .compute_valuation and .compute_shareholder in indicator_compute.R.
#'
#' @param p Numeric vector. Closing prices.
#' @param shares Numeric vector. Shares outstanding.
#' @param eps Numeric vector. Diluted EPS.
#' @param equity Numeric vector. Stockholders' equity.
#' @param rev Numeric vector. Revenue.
#' @param fcf_v Numeric vector. Free cash flow.
#' @param ebitda Numeric vector. EBITDA.
#' @param td Numeric vector. Total debt.
#' @param nd Numeric vector. Net debt.
#' @param div Numeric vector. Dividends paid (negative = outflow).
#' @param buy Numeric vector. Buybacks (negative = outflow).
#' @param eps_g Numeric vector. EPS growth YoY (for PEG).
#' @return data.table with 12 price-sensitive indicator columns.
.compute_price_sensitive_vec <- function(p, shares, eps, equity, rev,
                                         fcf_v, ebitda, td, nd,
                                         div, buy, eps_g) {

  # Market cap & enterprise value
  mc <- ifelse(!is.na(p) & !is.na(shares), p * shares, NA_real_)
  ev <- ifelse(!is.na(mc),
               ifelse(!is.na(nd), mc + nd,
                      ifelse(!is.na(td), mc + td, NA_real_)),
               NA_real_)

  # P/E (guard: |EPS| >= 0.01)
  pe <- ifelse(!is.na(p) & !is.na(eps) & abs(eps) >= 0.01,
               p / eps, NA_real_)

  # PEG = P/E / (EPS growth * 100), only when growth > 0
  peg <- ifelse(!is.na(pe) & !is.na(eps_g) & eps_g > 0 &
                  abs(eps_g * 100) >= 1e-9,
                pe / (eps_g * 100), NA_real_)

  # P/B (equity must be positive)
  pb <- ifelse(!is.na(mc) & !is.na(equity) & equity > 0,
               mc / equity, NA_real_)

  # P/S
  ps <- ifelse(!is.na(mc) & !is.na(rev) & abs(rev) >= 1e-9,
               mc / rev, NA_real_)

  # P/FCF (FCF must be positive)
  pfcf <- ifelse(!is.na(mc) & !is.na(fcf_v) & fcf_v > 0,
                 mc / fcf_v, NA_real_)

  # EV/EBITDA (EBITDA must be positive)
  ev_ebitda <- ifelse(!is.na(ev) & !is.na(ebitda) & ebitda > 0,
                      ev / ebitda, NA_real_)

  # EV/Revenue
  ev_revenue <- ifelse(!is.na(ev) & !is.na(rev) & abs(rev) >= 1e-9,
                       ev / rev, NA_real_)

  # Earnings yield = EPS / price (guard: |price| >= 0.01)
  ey <- ifelse(!is.na(eps) & !is.na(p) & abs(p) >= 0.01,
               eps / p, NA_real_)

  # Dividend yield = |dividends_paid| / market_cap
  abs_div <- ifelse(!is.na(div), abs(div), NA_real_)
  div_yield <- ifelse(!is.na(abs_div) & !is.na(mc) & abs(mc) >= 1e-9,
                      abs_div / mc, NA_real_)

  # Buyback yield = |buybacks| / market_cap
  abs_buy <- ifelse(!is.na(buy), abs(buy), NA_real_)
  bb_yield <- ifelse(!is.na(abs_buy) & !is.na(mc) & abs(mc) >= 1e-9,
                     abs_buy / mc, NA_real_)

  data.table(
    pe_trailing      = pe,
    peg              = peg,
    pb               = pb,
    ps               = ps,
    pfcf             = pfcf,
    ev_ebitda        = ev_ebitda,
    ev_revenue       = ev_revenue,
    earnings_yield   = ey,
    dividend_yield   = div_yield,
    buyback_yield    = bb_yield,
    market_cap       = mc,
    enterprise_value = ev
  )
}


# =============================================================================
# SECTION 3: build_ticker_fundamentals()
# =============================================================================
#' Build the fundamentals layer for a single ticker
#'
#' Reads cached XBRL data, computes fundamental-only indicators and extracts
#' accounting stubs for each available fiscal year. Saves to parquet.
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit CIK.
#' @param sector Character. Sector classification.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param ts_dir Character. Time series output directory.
#' @param force Logical. Rebuild even if file exists.
#' @return data.table (the fundamentals layer), or NULL on failure.
build_ticker_fundamentals <- function(ticker, cik, sector,
                                      fund_dir = "cache/fundamentals",
                                      ts_dir   = .TS_DIR,
                                      force    = FALSE) {

  if (!dir.exists(ts_dir)) dir.create(ts_dir, recursive = TRUE)

  out_path <- file.path(ts_dir, sprintf("%s_fund.parquet", ticker))
  if (file.exists(out_path) && !force) {
    return(as.data.table(arrow::read_parquet(out_path)))
  }

  # Load raw XBRL long-format data
  fund_dt <- tryCatch(
    get_fundamentals(ticker, cik, cache_dir = fund_dir),
    error = function(e) NULL
  )
  if (is.null(fund_dt) || nrow(fund_dt) == 0) return(NULL)

  # Pivot to wide format for stub extraction
  wide <- pivot_fundamentals(fund_dt)
  if (is.null(wide)) return(NULL)
  wide <- .derive_quantities(wide)

  fy_rows <- wide[period_type == "FY"]
  if (nrow(fy_rows) == 0) return(NULL)

  fiscal_years <- sort(unique(fy_rows$fiscal_year))

  results <- vector("list", length(fiscal_years))
  n_ok <- 0L

  for (fy in fiscal_years) {
    curr <- fy_rows[fiscal_year == fy]
    if (nrow(curr) == 0) next

    filed_date <- as.Date(max(curr$filed, na.rm = TRUE))
    period_end <- as.Date(max(curr$period_end, na.rm = TRUE))

    # Require minimum data density
    data_cols <- setdiff(names(curr),
                         c("fiscal_year", "period_type", "period_end", "filed"))
    n_concepts <- sum(!is.na(as.matrix(curr[1, ..data_cols])))
    if (n_concepts < 5) next

    # Compute indicators with price=NA (price-sensitive ones become NA)
    indicators <- tryCatch(
      compute_ticker_indicators(fund_dt, price_on_filed = NA_real_,
                                sector = sector, target_fy = fy),
      error = function(e) NULL
    )
    if (is.null(indicators)) next

    # Extract fundamental-only indicator values
    fund_only <- as.list(indicators[.FUNDAMENTAL_INDICATORS])

    # Extract accounting stubs from pivoted data
    stubs <- .extract_stubs(wide, fy)
    if (is.null(stubs)) next

    n_ok <- n_ok + 1L
    results[[n_ok]] <- c(
      list(fiscal_year = as.integer(fy),
           filed_date  = filed_date,
           period_end  = period_end),
      fund_only,
      stubs
    )
  }

  if (n_ok == 0) return(NULL)

  dt <- rbindlist(results[seq_len(n_ok)], fill = TRUE)
  setorder(dt, fiscal_year)

  .assert_output_ts(dt, "build_ticker_fundamentals", list(
    "is data.table"      = is.data.table,
    "has fiscal_year"    = function(x) "fiscal_year" %in% names(x),
    "has filed_date"     = function(x) "filed_date" %in% names(x),
    "has stubs"          = function(x) all(.STUB_NAMES %in% names(x)),
    "at least one row"   = function(x) nrow(x) > 0
  ))

  arrow::write_parquet(dt, out_path)
  dt
}


# =============================================================================
# SECTION 4: update_ticker_daily()
# =============================================================================
#' Update daily time series for a single ticker
#'
#' Reads the fundamentals layer, identifies new trading days since the last
#' stored date, and appends rows. Uses vectorized computation: findInterval
#' for point-in-time filing lookup, vectorized ifelse for price-sensitive
#' indicators.
#'
#' @param ticker Character. Ticker symbol.
#' @param through_date Date. Update through this date (default today).
#' @param start_date Date. Earliest date to compute (default 2010-01-04).
#' @param price_dir Character. Price cache directory.
#' @param ts_dir Character. Time series directory.
#' @param refresh_price Logical. Delete and re-fetch price cache for this ticker.
#' @return data.table (full daily layer), or NULL on failure.
update_ticker_daily <- function(ticker,
                                through_date  = Sys.Date(),
                                start_date    = as.Date("2010-01-04"),
                                price_dir     = "cache/prices",
                                ts_dir        = .TS_DIR,
                                refresh_price = FALSE) {

  through_date <- as.Date(through_date)
  start_date   <- as.Date(start_date)

  # -- Load fundamentals layer --
  fund_path <- file.path(ts_dir, sprintf("%s_fund.parquet", ticker))
  if (!file.exists(fund_path)) return(NULL)

  fund_dt <- as.data.table(arrow::read_parquet(fund_path))
  if (nrow(fund_dt) == 0) return(NULL)
  fund_dt[, filed_date := as.Date(filed_date)]

  # -- Load existing daily layer --
  daily_path <- file.path(ts_dir, sprintf("%s_daily.parquet", ticker))
  existing_dt <- NULL
  last_date <- start_date - 1L

  if (file.exists(daily_path)) {
    existing_dt <- tryCatch(
      as.data.table(arrow::read_parquet(daily_path)),
      error = function(e) NULL
    )
    if (!is.null(existing_dt) && nrow(existing_dt) > 0) {
      existing_dt[, date := as.Date(date)]
      last_date <- max(existing_dt$date)
    }
  }

  if (last_date >= through_date) return(existing_dt)

  # -- Handle price refresh --
  if (refresh_price) {
    escaped_tk <- gsub("\\.", "\\\\.", ticker)
    cache_files <- list.files(price_dir,
                              pattern = sprintf("^%s_yahoo_", escaped_tk),
                              full.names = TRUE)
    for (f in cache_files) file.remove(f)
  }

  # -- Load price data --
  price_xts <- tryCatch(
    fetch_ticker_prices(ticker, cache_dir = price_dir),
    error = function(e) NULL
  )
  if (is.null(price_xts) || nrow(price_xts) == 0) return(existing_dt)

  # Extract adjusted close for all trading days at once
  nc <- ncol(price_xts)
  price_col <- if (nc >= 6) 6 else if (nc >= 4) 4 else 1
  all_prices <- as.numeric(coredata(price_xts[, price_col]))
  all_dates  <- as.Date(index(price_xts))

  # -- Filter to new dates --
  new_mask <- all_dates > last_date & all_dates <= through_date
  if (sum(new_mask) == 0) return(existing_dt)

  new_dates  <- all_dates[new_mask]
  new_prices <- all_prices[new_mask]

  # -- Point-in-time lookup: find latest fiscal year filed on or before each day --
  # Must find max(fiscal_year) WHERE filed_date <= d, NOT just latest filing.
  # Simple findInterval on filed_date is wrong when prior-year amendments
  # produce non-monotonic filed_dates (e.g., FY2022 amended after FY2023 filed).
  # Fix: sort by fiscal_year DESC, iterate ~15 rows, assign greedily.
  setorder(fund_dt, -fiscal_year)
  n_days <- length(new_dates)
  fund_idx <- rep(NA_integer_, n_days)

  for (k in seq_len(nrow(fund_dt))) {
    eligible <- is.na(fund_idx) & new_dates >= fund_dt$filed_date[k]
    fund_idx[eligible] <- k
  }

  # Remove dates before any filing or with NA price
  valid <- !is.na(fund_idx) & !is.na(new_prices)
  if (sum(valid) == 0) return(existing_dt)

  new_dates  <- new_dates[valid]
  new_prices <- new_prices[valid]
  fund_idx   <- fund_idx[valid]

  # -- Build result data.table --
  result_dt <- data.table(
    date        = new_dates,
    price       = new_prices,
    fiscal_year = fund_dt$fiscal_year[fund_idx],
    filed_date  = fund_dt$filed_date[fund_idx]
  )

  # -- Map fundamental-only indicators from fund layer --
  for (col in .FUNDAMENTAL_INDICATORS) {
    if (col %in% names(fund_dt)) {
      set(result_dt, j = col, value = as.numeric(fund_dt[[col]][fund_idx]))
    } else {
      set(result_dt, j = col, value = NA_real_)
    }
  }

  # -- Vectorized price-sensitive indicator computation --
  price_dt <- .compute_price_sensitive_vec(
    p       = new_prices,
    shares  = as.numeric(fund_dt$stub_shares[fund_idx]),
    eps     = as.numeric(fund_dt$stub_eps[fund_idx]),
    equity  = as.numeric(fund_dt$stub_equity[fund_idx]),
    rev     = as.numeric(fund_dt$stub_revenue[fund_idx]),
    fcf_v   = as.numeric(fund_dt$stub_fcf[fund_idx]),
    ebitda  = as.numeric(fund_dt$stub_ebitda[fund_idx]),
    td      = as.numeric(fund_dt$stub_total_debt[fund_idx]),
    nd      = as.numeric(fund_dt$stub_net_debt[fund_idx]),
    div     = as.numeric(fund_dt$stub_dividends[fund_idx]),
    buy     = as.numeric(fund_dt$stub_buybacks[fund_idx]),
    eps_g   = result_dt$eps_growth_yoy  # from fundamental indicators
  )

  # Bind price-sensitive columns into result
  for (col in names(price_dt)) {
    set(result_dt, j = col, value = price_dt[[col]])
  }

  # -- Combine with existing and save --
  if (!is.null(existing_dt) && nrow(existing_dt) > 0) {
    combined <- rbindlist(list(existing_dt, result_dt), fill = TRUE)
  } else {
    combined <- result_dt
  }

  setorder(combined, date)
  arrow::write_parquet(combined, daily_path)

  combined
}


# =============================================================================
# SECTION 5: build_timeseries()  --  Historical build
# =============================================================================
#' Build complete daily time series for all tickers
#'
#' Initial historical build. For each ticker: builds the fundamentals layer
#' (if missing) then builds the daily layer from start_date through end_date.
#' Resumable: skips existing fundamentals layers, appends only new dates to
#' daily layers.
#'
#' @param start_date Character. Earliest date (default "2010-01-04").
#' @param end_date Date. Latest date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_dir Character. Price cache directory.
#' @param ts_dir Character. Time series output directory.
#' @param tickers Character vector or NULL. Specific tickers to process.
#'   NULL = all tickers with cached fundamentals.
#' @return Invisible list of stats.
build_timeseries <- function(start_date  = "2010-01-04",
                             end_date    = Sys.Date(),
                             master_path = "cache/lookups/constituent_master.parquet",
                             sector_path = "cache/lookups/sector_industry.parquet",
                             fund_dir    = "cache/fundamentals",
                             price_dir   = "cache/prices",
                             ts_dir      = .TS_DIR,
                             tickers     = NULL) {

  t0 <- Sys.time()
  message("build_timeseries: starting historical build...")

  # Load lookups
  master  <- as.data.table(arrow::read_parquet(master_path))
  sectors <- as.data.table(arrow::read_parquet(sector_path))

  # Determine tickers to process
  if (is.null(tickers)) {
    fund_files <- list.files(fund_dir, pattern = "\\.parquet$")
    tickers <- gsub("^\\d+_(.+)\\.parquet$", "\\1", fund_files)
  }

  # Merge metadata
  ticker_dt <- data.table(ticker = tickers)
  ticker_dt <- merge(ticker_dt, master[, .(ticker, cik)],
                     by = "ticker", all.x = TRUE)
  ticker_dt <- merge(ticker_dt, sectors[, .(ticker, sector, industry)],
                     by = "ticker", all.x = TRUE)
  ticker_dt[is.na(sector), sector := "Unknown"]
  ticker_dt[is.na(industry), industry := "Unknown"]

  n <- nrow(ticker_dt)
  message(sprintf("  tickers: %d, range: %s to %s", n, start_date, end_date))

  n_fund_built <- 0L; n_fund_cached <- 0L
  n_daily_ok <- 0L; n_fail <- 0L

  for (i in seq_len(n)) {
    tk  <- ticker_dt$ticker[i]
    cik <- ticker_dt$cik[i]
    sec <- ticker_dt$sector[i]

    if (i %% 50 == 0 || i == 1) {
      message(sprintf("  [%d/%d] %s", i, n, tk))
    }

    # Step 1: Build fundamentals layer (skip if exists)
    fund_path <- file.path(ts_dir, sprintf("%s_fund.parquet", tk))
    if (file.exists(fund_path)) {
      n_fund_cached <- n_fund_cached + 1L
    } else {
      fund_result <- tryCatch(
        build_ticker_fundamentals(tk, cik, sec, fund_dir, ts_dir),
        error = function(e) {
          warning(sprintf("  fund failed: %s -- %s", tk, e$message),
                  call. = FALSE)
          NULL
        }
      )
      if (!is.null(fund_result)) {
        n_fund_built <- n_fund_built + 1L
      } else {
        n_fail <- n_fail + 1L
        next
      }
    }

    # Step 2: Update daily layer
    daily_result <- tryCatch(
      update_ticker_daily(tk, through_date = end_date,
                          start_date = as.Date(start_date),
                          price_dir = price_dir, ts_dir = ts_dir),
      error = function(e) {
        warning(sprintf("  daily failed: %s -- %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )

    if (!is.null(daily_result)) {
      n_daily_ok <- n_daily_ok + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)

  message(sprintf(paste0(
    "\nbuild_timeseries: done in %.1f min\n",
    "  fund layers: %d built, %d cached\n",
    "  daily layers: %d ok, %d failed (of %d)"),
    elapsed, n_fund_built, n_fund_cached, n_daily_ok, n_fail, n))

  invisible(list(
    n_fund_built  = n_fund_built,
    n_fund_cached = n_fund_cached,
    n_daily_ok    = n_daily_ok,
    n_fail        = n_fail,
    elapsed_min   = elapsed
  ))
}


# =============================================================================
# SECTION 6: update_all_daily()  --  Daily incremental
# =============================================================================
#' Incremental daily update for all tickers
#'
#' For each ticker with a fundamentals layer, updates the daily layer
#' through the specified date. Set refresh_prices=TRUE (default) to
#' re-download Yahoo price data so the latest close is available.
#'
#' @param through_date Date. Update through this date (default today).
#' @param ts_dir Character. Time series directory.
#' @param price_dir Character. Price cache directory.
#' @param refresh_prices Logical. Re-download price data for all tickers.
#' @param master_path Character. Path to constituent_master.parquet (for
#'   new tickers that might need fundamentals built).
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @return Invisible list of stats.
update_all_daily <- function(through_date   = Sys.Date(),
                             ts_dir         = .TS_DIR,
                             price_dir      = "cache/prices",
                             refresh_prices = TRUE,
                             master_path    = "cache/lookups/constituent_master.parquet",
                             sector_path    = "cache/lookups/sector_industry.parquet",
                             fund_dir       = "cache/fundamentals") {

  t0 <- Sys.time()
  through_date <- as.Date(through_date)

  # Find tickers with fundamentals layers
  fund_files <- list.files(ts_dir, pattern = "_fund\\.parquet$")
  tickers <- gsub("_fund\\.parquet$", "", fund_files)

  n <- length(tickers)
  message(sprintf("update_all_daily: %d tickers through %s (refresh_prices=%s)",
                  n, through_date, refresh_prices))

  n_updated <- 0L; n_fail <- 0L

  for (i in seq_along(tickers)) {
    tk <- tickers[i]

    if (i %% 100 == 0 || i == 1) {
      message(sprintf("  [%d/%d] %s", i, n, tk))
    }

    result <- tryCatch(
      update_ticker_daily(tk, through_date = through_date,
                          price_dir = price_dir, ts_dir = ts_dir,
                          refresh_price = refresh_prices),
      error = function(e) {
        warning(sprintf("  failed: %s -- %s", tk, e$message), call. = FALSE)
        NULL
      }
    )

    if (!is.null(result)) {
      n_updated <- n_updated + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("update_all_daily: done in %.1f min -- %d updated, %d failed",
                  elapsed, n_updated, n_fail))

  invisible(list(
    n_updated   = n_updated,
    n_fail      = n_fail,
    elapsed_min = elapsed
  ))
}


# =============================================================================
# SECTION 7: load_daily_cross_section()  --  Cross-section reader
# =============================================================================
#' Load cross-sectional data for a specific date
#'
#' Reads individual ticker daily files and assembles a cross-section
#' (one row per ticker, all indicators) for the given date.
#'
#' @param target_date Date or character. The date to load.
#' @param ts_dir Character. Time series directory.
#' @param sector_path Character. Path to sector_industry.parquet (for metadata).
#' @param zscore Logical. Also compute and return z-scored version (default TRUE).
#' @return If zscore=FALSE: data.table. If zscore=TRUE: list with $raw, $zscored.
#'   NULL if no data found.
load_daily_cross_section <- function(target_date,
                                     ts_dir      = .TS_DIR,
                                     sector_path = "cache/lookups/sector_industry.parquet",
                                     zscore      = TRUE) {

  target_date <- as.Date(target_date)

  daily_files <- list.files(ts_dir, pattern = "_daily\\.parquet$",
                            full.names = TRUE)
  if (length(daily_files) == 0) {
    warning("load_daily_cross_section: no daily files found")
    return(NULL)
  }

  rows <- vector("list", length(daily_files))
  n_ok <- 0L

  for (f in daily_files) {
    tk <- gsub("_daily\\.parquet$", "", basename(f))

    dt <- tryCatch({
      d <- as.data.table(arrow::read_parquet(f))
      d[, date := as.Date(date)]
      d[date == target_date]
    }, error = function(e) NULL)

    if (!is.null(dt) && nrow(dt) > 0) {
      dt[, ticker := tk]
      n_ok <- n_ok + 1L
      rows[[n_ok]] <- dt[1]
    }
  }

  if (n_ok == 0) return(NULL)

  cs <- rbindlist(rows[seq_len(n_ok)], fill = TRUE)

  # Add sector/industry metadata
  if (file.exists(sector_path)) {
    sec_dt <- as.data.table(arrow::read_parquet(sector_path))
    cs <- merge(cs, sec_dt[, .(ticker, sector, industry)],
                by = "ticker", all.x = TRUE)
    cs[is.na(sector), sector := "Unknown"]
    cs[is.na(industry), industry := "Unknown"]
  }

  # Order columns: metadata, then indicators in canonical order
  ind_names  <- get_indicator_names()
  meta_cols  <- c("date", "ticker", "sector", "industry", "price",
                  "fiscal_year", "filed_date")
  present_meta <- intersect(meta_cols, names(cs))
  present_ind  <- intersect(ind_names, names(cs))
  other_cols   <- setdiff(names(cs), c(present_meta, present_ind))
  setcolorder(cs, c(present_meta, present_ind, other_cols))

  if (!zscore) return(cs)

  # Z-score using the existing cross-sectional z-scoring function
  if ("sector" %in% names(cs) && length(present_ind) > 0) {
    zscore_input <- cs[, c("ticker", "sector", present_ind), with = FALSE]
    zscored_dt <- zscore_cross_section(zscore_input)
    setcolorder(zscored_dt, c("ticker", present_ind))

    return(list(raw = cs, zscored = zscored_dt))
  }

  cs
}


# =============================================================================
# SECTION 8: load_ticker_timeseries()  --  Per-ticker reader
# =============================================================================
#' Load daily time series for a single ticker
#'
#' @param ticker Character. Ticker symbol.
#' @param ts_dir Character. Time series directory.
#' @param from Date or character. Start date filter (optional).
#' @param to Date or character. End date filter (optional).
#' @return data.table, or NULL if file doesn't exist.
load_ticker_timeseries <- function(ticker, ts_dir = .TS_DIR,
                                   from = NULL, to = NULL) {

  path <- file.path(ts_dir, sprintf("%s_daily.parquet", ticker))
  if (!file.exists(path)) return(NULL)

  dt <- tryCatch(
    as.data.table(arrow::read_parquet(path)),
    error = function(e) NULL
  )
  if (is.null(dt) || nrow(dt) == 0) return(NULL)

  dt[, date := as.Date(date)]

  if (!is.null(from)) dt <- dt[date >= as.Date(from)]
  if (!is.null(to))   dt <- dt[date <= as.Date(to)]

  dt
}


# =============================================================================
# SECTION 9: list_timeseries_tickers()  --  Discovery utility
# =============================================================================
#' List tickers that have daily time series available
#'
#' @param ts_dir Character. Time series directory.
#' @return Character vector of ticker symbols, or character(0).
list_timeseries_tickers <- function(ts_dir = .TS_DIR) {
  if (!dir.exists(ts_dir)) return(character(0))
  files <- list.files(ts_dir, pattern = "_daily\\.parquet$")
  sort(gsub("_daily\\.parquet$", "", files))
}
