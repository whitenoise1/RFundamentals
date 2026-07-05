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
  "market_cap", "enterprise_value",
  "net_payout_yield",
  # Wave 4 ME-scaled levels
  "rd_me", "ebm", "bpebm", "cf_me", "cfp", "net_debt_price",
  "am", "leverage_mkt", "cash_prod", "payout_yield"
)

# Accounting stubs: raw items stored in the fundamentals layer that enable
# recomputation of price-sensitive indicators without re-reading XBRL data.
# Prefixed with "stub_" to avoid column name collisions with indicators.
# Wave 4 stubs are pre-resolved numerators/components (identity fallbacks
# and zero-if-na policies applied at extraction, mirroring .compute_levels):
#   stub_rnd      R&D expense (NA = non-reporter, rd_me stays NA)
#   stub_assets   total assets
#   stub_liabilities  total liabilities (Wave 1 identity fallback applied)
#   stub_cf_num   NI + depreciation-zero-if-na (LSV cash flow numerator)
#   stub_cfo      operating cash flow
#   stub_che      cash + st_investments (Compustat CHE equivalent)
#   stub_net_cash che - total_debt (Penman EBM net cash)
#   stub_ndp_num  FINL - che (net_debt_price numerator; NA for financials)
.STUB_NAMES <- c(
  "stub_shares", "stub_eps", "stub_equity",
  "stub_revenue", "stub_fcf", "stub_ebitda",
  "stub_total_debt", "stub_net_debt", "stub_cash",
  "stub_dividends", "stub_buybacks", "stub_equity_issuance",
  "stub_rnd", "stub_assets", "stub_liabilities", "stub_cf_num",
  "stub_cfo", "stub_che", "stub_net_cash", "stub_ndp_num"
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


# Stub column at fund_idx, or NA vector when the fund layer predates the
# stub (same degrade-to-NA convention as the Wave 2 iss fallback).
.stub_or_na <- function(fund_dt, stub, fund_idx) {
  if (stub %in% names(fund_dt)) {
    as.numeric(fund_dt[[stub]][fund_idx])
  } else NA_real_
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

  # Wave 4 components come from .level_components (indicator_compute.R
  # Section 6e) -- the SAME resolver the compute path uses, so the
  # filing-date and daily-recompute values cannot drift.
  lc <- .level_components(curr)

  list(
    stub_shares     = .col(curr, "shares_outstanding"),
    stub_eps        = .col(curr, "eps_diluted"),
    stub_equity     = lc$ceq,
    stub_revenue    = .col(curr, "revenue"),
    stub_fcf        = .col(curr, "fcf"),
    stub_ebitda     = .col(curr, "ebitda"),
    stub_total_debt = lc$td,
    stub_net_debt   = .col(curr, "net_debt"),
    stub_cash       = .col(curr, "cash"),
    stub_dividends  = .col(curr, "dividends_paid"),
    stub_buybacks   = .col(curr, "buybacks"),
    stub_equity_issuance = .col(curr, "equity_issuance"),
    stub_rnd        = .col(curr, "rnd"),
    stub_assets     = lc$at,
    stub_liabilities = lc$lt,
    stub_cf_num     = lc$cf_num,
    stub_cfo        = .col(curr, "operating_cashflow"),
    stub_che        = lc$che,
    stub_net_cash   = lc$ncash,
    stub_ndp_num    = lc$ndp_num
  )
}


#' Vectorized computation of price-sensitive indicators
#'
#' Given vectors of accounting stubs and prices (one element per trading day),
#' computes all 23 price-sensitive indicators. Replicates the logic from
#' .compute_valuation, .compute_shareholder and .compute_levels in
#' indicator_compute.R. Wave 4 stub params default to NA so pre-Wave-4
#' fund layers degrade to NA columns instead of erroring.
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
#' @param iss Numeric vector. Equity issuance (Wave 2 stub).
#' @param rnd Numeric vector. R&D expense (stub_rnd).
#' @param at_v Numeric vector. Total assets (stub_assets).
#' @param lt_v Numeric vector. Total liabilities (stub_liabilities).
#' @param cf_num Numeric vector. NI + depreciation (stub_cf_num).
#' @param cfo Numeric vector. Operating cash flow (stub_cfo).
#' @param che Numeric vector. Cash + ST investments (stub_che).
#' @param ncash Numeric vector. che - total debt (stub_net_cash).
#' @param ndp_num Numeric vector. FINL - che (stub_ndp_num).
#' @return data.table with 23 price-sensitive indicator columns.
.compute_price_sensitive_vec <- function(p, shares, eps, equity, rev,
                                         fcf_v, ebitda, td, nd,
                                         div, buy, eps_g,
                                         iss = NA_real_,
                                         rnd = NA_real_, at_v = NA_real_,
                                         lt_v = NA_real_, cf_num = NA_real_,
                                         cfo = NA_real_, che = NA_real_,
                                         ncash = NA_real_,
                                         ndp_num = NA_real_) {

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

  # Gross payout components, shared by net_payout_yield (Wave 2) and
  # payout_yield (Wave 4) -- one zero-fill site for the sign policy.
  any_gross <- !is.na(abs_div) | !is.na(abs_buy)
  gpy_num <- ifelse(is.na(abs_div), 0, abs_div) +
    ifelse(is.na(abs_buy), 0, abs_buy)

  # Net payout yield = (|dividends| + |buybacks| - equity issuance) /
  # market_cap (Boudoukh et al. 2007). Components zero-if-na; NA when
  # none of the three is reported.
  any_payout <- any_gross | !is.na(iss)
  npy_num <- gpy_num - ifelse(is.na(iss), 0, iss)
  npy <- ifelse(any_payout & !is.na(mc) & abs(mc) >= 1e-9,
                npy_num / mc, NA_real_)

  # -- Wave 4 ME-scaled levels (indicator_compute.R Section 6e) --
  mc_ok <- !is.na(mc) & abs(mc) >= 1e-9

  # R&D to market (missing R&D stays NA)
  rd_me <- ifelse(mc_ok & !is.na(rnd), rnd / mc, NA_real_)

  # Penman enterprise book-to-market + leverage component
  ebm <- ifelse(mc_ok & !is.na(equity) & !is.na(ncash) &
                  abs(mc + ncash) >= 1e-9,
                (equity + ncash) / (mc + ncash), NA_real_)
  bp <- ifelse(mc_ok & !is.na(equity), equity / mc, NA_real_)
  bpebm <- ifelse(!is.na(bp) & !is.na(ebm), bp - ebm, NA_real_)

  # LSV cash flow to price; Desai CFO to price
  cf_me <- ifelse(mc_ok & !is.na(cf_num), cf_num / mc, NA_real_)
  cfp   <- ifelse(mc_ok & !is.na(cfo), cfo / mc, NA_real_)

  # Penman net debt to price (stub NA'd for financials at extraction)
  net_debt_price <- ifelse(mc_ok & !is.na(ndp_num), ndp_num / mc, NA_real_)

  # FF assets to market; Bhandari market leverage
  am_v <- ifelse(mc_ok & !is.na(at_v), at_v / mc, NA_real_)
  lev_mkt <- ifelse(mc_ok & !is.na(lt_v), lt_v / mc, NA_real_)

  # Cash productivity (near-zero cash -> NA, same floor as Section 6e)
  cash_prod <- ifelse(!is.na(mc) & !is.na(at_v) & !is.na(che) &
                        abs(che) >= 1,
                      (mc - at_v) / che, NA_real_)

  # Gross payout yield (components zero-if-na, >= 1 reported; any_gross
  # and gpy_num are computed once above, next to net payout yield)
  payout_yield <- ifelse(any_gross & mc_ok, gpy_num / mc, NA_real_)

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
    enterprise_value = ev,
    net_payout_yield = npy,
    rd_me            = rd_me,
    ebm              = ebm,
    bpebm            = bpebm,
    cf_me            = cf_me,
    cfp              = cfp,
    net_debt_price   = net_debt_price,
    am               = am_v,
    leverage_mkt     = lev_mkt,
    cash_prod        = cash_prod,
    payout_yield     = payout_yield
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

  # Load raw XBRL vintages (one row per concept x period x filing), so each
  # fiscal year can be computed from the data that was public when its own
  # annual report was filed. Old collapsed caches degrade gracefully: the
  # vintage table then has one row per period and the as-of resolution is a
  # no-op.
  fund_v <- tryCatch(
    get_fundamentals(ticker, cik, cache_dir = fund_dir, vintages = TRUE),
    error = function(e) NULL
  )
  if (is.null(fund_v) || nrow(fund_v) == 0) return(NULL)

  # Enumerate fiscal years from the latest view
  fy_all <- pit_dedup(fund_v)[period_type == "FY"]
  if (nrow(fy_all) == 0) return(NULL)

  fiscal_years <- sort(unique(fy_all$fiscal_year))
  annual_forms <- c("10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A")

  # Split events for share-issuance adjustment (loaded once per ticker;
  # guarded so the builder works when ttm_eps.R is not sourced)
  splits <- if (exists("load_ticker_splits")) {
    tryCatch(load_ticker_splits(ticker, fetch = FALSE), error = function(e) NULL)
  } else NULL

  results <- vector("list", length(fiscal_years))
  n_ok <- 0L

  for (fy in fiscal_years) {
    # Original availability date: earliest annual-form filing that reports
    # this fiscal year. Amendments and later re-reports come afterwards and
    # must not push the stamp forward.
    fy_grp <- fund_v[fiscal_year == fy & period_type == "FY" & !is.na(filed)]
    if (nrow(fy_grp) == 0) next
    ann <- fy_grp[form %in% annual_forms]
    filed_date <- as.Date(min(if (nrow(ann)) ann$filed else fy_grp$filed))

    # Point-in-time view as of that filing date; prior years resolve to the
    # vintages that were public then.
    fund_asof <- pit_dedup(fund_v, as_of = filed_date)

    wide <- pivot_fundamentals(fund_asof)
    if (is.null(wide)) next
    wide <- .derive_quantities(wide)

    curr <- wide[period_type == "FY" & fiscal_year == fy]
    if (nrow(curr) == 0) next

    period_end <- as.Date(max(curr$period_end, na.rm = TRUE))

    # Require minimum data density
    data_cols <- setdiff(names(curr),
                         c("fiscal_year", "period_type", "period_end", "filed"))
    n_concepts <- sum(!is.na(as.matrix(curr[1, ..data_cols])))
    if (n_concepts < 5) next

    # Compute indicators with price=NA (price-sensitive ones become NA)
    indicators <- tryCatch(
      compute_ticker_indicators(fund_asof, price_on_filed = NA_real_,
                                sector = sector, target_fy = fy,
                                splits = splits),
      error = function(e) NULL
    )
    if (is.null(indicators)) next

    # Extract fundamental-only indicator values
    fund_only <- as.list(indicators[.FUNDAMENTAL_INDICATORS])

    # Extract accounting stubs from the as-of pivoted data
    stubs <- .extract_stubs(wide, fy)
    if (is.null(stubs)) next

    # net_debt_price is financial-NA (OpenAP SIC 6000-6999 exclusion);
    # the compute-path mask cannot reach the daily recompute, so NA the
    # stub at the source for financial tickers.
    if (!is.na(sector) && sector == "Financial") {
      stubs$stub_ndp_num <- NA_real_
    }

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
    eps_g   = result_dt$eps_growth_yoy,  # from fundamental indicators
    # pre-Wave-2 / pre-Wave-4 fund layers lack these stubs -> NA columns
    iss     = .stub_or_na(fund_dt, "stub_equity_issuance", fund_idx),
    rnd     = .stub_or_na(fund_dt, "stub_rnd", fund_idx),
    at_v    = .stub_or_na(fund_dt, "stub_assets", fund_idx),
    lt_v    = .stub_or_na(fund_dt, "stub_liabilities", fund_idx),
    cf_num  = .stub_or_na(fund_dt, "stub_cf_num", fund_idx),
    cfo     = .stub_or_na(fund_dt, "stub_cfo", fund_idx),
    che     = .stub_or_na(fund_dt, "stub_che", fund_idx),
    ncash   = .stub_or_na(fund_dt, "stub_net_cash", fund_idx),
    ndp_num = .stub_or_na(fund_dt, "stub_ndp_num", fund_idx)
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

  # -- Augment daily layer with split-adjusted trailing-TTM EPS + P/E -----------
  # Existence-gated (no-op unless ttm_eps.R is sourced), mirroring the pipeline's
  # build_all_features hook. Runs once, after every _daily parquet is (re)built,
  # so the analytics ingest's eps_ttm/pe_ttm columns survive a full rebuild rather
  # than needing a manual augment_daily_ttm() call. Split factors are cached
  # (cache/splits), so re-runs hit no network. See ttm_eps.R.
  if (exists("augment_daily_ttm", mode = "function")) {
    message("\n  augmenting daily layer: eps_ttm / pe_ttm (split-adjusted)...")
    tryCatch(augment_daily_ttm(fund_dir = fund_dir, ts_dir = ts_dir),
             error = function(e)
               warning(sprintf("augment_daily_ttm failed: %s", e$message), call. = FALSE))
  }

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

  # -- Re-augment daily layer with eps_ttm / pe_ttm (split-adjusted) ------------
  # Existence-gated; keeps the analytics columns fresh after an incremental run.
  # All-ticker (augment_daily_ttm is not per-ticker scoped); cheap on network
  # because split factors are cached, the cost is re-reading/writing the _daily
  # parquets. NOTE: a NEW split is only picked up once cache/splits/{tk}.parquet
  # is refreshed (delete it, or it is fetched fresh for tickers with no cache).
  if (exists("augment_daily_ttm", mode = "function")) {
    message("  augmenting daily layer: eps_ttm / pe_ttm (split-adjusted)...")
    tryCatch(augment_daily_ttm(fund_dir = fund_dir, ts_dir = ts_dir),
             error = function(e)
               warning(sprintf("augment_daily_ttm failed: %s", e$message), call. = FALSE))
  }

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

    # Finalize sector-demeaned indicators (ch_inv_ia): the per-ticker
    # daily layers store the firm-level ingredient; the cross-section
    # is where the sector mean exists. Runs before z-scoring so both
    # raw and z-scored outputs carry the industry-adjusted value.
    industry_adjust_cross_section(cs)

    # Financial-sector mask on the raw cross-section. Fundamental
    # indicators are already NA'd per ticker at compute time, but the
    # price-sensitive net_debt_price is recomputed daily from stubs
    # frozen at fund-layer build -- masking here with the CURRENT
    # sector also covers tickers built before their sector resolved.
    fin_rows <- which(cs$sector %in% "Financial")
    if (length(fin_rows)) {
      for (cn in intersect(.FINANCIAL_NA_INDICATORS, names(cs))) {
        set(cs, i = fin_rows, j = cn, value = NA_real_)
      }
    }
  } else {
    # Without the sector lookup the demean cannot run; a value under
    # the ch_inv_ia name must be the industry-adjusted quantity, so
    # degrade to NA rather than serving the raw ingredient.
    warning("load_daily_cross_section: sector lookup missing; ",
            "sector-demeaned indicators set to NA", call. = FALSE)
    for (cn in intersect(.SECTOR_DEMEAN_INDICATORS, names(cs))) {
      set(cs, j = cn, value = NA_real_)
    }
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
