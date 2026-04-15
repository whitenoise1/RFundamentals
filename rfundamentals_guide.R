# ============================================================================
# rfundamentals_guide.R -- RFundamentals Library Reference
# ============================================================================
#
# Point-in-time fundamental database for S&P 500 constituents.
# 57 indicators x ~500 tickers x daily frequency, from free public data.
#
# This file is the complete reference: function signatures, arguments, and
# copy-paste examples. Run any example line in an interactive R session.
#
# Prerequisites (one-time):
#   Rscript run_timeseries.R build
#
# ============================================================================


# --- Setup ------------------------------------------------------------------
# Source all modules. Run this block once per session.

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
source("R/timeseries_builder.R")


# ============================================================================
# PART 1: MAIN FUNCTIONS
# ============================================================================
#
# The library has 8 user-facing functions. Everything else is internal.
#
# BUILD (run once, then forget):
#   build_timeseries()          Build full historical database
#   update_all_daily()          Append today's data to all tickers
#
# READ (this is what you use daily):
#   load_ticker_timeseries()    One ticker, all dates -> data.table
#   load_daily_cross_section()  All tickers, one date -> data.table
#   get_fundamentals()          Raw SEC filings for one ticker -> data.table
#   list_timeseries_tickers()   What tickers are available -> character vector
#   get_indicator_names()       What indicators exist -> character vector
#
# SINGLE-TICKER BUILD (for debugging or adding a new name):
#   build_ticker_fundamentals() Build fundamentals layer for one ticker
#   update_ticker_daily()       Build/update daily layer for one ticker


# ============================================================================
# PART 2: FUNCTION REFERENCE
# ============================================================================


# --- load_ticker_timeseries() -----------------------------------------------
# Returns: data.table with one row per trading day.
#   Columns: date, price, fiscal_year, filed_date, + 57 indicator columns.
#   Price-sensitive indicators (P/E, P/B, market_cap, ...) update daily.
#   Fundamental indicators (ROE, margins, ...) carry forward until next filing.
#
# Arguments:
#   ticker   Character. Ticker symbol. Required.
#   from     Date or character. Start date filter. Optional, default NULL (all).
#   to       Date or character. End date filter. Optional, default NULL (all).
#   ts_dir   Character. Directory with daily parquet files.
#            Default "cache/timeseries".


# --- load_daily_cross_section() ---------------------------------------------
# Returns: list with two elements:
#   $raw      data.table [tickers x indicators]. One row per ticker.
#             Columns: date, ticker, sector, industry, price, fiscal_year,
#             filed_date, + 57 indicator columns.
#   $zscored  data.table [tickers x indicators]. Cross-sectional z-scores.
#             Mean ~0, SD ~1, winsorized at [-3, 3].
#   If zscore=FALSE, returns the raw data.table directly (not a list).
#
# Arguments:
#   target_date  Date or character. The date to load. Required.
#   zscore       Logical. Compute z-scored version? Default TRUE.
#   ts_dir       Character. Directory with daily parquet files.
#                Default "cache/timeseries".
#   sector_path  Character. Path to sector lookup parquet.
#                Default "cache/lookups/sector_industry.parquet".


# --- get_fundamentals() -----------------------------------------------------
# Returns: data.table in long format (one row per data point from SEC EDGAR).
#   Columns: concept, value, period_end, filed, form, fiscal_year, period_type.
#   This is the raw material before any indicator computation.
#
# Arguments:
#   ticker     Character. Ticker symbol. Required.
#   cik        Character or NULL. 10-digit CIK. Optional (auto-detected).
#   cache_dir  Character. Fundamentals cache directory.
#              Default "cache/fundamentals".


# --- list_timeseries_tickers() ----------------------------------------------
# Returns: character vector of ticker symbols that have daily data available.
#
# Arguments:
#   ts_dir  Character. Directory with daily parquet files.
#           Default "cache/timeseries".


# --- get_indicator_names() --------------------------------------------------
# Returns: character vector of 57 indicator names in canonical order.
#   Groups: valuation (8), profitability (6), growth (5), leverage (5),
#   efficiency (3), cash flow quality (3), shareholder return (3), size (3),
#   tier 1 research (15), tier 2 research (6).
#
# Arguments: none.


# --- build_timeseries() -----------------------------------------------------
# Returns: invisible list of stats (n_fund_built, n_daily_ok, n_fail, ...).
#   Side effect: writes {ticker}_fund.parquet and {ticker}_daily.parquet
#   for every ticker in cache/timeseries/. Resumable -- skips existing files.
#
# Arguments:
#   start_date   Character. Earliest date. Default "2010-01-04".
#   end_date     Date. Latest date. Default Sys.Date().
#   tickers      Character vector or NULL. Specific tickers to process.
#                Default NULL (all tickers with cached fundamentals).
#   master_path  Character. Path to constituent_master.parquet.
#   sector_path  Character. Path to sector_industry.parquet.
#   fund_dir     Character. Fundamentals cache dir. Default "cache/fundamentals".
#   price_dir    Character. Price cache dir. Default "cache/prices".
#   ts_dir       Character. Output dir. Default "cache/timeseries".


# --- update_all_daily() -----------------------------------------------------
# Returns: invisible list of stats (n_updated, n_fail, elapsed_min).
#   Side effect: appends new trading days to all existing daily parquet files.
#
# Arguments:
#   through_date    Date. Update through this date. Default Sys.Date().
#   refresh_prices  Logical. Re-download Yahoo prices? Default TRUE.
#   ts_dir          Character. Time series dir. Default "cache/timeseries".
#   price_dir       Character. Price cache dir. Default "cache/prices".


# --- build_ticker_fundamentals() --------------------------------------------
# Returns: data.table (one row per fiscal year) with fundamental indicators
#   and accounting stubs. NULL on failure.
#
# Arguments:
#   ticker    Character. Ticker symbol. Required.
#   cik       Character. 10-digit CIK. Required.
#   sector    Character. Sector classification. Required.
#   force     Logical. Rebuild even if file exists? Default FALSE.
#   fund_dir  Character. Fundamentals cache dir. Default "cache/fundamentals".
#   ts_dir    Character. Output dir. Default "cache/timeseries".


# --- update_ticker_daily() --------------------------------------------------
# Returns: data.table (full daily layer for this ticker). NULL on failure.
#
# Arguments:
#   ticker         Character. Ticker symbol. Required.
#   through_date   Date. Update through this date. Default Sys.Date().
#   start_date     Date. Earliest date to compute. Default 2010-01-04.
#   refresh_price  Logical. Delete and re-fetch price cache? Default FALSE.
#   price_dir      Character. Price cache dir. Default "cache/prices".
#   ts_dir         Character. Time series dir. Default "cache/timeseries".


# ============================================================================
# PART 3: EXAMPLES
# ============================================================================
# Each example is one line. The comment above it says what you get.
# All results are data.tables ready for immediate use.


# --- What is available? -----------------------------------------------------

# character vector of ~500 ticker symbols with daily data
list_timeseries_tickers()

# character vector of 57 indicator names
get_indicator_names()


# --- Single-stock time series -----------------------------------------------

# AAPL daily data.table: date, price, pe_trailing, roe, ... (57 indicators)
aapl <- load_ticker_timeseries("AAPL")

# Same, filtered to 2024 onward
aapl_recent <- load_ticker_timeseries("AAPL", from = "2024-01-01")

# Last row = most recent trading day
tail(aapl, 1)

# Plot P/E over time (base R)
plot(aapl$date, aapl$pe_trailing, type = "l", main = "AAPL trailing P/E")

# When did each fiscal year become the active filing?
aapl[, .(first_active = min(date), last_active = max(date)), by = fiscal_year]

# Year-over-year ROE change
aapl[format(date, "%m-%d") == "01-02", .(date, roe, revenue_growth_yoy)]


# --- Cross-section for a single date ---------------------------------------

# All ~500 tickers on one date. Returns list: $raw and $zscored data.tables.
#cs <- load_daily_cross_section("2024-06-28")
cs <- load_daily_cross_section("2026-03-31")

# The raw factor matrix: one row per ticker, 57 indicator columns
cs$raw

# The z-scored version: cross-sectional z-scores, ready for a factor model
cs$zscored

# How many tickers and indicators?
sprintf("%d tickers x %d indicators", nrow(cs$raw), length(get_indicator_names()))


# --- Factor screens (filter the cross-section) -----------------------------

# Cheapest 10 stocks by earnings yield
cs$raw[order(-earnings_yield)][1:10, .(ticker, sector, earnings_yield, pe_trailing)]

# Highest quality: top 10 ROE with moderate leverage
cs$raw[debt_equity < 2][order(-roe)][1:10, .(ticker, sector, roe, roa, debt_equity)]

# Fastest growers: top 10 revenue growth
cs$raw[order(-revenue_growth_yoy)][1:10, .(ticker, sector, revenue_growth_yoy)]

# Piotroski value picks: F-score >= 8 AND cheap (bottom quartile P/E)
cs$raw[f_score >= 8 & pe_trailing < quantile(pe_trailing, 0.25, na.rm = TRUE),
       .(ticker, sector, f_score, pe_trailing, roe)]

# Sector median P/E
cs$raw[, .(median_pe = median(pe_trailing, na.rm = TRUE)), by = sector][order(median_pe)]


# --- Feed a factor model ----------------------------------------------------

# Z-scored matrix as input to your model (rows = tickers, cols = factors)
z_matrix <- as.matrix(cs$zscored[, -"ticker"])
rownames(z_matrix) <- cs$zscored$ticker

# Subset to specific factors
value_factors <- cs$zscored[, .(ticker, earnings_yield, pb, ps, pfcf)]

# Merge z-scores with raw metadata for labeled output
model_input <- merge(cs$raw[, .(ticker, sector, industry)], cs$zscored, by = "ticker")

# Sector-level factor profile: median z-score per indicator, sorted by sector
sector_profile <- model_input[, lapply(.SD, median, na.rm = TRUE),
                              by = sector, .SDcols = get_indicator_names()][order(sector)]

View(sector_profile)

industry_profile <- model_input[, lapply(.SD, median, na.rm = TRUE),
                              by = industry, .SDcols = get_indicator_names()][order(industry)]

View(industry_profile)

# --- Raw SEC filings --------------------------------------------------------

# Long-format XBRL data for one ticker (before any indicator computation)
aapl_sec <- get_fundamentals("AAPL")

# What concepts are reported?
unique(aapl_sec$concept)

# Revenue by fiscal year
aapl_sec[concept == "revenue" & period_type == "FY", .(fiscal_year, value=value/1000000, filed)]


# --- Compare two stocks -----------------------------------------------------

# Side-by-side: load two tickers, pick a date, compare
msft <- load_ticker_timeseries("MSFT", from = "2024-01-01")
nvda <- load_ticker_timeseries("NVDA", from = "2024-01-01")

# Most recent indicators for each
rbind(tail(aapl_recent, 1)[, .(ticker = "AAPL", pe_trailing, roe, revenue_growth_yoy)],
      tail(msft, 1)[, .(ticker = "MSFT", pe_trailing, roe, revenue_growth_yoy)],
      tail(nvda, 1)[, .(ticker = "NVDA", pe_trailing, roe, revenue_growth_yoy)])


# --- Build / update (run once, then daily) ----------------------------------

# Initial historical build: all tickers, 2010 to today (~5 min, resumable)
# build_timeseries()

# Daily update: refresh prices, append new rows (~1-2 min)
# update_all_daily()

# Single ticker: build or update just one name
# update_ticker_daily("AAPL", refresh_price = TRUE)


# ============================================================================
# INTERNAL HELPERS (for reference, not for direct use)
# ============================================================================
#
# These are prefixed with a dot (.) or are intermediate pipeline functions.
# You should never need to call them directly.
#
# constituent_master.R:
#   .assert_output, .pad_cik, .edgar_fetch
#   load_raw_constituents, resolve_all_ciks, classify_duplicates,
#   assign_status, build_constituent_master
#
# fundamental_fetcher.R:
#   .assert_output, .pad_cik, .edgar_fetch
#   fetch_companyfacts, parse_companyfacts, dedup_fundamentals,
#   classify_period, fetch_and_cache_ticker, fetch_fundamentals_batch,
#   coverage_report, build_fundamentals, validate_fundamentals_build,
#   prototype_20_tickers
#
# sector_classifier.R:
#   .assert_output, .finviz_fetch_one, .extract_finviz_sector_industry,
#   .decode_html_entities, .write_sector_cache
#   load_sector_fallback, load_cached_sectors, fetch_finviz_sectors,
#   build_sector_industry, get_sector_lookup
#
# indicator_compute.R:
#   .assert_output, .safe_divide, .safe_growth, .winsorize, .col,
#   .derive_quantities, .compute_valuation, .compute_profitability,
#   .compute_growth, .compute_leverage, .compute_efficiency,
#   .compute_cashflow_quality, .compute_shareholder, .compute_tier1,
#   .compute_piotroski, .compute_tier2
#   pivot_fundamentals, zscore_cross_section, compute_ticker_indicators,
#   compute_cross_section
#
# pit_assembler.R:
#   .assert_output, .generate_snapshot_dates, .prefetch_universe_prices
#   get_universe_at_date, fetch_ticker_prices, get_price_on_date,
#   get_latest_annual_filing, assemble_snapshot,
#   build_historical_snapshots, load_snapshot
#
# pipeline_runner.R:
#   .assert_output, .check_prerequisite
#   run_full_build, run_daily_update, validate_snapshot,
#   summarize_coverage, list_snapshots
#
# timeseries_builder.R:
#   .assert_output_ts, .extract_stubs, .compute_price_sensitive_vec
