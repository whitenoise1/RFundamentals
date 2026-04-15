# ============================================================================
# test_timeseries_builder.R  --  Unit + Gate Tests for Module 7
# ============================================================================
# Run: Rscript tests/test_timeseries_builder.R
#
# Structure:
#   1. Unit tests: constants, vectorized computation, edge cases, PIT lookup
#   2. Gate tests: integration with real cached data (AAPL, JPM), consistency
#      with indicator_compute.R, incremental update, cross-section
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
source("R/timeseries_builder.R")

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

all_indicators <- get_indicator_names()

test("price-sensitive + fundamental-only = all indicators",
     length(.PRICE_SENSITIVE_INDICATORS) + length(.FUNDAMENTAL_INDICATORS) ==
       length(all_indicators))

test("no overlap between price-sensitive and fundamental-only",
     length(intersect(.PRICE_SENSITIVE_INDICATORS, .FUNDAMENTAL_INDICATORS)) == 0)

test("all price-sensitive are valid indicator names",
     all(.PRICE_SENSITIVE_INDICATORS %in% all_indicators))

test("all fundamental-only are valid indicator names",
     all(.FUNDAMENTAL_INDICATORS %in% all_indicators))

test("12 price-sensitive indicators",
     length(.PRICE_SENSITIVE_INDICATORS) == 12)

test("45 fundamental-only indicators",
     length(.FUNDAMENTAL_INDICATORS) == 45)

test("11 stubs (no unused stubs)",
     length(.STUB_NAMES) == 11)

test("all stub names start with stub_",
     all(grepl("^stub_", .STUB_NAMES)))

test("payout_ratio is fundamental-only (uses NI, not price)",
     "payout_ratio" %in% .FUNDAMENTAL_INDICATORS)

test("revenue_raw is fundamental-only",
     "revenue_raw" %in% .FUNDAMENTAL_INDICATORS)


# -- 2. Vectorized price-sensitive computation: known values --
message("\n=== Unit: Price-Sensitive Known Values ===")

ps_result <- .compute_price_sensitive_vec(
  p       = 150,
  shares  = 1e9,
  eps     = 6.0,
  equity  = 50e9,
  rev     = 400e9,
  fcf_v   = 100e9,
  ebitda  = 130e9,
  td      = 120e9,
  nd      = 70e9,
  div     = -15e9,
  buy     = -80e9,
  eps_g   = 0.12
)

test("market_cap = 150 * 1e9 = 150e9",
     abs(ps_result$market_cap - 150e9) < 1)

test("enterprise_value = 150e9 + 70e9 = 220e9",
     abs(ps_result$enterprise_value - 220e9) < 1)

test("pe_trailing = 150 / 6 = 25",
     abs(ps_result$pe_trailing - 25) < 0.01)

test("peg = 25 / (0.12 * 100) = 2.083",
     abs(ps_result$peg - (25 / 12)) < 0.01)

test("pb = 150e9 / 50e9 = 3.0",
     abs(ps_result$pb - 3.0) < 0.01)

test("ps = 150e9 / 400e9 = 0.375",
     abs(ps_result$ps - 0.375) < 0.001)

test("pfcf = 150e9 / 100e9 = 1.5",
     abs(ps_result$pfcf - 1.5) < 0.01)

test("ev_ebitda = 220e9 / 130e9",
     abs(ps_result$ev_ebitda - (220e9 / 130e9)) < 0.001)

test("ev_revenue = 220e9 / 400e9",
     abs(ps_result$ev_revenue - (220e9 / 400e9)) < 0.001)

test("earnings_yield = 6 / 150",
     abs(ps_result$earnings_yield - (6.0 / 150)) < 0.0001)

test("dividend_yield = 15e9 / 150e9",
     abs(ps_result$dividend_yield - (15e9 / 150e9)) < 0.0001)

test("buyback_yield = 80e9 / 150e9",
     abs(ps_result$buyback_yield - (80e9 / 150e9)) < 0.0001)

test("returns data.table with 12 columns",
     is.data.table(ps_result) && ncol(ps_result) == 12)


# -- 3. Vectorized: multiple elements --
message("\n=== Unit: Price-Sensitive Vectorized ===")

ps_vec <- .compute_price_sensitive_vec(
  p       = c(100, 200, NA),
  shares  = c(1e6, 2e6, 3e6),
  eps     = c(5, 10, 8),
  equity  = c(50e6, 100e6, 80e6),
  rev     = c(200e6, 500e6, 300e6),
  fcf_v   = c(20e6, 50e6, 30e6),
  ebitda  = c(30e6, 80e6, 60e6),
  td      = c(10e6, 20e6, 15e6),
  nd      = c(5e6, 10e6, 8e6),
  div     = c(-2e6, -5e6, -3e6),
  buy     = c(-1e6, -3e6, -2e6),
  eps_g   = c(0.1, 0.2, 0.15)
)

test("vectorized: 3 rows returned",
     nrow(ps_vec) == 3)

test("vectorized: NA price -> NA market_cap",
     is.na(ps_vec$market_cap[3]))

test("vectorized: NA price -> NA for all 12 indicators",
     all(is.na(as.numeric(ps_vec[3]))))

test("vectorized: row 1 market_cap = 100 * 1e6",
     abs(ps_vec$market_cap[1] - 100e6) < 1)

test("vectorized: row 2 market_cap = 200 * 2e6",
     abs(ps_vec$market_cap[2] - 400e6) < 1)

test("vectorized: row 1 PE = 100/5 = 20",
     abs(ps_vec$pe_trailing[1] - 20) < 0.01)

test("vectorized: row 2 PE = 200/10 = 20",
     abs(ps_vec$pe_trailing[2] - 20) < 0.01)


# -- 4. Edge cases --
message("\n=== Unit: Price-Sensitive Edge Cases ===")

ps_edge <- .compute_price_sensitive_vec(
  p       = c(100, 100, 100, 100),
  shares  = c(NA, 1e6, 1e6, 1e6),
  eps     = c(5, 0.001, -2, 5),
  equity  = c(50e6, -10e6, 0, 50e6),
  rev     = c(0, 200e6, 200e6, 200e6),
  fcf_v   = c(20e6, -5e6, 0, 20e6),
  ebitda  = c(30e6, -10e6, 0, 30e6),
  td      = c(10e6, 20e6, 15e6, NA),
  nd      = c(NA, 10e6, 8e6, NA),
  div     = c(-2e6, NA, -3e6, -2e6),
  buy     = c(-1e6, -3e6, NA, -1e6),
  eps_g   = c(0, -0.1, 0.15, 0.15)
)

test("edge: NA shares -> NA market_cap",
     is.na(ps_edge$market_cap[1]))

test("edge: tiny eps (|eps| < 0.01) -> NA pe",
     is.na(ps_edge$pe_trailing[2]))

test("edge: negative equity -> NA pb",
     is.na(ps_edge$pb[2]))

test("edge: zero equity -> NA pb",
     is.na(ps_edge$pb[3]))

test("edge: negative fcf -> NA pfcf",
     is.na(ps_edge$pfcf[2]))

test("edge: zero fcf -> NA pfcf",
     is.na(ps_edge$pfcf[3]))

test("edge: negative ebitda -> NA ev_ebitda",
     is.na(ps_edge$ev_ebitda[2]))

test("edge: zero ebitda -> NA ev_ebitda",
     is.na(ps_edge$ev_ebitda[3]))

test("edge: zero growth -> NA peg",
     is.na(ps_edge$peg[1]))

test("edge: negative growth -> NA peg",
     is.na(ps_edge$peg[2]))

test("edge: NA dividends -> NA div_yield",
     is.na(ps_edge$dividend_yield[2]))

test("edge: NA buybacks -> NA bb_yield",
     is.na(ps_edge$buyback_yield[3]))

test("edge: NA net_debt + NA total_debt -> NA EV",
     is.na(ps_edge$enterprise_value[4]))

test("edge: zero revenue -> NA ps",
     is.na(ps_edge$ps[1]))


# -- 5. EV fallback chain --
message("\n=== Unit: EV Fallback Chain ===")

ev_test <- .compute_price_sensitive_vec(
  p       = c(100, 100, 100),
  shares  = c(1e6, 1e6, 1e6),
  eps     = c(5, 5, 5),
  equity  = c(50e6, 50e6, 50e6),
  rev     = c(200e6, 200e6, 200e6),
  fcf_v   = c(20e6, 20e6, 20e6),
  ebitda  = c(30e6, 30e6, 30e6),
  td      = c(20e6, 20e6, NA),
  nd      = c(10e6, NA, NA),
  div     = c(-2e6, -2e6, -2e6),
  buy     = c(-1e6, -1e6, -1e6),
  eps_g   = c(0.1, 0.1, 0.1)
)

test("EV fallback: net_debt available -> mc + nd",
     abs(ev_test$enterprise_value[1] - (100e6 + 10e6)) < 1)

test("EV fallback: nd NA, td available -> mc + td",
     abs(ev_test$enterprise_value[2] - (100e6 + 20e6)) < 1)

test("EV fallback: nd NA, td NA -> NA",
     is.na(ev_test$enterprise_value[3]))


# -- 6. Point-in-time lookup: non-monotonic filed dates --
message("\n=== Unit: PIT Lookup (Non-Monotonic Filed Dates) ===")

# Simulate: FY2022 amended AFTER FY2023 was filed
# FY2021 filed 2022-02-20, FY2022 filed 2024-07-20 (amendment),
# FY2023 filed 2024-02-15
pit_fund <- data.table(
  fiscal_year = c(2021L, 2022L, 2023L),
  filed_date  = as.Date(c("2022-02-20", "2024-07-20", "2024-02-15")),
  period_end  = as.Date(c("2021-12-31", "2022-12-31", "2023-12-31")),
  roe         = c(0.10, 0.12, 0.15),
  roa         = c(0.05, 0.06, 0.08),
  stub_shares = c(1e9, 1e9, 1e9),
  stub_eps    = c(3, 4, 5),
  stub_equity = c(30e9, 35e9, 40e9),
  stub_revenue = c(200e9, 250e9, 300e9),
  stub_fcf    = c(20e9, 25e9, 30e9),
  stub_ebitda = c(30e9, 35e9, 40e9),
  stub_total_debt = c(50e9, 55e9, 60e9),
  stub_net_debt = c(30e9, 33e9, 35e9),
  stub_cash   = c(20e9, 22e9, 25e9),
  stub_dividends = c(-5e9, -6e9, -7e9),
  stub_buybacks = c(-3e9, -4e9, -5e9),
  eps_growth_yoy = c(NA, 0.10, 0.15)
)

# Also need the fundamental-only indicators as columns
for (col in .FUNDAMENTAL_INDICATORS) {
  if (!(col %in% names(pit_fund))) {
    pit_fund[, (col) := NA_real_]
  }
}

# Write temp fund file
pit_ts_dir <- tempfile("pit_test_")
dir.create(pit_ts_dir)
arrow::write_parquet(pit_fund, file.path(pit_ts_dir, "TEST_fund.parquet"))

# Create minimal price xts
pit_dates <- as.Date(c("2023-01-15", "2024-03-01", "2024-08-01"))
pit_prices <- c(100, 150, 180)
pit_xts <- xts(matrix(rep(pit_prices, 6), ncol = 6), order.by = pit_dates)

# Save price cache so fetch_ticker_prices returns it
price_cache_dir <- file.path(pit_ts_dir, "prices")
dir.create(price_cache_dir)
pit_price_df <- data.frame(date = as.character(pit_dates),
                            V1 = pit_prices, V2 = pit_prices, V3 = pit_prices,
                            V4 = pit_prices, V5 = pit_prices, V6 = pit_prices)
arrow::write_parquet(pit_price_df,
                     file.path(price_cache_dir, "TEST_yahoo_2009-01-01.parquet"))

# Run update
pit_daily <- update_ticker_daily("TEST",
                                  through_date = as.Date("2024-12-31"),
                                  start_date   = as.Date("2023-01-01"),
                                  price_dir    = price_cache_dir,
                                  ts_dir       = pit_ts_dir)

test("PIT: daily produced",
     !is.null(pit_daily) && nrow(pit_daily) > 0)

if (!is.null(pit_daily) && nrow(pit_daily) > 0) {
  # 2023-01-15: only FY2021 filed (2022-02-20 <= 2023-01-15). Should use FY2021.
  row_2023 <- pit_daily[date == as.Date("2023-01-15")]
  test("PIT 2023-01-15: uses FY2021 (only one available)",
       nrow(row_2023) == 1 && row_2023$fiscal_year == 2021)

  # 2024-03-01: FY2023 filed 2024-02-15 <= 2024-03-01. FY2022 filed 2024-07-20 > 2024-03-01.
  # Should use FY2023 (highest available FY).
  row_2024a <- pit_daily[date == as.Date("2024-03-01")]
  test("PIT 2024-03-01: uses FY2023 (not FY2022 amendment)",
       nrow(row_2024a) == 1 && row_2024a$fiscal_year == 2023)

  # 2024-08-01: FY2023 filed 2024-02-15, FY2022 amendment filed 2024-07-20.
  # Both available. Should use FY2023 (highest FY), NOT FY2022 amendment.
  row_2024b <- pit_daily[date == as.Date("2024-08-01")]
  test("PIT 2024-08-01: uses FY2023 (highest FY, ignores prior-year amendment)",
       nrow(row_2024b) == 1 && row_2024b$fiscal_year == 2023)

  # Verify indicators match the correct FY
  test("PIT 2024-03-01: ROE from FY2023 = 0.15",
       nrow(row_2024a) == 1 && !is.na(row_2024a$roe) && abs(row_2024a$roe - 0.15) < 1e-10)

  test("PIT 2024-08-01: ROE from FY2023 = 0.15 (not FY2022 = 0.12)",
       nrow(row_2024b) == 1 && !is.na(row_2024b$roe) && abs(row_2024b$roe - 0.15) < 1e-10)

  # Price-sensitive should use correct stubs
  test("PIT 2024-03-01: market_cap uses FY2023 shares",
       nrow(row_2024a) == 1 && !is.na(row_2024a$market_cap) &&
         abs(row_2024a$market_cap - 150 * 1e9) < 1)
}

unlink(pit_ts_dir, recursive = TRUE)


# ============================================================================
# GATE TESTS (integration with real cached data)
# ============================================================================

message("\n=== Gate: Prerequisites ===")

master_exists <- file.exists("cache/lookups/constituent_master.parquet")
sector_exists <- file.exists("cache/lookups/sector_industry.parquet")

test("constituent_master.parquet exists", master_exists)
test("sector_industry.parquet exists", sector_exists)

if (!master_exists || !sector_exists) {
  message("  SKIP gate tests (lookups not available)")
} else {

  master  <- as.data.table(arrow::read_parquet(
    "cache/lookups/constituent_master.parquet"))
  sectors <- as.data.table(arrow::read_parquet(
    "cache/lookups/sector_industry.parquet"))


  # -- Gate 1: AAPL full pipeline --
  message("\n=== Gate 1: AAPL Full Pipeline ===")

  aapl_cik <- master[ticker == "AAPL"]$cik[1]
  aapl_sec <- sectors[ticker == "AAPL"]$sector[1]
  fund_exists <- length(list.files("cache/fundamentals",
                                    pattern = "AAPL\\.parquet$")) > 0

  if (!fund_exists) {
    message("  SKIP (AAPL fundamentals not cached)")
  } else {

    test_ts_dir <- tempfile("gate1_")
    dir.create(test_ts_dir)

    # Build fundamentals layer
    fund <- build_ticker_fundamentals("AAPL", aapl_cik,
                                       if (is.na(aapl_sec)) "Technology" else aapl_sec,
                                       ts_dir = test_ts_dir)

    test("AAPL fund: not NULL", !is.null(fund))
    test("AAPL fund: is data.table", is.data.table(fund))
    test("AAPL fund: has fiscal_year", "fiscal_year" %in% names(fund))
    test("AAPL fund: has filed_date", "filed_date" %in% names(fund))
    test("AAPL fund: has all stubs", all(.STUB_NAMES %in% names(fund)))
    test("AAPL fund: has fundamental indicators",
         sum(.FUNDAMENTAL_INDICATORS %in% names(fund)) > 40)
    test("AAPL fund: multiple fiscal years", nrow(fund) >= 5)
    test("AAPL fund: fiscal years sorted", !is.unsorted(fund$fiscal_year))
    test("AAPL fund: parquet written",
         file.exists(file.path(test_ts_dir, "AAPL_fund.parquet")))

    # Stub plausibility
    latest <- fund[nrow(fund)]
    test("AAPL stubs: shares > 0",
         !is.na(latest$stub_shares) && latest$stub_shares > 0)
    test("AAPL stubs: revenue > 0",
         !is.na(latest$stub_revenue) && latest$stub_revenue > 0)
    test("AAPL stubs: eps is numeric",
         !is.na(latest$stub_eps))

    # Fundamental indicator density
    fund_ind_cols <- intersect(.FUNDAMENTAL_INDICATORS, names(fund))
    na_rate <- mean(is.na(as.matrix(latest[, ..fund_ind_cols])))
    test("AAPL fund: latest row <50% NA", na_rate < 0.5)

    # Build daily layer (limited range)
    daily <- update_ticker_daily("AAPL",
                                  through_date = as.Date("2024-12-31"),
                                  start_date   = as.Date("2024-01-01"),
                                  ts_dir       = test_ts_dir)

    test("AAPL daily: not NULL", !is.null(daily))
    test("AAPL daily: is data.table", is.data.table(daily))
    test("AAPL daily: has date", "date" %in% names(daily))
    test("AAPL daily: has price", "price" %in% names(daily))
    test("AAPL daily: has market_cap", "market_cap" %in% names(daily))
    test("AAPL daily: has pe_trailing", "pe_trailing" %in% names(daily))
    test("AAPL daily: has roe (fundamental)", "roe" %in% names(daily))

    if (!is.null(daily) && nrow(daily) > 0) {
      test("AAPL daily: >100 trading days in 2024", nrow(daily) > 100)
      test("AAPL daily: dates sorted", !is.unsorted(daily$date))
      test("AAPL daily: no duplicate dates", !anyDuplicated(daily$date))
      test("AAPL daily: prices are positive", all(daily$price > 0, na.rm = TRUE))
      test("AAPL daily: market_cap > 0", all(daily$market_cap > 0, na.rm = TRUE))
      test("AAPL daily: parquet written",
           file.exists(file.path(test_ts_dir, "AAPL_daily.parquet")))

      # Fundamental stability: same FY -> identical fundamental indicators
      if (nrow(daily) >= 2) {
        same_fy <- daily[fiscal_year == daily$fiscal_year[1]]
        if (nrow(same_fy) >= 2) {
          d1 <- same_fy[1]
          d2 <- same_fy[nrow(same_fy)]
          test("AAPL daily: same FY -> identical ROE",
               (is.na(d1$roe) && is.na(d2$roe)) ||
                 (!is.na(d1$roe) && !is.na(d2$roe) &&
                    abs(d1$roe - d2$roe) < 1e-10))
          test("AAPL daily: same FY -> different market_cap (price varies)",
               is.na(d1$market_cap) || is.na(d2$market_cap) ||
                 abs(d1$market_cap - d2$market_cap) > 1)
        }
      }

      # Incremental update idempotence
      daily2 <- update_ticker_daily("AAPL",
                                     through_date = as.Date("2024-12-31"),
                                     start_date   = as.Date("2024-01-01"),
                                     ts_dir       = test_ts_dir)
      test("AAPL incremental: same row count", nrow(daily2) == nrow(daily))

      # Extend: add more dates
      daily3 <- update_ticker_daily("AAPL",
                                     through_date = as.Date("2025-03-31"),
                                     start_date   = as.Date("2024-01-01"),
                                     ts_dir       = test_ts_dir)
      test("AAPL extend: more rows after extending range",
           nrow(daily3) > nrow(daily))
      test("AAPL extend: original dates preserved",
           all(daily$date %in% daily3$date))
    }

    # Reader functions
    ts <- load_ticker_timeseries("AAPL", ts_dir = test_ts_dir)
    test("load_ticker_timeseries: returns data", !is.null(ts) && nrow(ts) > 0)

    ts_filtered <- load_ticker_timeseries("AAPL", ts_dir = test_ts_dir,
                                           from = "2024-06-01", to = "2024-06-30")
    test("load_ticker_timeseries: date filter works",
         !is.null(ts_filtered) && nrow(ts_filtered) < nrow(ts))

    tickers_list <- list_timeseries_tickers(ts_dir = test_ts_dir)
    test("list_timeseries_tickers: finds AAPL", "AAPL" %in% tickers_list)

    unlink(test_ts_dir, recursive = TRUE)
  }


  # -- Gate 2: JPM (Financial sector) --
  message("\n=== Gate 2: JPM (Financial Sector) ===")

  jpm_cik <- master[ticker == "JPM"]$cik[1]
  jpm_sec <- sectors[ticker == "JPM"]$sector[1]
  jpm_fund_exists <- length(list.files("cache/fundamentals",
                                        pattern = "JPM\\.parquet$")) > 0

  if (!jpm_fund_exists) {
    message("  SKIP (JPM fundamentals not cached)")
  } else {

    test_ts_dir <- tempfile("gate2_")
    dir.create(test_ts_dir)

    fund_jpm <- build_ticker_fundamentals("JPM", jpm_cik,
                                           if (is.na(jpm_sec)) "Financial" else jpm_sec,
                                           ts_dir = test_ts_dir)

    test("JPM fund: not NULL", !is.null(fund_jpm))

    if (!is.null(fund_jpm)) {
      # Financial NA indicators should be NA
      latest_jpm <- fund_jpm[nrow(fund_jpm)]
      test("JPM: gpa is NA (financial sector)",
           is.na(latest_jpm$gpa))
      test("JPM: inventory_turnover is NA (financial sector)",
           is.na(latest_jpm$inventory_turnover))

      daily_jpm <- update_ticker_daily("JPM",
                                        through_date = as.Date("2024-06-30"),
                                        start_date   = as.Date("2024-01-01"),
                                        ts_dir       = test_ts_dir)

      test("JPM daily: not NULL", !is.null(daily_jpm))
      if (!is.null(daily_jpm) && nrow(daily_jpm) > 0) {
        test("JPM daily: gpa stays NA in daily",
             all(is.na(daily_jpm$gpa)))
        test("JPM daily: market_cap is computed",
             !all(is.na(daily_jpm$market_cap)))
      }
    }

    unlink(test_ts_dir, recursive = TRUE)
  }


  # -- Gate 3: Consistency with compute_ticker_indicators --
  message("\n=== Gate 3: Two-Layer vs Original Consistency ===")

  if (fund_exists) {

    fund_dt_raw <- get_fundamentals("AAPL", aapl_cik)
    test_price <- 200.0
    test_fy <- max(fund_dt_raw[!is.na(fiscal_year) & period_type == "FY"]$fiscal_year,
                   na.rm = TRUE)

    # Original full computation
    original <- compute_ticker_indicators(fund_dt_raw, price_on_filed = test_price,
                                           sector = aapl_sec, target_fy = test_fy)

    # Two-layer approach: stubs + recompute
    wide <- pivot_fundamentals(fund_dt_raw)
    wide <- .derive_quantities(wide)
    stubs <- .extract_stubs(wide, test_fy)

    fund_only_result <- compute_ticker_indicators(fund_dt_raw, price_on_filed = NA_real_,
                                                   sector = aapl_sec, target_fy = test_fy)

    recomputed <- .compute_price_sensitive_vec(
      p       = test_price,
      shares  = stubs$stub_shares,
      eps     = stubs$stub_eps,
      equity  = stubs$stub_equity,
      rev     = stubs$stub_revenue,
      fcf_v   = stubs$stub_fcf,
      ebitda  = stubs$stub_ebitda,
      td      = stubs$stub_total_debt,
      nd      = stubs$stub_net_debt,
      div     = stubs$stub_dividends,
      buy     = stubs$stub_buybacks,
      eps_g   = fund_only_result[["eps_growth_yoy"]]
    )

    # All 12 price-sensitive indicators must match
    for (ind in .PRICE_SENSITIVE_INDICATORS) {
      orig_val <- original[[ind]]
      recomp_val <- recomputed[[ind]]

      match <- (is.na(orig_val) && is.na(recomp_val)) ||
        (!is.na(orig_val) && !is.na(recomp_val) &&
           abs(orig_val - recomp_val) < abs(orig_val) * 1e-6 + 1e-10)

      test(sprintf("consistency: %s", ind), match)
    }

    # All 45 fundamental-only indicators must match
    for (ind in .FUNDAMENTAL_INDICATORS) {
      orig_val <- original[[ind]]
      fund_val <- fund_only_result[[ind]]

      match <- (is.na(orig_val) && is.na(fund_val)) ||
        (!is.na(orig_val) && !is.na(fund_val) &&
           abs(orig_val - fund_val) < abs(orig_val) * 1e-6 + 1e-10)

      test(sprintf("fundamental: %s", ind), match)
    }
  }


  # -- Gate 4: Fund layer cache hit --
  message("\n=== Gate 4: Fund Layer Cache ===")

  if (fund_exists) {
    test_ts_dir <- tempfile("gate4_")
    dir.create(test_ts_dir)

    # First build
    t0 <- Sys.time()
    fund_first <- build_ticker_fundamentals("AAPL", aapl_cik,
                                             if (is.na(aapl_sec)) "Technology" else aapl_sec,
                                             ts_dir = test_ts_dir, force = FALSE)
    t_build <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

    # Second call (cache hit)
    t0 <- Sys.time()
    fund_cached <- build_ticker_fundamentals("AAPL", aapl_cik,
                                              if (is.na(aapl_sec)) "Technology" else aapl_sec,
                                              ts_dir = test_ts_dir, force = FALSE)
    t_cache <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

    test("cache hit: returns data", !is.null(fund_cached))
    test("cache hit: same dimensions",
         nrow(fund_cached) == nrow(fund_first) &&
           ncol(fund_cached) == ncol(fund_first))
    test("cache hit: faster than build",
         t_cache < t_build || t_build < 0.5)  # allow for very fast builds

    # Force rebuild
    fund_forced <- build_ticker_fundamentals("AAPL", aapl_cik,
                                              if (is.na(aapl_sec)) "Technology" else aapl_sec,
                                              ts_dir = test_ts_dir, force = TRUE)
    test("force rebuild: returns data", !is.null(fund_forced))
    test("force rebuild: same dimensions",
         nrow(fund_forced) == nrow(fund_first))

    unlink(test_ts_dir, recursive = TRUE)
  }
}


# ============================================================================
# Summary
# ============================================================================
message(sprintf("\n========================================"))
message(sprintf("  RESULTS: %d passed, %d failed (of %d)",
                .n_pass, .n_fail, .n_pass + .n_fail))
message(sprintf("========================================\n"))

if (.n_fail > 0) quit(status = 1)
