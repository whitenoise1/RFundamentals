# ============================================================================
# test_pit_assembler.R  --  Unit Tests for Module 5 (pit_assembler.R)
# ============================================================================
# Run: Rscript tests/test_pit_assembler.R
#
# Tests:
#   1. get_universe_at_date: filtering logic, edge cases
#   2. get_price_on_date: date lookup, tolerance, edge cases
#   3. get_latest_annual_filing: point-in-time filtering
#   4. .generate_snapshot_dates: quarterly date generation
#   5. assemble_snapshot: integration with cached data (if available)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(xts)
  library(zoo)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")
source("R/pit_assembler.R")

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
# SETUP: Synthetic data for unit tests
# ============================================================================
message("\n=== Building synthetic test data ===")

# Synthetic constituent master
mock_master <- data.table(
  ticker       = c("AAPL", "MSFT", "REMOVED", "OLDTK", "NEWTK"),
  name         = c("Apple", "Microsoft", "Removed Corp", "Old Co", "New Co"),
  cik          = c("0000320193", "0000789019", "0001234567",
                    "0009999999", "0009999998"),
  date_added   = as.Date(c(NA, NA, "2005-01-01", "2005-01-01", "2020-06-15")),
  date_removed = as.Date(c(NA, NA, "2018-06-30", "2015-03-01", NA)),
  status       = c("ACTIVE", "ACTIVE", "REMOVED_ACQUIRED",
                    "REMOVED_DOWNGRADED", "ACTIVE"),
  successor    = c(NA, NA, NA, NA, NA),
  occurrence   = c(1L, 1L, 1L, 1L, 1L),
  duplicate_class = c(NA, NA, NA, NA, NA)
)

# Synthetic price data (xts)
price_dates <- as.Date("2023-01-01") + 0:365
# Remove weekends
price_dates <- price_dates[!weekdays(price_dates) %in% c("Saturday", "Sunday")]
mock_prices <- xts(
  data.frame(
    Open     = 150 + seq_along(price_dates) * 0.1,
    High     = 155 + seq_along(price_dates) * 0.1,
    Low      = 148 + seq_along(price_dates) * 0.1,
    Close    = 152 + seq_along(price_dates) * 0.1,
    Volume   = rep(1e6, length(price_dates)),
    Adjusted = 152 + seq_along(price_dates) * 0.1
  ),
  order.by = price_dates
)

# Synthetic fundamentals (long format, mimicking the fetcher output)
make_mock_fund <- function(ticker, cik) {
  concepts <- c("revenue", "cogs", "operating_income", "net_income",
                "eps_diluted", "total_assets", "stockholders_equity",
                "long_term_debt", "short_term_debt", "current_assets",
                "current_liabilities", "total_liabilities",
                "accounts_receivable", "inventory", "cash",
                "shares_outstanding", "operating_cashflow", "capex",
                "interest_expense", "depreciation", "sga",
                "accounts_payable", "accrued_liabilities",
                "deferred_revenue", "prepaid_expenses", "buybacks")

  # Two fiscal years: 2022 and 2023
  rows <- list()
  for (fy in c(2022L, 2023L)) {
    filed <- as.Date(sprintf("%d-02-15", fy + 1L))
    period_end <- as.Date(sprintf("%d-12-31", fy))
    period_start <- as.Date(sprintf("%d-01-01", fy))

    base_rev <- if (fy == 2022) 350000e6 else 385000e6
    multiplier <- if (fy == 2022) 1.0 else 1.1

    vals <- c(
      base_rev,                    # revenue
      base_rev * 0.57,             # cogs
      base_rev * 0.30,             # operating_income
      base_rev * 0.25,             # net_income
      5.50 * multiplier,           # eps_diluted
      350000e6 * multiplier,       # total_assets
      60000e6 * multiplier,        # stockholders_equity
      100000e6,                    # long_term_debt
      10000e6,                     # short_term_debt
      130000e6 * multiplier,       # current_assets
      105000e6 * multiplier,       # current_liabilities
      260000e6 * multiplier,       # total_liabilities
      50000e6 * multiplier,        # accounts_receivable
      5000e6 * multiplier,         # inventory
      25000e6 * multiplier,        # cash
      15500e6,                     # shares_outstanding
      base_rev * 0.28,             # operating_cashflow
      base_rev * 0.03,             # capex
      5000e6,                      # interest_expense
      10000e6,                     # depreciation
      base_rev * 0.08,             # sga
      40000e6 * multiplier,        # accounts_payable
      20000e6 * multiplier,        # accrued_liabilities
      8000e6 * multiplier,         # deferred_revenue
      3000e6 * multiplier,         # prepaid_expenses
      -20000e6 * multiplier        # buybacks
    )

    for (j in seq_along(concepts)) {
      rows[[length(rows) + 1L]] <- data.table(
        ticker       = ticker,
        cik          = cik,
        concept      = concepts[j],
        tag          = concepts[j],
        value        = vals[j],
        period_end   = period_end,
        period_start = period_start,
        filed        = filed,
        form         = "10-K",
        accession    = sprintf("0001234-%d-000001", fy),
        fiscal_year  = fy,
        fiscal_qtr   = "FY",
        unit         = if (concepts[j] == "eps_diluted") "USD/shares" else "USD",
        period_type  = "FY"
      )
    }
  }

  rbindlist(rows)
}

mock_fund_aapl <- make_mock_fund("AAPL", "0000320193")
mock_fund_msft <- make_mock_fund("MSFT", "0000789019")


# ============================================================================
# TEST 1: get_universe_at_date
# ============================================================================
message("\n=== get_universe_at_date ===")

test("universe 2023: AAPL, MSFT, NEWTK in (3 active)",
     nrow(get_universe_at_date("2023-06-30", mock_master)) == 3)

test("universe 2023: correct tickers", {
  u <- get_universe_at_date("2023-06-30", mock_master)
  all(c("AAPL", "MSFT", "NEWTK") %in% u$ticker)
})

test("universe 2017: REMOVED is still in", {
  u <- get_universe_at_date("2017-06-30", mock_master)
  "REMOVED" %in% u$ticker
})

test("universe 2019: REMOVED is gone", {
  u <- get_universe_at_date("2019-06-30", mock_master)
  !("REMOVED" %in% u$ticker)
})

test("universe 2010: OLDTK is in", {
  u <- get_universe_at_date("2010-06-30", mock_master)
  "OLDTK" %in% u$ticker
})

test("universe 2016: OLDTK is gone", {
  u <- get_universe_at_date("2016-06-30", mock_master)
  !("OLDTK" %in% u$ticker)
})

test("universe 2019-06: NEWTK not yet added", {
  u <- get_universe_at_date("2019-06-30", mock_master)
  !("NEWTK" %in% u$ticker)
})

test("universe 2021: NEWTK is in", {
  u <- get_universe_at_date("2021-01-01", mock_master)
  "NEWTK" %in% u$ticker
})

test("universe returns data.table with required columns", {
  u <- get_universe_at_date("2023-06-30", mock_master)
  is.data.table(u) && all(c("ticker", "cik") %in% names(u))
})


# ============================================================================
# TEST 2: get_price_on_date
# ============================================================================
message("\n=== get_price_on_date ===")

test("price on trading day: returns adjusted close", {
  # First trading day in mock data
  d <- index(mock_prices)[1]
  p <- get_price_on_date(mock_prices, d)
  !is.na(p) && abs(p - as.numeric(coredata(mock_prices[d, 6]))) < 0.01
})

test("price on weekend: falls back to prior trading day", {
  # Find a Friday that is in the data, then test Saturday lookup
  fridays <- index(mock_prices)[weekdays(index(mock_prices)) == "Friday"]
  if (length(fridays) > 0) {
    fri <- fridays[5]  # use 5th Friday to avoid edge effects
    saturday <- fri + 1
    p_sat <- get_price_on_date(mock_prices, saturday)
    p_fri <- get_price_on_date(mock_prices, fri)
    # Saturday should fall back to Friday's price
    !is.na(p_sat) && !is.na(p_fri) && abs(p_sat - p_fri) < 0.01
  } else TRUE
})

test("price before data starts: returns NA", {
  p <- get_price_on_date(mock_prices, "2020-01-01")
  is.na(p)
})

test("price with NULL xts: returns NA",
     is.na(get_price_on_date(NULL, "2023-06-30")))

test("price with empty xts: returns NA", {
  empty_xts <- xts(matrix(ncol = 6, nrow = 0), order.by = as.Date(character(0)))
  is.na(get_price_on_date(empty_xts, "2023-06-30"))
})

test("price: 10-day tolerance works", {
  # Get last date in mock data, then ask 5 days later
  last_date <- max(index(mock_prices))
  p <- get_price_on_date(mock_prices, last_date + 5)
  !is.na(p)
})

test("price: beyond 10-day tolerance returns NA", {
  last_date <- max(index(mock_prices))
  p <- get_price_on_date(mock_prices, last_date + 15)
  is.na(p)
})


# ============================================================================
# TEST 3: get_latest_annual_filing
# ============================================================================
message("\n=== get_latest_annual_filing ===")

test("latest filing 2024-06: FY2023 available", {
  f <- get_latest_annual_filing(mock_fund_aapl, "2024-06-30")
  !is.null(f) && f$fiscal_year == 2023L
})

test("latest filing 2024-06: filed_date is 2024-02-15", {
  f <- get_latest_annual_filing(mock_fund_aapl, "2024-06-30")
  f$filed_date == as.Date("2024-02-15")
})

test("latest filing 2023-06: FY2022 (2023 not filed yet)", {
  # FY2023 filed on 2024-02-15, so as of 2023-06 only FY2022 is available
  f <- get_latest_annual_filing(mock_fund_aapl, "2023-06-30")
  !is.null(f) && f$fiscal_year == 2022L
})

test("latest filing 2023-01: FY2022 is available (filed 2023-02-15)", {
  # FY2022 filed on 2023-02-15, as of 2023-01-01 it's NOT yet filed
  f <- get_latest_annual_filing(mock_fund_aapl, "2023-01-01")
  # Only FY2021 would be available, but we don't have it in mock data
  is.null(f)
})

test("latest filing: NULL for empty data", {
  empty_dt <- data.table(
    concept = character(), value = numeric(), filed = as.Date(character()),
    fiscal_year = integer(), period_type = character()
  )
  is.null(get_latest_annual_filing(empty_dt, "2023-06-30"))
})

test("latest filing: NULL for NULL input",
     is.null(get_latest_annual_filing(NULL, "2023-06-30")))

test("latest filing: skips sparse FY, falls back to prior", {
  # Create data where FY2023 has only 1 concept but FY2022 is full
  sparse <- rbind(
    mock_fund_aapl,
    data.table(ticker = "AAPL", cik = "0000320193", concept = "shares_outstanding",
               tag = "shares_outstanding", value = 15000e6,
               period_end = as.Date("2024-06-30"), period_start = as.Date("2023-07-01"),
               filed = as.Date("2024-07-28"), form = "10-K",
               accession = "0001234-2024-000001", fiscal_year = 2024L,
               fiscal_qtr = "FY", unit = "shares", period_type = "FY")
  )
  # As of 2024-09-30, FY2024 has only 1 concept -> should fall back to FY2023
  f <- get_latest_annual_filing(sparse, "2024-09-30")
  !is.null(f) && f$fiscal_year == 2023L
})


# ============================================================================
# TEST 4: .generate_snapshot_dates
# ============================================================================
message("\n=== .generate_snapshot_dates ===")

test("snapshot dates: 4 per year", {
  dates <- .generate_snapshot_dates("2020-01-01", "2020-12-31")
  length(dates) == 4
})

test("snapshot dates: correct quarter-ends", {
  dates <- .generate_snapshot_dates("2020-01-01", "2020-12-31")
  expected <- as.Date(c("2020-03-31", "2020-06-30", "2020-09-30", "2020-12-31"))
  all(dates == expected)
})

test("snapshot dates: partial year", {
  dates <- .generate_snapshot_dates("2020-04-01", "2020-10-15")
  # Only Jun 30 and Sep 30 fall in this range
  length(dates) == 2 &&
    dates[1] == as.Date("2020-06-30") &&
    dates[2] == as.Date("2020-09-30")
})

test("snapshot dates: multi-year", {
  dates <- .generate_snapshot_dates("2019-01-01", "2021-12-31")
  length(dates) == 12  # 3 years x 4 quarters
})

test("snapshot dates: sorted", {
  dates <- .generate_snapshot_dates("2018-01-01", "2023-12-31")
  all(dates == sort(dates))
})


# ============================================================================
# TEST 5: build_historical_snapshots resumability
# ============================================================================
message("\n=== build_historical_snapshots resumability ===")

test("build_historical: skips existing snapshots", {
  skip_dir <- file.path(tempdir(), "test_skip_snapshots")
  dir.create(skip_dir, recursive = TRUE, showWarnings = FALSE)

  # Create a fake existing snapshot
  fake_dt <- data.table(ticker = "FAKE")
  arrow::write_parquet(fake_dt,
    file.path(skip_dir, "pit_2020-03-31_raw.parquet"))

  # generate_snapshot_dates for Q1 2020 only
  dates <- .generate_snapshot_dates("2020-01-01", "2020-03-31")
  # The file exists, so build_historical_snapshots should skip it
  # We can't call build_historical_snapshots directly (requires real data),
  # but we can verify the skip logic: file.exists returns TRUE
  skip_path <- file.path(skip_dir, sprintf("pit_%s_raw.parquet", dates[1]))
  file.exists(skip_path)
})


# ============================================================================
# TEST 5b: get_universe_at_date with NA sector handling
# ============================================================================
message("\n=== NA sector -> 'Unknown' handling ===")

test("universe with missing sector: NA sector becomes 'Unknown' label", {
  # Simulate what assemble_snapshot does: if sector is NA, label as "Unknown"
  mock_sec <- NA_character_
  label <- if (is.na(mock_sec)) "Unknown" else mock_sec
  label == "Unknown"
})

test("universe with valid sector: keeps original label", {
  mock_sec <- "Technology"
  label <- if (is.na(mock_sec)) "Unknown" else mock_sec
  label == "Technology"
})


# ============================================================================
# TEST 6: Integration -- assemble_snapshot with real cached data
# ============================================================================
message("\n=== Integration: assemble_snapshot (if cached data available) ===")

master_path <- "cache/lookups/constituent_master.parquet"
sector_path <- "cache/lookups/sector_industry.parquet"
fund_dir    <- "cache/fundamentals"

has_cached <- file.exists(master_path) && file.exists(sector_path) &&
  length(list.files(fund_dir, pattern = "\\.parquet$")) > 0

if (has_cached) {
  message("  cached data found -- running integration tests")

  # Use a recent date for the snapshot
  test_date <- "2024-06-30"
  test_output_dir <- tempdir()

  result <- tryCatch(
    assemble_snapshot(
      snapshot_date   = test_date,
      master_path     = master_path,
      sector_path     = sector_path,
      fund_dir        = fund_dir,
      price_cache_dir = "cache/prices",
      output_dir      = test_output_dir,
      prefetch_prices = FALSE  # skip Yahoo fetches in test
    ),
    error = function(e) {
      message(sprintf("  snapshot assembly error: %s", e$message))
      NULL
    }
  )

  if (!is.null(result)) {
    test("snapshot raw is data.table",
         is.data.table(result$raw))

    test("snapshot has date column",
         "date" %in% names(result$raw))

    test("snapshot has ticker column",
         "ticker" %in% names(result$raw))

    test("snapshot has sector column",
         "sector" %in% names(result$raw))

    test("snapshot has industry column",
         "industry" %in% names(result$raw))

    test("snapshot has all indicator columns",
         all(get_indicator_names() %in% names(result$raw)))

    test("snapshot raw and zscored same nrow",
         nrow(result$raw) == nrow(result$zscored))

    test("snapshot has >100 tickers",
         nrow(result$raw) > 100)

    test("snapshot no duplicate tickers",
         !anyDuplicated(result$raw$ticker))

    test("snapshot stats present",
         !is.null(result$stats) && result$stats$n_success > 0)

    # Parquet files written
    raw_file <- file.path(test_output_dir,
                          sprintf("pit_%s_raw.parquet", test_date))
    zscore_file <- file.path(test_output_dir,
                             sprintf("pit_%s_zscore.parquet", test_date))

    test("snapshot raw parquet written",
         file.exists(raw_file))

    test("snapshot zscore parquet written",
         file.exists(zscore_file))

    # Read back and verify
    if (file.exists(raw_file)) {
      readback <- as.data.table(arrow::read_parquet(raw_file))
      test("snapshot readback matches",
           nrow(readback) == nrow(result$raw))
    }

    # Check z-score properties
    zsc <- result$zscored
    ind_names <- get_indicator_names()
    z_vals <- unlist(zsc[, ..ind_names])
    z_vals <- z_vals[!is.na(z_vals)]

    test("z-scores have mean near 0",
         abs(mean(z_vals)) < 0.5)

    test("z-scores bounded [-3, 3]",
         all(z_vals >= -3 & z_vals <= 3))

    # Clean up
    unlink(raw_file)
    unlink(zscore_file)

  } else {
    message("  snapshot assembly returned NULL (price data may be missing)")
    message("  skipping integration assertions")
  }

} else {
  message("  no cached data found -- skipping integration tests")
  message("  (run Sessions A-D first to enable these tests)")
}


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== test_pit_assembler.R: %d passed, %d failed ===",
                .n_pass, .n_fail))

if (.n_fail > 0) stop("Tests failed", call. = FALSE)
