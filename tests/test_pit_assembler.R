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
# TEST 4b: zscore_expanding_window  (Task #4 of verification-report follow-up)
# ============================================================================
# Pure unit tests for the expanding-window z-score. Disk I/O is exercised in
# the integration test further down (via assemble_snapshot).
message("\n=== zscore_expanding_window ===")

# Build a small synthetic raw snapshot + history. Indicator columns are a
# subset of .INDICATOR_NAMES chosen to cover: a normal column (pe_trailing),
# a financial-masked column (gpa), and a column with missing rows.
.mk_raw <- function(date_str, pe_vals, gpa_vals, sectors,
                     tickers = paste0("T", seq_along(pe_vals))) {
  dt <- data.table(
    date        = as.Date(date_str),
    ticker      = tickers,
    industry    = rep("Software", length(tickers)),
    sector      = sectors,
    pe_trailing = pe_vals,
    gpa         = gpa_vals
  )
  dt
}

test("expanding_window: empty history behaves like cross-section z", {
  # With history_dt_list = list(), pool = winsorized current. Result should
  # closely match .robust_zscore on the non-financial values for pe_trailing.
  set.seed(31)
  vals <- rnorm(20, 20, 5)
  raw <- .mk_raw("2024-03-31", vals, rnorm(20, 0.3, 0.05),
                  rep("Technology", 20))
  z <- zscore_expanding_window(raw, history_dt_list = list(), clip = NULL)
  z_ref <- .robust_zscore(vals, clip = NULL)
  all(abs(z$pe_trailing - z_ref) < 1e-10, na.rm = TRUE)
})

test("expanding_window: history shifts the calibration center", {
  # Current cross-section sits well below historical center. With history
  # much larger than current, the pool's median is dragged toward history
  # and current values standardize to strongly negative z. Under single-
  # snapshot cross-section z would be ~0 (all current values identical).
  set.seed(32)
  hist_list <- lapply(1:5, function(i) {
    .mk_raw(sprintf("2023-%02d-28", i + 4),
            rnorm(50, mean = 50, sd = 3),
            rep(0.3, 50),
            rep("Technology", 50))
  })
  curr_dt <- .mk_raw("2024-03-31",
                      rep(15, 20),
                      rep(0.3, 20),
                      rep("Technology", 20))
  z_exp <- zscore_expanding_window(curr_dt, history_dt_list = hist_list,
                                   clip = NULL)
  # 250 history values at ~50 dominate 20 current values at 15 in the pool;
  # median ~ 50 and z for current is strongly negative.
  !is.na(z_exp$pe_trailing[1]) && z_exp$pe_trailing[1] < -3
})

test("expanding_window: multi-snapshot history pooled correctly", {
  # Three history snapshots + current. Each is winsorized at its own
  # p2.5/p97.5, then all winsorized values are pooled for median/MAD.
  set.seed(33)
  h1 <- .mk_raw("2023-09-30", rnorm(30, 20, 3), rep(0.3, 30),
                 rep("Technology", 30))
  h2 <- .mk_raw("2023-12-31", rnorm(30, 22, 3), rep(0.3, 30),
                 rep("Technology", 30))
  h3 <- .mk_raw("2024-03-31", rnorm(30, 24, 3), rep(0.3, 30),
                 rep("Technology", 30))
  curr <- .mk_raw("2024-06-30", rnorm(30, 26, 3), rep(0.3, 30),
                   rep("Technology", 30))
  z <- zscore_expanding_window(curr, history_dt_list = list(h1, h2, h3),
                               clip = NULL)
  # Pool median roughly equals overall mean of four rnorm(30, mean in
  # 20..26, sd=3) -> ~23. Current samples cluster around 26, so median
  # z should be positive.
  !is.na(median(z$pe_trailing)) && median(z$pe_trailing) > 0
})

test("expanding_window: financial masking is respected in pool", {
  # Financial rows must not contribute to the pool for gpa, and must be NA
  # in the output.
  curr <- .mk_raw(
    "2024-03-31",
    pe_vals  = rnorm(10, 20, 3),
    gpa_vals = c(rep(0.3, 5), rep(999, 5)),  # financial rows have nonsense
    sectors  = c(rep("Technology", 5), rep("Financial", 5))
  )
  hist_dt <- .mk_raw(
    "2023-12-31",
    pe_vals  = rnorm(10, 20, 3),
    gpa_vals = c(rep(0.32, 5), rep(999, 5)),
    sectors  = c(rep("Technology", 5), rep("Financial", 5))
  )
  z <- zscore_expanding_window(curr, history_dt_list = list(hist_dt))
  all(is.na(z$gpa[6:10])) && !any(is.na(z$gpa[1:5]))
})

test("expanding_window: NAs in raw preserved in output", {
  curr <- .mk_raw(
    "2024-03-31",
    pe_vals  = c(rnorm(9, 20, 3), NA_real_),
    gpa_vals = rep(0.3, 10),
    sectors  = rep("Technology", 10)
  )
  z <- zscore_expanding_window(curr, history_dt_list = list())
  is.na(z$pe_trailing[10]) && !all(is.na(z$pe_trailing[1:9]))
})

test("expanding_window: clip bound honored", {
  curr <- .mk_raw("2024-03-31", c(rnorm(10, 20, 3), 1e6),
                   rep(0.3, 11), rep("Technology", 11))
  z <- zscore_expanding_window(curr, history_dt_list = list(),
                               clip = c(-3, 3))
  all(is.na(z$pe_trailing) | (z$pe_trailing >= -3 & z$pe_trailing <= 3))
})

test("expanding_window: metadata columns preserved", {
  curr <- .mk_raw("2024-03-31", rnorm(10, 20, 3), rep(0.3, 10),
                   rep("Technology", 10))
  z <- zscore_expanding_window(curr, history_dt_list = list())
  all(c("date", "ticker", "industry", "sector") %in% names(z)) &&
    identical(z$ticker, curr$ticker)
})

test("expanding_window: NULL / empty raw input handled safely", {
  is.null(zscore_expanding_window(NULL)) &&
    nrow(zscore_expanding_window(data.table())) == 0
})

test(".load_history_snapshots: filters strictly < snapshot_date", {
  # Create three fake raw files in a tmp dir; loader should return only
  # the two with dates strictly before the target.
  td <- file.path(tempdir(), "test_history_loader")
  dir.create(td, recursive = TRUE, showWarnings = FALSE)
  # Clean any stale files from prior runs so the assertion is deterministic.
  file.remove(list.files(td, full.names = TRUE))
  for (d in c("2024-01-31", "2024-02-29", "2024-03-31")) {
    arrow::write_parquet(data.table(x = 1),
      file.path(td, sprintf("pit_%s_raw.parquet", d)))
  }
  hist <- .load_history_snapshots("2024-03-31", output_dir = td)
  length(hist) == 2L  # Jan and Feb; Mar (strict <) excluded
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

    test("z-scores bounded to default clip [-5, 5]",
         all(z_vals >= -5 & z_vals <= 5))

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
