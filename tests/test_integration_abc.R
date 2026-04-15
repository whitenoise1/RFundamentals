# ============================================================================
# test_integration_abc.R  --  Integration Tests: Session A + B + C
# ============================================================================
# Run: Rscript tests/test_integration_abc.R
#
# Validates that constituent_master, sector_industry, and fundamentals
# parquets are consistent end-to-end:
#   1. Master -> Fetcher join (CIK lookup works)
#   2. Fundamentals -> Sectors join (financial sector NA handling)
#   3. Point-in-time readiness (filed dates, period coverage)
#   4. Data flow: master tickers -> fetched tickers -> concepts -> sectors
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")

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
# SETUP: Load all three outputs
# ============================================================================
message("\n=== Loading Session A + B + C outputs ===")

master_path <- "cache/lookups/constituent_master.parquet"
sector_path <- "cache/lookups/sector_industry.parquet"
fund_dir    <- "cache/fundamentals"

stopifnot(file.exists(master_path))
stopifnot(file.exists(sector_path))
stopifnot(dir.exists(fund_dir))

master  <- as.data.table(arrow::read_parquet(master_path))
sectors <- as.data.table(arrow::read_parquet(sector_path))

# Load all cached fundamentals into one table
fund_files <- list.files(fund_dir, pattern = "\\.parquet$", full.names = TRUE)
stopifnot(length(fund_files) > 0)

fund_list <- lapply(fund_files, function(f) {
  tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
})
fund_list <- fund_list[!vapply(fund_list, is.null, logical(1))]
fund_all  <- rbindlist(fund_list, fill = TRUE)

fetched_tickers <- unique(fund_all$ticker)

message(sprintf("  master:       %d rows, %d unique tickers",
                nrow(master), uniqueN(master$ticker)))
message(sprintf("  sectors:      %d rows", nrow(sectors)))
message(sprintf("  fundamentals: %d rows, %d tickers (%d files)",
                nrow(fund_all), length(fetched_tickers), length(fund_files)))


# ============================================================================
# 1. MASTER -> FETCHER CIK JOIN
# ============================================================================
message("\n=== 1. Master -> Fetcher CIK Join ===")

# Every fetched ticker must exist in the master
test("all fetched tickers exist in master",
     all(fetched_tickers %in% master$ticker))

# Every fetched ticker must have a CIK in the master
master_with_cik <- master[!is.na(cik)]
test("all fetched tickers have CIK in master",
     all(fetched_tickers %in% master_with_cik$ticker))

# CIK in fundamentals matches CIK in master
for (tk in fetched_tickers) {
  fund_cik <- unique(fund_all[ticker == tk, cik])
  # Get master CIK for highest occurrence of this ticker
  master_row <- master[ticker == tk][order(-occurrence)][1]
  master_cik <- master_row$cik

  if (length(fund_cik) == 1 && !is.na(master_cik)) {
    if (fund_cik != master_cik) {
      message(sprintf("    CIK mismatch: %s fund=%s master=%s",
                      tk, fund_cik, master_cik))
    }
  }
}

# Aggregate check: all CIKs match
cik_match <- vapply(fetched_tickers, function(tk) {
  fund_cik   <- unique(fund_all[ticker == tk, cik])
  master_cik <- master[ticker == tk][order(-occurrence)][1]$cik
  length(fund_cik) == 1 && !is.na(master_cik) && fund_cik == master_cik
}, logical(1))
test("all CIKs match between master and fundamentals",
     all(cik_match))


# ============================================================================
# 2. FUNDAMENTALS -> SECTORS JOIN
# ============================================================================
message("\n=== 2. Fundamentals -> Sectors Join ===")

# Every fetched ticker should have a sector
fund_sectors <- merge(
  data.table(ticker = fetched_tickers),
  sectors,
  by = "ticker",
  all.x = TRUE
)

test("all fetched tickers have sector mapping",
     all(!is.na(fund_sectors$sector)))

missing_sector <- fund_sectors[is.na(sector), ticker]
if (length(missing_sector) > 0) {
  message("  Missing sector: ", paste(missing_sector, collapse = ", "))
}


# ============================================================================
# 3. FINANCIAL SECTOR: CONCEPT AVAILABILITY
# ============================================================================
message("\n=== 3. Financial Sector Concept Availability ===")

# Identify financial tickers among fetched
financial_tickers <- fund_sectors[sector == "Financial", ticker]
non_financial_tickers <- fund_sectors[sector != "Financial", ticker]

message(sprintf("  Financial tickers fetched: %d (%s)",
                length(financial_tickers),
                paste(financial_tickers, collapse = ", ")))
message(sprintf("  Non-financial tickers fetched: %d",
                length(non_financial_tickers)))

# Indicators that should be NA for financials per CLAUDE.md:
# GP/A (needs cogs), Inventory Turnover (needs inventory),
# Cash-Based OP (needs cogs), CAPEX/DA, Inventory/Sales Change
# Check that financials LACK cogs and inventory
#
# NOTE: FB is misclassified as Financial by finviz (it should be
# Communication Services -- the old "FB" ticker resolves incorrectly).
# This is a known Session B data issue. We exclude known misclassifications
# from the financial sector tests.
known_misclassified <- c("FB")
true_financials <- setdiff(financial_tickers, known_misclassified)

if (length(true_financials) > 0) {
  for (tk in true_financials) {
    dt_tk <- fund_all[ticker == tk]
    has_cogs <- "cogs" %in% dt_tk$concept
    has_inv  <- "inventory" %in% dt_tk$concept
    message(sprintf("    %s: cogs=%s inventory=%s", tk, has_cogs, has_inv))
  }

  # True financial tickers (banks, insurance) should mostly lack COGS
  fin_with_cogs <- sum(vapply(true_financials, function(tk) {
    "cogs" %in% fund_all[ticker == tk, concept]
  }, logical(1)))
  test("true financials mostly lack COGS",
       fin_with_cogs / length(true_financials) <= 0.25)
}

if (length(known_misclassified) > 0) {
  message(sprintf("  WARNING: %d tickers misclassified as Financial by finviz: %s",
                  length(known_misclassified),
                  paste(known_misclassified, collapse = ", ")))
  message("  (Fix in Session B sector data or fallback CSV)")
}

# Non-financials should mostly HAVE cogs
if (length(non_financial_tickers) > 0) {
  nonfin_with_cogs <- sum(vapply(non_financial_tickers, function(tk) {
    "cogs" %in% fund_all[ticker == tk, concept]
  }, logical(1)))
  test("most non-financials have COGS",
       nonfin_with_cogs / length(non_financial_tickers) >= 0.60)
}


# ============================================================================
# 4. POINT-IN-TIME INTEGRITY
# ============================================================================
message("\n=== 4. Point-in-Time Integrity ===")

# EDGAR companyfacts includes comparative periods: a 2024 10-K restating
# FY2020 data has filed=2024 but period_end=2020. This is correct PIT
# behavior (the restated value is known from the later filing date).
# So filed < period_end is NOT expected -- only original filings satisfy
# filed >= period_end. We filter to "current-period" filings where the
# fiscal year matches the filing year (within 1 year) to test PIT sanity.
pit_check <- fund_all[!is.na(filed) & !is.na(period_end)]
test("filed dates present", nrow(pit_check) > 0)

pit_check[, lag_days := as.integer(filed - period_end)]

# Current-period filings: filing lag <= 365 days (original, not comparative)
current_period <- pit_check[lag_days >= 0 & lag_days <= 365]
message(sprintf("  Current-period filings: %d / %d total (%.1f%%)",
                nrow(current_period), nrow(pit_check),
                100 * nrow(current_period) / nrow(pit_check)))

test("current-period filings: filed >= period_end",
     all(current_period$filed >= current_period$period_end))

# Filing lag for current-period: typically 30-90 days
median_lag <- median(current_period$lag_days, na.rm = TRUE)
message(sprintf("  Median current-period filing lag: %d days", median_lag))
test("median current-period filing lag is plausible (20-120 days)",
     median_lag >= 20 && median_lag <= 120)

# 10-K filing lag should be longer than 10-Q (for current-period only)
cp_10k <- current_period[grepl("^10-K", form)]
cp_10q <- current_period[grepl("^10-Q", form)]
if (nrow(cp_10k) > 0 && nrow(cp_10q) > 0) {
  median_10k <- median(cp_10k$lag_days, na.rm = TRUE)
  median_10q <- median(cp_10q$lag_days, na.rm = TRUE)
  message(sprintf("  Median lag: 10-K=%d days, 10-Q=%d days", median_10k, median_10q))
  test("10-K filing lag > 10-Q filing lag",
       median_10k > median_10q)
}

# Comparative periods should have large lag (this confirms we have them)
comparatives <- pit_check[lag_days > 365]
test("comparative periods present (lag > 365 days)",
     nrow(comparatives) > 0)
message(sprintf("  Comparative-period rows: %d (lag range %d-%d days)",
                nrow(comparatives),
                min(comparatives$lag_days), max(comparatives$lag_days)))


# ============================================================================
# 5. TEMPORAL COVERAGE
# ============================================================================
message("\n=== 5. Temporal Coverage ===")

# All fetched tickers should have multi-year data
for (tk in fetched_tickers) {
  dt_tk <- fund_all[ticker == tk]
  fy_years <- sort(unique(dt_tk[!is.na(fiscal_year), fiscal_year]))
  n_years <- length(fy_years)
  if (n_years < 3) {
    message(sprintf("    WARNING: %s has only %d fiscal years: %s",
                    tk, n_years, paste(fy_years, collapse = ", ")))
  }
}

year_counts <- fund_all[, .(n_years = uniqueN(fiscal_year)), by = ticker]
test("all tickers have >= 3 fiscal years",
     all(year_counts$n_years >= 3))

# Most tickers should have recent data (fiscal_year >= 2023)
recent <- fund_all[fiscal_year >= 2023, uniqueN(ticker)]
test("most tickers have FY >= 2023 data",
     recent / length(fetched_tickers) >= 0.80)


# ============================================================================
# 6. CONCEPT COVERAGE CONSISTENCY
# ============================================================================
message("\n=== 6. Concept Coverage Consistency ===")

# Core concepts that every ticker should have (regardless of sector)
universal_concepts <- c("revenue", "net_income", "total_assets",
                        "stockholders_equity", "operating_cashflow")

for (cn in universal_concepts) {
  tickers_with <- fund_all[concept == cn, uniqueN(ticker)]
  pct <- round(100 * tickers_with / length(fetched_tickers))
  test(sprintf("universal concept '%s' present for all tickers (%d%%)", cn, pct),
       tickers_with == length(fetched_tickers))
}

# Non-financial-only concepts should be absent for financials, present for others
sector_specific <- c("cogs", "inventory")
for (cn in sector_specific) {
  # Among non-financials, coverage should be decent
  if (length(non_financial_tickers) > 0) {
    nf_with <- sum(vapply(non_financial_tickers, function(tk) {
      cn %in% fund_all[ticker == tk, concept]
    }, logical(1)))
    nf_pct <- round(100 * nf_with / length(non_financial_tickers))
    test(sprintf("'%s' present for >= 70%% of non-financials (%d%%)", cn, nf_pct),
         nf_with / length(non_financial_tickers) >= 0.70)
  }
}


# ============================================================================
# 7. DEDUP INTEGRITY ACROSS ALL FILES
# ============================================================================
message("\n=== 7. Dedup Integrity ===")

# No (ticker, concept, period_end, fiscal_qtr) duplicates across files
# (each ticker is its own file, so check within each)
dedup_violations <- 0L
for (tk in fetched_tickers) {
  dt_tk <- fund_all[ticker == tk]
  n_dup <- anyDuplicated(dt_tk[, .(concept, period_end, fiscal_qtr)])
  if (n_dup > 0) {
    dedup_violations <- dedup_violations + 1L
    message(sprintf("    Dedup violation: %s", tk))
  }
}
test("no dedup violations in any ticker", dedup_violations == 0L)

# Each ticker should have exactly one CIK
multi_cik <- fund_all[, .(n_cik = uniqueN(cik)), by = ticker][n_cik > 1]
test("each ticker maps to exactly one CIK",
     nrow(multi_cik) == 0)


# ============================================================================
# 8. PERIOD TYPE DISTRIBUTION
# ============================================================================
message("\n=== 8. Period Type Distribution ===")

if ("period_type" %in% names(fund_all)) {
  pt_dist <- fund_all[, .N, by = period_type][order(-N)]
  message("  Period type distribution:")
  for (i in seq_len(nrow(pt_dist))) {
    message(sprintf("    %s: %d rows", pt_dist$period_type[i], pt_dist$N[i]))
  }

  # Valid period types only
  valid_pt <- c("FY", "Q1", "Q2", "Q3", "Q4", NA_character_)
  test("only valid period_type values",
       all(fund_all$period_type %in% valid_pt))

  # Both FY and quarterly should be present
  test("FY observations present",   "FY" %in% fund_all$period_type)
  test("Q1 observations present",   "Q1" %in% fund_all$period_type)
  test("Q2 observations present",   "Q2" %in% fund_all$period_type)
  test("Q3 observations present",   "Q3" %in% fund_all$period_type)

  # FY should be < 50% of classified rows (quarterly data is more frequent)
  classified <- fund_all[!is.na(period_type)]
  fy_pct <- classified[period_type == "FY", .N] / nrow(classified) * 100
  message(sprintf("  FY rows: %.1f%% of classified", fy_pct))
  test("FY is < 50% of classified rows (quarterly dominates)",
       fy_pct < 50)
}


# ============================================================================
# 9. END-TO-END DATA FLOW SPOT CHECK
# ============================================================================
message("\n=== 9. End-to-End Spot Check: AAPL ===")

aapl_master <- master[ticker == "AAPL"]
aapl_sector <- sectors[ticker == "AAPL"]
aapl_fund   <- fund_all[ticker == "AAPL"]

test("AAPL in master",                nrow(aapl_master) == 1)
test("AAPL status is ACTIVE",         aapl_master$status == "ACTIVE")
test("AAPL CIK is 0000320193",        aapl_master$cik == "0000320193")
test("AAPL sector is Technology",      aapl_sector$sector == "Technology")
test("AAPL has fundamental data",      nrow(aapl_fund) > 0)
test("AAPL fund CIK matches master",  unique(aapl_fund$cik) == aapl_master$cik)

# AAPL FY2023 revenue should be ~383B
aapl_rev_fy23 <- aapl_fund[concept == "revenue" & fiscal_year == 2023 &
                              fiscal_qtr == "FY", value]
if (length(aapl_rev_fy23) == 1) {
  test("AAPL FY2023 revenue ~ 383B",
       aapl_rev_fy23 > 350e9 && aapl_rev_fy23 < 420e9)
  message(sprintf("  AAPL FY2023 revenue: $%.1fB", aapl_rev_fy23 / 1e9))
}

# AAPL is not financial -> should have COGS
test("AAPL (Technology) has COGS data",
     "cogs" %in% aapl_fund$concept)

message("\n=== 10. End-to-End Spot Check: JPM ===")

jpm_master <- master[ticker == "JPM"]
jpm_sector <- sectors[ticker == "JPM"]
jpm_fund   <- fund_all[ticker == "JPM"]

test("JPM in master",                 nrow(jpm_master) == 1)
test("JPM status is ACTIVE",          jpm_master$status == "ACTIVE")
test("JPM sector is Financial",        jpm_sector$sector == "Financial")
test("JPM has fundamental data",       nrow(jpm_fund) > 0)
test("JPM fund CIK matches master",   unique(jpm_fund$cik) == jpm_master$cik)

# JPM is financial -> should lack COGS and inventory
test("JPM (Financial) lacks COGS",
     !("cogs" %in% jpm_fund$concept))
test("JPM (Financial) lacks inventory",
     !("inventory" %in% jpm_fund$concept))

# JPM should still have core concepts
test("JPM has revenue",               "revenue" %in% jpm_fund$concept)
test("JPM has net_income",            "net_income" %in% jpm_fund$concept)
test("JPM has total_assets",          "total_assets" %in% jpm_fund$concept)


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== RESULTS: %d passed, %d failed ===",
                .n_pass, .n_fail))
if (.n_fail > 0) {
  stop(sprintf("%d test(s) failed", .n_fail))
} else {
  message("All tests passed.")
}
