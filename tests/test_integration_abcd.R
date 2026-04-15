# ============================================================================
# test_integration_abcd.R  --  Integration Tests: Session A + B + C + D
# ============================================================================
# Run: Rscript tests/test_integration_abcd.R
#
# Validates end-to-end consistency across all four modules:
#   Session A: constituent_master.parquet (roster, CIK, duplicates)
#   Session B: sector_industry.parquet (finviz sectors)
#   Session C: fundamental_fetcher.R prototype (20 tickers)
#   Session D: fundamental_fetcher.R full build (639+ tickers)
#
# Tests:
#   1. Master -> Fetcher CIK join
#   2. Fundamentals -> Sectors join (with removed-ticker tolerance)
#   3. Financial sector concept availability
#   4. Point-in-time integrity
#   5. Temporal coverage
#   6. Concept coverage consistency
#   7. Dedup integrity
#   8. Period type distribution
#   9. Full build scale checks
#  10. DIFFERENT_COMPANY duplicate handling
#  11. End-to-end spot checks (AAPL, JPM, removed ticker)
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
# SETUP: Load all outputs
# ============================================================================
message("\n=== Loading Session A + B + C + D outputs ===")

master_path <- "cache/lookups/constituent_master.parquet"
sector_path <- "cache/lookups/sector_industry.parquet"
fund_dir    <- "cache/fundamentals"

stopifnot(file.exists(master_path))
stopifnot(file.exists(sector_path))
stopifnot(dir.exists(fund_dir))

master  <- as.data.table(arrow::read_parquet(master_path))
sectors <- as.data.table(arrow::read_parquet(sector_path))

# Load all cached fundamentals into one table
fund_files <- list.files(fund_dir, pattern = "[.]parquet$", full.names = TRUE)
stopifnot(length(fund_files) > 0)

fund_list <- lapply(fund_files, function(f) {
  tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
})
fund_list <- fund_list[!vapply(fund_list, is.null, logical(1))]
fund_all  <- rbindlist(fund_list, fill = TRUE)

fetched_tickers <- unique(fund_all$ticker)

# Partition into active vs removed
active_tickers  <- master[status == "ACTIVE", unique(ticker)]
removed_tickers <- setdiff(master$ticker, active_tickers)

fetched_active  <- intersect(fetched_tickers, active_tickers)
fetched_removed <- intersect(fetched_tickers, removed_tickers)

message(sprintf("  master:       %d rows, %d unique tickers (%d active, %d removed)",
                nrow(master), uniqueN(master$ticker),
                length(active_tickers), length(removed_tickers)))
message(sprintf("  sectors:      %d rows", nrow(sectors)))
message(sprintf("  fundamentals: %s rows, %d tickers (%d files)",
                format(nrow(fund_all), big.mark = ","),
                length(fetched_tickers), length(fund_files)))
message(sprintf("    active:  %d tickers fetched", length(fetched_active)))
message(sprintf("    removed: %d tickers fetched", length(fetched_removed)))


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
cik_mismatches <- character()
for (tk in fetched_tickers) {
  fund_cik <- unique(fund_all[ticker == tk, cik])
  master_cik <- master[ticker == tk][order(-occurrence)][1]$cik
  if (length(fund_cik) == 1 && !is.na(master_cik) && fund_cik != master_cik) {
    cik_mismatches <- c(cik_mismatches,
                        sprintf("%s fund=%s master=%s", tk, fund_cik, master_cik))
  }
}
if (length(cik_mismatches) > 0) {
  message("  CIK mismatches:")
  for (m in cik_mismatches) message("    ", m)
}
test("all CIKs match between master and fundamentals",
     length(cik_mismatches) == 0)

# Fetch coverage: what fraction of master tickers with CIK were fetched?
master_deduped <- master_with_cik[order(ticker, cik, -occurrence)]
master_deduped <- master_deduped[!duplicated(master_deduped[, .(ticker, cik)])]
expected_tickers <- unique(master_deduped$ticker)
fetch_pct <- round(100 * length(fetched_tickers) / length(expected_tickers), 1)
message(sprintf("  Fetch coverage: %d / %d tickers (%.1f%%)",
                length(fetched_tickers), length(expected_tickers), fetch_pct))
test(sprintf("fetch coverage >= 98%% (%.1f%%)", fetch_pct), fetch_pct >= 98)


# ============================================================================
# 2. FUNDAMENTALS -> SECTORS JOIN
# ============================================================================
message("\n=== 2. Fundamentals -> Sectors Join ===")

# Sector data was fetched for active/current tickers. Removed tickers may lack it.
fund_sectors <- merge(
  data.table(ticker = fetched_tickers),
  sectors,
  by = "ticker",
  all.x = TRUE
)

# Active tickers should all have sector data
active_with_sector <- fund_sectors[ticker %in% active_tickers & !is.na(sector)]
active_missing     <- fund_sectors[ticker %in% active_tickers & is.na(sector), ticker]
active_sector_pct  <- round(100 * nrow(active_with_sector) / length(fetched_active), 1)
message(sprintf("  Active tickers with sector: %d / %d (%.1f%%)",
                nrow(active_with_sector), length(fetched_active), active_sector_pct))
test("all active fetched tickers have sector mapping",
     length(active_missing) == 0)
if (length(active_missing) > 0) {
  message("  Missing sector (active): ", paste(active_missing, collapse = ", "))
}

# Removed tickers: sector coverage is best-effort
removed_with_sector <- fund_sectors[ticker %in% removed_tickers & !is.na(sector)]
removed_total       <- length(fetched_removed)
if (removed_total > 0) {
  removed_pct <- round(100 * nrow(removed_with_sector) / removed_total, 1)
  message(sprintf("  Removed tickers with sector: %d / %d (%.1f%%)",
                  nrow(removed_with_sector), removed_total, removed_pct))
}

# Overall sector coverage
total_with_sector <- sum(!is.na(fund_sectors$sector))
total_pct <- round(100 * total_with_sector / length(fetched_tickers), 1)
message(sprintf("  Overall sector coverage: %d / %d (%.1f%%)",
                total_with_sector, length(fetched_tickers), total_pct))
test("overall sector coverage >= 85%", total_pct >= 85)


# ============================================================================
# 3. FINANCIAL SECTOR: CONCEPT AVAILABILITY
# ============================================================================
message("\n=== 3. Financial Sector Concept Availability ===")

# Identify financial tickers among fetched (only those with sector data)
financial_tickers     <- fund_sectors[sector == "Financial", ticker]
non_financial_tickers <- fund_sectors[!is.na(sector) & sector != "Financial", ticker]

# Known finviz misclassifications
known_misclassified <- c("FB")
true_financials <- setdiff(financial_tickers, known_misclassified)

message(sprintf("  Financial tickers: %d (true: %d, misclassified: %d)",
                length(financial_tickers), length(true_financials),
                length(intersect(financial_tickers, known_misclassified))))
message(sprintf("  Non-financial tickers: %d", length(non_financial_tickers)))

if (length(true_financials) > 0) {
  fin_with_cogs <- sum(vapply(true_financials, function(tk) {
    "cogs" %in% fund_all[ticker == tk, concept]
  }, logical(1)))
  fin_cogs_pct <- round(100 * fin_with_cogs / length(true_financials))
  test(sprintf("true financials mostly lack COGS (%d%% have it)", fin_cogs_pct),
       fin_with_cogs / length(true_financials) <= 0.25)
}

if (length(non_financial_tickers) > 0) {
  nonfin_with_cogs <- sum(vapply(non_financial_tickers, function(tk) {
    "cogs" %in% fund_all[ticker == tk, concept]
  }, logical(1)))
  nonfin_pct <- round(100 * nonfin_with_cogs / length(non_financial_tickers))
  test(sprintf("most non-financials have COGS (%d%%)", nonfin_pct),
       nonfin_with_cogs / length(non_financial_tickers) >= 0.60)
}


# ============================================================================
# 4. POINT-IN-TIME INTEGRITY
# ============================================================================
message("\n=== 4. Point-in-Time Integrity ===")

pit_check <- fund_all[!is.na(filed) & !is.na(period_end)]
test("filed dates present", nrow(pit_check) > 0)

pit_check[, lag_days := as.integer(filed - period_end)]

# Current-period filings: filing lag 0-365 days
current_period <- pit_check[lag_days >= 0 & lag_days <= 365]
message(sprintf("  Current-period filings: %s / %s total (%.1f%%)",
                format(nrow(current_period), big.mark = ","),
                format(nrow(pit_check), big.mark = ","),
                100 * nrow(current_period) / nrow(pit_check)))

test("current-period filings: filed >= period_end",
     all(current_period$filed >= current_period$period_end))

median_lag <- median(current_period$lag_days, na.rm = TRUE)
message(sprintf("  Median current-period filing lag: %d days", median_lag))
test("median filing lag is plausible (20-120 days)",
     median_lag >= 20 && median_lag <= 120)

# 10-K filing lag > 10-Q
cp_10k <- current_period[grepl("^10-K", form)]
cp_10q <- current_period[grepl("^10-Q", form)]
if (nrow(cp_10k) > 0 && nrow(cp_10q) > 0) {
  median_10k <- median(cp_10k$lag_days, na.rm = TRUE)
  median_10q <- median(cp_10q$lag_days, na.rm = TRUE)
  message(sprintf("  Median lag: 10-K=%d days, 10-Q=%d days", median_10k, median_10q))
  test("10-K filing lag > 10-Q filing lag", median_10k > median_10q)
}

# Comparative periods confirm XBRL includes restated data
comparatives <- pit_check[lag_days > 365]
test("comparative periods present (lag > 365 days)", nrow(comparatives) > 0)


# ============================================================================
# 5. TEMPORAL COVERAGE
# ============================================================================
message("\n=== 5. Temporal Coverage ===")

year_counts <- fund_all[, .(n_years = uniqueN(fiscal_year)), by = ticker]

# Active tickers should all have >= 3 years (they're in the index now)
active_year_counts <- year_counts[ticker %in% active_tickers]
short_active <- active_year_counts[n_years < 3]
if (nrow(short_active) > 0) {
  message("  Active tickers with < 3 fiscal years:")
  for (i in seq_len(nrow(short_active))) {
    tk <- short_active$ticker[i]
    yrs <- sort(fund_all[ticker == tk & !is.na(fiscal_year), unique(fiscal_year)])
    message(sprintf("    %s: %d years (%s)", tk, short_active$n_years[i],
                    paste(yrs, collapse = ", ")))
  }
}
active_3yr_pct <- round(100 * sum(active_year_counts$n_years >= 3) /
                           nrow(active_year_counts), 1)
test(sprintf("active tickers >= 3 FY: %.1f%% (>= 95%%)", active_3yr_pct),
     active_3yr_pct >= 95)

# Removed tickers: some were short-lived, so relax to >= 80%
removed_year_counts <- year_counts[ticker %in% removed_tickers]
if (nrow(removed_year_counts) > 0) {
  removed_3yr_pct <- round(100 * sum(removed_year_counts$n_years >= 3) /
                              nrow(removed_year_counts), 1)
  message(sprintf("  Removed tickers >= 3 FY: %.1f%%", removed_3yr_pct))
  test(sprintf("removed tickers >= 3 FY: %.1f%% (>= 80%%)", removed_3yr_pct),
       removed_3yr_pct >= 80)
}

# Most tickers should have recent data
recent <- fund_all[fiscal_year >= 2023, uniqueN(ticker)]
recent_pct <- round(100 * recent / length(fetched_tickers), 1)
test(sprintf("tickers with FY >= 2023: %.1f%% (>= 70%%)", recent_pct),
     recent_pct >= 70)


# ============================================================================
# 6. CONCEPT COVERAGE CONSISTENCY
# ============================================================================
message("\n=== 6. Concept Coverage Consistency ===")

# Core concepts: >= 98% across all fetched tickers
universal_concepts <- c("revenue", "net_income", "total_assets",
                        "stockholders_equity", "operating_cashflow")

for (cn in universal_concepts) {
  tickers_with <- fund_all[concept == cn, uniqueN(ticker)]
  pct <- round(100 * tickers_with / length(fetched_tickers), 1)
  test(sprintf("'%s' coverage >= 98%% (%d/%d = %.1f%%)",
               cn, tickers_with, length(fetched_tickers), pct),
       pct >= 98)
}

# Non-financial sector concepts
sector_specific <- c("cogs", "inventory")
for (cn in sector_specific) {
  if (length(non_financial_tickers) > 0) {
    nf_with <- sum(vapply(non_financial_tickers, function(tk) {
      cn %in% fund_all[ticker == tk, concept]
    }, logical(1)))
    nf_pct <- round(100 * nf_with / length(non_financial_tickers))
    test(sprintf("'%s' >= 70%% of non-financials (%d%%)", cn, nf_pct),
         nf_with / length(non_financial_tickers) >= 0.70)
  }
}


# ============================================================================
# 7. DEDUP INTEGRITY
# ============================================================================
message("\n=== 7. Dedup Integrity ===")

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
test("each ticker maps to exactly one CIK", nrow(multi_cik) == 0)


# ============================================================================
# 8. PERIOD TYPE DISTRIBUTION
# ============================================================================
message("\n=== 8. Period Type Distribution ===")

if ("period_type" %in% names(fund_all)) {
  pt_dist <- fund_all[, .N, by = period_type][order(-N)]
  message("  Period type distribution:")
  for (i in seq_len(nrow(pt_dist))) {
    message(sprintf("    %-4s %s rows",
                    ifelse(is.na(pt_dist$period_type[i]), "NA",
                           pt_dist$period_type[i]),
                    format(pt_dist$N[i], big.mark = ",")))
  }

  valid_pt <- c("FY", "Q1", "Q2", "Q3", "Q4", NA_character_)
  test("only valid period_type values",
       all(fund_all$period_type %in% valid_pt))

  test("FY observations present", "FY" %in% fund_all$period_type)
  test("Q1 observations present", "Q1" %in% fund_all$period_type)
  test("Q2 observations present", "Q2" %in% fund_all$period_type)
  test("Q3 observations present", "Q3" %in% fund_all$period_type)

  classified <- fund_all[!is.na(period_type)]
  fy_pct <- classified[period_type == "FY", .N] / nrow(classified) * 100
  message(sprintf("  FY rows: %.1f%% of classified", fy_pct))
  test("FY is < 50% of classified (quarterly dominates)", fy_pct < 50)
}


# ============================================================================
# 9. FULL BUILD SCALE CHECKS (Session D)
# ============================================================================
message("\n=== 9. Full Build Scale Checks ===")

test("total cached files >= 600", length(fund_files) >= 600)
test("total rows > 1M", nrow(fund_all) > 1e6)

total_mb <- sum(file.size(fund_files)) / 1024^2
message(sprintf("  Total cache size: %.1f MB", total_mb))
test("total cache size > 5 MB", total_mb > 5)

# Median rows per ticker should be substantial (multi-year XBRL history)
median_rows <- median(fund_all[, .N, by = ticker]$N)
message(sprintf("  Median rows/ticker: %.0f", median_rows))
test("median rows/ticker > 1000", median_rows > 1000)

# Concepts per ticker
median_concepts <- median(fund_all[, .(n = uniqueN(concept)), by = ticker]$n)
message(sprintf("  Median concepts/ticker: %.0f", median_concepts))
test("median concepts/ticker >= 20", median_concepts >= 20)


# ============================================================================
# 10. DIFFERENT_COMPANY DUPLICATE HANDLING
# ============================================================================
message("\n=== 10. Duplicate Ticker Handling ===")

# SAME_COMPANY duplicates: same CIK, multiple occurrences -> one fetch
same_company_dups <- master[!is.na(duplicate_class) & duplicate_class == "SAME_COMPANY" &
                              !is.na(cik)]
if (nrow(same_company_dups) > 0) {
  sc_tickers <- unique(same_company_dups$ticker)
  sc_fetched <- sum(sc_tickers %in% fetched_tickers)
  message(sprintf("  SAME_COMPANY tickers with CIK: %d, fetched: %d",
                  length(sc_tickers), sc_fetched))
  test("all SAME_COMPANY tickers with CIK are fetched",
       sc_fetched == length(sc_tickers))

  # Each should have exactly one CIK in the fundamental data
  for (tk in intersect(sc_tickers, fetched_tickers)) {
    n_cik <- uniqueN(fund_all[ticker == tk, cik])
    if (n_cik != 1) {
      message(sprintf("    WARNING: %s has %d CIKs in fundamentals", tk, n_cik))
    }
  }
}

# DIFFERENT_COMPANY duplicates: may have different CIKs
diff_company_dups <- master[!is.na(duplicate_class) & duplicate_class == "DIFFERENT_COMPANY"]
if (nrow(diff_company_dups) > 0) {
  dc_tickers <- unique(diff_company_dups$ticker)
  message(sprintf("  DIFFERENT_COMPANY tickers: %d (%s)",
                  length(dc_tickers), paste(dc_tickers, collapse = ", ")))

  # Check which have CIK and whether they were fetched
  dc_with_cik <- diff_company_dups[!is.na(cik)]
  if (nrow(dc_with_cik) > 0) {
    dc_fetched <- unique(dc_with_cik$ticker) %in% fetched_tickers
    message(sprintf("  DIFFERENT_COMPANY with CIK: %d tickers, %d fetched",
                    uniqueN(dc_with_cik$ticker), sum(dc_fetched)))
  }

  # Those without CIK cannot be fetched -- that's expected
  dc_no_cik <- diff_company_dups[is.na(cik)]
  if (nrow(dc_no_cik) > 0) {
    message(sprintf("  DIFFERENT_COMPANY without CIK: %d rows (%s) -- expected, cannot fetch",
                    nrow(dc_no_cik),
                    paste(unique(dc_no_cik$ticker), collapse = ", ")))
  }
}


# ============================================================================
# 11. END-TO-END SPOT CHECKS
# ============================================================================
message("\n=== 11. End-to-End Spot Check: AAPL ===")

aapl_master <- master[ticker == "AAPL"]
aapl_sector <- sectors[ticker == "AAPL"]
aapl_fund   <- fund_all[ticker == "AAPL"]

test("AAPL in master",                nrow(aapl_master) >= 1)
test("AAPL status is ACTIVE",         aapl_master[occurrence == max(occurrence)]$status == "ACTIVE")
test("AAPL CIK is 0000320193",        aapl_master$cik[1] == "0000320193")
test("AAPL sector is Technology",      aapl_sector$sector == "Technology")
test("AAPL has fundamental data",      nrow(aapl_fund) > 0)
test("AAPL fund CIK matches master",  unique(aapl_fund$cik) == aapl_master$cik[1])

aapl_rev_fy23 <- aapl_fund[concept == "revenue" & fiscal_year == 2023 &
                              fiscal_qtr == "FY", value]
if (length(aapl_rev_fy23) == 1) {
  test("AAPL FY2023 revenue ~ 383B",
       aapl_rev_fy23 > 350e9 && aapl_rev_fy23 < 420e9)
  message(sprintf("  AAPL FY2023 revenue: $%.1fB", aapl_rev_fy23 / 1e9))
}

test("AAPL (Technology) has COGS", "cogs" %in% aapl_fund$concept)

message("\n=== 12. End-to-End Spot Check: JPM ===")

jpm_master <- master[ticker == "JPM"]
jpm_sector <- sectors[ticker == "JPM"]
jpm_fund   <- fund_all[ticker == "JPM"]

test("JPM in master",               nrow(jpm_master) == 1)
test("JPM status is ACTIVE",        jpm_master$status == "ACTIVE")
test("JPM sector is Financial",     jpm_sector$sector == "Financial")
test("JPM has fundamental data",    nrow(jpm_fund) > 0)
test("JPM fund CIK matches master", unique(jpm_fund$cik) == jpm_master$cik)
test("JPM lacks COGS",              !("cogs" %in% jpm_fund$concept))
test("JPM lacks inventory",         !("inventory" %in% jpm_fund$concept))
test("JPM has revenue",             "revenue" %in% jpm_fund$concept)
test("JPM has net_income",          "net_income" %in% jpm_fund$concept)
test("JPM has total_assets",        "total_assets" %in% jpm_fund$concept)

message("\n=== 13. End-to-End Spot Check: Removed Ticker (XOM or similar) ===")

# Pick a removed ticker that was fetched and has sector data
removed_with_data <- intersect(fetched_removed,
                               fund_sectors[!is.na(sector), ticker])
if (length(removed_with_data) > 0) {
  # Use first one alphabetically for reproducibility
  rtk <- sort(removed_with_data)[1]
  rtk_master <- master[ticker == rtk][order(-occurrence)][1]
  rtk_sector <- sectors[ticker == rtk]
  rtk_fund   <- fund_all[ticker == rtk]

  message(sprintf("  Selected: %s (status=%s, sector=%s)",
                  rtk, rtk_master$status, rtk_sector$sector[1]))
  test(sprintf("%s has fundamental data", rtk), nrow(rtk_fund) > 0)
  test(sprintf("%s has date_removed in master", rtk),
       !is.na(rtk_master$date_removed))
  test(sprintf("%s fund CIK matches master", rtk),
       unique(rtk_fund$cik) == rtk_master$cik)
} else {
  message("  SKIP: no removed ticker with sector data found")
}


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
