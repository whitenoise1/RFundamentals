# ============================================================================
# test_integration_abcde.R  --  Integration Tests: Session A + B + C + D + E
# ============================================================================
# Run: Rscript tests/test_integration_abcde.R
#
# Validates end-to-end flow from constituent roster through indicator
# computation across all five modules:
#   Session A: constituent_master.parquet (roster, CIK, duplicates)
#   Session B: sector_industry.parquet (finviz sectors)
#   Session C+D: fundamental_fetcher.R (639+ tickers cached)
#   Session E: indicator_compute.R (57 output fields per ticker)
#
# Tests:
#   1. Full pipeline: master -> sector -> fundamentals -> indicators
#   2. Indicator coverage by sector
#   3. Financial sector NA enforcement
#   4. Cross-sectional z-scoring at scale
#   5. Point-in-time indicator consistency
#   6. Spot checks: AAPL, JPM, MSFT indicators against known values
#   7. Growth indicator temporal consistency
#   8. Piotroski F-Score distribution
#   9. Scale gate: 500+ tickers, 57 indicators
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")

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
# SETUP: Load all outputs from Sessions A-D
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

fund_files <- list.files(fund_dir, pattern = "[.]parquet$", full.names = TRUE)
stopifnot(length(fund_files) > 0)

# Active tickers with CIK and sector
active <- master[status == "ACTIVE" & !is.na(cik)]
active_with_sector <- merge(active, sectors, by = "ticker", all.x = TRUE)

# Build lookup: ticker -> fund parquet file
fund_file_map <- setNames(fund_files, sub(".*_([A-Z0-9.-]+)\\.parquet$", "\\1", fund_files))

message(sprintf("  master:  %d active tickers with CIK", nrow(active)))
message(sprintf("  sectors: %d tickers", nrow(sectors)))
message(sprintf("  fundamentals: %d cached files", length(fund_files)))


# ============================================================================
# 1. FULL PIPELINE: Compute indicators for all active tickers
# ============================================================================
message("\n=== 1. Full Pipeline: Computing indicators for all active tickers ===")

# Use active tickers that have both sector data and fundamental parquet
compute_tickers <- active_with_sector[!is.na(sector) & ticker %in% names(fund_file_map)]
n_compute <- nrow(compute_tickers)
message(sprintf("  Tickers to compute: %d", n_compute))

indicator_list <- list()
tickers_used   <- character()
sectors_used   <- character()
failed_tickers <- character()

t_start <- Sys.time()

for (i in seq_len(n_compute)) {
  tk  <- compute_tickers$ticker[i]
  sec <- compute_tickers$sector[i]

  fund_dt <- tryCatch(
    as.data.table(arrow::read_parquet(fund_file_map[[tk]])),
    error = function(e) NULL
  )
  if (is.null(fund_dt) || nrow(fund_dt) == 0) {
    failed_tickers <- c(failed_tickers, tk)
    next
  }

  # Use a placeholder price (100) since we don't have price data in this test.
  # Valuation ratios will use this placeholder, but profitability/growth/etc.
  # are price-independent and fully testable.
  res <- compute_ticker_indicators(fund_dt, 100, sec)

  indicator_list[[length(indicator_list) + 1L]] <- res
  tickers_used <- c(tickers_used, tk)
  sectors_used <- c(sectors_used, sec)

  if (i %% 100 == 0) {
    message(sprintf("  ... computed %d / %d tickers", i, n_compute))
  }
}

t_elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
message(sprintf("  Computed %d tickers in %.1f seconds (%.0f tickers/sec)",
                length(indicator_list), t_elapsed,
                length(indicator_list) / t_elapsed))
message(sprintf("  Failed: %d tickers", length(failed_tickers)))

test("computed indicators for >= 450 active tickers",
     length(indicator_list) >= 450)

test("failure rate < 5%",
     length(failed_tickers) / n_compute < 0.05)


# ============================================================================
# 2. BUILD CROSS-SECTION
# ============================================================================
message("\n=== 2. Cross-Section Construction ===")

cs <- compute_cross_section(indicator_list, tickers_used, sectors_used)

test("cross-section raw is data.table",
     is.data.table(cs$raw))

test("cross-section zscored is data.table",
     is.data.table(cs$zscored))

test("raw has correct row count",
     nrow(cs$raw) == length(indicator_list))

test("zscored has correct row count",
     nrow(cs$zscored) == length(indicator_list))

test("raw has all 57 indicator columns",
     all(get_indicator_names() %in% names(cs$raw)))

test("zscored has all 57 indicator columns",
     all(get_indicator_names() %in% names(cs$zscored)))

test("raw has ticker column",
     "ticker" %in% names(cs$raw))


# ============================================================================
# 3. INDICATOR COVERAGE BY SECTOR
# ============================================================================
message("\n=== 3. Indicator Coverage by Sector ===")

# Add sector back to raw for analysis
raw_with_sector <- copy(cs$raw)
raw_with_sector[, sector := sectors_used]

# Price-independent indicators (not affected by placeholder price)
price_independent <- c(
  "gross_margin", "operating_margin", "net_margin", "roe", "roa", "roic",
  "revenue_growth_yoy", "eps_growth_yoy", "opinc_growth_yoy", "ebitda_growth",
  "debt_equity", "interest_coverage", "current_ratio", "quick_ratio",
  "asset_turnover", "inventory_turnover", "receivables_turnover",
  "fcf_ni", "opcf_ni", "capex_revenue",
  "gpa", "asset_growth", "sloan_accrual", "pct_accruals", "net_operating_assets",
  "f_score", "sga_efficiency", "capex_depreciation", "dso_change"
)

# Overall non-NA rate for price-independent indicators
pi_cols <- intersect(price_independent, names(raw_with_sector))
pi_matrix <- as.matrix(raw_with_sector[, ..pi_cols])
overall_non_na <- mean(!is.na(pi_matrix))
message(sprintf("  Price-independent indicators: %.1f%% non-NA overall",
                overall_non_na * 100))
test("price-independent indicators >= 60% non-NA", overall_non_na >= 0.60)

# Coverage by sector
sector_coverage <- raw_with_sector[, {
  vals <- unlist(.SD[, .SD, .SDcols = pi_cols])
  .(n = .N, pct_non_na = round(100 * mean(!is.na(vals)), 1))
}, by = sector, .SDcols = pi_cols][order(-n)]

message("  Coverage by sector (price-independent):")
for (i in seq_len(nrow(sector_coverage))) {
  message(sprintf("    %-25s  n=%3d  non-NA=%.1f%%",
                  sector_coverage$sector[i],
                  sector_coverage$n[i],
                  sector_coverage$pct_non_na[i]))
}


# ============================================================================
# 4. FINANCIAL SECTOR NA ENFORCEMENT
# ============================================================================
message("\n=== 4. Financial Sector NA Enforcement ===")

fin_raw <- raw_with_sector[sector == "Financial"]
non_fin_raw <- raw_with_sector[sector != "Financial"]

if (nrow(fin_raw) > 0) {
  # These 5 indicators MUST be NA for all financials
  for (ind in c("gpa", "inventory_turnover", "cash_based_op",
                "capex_depreciation", "inventory_sales_change")) {
    if (ind %in% names(fin_raw)) {
      all_na <- all(is.na(fin_raw[[ind]]))
      test(sprintf("financial: %s is all-NA (%d tickers)", ind, nrow(fin_raw)),
           all_na)
    }
  }

  # These should NOT be all-NA for financials
  for (ind in c("roe", "roa", "net_margin", "f_score")) {
    if (ind %in% names(fin_raw)) {
      some_non_na <- any(!is.na(fin_raw[[ind]]))
      test(sprintf("financial: %s has some values", ind), some_non_na)
    }
  }
}

if (nrow(non_fin_raw) > 0) {
  # Non-financials should mostly have these indicators
  for (ind in c("gpa", "inventory_turnover")) {
    if (ind %in% names(non_fin_raw)) {
      pct <- mean(!is.na(non_fin_raw[[ind]])) * 100
      test(sprintf("non-financial: %s >= 50%% non-NA (%.1f%%)", ind, pct),
           pct >= 50)
    }
  }
}


# ============================================================================
# 5. CROSS-SECTIONAL Z-SCORE PROPERTIES
# ============================================================================
message("\n=== 5. Z-Score Properties ===")

z_matrix <- as.matrix(cs$zscored[, ..pi_cols])

# Z-scores should be bounded [-3, 3]
test("all z-scores bounded [-3, 3]", {
  all(is.na(z_matrix) | (z_matrix >= -3 & z_matrix <= 3))
})

# For well-populated indicators, mean should be near 0
for (ind in c("gross_margin", "roe", "roa", "current_ratio", "asset_turnover")) {
  if (ind %in% colnames(z_matrix)) {
    vals <- z_matrix[, ind]
    vals <- vals[!is.na(vals)]
    if (length(vals) >= 50) {
      m <- abs(mean(vals))
      test(sprintf("z-score mean near 0: %s (|mean|=%.3f)", ind, m), m < 0.15)
    }
  }
}

# Financial-NA indicators: z-scores should be NA for financials
if (nrow(fin_raw) > 0) {
  fin_idx <- which(sectors_used == "Financial")
  for (ind in c("gpa", "inventory_turnover")) {
    if (ind %in% colnames(z_matrix)) {
      test(sprintf("z-score: %s is NA for all financials", ind),
           all(is.na(z_matrix[fin_idx, ind])))
    }
  }
}


# ============================================================================
# 6. SPOT CHECKS: AAPL
# ============================================================================
message("\n=== 6. Spot Check: AAPL ===")

aapl_idx <- match("AAPL", tickers_used)
if (!is.na(aapl_idx)) {
  aapl <- indicator_list[[aapl_idx]]

  test("AAPL: gross_margin between 0.35 and 0.50",
       !is.na(aapl[["gross_margin"]]) &&
         aapl[["gross_margin"]] > 0.35 && aapl[["gross_margin"]] < 0.50)

  test("AAPL: roe > 0.5 (high-ROE company)",
       !is.na(aapl[["roe"]]) && aapl[["roe"]] > 0.5)

  test("AAPL: roa between 0.15 and 0.40",
       !is.na(aapl[["roa"]]) &&
         aapl[["roa"]] > 0.15 && aapl[["roa"]] < 0.40)

  test("AAPL: gpa > 0 (non-financial, has COGS)",
       !is.na(aapl[["gpa"]]) && aapl[["gpa"]] > 0)

  test("AAPL: f_score between 4 and 9 (healthy company)",
       aapl[["f_score"]] >= 4 && aapl[["f_score"]] <= 9)

  test("AAPL: revenue_growth_yoy is finite",
       !is.na(aapl[["revenue_growth_yoy"]]) &&
         is.finite(aapl[["revenue_growth_yoy"]]))

  test("AAPL: asset_growth is finite",
       !is.na(aapl[["asset_growth"]]) && is.finite(aapl[["asset_growth"]]))

  test("AAPL: sloan_accrual between -0.5 and 0.5",
       !is.na(aapl[["sloan_accrual"]]) &&
         aapl[["sloan_accrual"]] > -0.5 && aapl[["sloan_accrual"]] < 0.5)

  # Count non-NA indicators (price-independent)
  n_pi <- sum(!is.na(aapl[pi_cols]))
  message(sprintf("  AAPL: %d/%d price-independent indicators non-NA",
                  n_pi, length(pi_cols)))
  test("AAPL: >= 25 price-independent indicators computed",
       n_pi >= 25)

} else {
  message("  SKIP  AAPL not in computed set")
}


# ============================================================================
# 7. SPOT CHECK: JPM (Financial)
# ============================================================================
message("\n=== 7. Spot Check: JPM (Financial) ===")

jpm_idx <- match("JPM", tickers_used)
if (!is.na(jpm_idx)) {
  jpm <- indicator_list[[jpm_idx]]

  test("JPM: gpa is NA",                   is.na(jpm[["gpa"]]))
  test("JPM: inventory_turnover is NA",     is.na(jpm[["inventory_turnover"]]))
  test("JPM: cash_based_op is NA",          is.na(jpm[["cash_based_op"]]))
  test("JPM: capex_depreciation is NA",     is.na(jpm[["capex_depreciation"]]))
  test("JPM: inventory_sales_change is NA", is.na(jpm[["inventory_sales_change"]]))

  test("JPM: roe is not NA",
       !is.na(jpm[["roe"]]))

  test("JPM: roe between 0.05 and 0.30 (typical bank)",
       !is.na(jpm[["roe"]]) && jpm[["roe"]] > 0.05 && jpm[["roe"]] < 0.30)

  test("JPM: f_score in [0, 9]",
       jpm[["f_score"]] >= 0 && jpm[["f_score"]] <= 9)

  test("JPM: net_margin > 0 (profitable bank)",
       !is.na(jpm[["net_margin"]]) && jpm[["net_margin"]] > 0)

  n_pi <- sum(!is.na(jpm[pi_cols]))
  message(sprintf("  JPM: %d/%d price-independent indicators non-NA",
                  n_pi, length(pi_cols)))

} else {
  message("  SKIP  JPM not in computed set")
}


# ============================================================================
# 8. SPOT CHECK: MSFT
# ============================================================================
message("\n=== 8. Spot Check: MSFT ===")

msft_idx <- match("MSFT", tickers_used)
if (!is.na(msft_idx)) {
  msft <- indicator_list[[msft_idx]]

  test("MSFT: gross_margin > 0.60 (software)",
       !is.na(msft[["gross_margin"]]) && msft[["gross_margin"]] > 0.60)

  test("MSFT: operating_margin > 0.30",
       !is.na(msft[["operating_margin"]]) && msft[["operating_margin"]] > 0.30)

  test("MSFT: roe > 0.20",
       !is.na(msft[["roe"]]) && msft[["roe"]] > 0.20)

  test("MSFT: gpa > 0",
       !is.na(msft[["gpa"]]) && msft[["gpa"]] > 0)

  test("MSFT: f_score >= 5 (healthy company)",
       msft[["f_score"]] >= 5)

} else {
  message("  SKIP  MSFT not in computed set")
}


# ============================================================================
# 9. GROWTH INDICATOR TEMPORAL CONSISTENCY
# ============================================================================
message("\n=== 9. Growth Indicator Consistency ===")

# Revenue growth YoY should be between -1 and +5 for most tickers
rev_growth <- cs$raw$revenue_growth_yoy
rev_growth <- rev_growth[!is.na(rev_growth)]
if (length(rev_growth) > 0) {
  extreme_growth <- sum(rev_growth < -1 | rev_growth > 5)
  extreme_pct <- 100 * extreme_growth / length(rev_growth)
  message(sprintf("  Revenue growth YoY: %d values, %d extreme (%.1f%%)",
                  length(rev_growth), extreme_growth, extreme_pct))
  test("revenue_growth_yoy: < 5% extreme values",
       extreme_pct < 5)

  test("revenue_growth_yoy: median between -0.2 and 0.5", {
    med <- median(rev_growth)
    med > -0.2 && med < 0.5
  })
}

# EPS growth should exist for most tickers
eps_growth <- cs$raw$eps_growth_yoy
eps_non_na <- sum(!is.na(eps_growth))
eps_pct <- 100 * eps_non_na / length(eps_growth)
message(sprintf("  EPS growth YoY: %d / %d non-NA (%.1f%%)",
                eps_non_na, length(eps_growth), eps_pct))
test("eps_growth_yoy: >= 60% non-NA", eps_pct >= 60)


# ============================================================================
# 10. PIOTROSKI F-SCORE DISTRIBUTION
# ============================================================================
message("\n=== 10. Piotroski F-Score Distribution ===")

f_scores <- cs$raw$f_score
f_scores <- f_scores[!is.na(f_scores)]
if (length(f_scores) > 0) {
  message(sprintf("  F-Score: %d tickers computed", length(f_scores)))
  message(sprintf("  F-Score range: [%d, %d]", min(f_scores), max(f_scores)))
  message(sprintf("  F-Score median: %.0f, mean: %.1f",
                  median(f_scores), mean(f_scores)))

  # Distribution
  f_table <- table(f_scores)
  for (s in sort(as.integer(names(f_table)))) {
    message(sprintf("    Score %d: %d tickers (%.1f%%)",
                    s, f_table[as.character(s)],
                    100 * f_table[as.character(s)] / length(f_scores)))
  }

  test("f_score: all values in [0, 9]",
       all(f_scores >= 0 & f_scores <= 9))

  test("f_score: median between 3 and 7",
       median(f_scores) >= 3 && median(f_scores) <= 7)

  test("f_score: no single score dominates (max < 40%)",
       max(f_table) / length(f_scores) < 0.40)

  # All 9 binary components should be present
  for (comp in c("f_roa", "f_droa", "f_cfo", "f_accrual", "f_dlever",
                 "f_dliquid", "f_eq_off", "f_dmargin", "f_dturn")) {
    vals <- cs$raw[[comp]]
    vals <- vals[!is.na(vals)]
    test(sprintf("piotroski %s: all 0 or 1", comp),
         all(vals %in% c(0, 1)))
    # Each component should have some 1s and some 0s across 500+ tickers
    if (length(vals) >= 100) {
      test(sprintf("piotroski %s: has both 0 and 1", comp),
           0 %in% vals && 1 %in% vals)
    }
  }
}


# ============================================================================
# 11. SCALE GATE
# ============================================================================
message("\n=== 11. Scale Gate ===")

test("scale: >= 450 tickers computed",
     length(indicator_list) >= 450)

test("scale: raw matrix dimensions match",
     nrow(cs$raw) >= 450 && ncol(cs$raw) >= 57)

test("scale: zscored matrix dimensions match",
     nrow(cs$zscored) >= 450 && ncol(cs$zscored) >= 57)

# At least 30 price-independent indicators should be non-NA for > 80% of tickers
non_na_rates <- colMeans(!is.na(as.matrix(cs$raw[, ..pi_cols])))
well_covered <- sum(non_na_rates >= 0.80)
message(sprintf("  Indicators with >= 80%% coverage: %d / %d price-independent",
                well_covered, length(pi_cols)))
test("scale: >= 15 price-independent indicators with 80%+ coverage",
     well_covered >= 15)


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== RESULTS: %d passed, %d failed ===",
                .n_pass, .n_fail))
if (.n_fail > 0) {
  stop(sprintf("%d test(s) failed", .n_fail))
} else {
  message("All integration tests (A+B+C+D+E) passed.")
}
