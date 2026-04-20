# ============================================================================
# test_indicator_compute.R  --  Unit + Integration Tests for Module 4
# ============================================================================
# Run: Rscript tests/test_indicator_compute.R
#
# Unit tests: synthetic data, helpers, pivot, each indicator group.
# Integration tests: real cached parquet data (AAPL, JPM), cross-section.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

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
# UNIT TESTS: Helper functions
# ============================================================================
message("\n=== Helper Functions ===")

# .safe_divide
test("safe_divide: normal case",
     .safe_divide(10, 5) == 2)

test("safe_divide: NA numerator",
     is.na(.safe_divide(NA, 5)))

test("safe_divide: NA denominator",
     is.na(.safe_divide(10, NA)))

test("safe_divide: zero denominator",
     is.na(.safe_divide(10, 0)))

test("safe_divide: near-zero denominator",
     is.na(.safe_divide(10, 1e-12)))

test("safe_divide: negative denominator",
     .safe_divide(10, -2) == -5)

test("safe_divide: custom min_abs_denom",
     is.na(.safe_divide(10, 0.5, min_abs_denom = 1)))

# .safe_growth
test("safe_growth: normal case",
     abs(.safe_growth(120, 100) - 0.2) < 1e-10)

test("safe_growth: negative prior",
     abs(.safe_growth(-80, -100) - 0.2) < 1e-10)

test("safe_growth: zero prior",
     is.na(.safe_growth(100, 0)))

test("safe_growth: NA prior",
     is.na(.safe_growth(100, NA)))

# .winsorize
test("winsorize: all NA returns all NA",
     all(is.na(.winsorize(rep(NA_real_, 5)))))

test("winsorize: clips extremes", {
  x <- c(-100, 1, 2, 3, 4, 5, 200)
  w <- .winsorize(x, probs = c(0.1, 0.9))
  # extremes should be clipped inward; middle values unchanged
  w[1] > -100 && w[7] < 200 && w[3] == 2 && w[4] == 3
})

# .col
test("col: existing column", {
  dt <- data.table(revenue = 1000)
  .col(dt, "revenue") == 1000
})

test("col: missing column", {
  dt <- data.table(revenue = 1000)
  is.na(.col(dt, "cogs"))
})

test("col: NA value", {
  dt <- data.table(revenue = NA_real_)
  is.na(.col(dt, "revenue"))
})

test("col: NULL input returns NA",
     is.na(.col(NULL, "revenue")))


# ============================================================================
# UNIT TESTS: pivot_fundamentals
# ============================================================================
message("\n=== pivot_fundamentals ===")

# Build synthetic long-format data
.make_long <- function(concepts, values, fy = 2024L, pt = "FY",
                       pe = as.Date("2024-12-31"),
                       filed = as.Date("2025-02-15")) {
  data.table(
    concept     = concepts,
    value       = values,
    fiscal_year = fy,
    period_type = pt,
    period_end  = pe,
    filed       = filed
  )
}

fund_long <- rbindlist(list(
  .make_long(c("revenue", "cogs", "net_income", "total_assets", "operating_income"),
             c(1000, 400, 120, 5000, 200)),
  .make_long(c("revenue", "cogs", "net_income", "total_assets", "operating_income"),
             c(800, 350, 100, 4500, 170),
             fy = 2023L, pe = as.Date("2023-12-31"),
             filed = as.Date("2024-02-15"))
))

wide <- pivot_fundamentals(fund_long)

test("pivot: returns data.table",
     is.data.table(wide))

test("pivot: 2 rows (2 fiscal years)",
     nrow(wide) == 2)

test("pivot: has revenue column",
     "revenue" %in% names(wide))

test("pivot: has total_assets column",
     "total_assets" %in% names(wide))

test("pivot: correct revenue for FY2024",
     wide[fiscal_year == 2024, revenue] == 1000)

test("pivot: correct revenue for FY2023",
     wide[fiscal_year == 2023, revenue] == 800)

test("pivot: NULL input returns NULL",
     is.null(pivot_fundamentals(NULL)))

test("pivot: empty input returns NULL",
     is.null(pivot_fundamentals(data.table())))


# ============================================================================
# UNIT TESTS: .derive_quantities
# ============================================================================
message("\n=== .derive_quantities ===")

derived <- .derive_quantities(copy(wide))

test("derive: gross_profit computed",
     "gross_profit" %in% names(derived))

test("derive: gross_profit = revenue - cogs",
     derived[fiscal_year == 2024, gross_profit] == 600)

test("derive: ebitda computed",
     "ebitda" %in% names(derived))

test("derive: ebitda = operating_income + 0 (no depreciation)",
     derived[fiscal_year == 2024, ebitda] == 200)


# ============================================================================
# UNIT TESTS: Indicator groups with synthetic data
# ============================================================================
message("\n=== Baseline Indicators (synthetic) ===")

# Build a complete synthetic current row
.make_wide_row <- function() {
  data.table(
    fiscal_year = 2024L, period_type = "FY",
    period_end = as.Date("2024-12-31"),
    filed = as.Date("2025-02-15"),
    revenue = 10000, cogs = 4000, operating_income = 3000,
    net_income = 2000, eps_basic = 4.0, eps_diluted = 3.8,
    interest_expense = 200, sga = 2000, rnd = 500,
    depreciation = 800,
    total_assets = 50000, stockholders_equity = 20000,
    long_term_debt = 5000, short_term_debt = 1000,
    current_assets = 15000, current_liabilities = 8000,
    total_liabilities = 30000,
    accounts_receivable = 2000, inventory = 3000,
    cash = 4000,
    shares_outstanding = 1000,
    accounts_payable = 1500, accrued_liabilities = 500,
    deferred_revenue = 300, prepaid_expenses = 200,
    operating_cashflow = 3500, capex = 1200,
    buybacks = -800, dividends_paid = -600
  )
}

curr_row <- .make_wide_row()
curr_row <- .derive_quantities(curr_row)

prior_row <- .make_wide_row()
prior_row[, `:=`(fiscal_year = 2023L, revenue = 9000, net_income = 1800,
                 eps_diluted = 3.4, operating_income = 2700,
                 total_assets = 48000, stockholders_equity = 18000,
                 long_term_debt = 5500, current_assets = 14000,
                 current_liabilities = 7500, shares_outstanding = 1010,
                 accounts_receivable = 1800, inventory = 2800,
                 cogs = 3700, sga = 1900,
                 accounts_payable = 1400, accrued_liabilities = 450,
                 deferred_revenue = 250, prepaid_expenses = 180)]
prior_row <- .derive_quantities(prior_row)

price <- 150

# Valuation
val <- .compute_valuation(curr_row, price, 1000)

test("valuation: market_cap = price * shares",
     val$market_cap == 150000)

test("valuation: pe_trailing = price / eps",
     abs(val$pe_trailing - 150/3.8) < 0.01)

test("valuation: pb = market_cap / equity",
     abs(val$pb - 150000/20000) < 0.01)

test("valuation: negative EPS -> pe still computed (eps > 0.01)", {
  neg_row <- copy(curr_row)
  neg_row[, eps_diluted := -2.0]
  v <- .compute_valuation(neg_row, price, 1000)
  !is.na(v$pe_trailing)  # negative PE is valid, just indicates loss
})

test("valuation: zero EPS -> pe is NA", {
  zero_row <- copy(curr_row)
  zero_row[, eps_diluted := 0]
  v <- .compute_valuation(zero_row, price, 1000)
  is.na(v$pe_trailing)
})

test("valuation: EV computed when cash column missing", {
  no_cash <- copy(curr_row)
  no_cash[, cash := NULL]
  no_cash <- .derive_quantities(no_cash)
  v <- .compute_valuation(no_cash, price, 1000)
  # EV = mkt_cap + net_debt = 150000 + total_debt (6000) = 156000
  !is.na(v$enterprise_value) && v$enterprise_value == 156000
})

test("valuation: EV = mkt_cap + total_debt - cash", {
  v <- .compute_valuation(curr_row, price, 1000)
  # 150000 + 6000 - 4000 = 152000
  abs(v$enterprise_value - 152000) < 1
})

# Profitability
prof <- .compute_profitability(curr_row)

test("profitability: gross_margin = GP/Rev",
     abs(prof$gross_margin - 6000/10000) < 0.001)

test("profitability: operating_margin = OI/Rev",
     abs(prof$operating_margin - 3000/10000) < 0.001)

test("profitability: roe = NI/Equity",
     abs(prof$roe - 2000/20000) < 0.001)

test("profitability: negative equity -> roe is NA", {
  neg_eq <- copy(curr_row)
  neg_eq[, stockholders_equity := -1000]
  p <- .compute_profitability(neg_eq)
  is.na(p$roe)
})

# Growth
grow <- .compute_growth(curr_row, prior_row)

test("growth: revenue_growth_yoy = (10000-9000)/9000",
     abs(grow$revenue_growth_yoy - 1/9) < 0.001)

test("growth: eps_growth_yoy = (3.8-3.4)/3.4",
     abs(grow$eps_growth_yoy - 0.4/3.4) < 0.001)

test("growth: no prior -> all NA", {
  g <- .compute_growth(curr_row, NULL)
  is.na(g$revenue_growth_yoy) && is.na(g$eps_growth_yoy)
})

test("growth: qoq compares consecutive quarters", {
  q2 <- data.table(revenue = 2500)
  q3 <- data.table(revenue = 2700)
  g <- .compute_growth(curr_row, prior_row, curr_q = q3, prior_q = q2)
  abs(g$revenue_growth_qoq - (2700 - 2500) / 2500) < 0.001
})

# Leverage
lev <- .compute_leverage(curr_row)

test("leverage: current_ratio = CA/CL",
     abs(lev$current_ratio - 15000/8000) < 0.001)

test("leverage: interest_coverage = OI/IntExp",
     abs(lev$interest_coverage - 3000/200) < 0.001)

# Efficiency
eff <- .compute_efficiency(curr_row)

test("efficiency: asset_turnover = Rev/Assets",
     abs(eff$asset_turnover - 10000/50000) < 0.001)

test("efficiency: inventory_turnover = COGS/Inv",
     abs(eff$inventory_turnover - 4000/3000) < 0.001)

# Cash flow quality
cfq <- .compute_cashflow_quality(curr_row)

test("cf_quality: capex_revenue = CapEx/Rev",
     abs(cfq$capex_revenue - 1200/10000) < 0.001)

# Shareholder return
shr <- .compute_shareholder(curr_row, 150000)

test("shareholder: dividend_yield = |div|/mktcap",
     abs(shr$dividend_yield - 600/150000) < 0.001)

test("shareholder: buyback_yield = |buy|/mktcap",
     abs(shr$buyback_yield - 800/150000) < 0.001)


# ============================================================================
# UNIT TESTS: Tier 1 Research Indicators
# ============================================================================
message("\n=== Tier 1 Research Indicators ===")

t1 <- .compute_tier1(curr_row, prior_row)

test("tier1: gpa = GP/Assets",
     abs(t1$gpa - 6000/50000) < 0.001)

test("tier1: asset_growth = (50000-48000)/48000",
     abs(t1$asset_growth - 2000/48000) < 0.001)

test("tier1: sloan_accrual = (NI-CFO)/avg_assets", {
  accrual <- (2000 - 3500) / ((50000 + 48000) / 2)
  abs(t1$sloan_accrual - accrual) < 0.001
})

test("tier1: pct_accruals = (NI-CFO)/|NI|", {
  pct <- (2000 - 3500) / abs(2000)
  abs(t1$pct_accruals - pct) < 0.001
})

test("tier1: net_operating_assets formula", {
  # NOA = (assets - cash - (liab - debt)) / prior_assets
  # = (50000 - 4000 - (30000 - 6000)) / 48000
  # = (46000 - 24000) / 48000 = 22000 / 48000
  expected <- 22000 / 48000
  abs(t1$net_operating_assets - expected) < 0.001
})

# Piotroski
test("piotroski: f_roa = 1 (NI > 0)",
     t1$f_roa == 1L)

test("piotroski: f_cfo = 1 (CFO/Assets > 0)",
     t1$f_cfo == 1L)

test("piotroski: f_accrual = 1 (CFO > NI: 3500 > 2000)",
     t1$f_accrual == 1L)

test("piotroski: f_droa = 1 (ROA improved)", {
  # ROA curr = 2000/50000 = 0.04, prior = 1800/48000 = 0.0375
  t1$f_droa == 1L
})

test("piotroski: f_score in [0, 9]",
     t1$f_score >= 0 && t1$f_score <= 9)

test("piotroski: f_score is sum of components", {
  comps <- c(t1$f_roa, t1$f_droa, t1$f_cfo, t1$f_accrual,
             t1$f_dlever, t1$f_dliquid, t1$f_eq_off,
             t1$f_dmargin, t1$f_dturn)
  t1$f_score == sum(comps)
})

test("piotroski: all components are 0 or 1", {
  comps <- c(t1$f_roa, t1$f_droa, t1$f_cfo, t1$f_accrual,
             t1$f_dlever, t1$f_dliquid, t1$f_eq_off,
             t1$f_dmargin, t1$f_dturn)
  all(comps %in% c(0L, 1L))
})


# ---- Piotroski NA propagation (Fix #2) ----

test("piotroski NA: no prior -> all prior-dependent components NA", {
  t_no_prior <- .compute_tier1(curr_row, NULL)
  # Only current-only components (f_roa, f_cfo, f_accrual) are non-NA.
  # f_eq_off needs p_shares (prior), so it is NA without prior.
  is.na(t_no_prior$f_droa) && is.na(t_no_prior$f_dlever) &&
    is.na(t_no_prior$f_dliquid) && is.na(t_no_prior$f_dmargin) &&
    is.na(t_no_prior$f_dturn) && is.na(t_no_prior$f_eq_off)
})

test("piotroski NA: no prior -> f_score is NA", {
  t_no_prior <- .compute_tier1(curr_row, NULL)
  is.na(t_no_prior$f_score)
})

test("piotroski NA: current-only components still computed without prior", {
  t_no_prior <- .compute_tier1(curr_row, NULL)
  t_no_prior$f_roa == 1L && t_no_prior$f_cfo == 1L && t_no_prior$f_accrual == 1L
})

test("piotroski NA: missing net_income yields f_roa/f_droa/f_accrual NA", {
  no_ni <- copy(curr_row)
  no_ni[, net_income := NA_real_]
  t <- .compute_tier1(no_ni, prior_row)
  is.na(t$f_roa) && is.na(t$f_droa) && is.na(t$f_accrual) && is.na(t$f_score)
})

test("piotroski NA: missing prior shares only -> only f_eq_off NA", {
  p_no_shares <- copy(prior_row)
  p_no_shares[, shares_outstanding := NA_real_]
  t <- .compute_tier1(curr_row, p_no_shares)
  is.na(t$f_eq_off) && !is.na(t$f_roa) && !is.na(t$f_droa) &&
    is.na(t$f_score)  # any NA component -> composite NA
})

test("piotroski NA: all inputs present -> components all 0 or 1, f_score integer", {
  t <- .compute_tier1(curr_row, prior_row)
  comps <- c(t$f_roa, t$f_droa, t$f_cfo, t$f_accrual,
             t$f_dlever, t$f_dliquid, t$f_eq_off,
             t$f_dmargin, t$f_dturn)
  all(comps %in% c(0L, 1L)) && !is.na(t$f_score) &&
    t$f_score == sum(comps)
})


# ---- Deprecated-tag filter (Fix #3) ----

test("filter_deprecated_tags: drops InterestIncomeExpenseNet rows", {
  dt <- data.table(
    concept = c("interest_expense", "interest_expense", "revenue"),
    tag     = c("InterestExpense", "InterestIncomeExpenseNet", "Revenues"),
    value   = c(100, -50, 10000)
  )
  out <- .filter_deprecated_tags(dt)
  nrow(out) == 2 &&
    !("InterestIncomeExpenseNet" %in% out$tag)
})

test("filter_deprecated_tags: keeps same tag under a different concept", {
  dt <- data.table(
    concept = c("interest_expense", "other_concept"),
    tag     = c("InterestIncomeExpenseNet", "InterestIncomeExpenseNet"),
    value   = c(-50, 200)
  )
  out <- .filter_deprecated_tags(dt)
  nrow(out) == 1 && out$concept == "other_concept"
})

test("filter_deprecated_tags: NULL and empty input are safe", {
  is.null(.filter_deprecated_tags(NULL)) &&
    nrow(.filter_deprecated_tags(data.table())) == 0
})

test("filter_deprecated_tags: no tag column -> pass-through", {
  dt <- data.table(concept = "revenue", value = 1000)
  out <- .filter_deprecated_tags(dt)
  nrow(out) == 1 && identical(out, dt)
})

test("filter_deprecated_tags: does not mutate input", {
  dt <- data.table(
    concept = c("interest_expense", "interest_expense"),
    tag     = c("InterestExpense", "InterestIncomeExpenseNet"),
    value   = c(100, -50)
  )
  nrows_before <- nrow(dt)
  .filter_deprecated_tags(dt)
  nrow(dt) == nrows_before
})


# ============================================================================
# UNIT TESTS: Tier 2 Research Indicators
# ============================================================================
message("\n=== Tier 2 Research Indicators ===")

t2 <- .compute_tier2(curr_row, prior_row)

test("tier2: cash_based_op formula", {
  # (Rev - COGS - SGA + R&D - dAR - dINV - dPrepaid + dDefRev + dAP + dAccrLiab) / Assets
  # = (10000 - 4000 - 2000 + 500
  #    - (2000-1800) - (3000-2800) - (200-180)
  #    + (300-250) + (1500-1400) + (500-450)) / 50000
  # = (4500 - 200 - 200 - 20 + 50 + 100 + 50) / 50000
  # = 4280 / 50000 = 0.0856
  abs(t2$cash_based_op - 4280/50000) < 0.001
})

test("tier2: sga_efficiency is numeric",
     is.numeric(t2$sga_efficiency) && !is.na(t2$sga_efficiency))

test("tier2: sga_efficiency = %dSGA - %dRev", {
  sga_g <- (2000 - 1900) / 1900
  rev_g <- (10000 - 9000) / 9000
  abs(t2$sga_efficiency - (sga_g - rev_g)) < 0.001
})

test("tier2: capex_depreciation = capex/depr",
     abs(t2$capex_depreciation - 1200/800) < 0.001)

test("tier2: capex_depreciation capped at 5", {
  high_capex <- copy(curr_row)
  high_capex[, capex := 10000]
  high_capex[, depreciation := 100]
  t2h <- .compute_tier2(high_capex, prior_row)
  t2h$capex_depreciation == 5
})

test("tier2: dso_change computed", {
  dso_c <- (2000 / 10000) * 365
  dso_p <- (1800 / 9000) * 365
  abs(t2$dso_change - (dso_c - dso_p)) < 0.1
})

test("tier2: fcf_stability is NA without quarterly data",
     is.na(t2$fcf_stability))

# Test FCF Stability with quarterly data.
# qhist must carry fiscal_year / period_type / cfo_period_days so the
# de-cumulator can convert YTD-cumulative CFO into standalone-quarter values.
# Build 12 quarters (4 fiscal years x Q1/Q2/Q3) of YTD-cumulative CFO.
set.seed(123)
q_fy   <- rep(2022:2025, each = 3)
q_pt   <- rep(c("Q1", "Q2", "Q3"), 4)
q_days <- rep(c(90L, 181L, 272L), 4)
# Standalone-quarter CFO values (what the series "really" is)
q_std_true <- rnorm(12, mean = 1000, sd = 100)
# Convert to YTD-cumulative within each fiscal_year
q_ytd <- ave(q_std_true, q_fy, FUN = cumsum)

qhist <- data.table(
  fiscal_year        = q_fy,
  period_type        = q_pt,
  cfo_period_days    = q_days,
  operating_cashflow = q_ytd,
  total_assets       = rep(50000, 12)
)
t2_q <- .compute_tier2(curr_row, prior_row, quarterly_hist = qhist)
test("tier2: fcf_stability computed with 12 quarters",
     !is.na(t2_q$fcf_stability) && t2_q$fcf_stability > 0)

# Minimal legacy qhist (no metadata): de-cumulator refuses, fcf_stability NA
qhist_legacy <- data.table(
  operating_cashflow = rnorm(12, mean = 1000, sd = 100),
  total_assets = rep(50000, 12)
)
t2_legacy <- .compute_tier2(curr_row, prior_row, quarterly_hist = qhist_legacy)
test("tier2: fcf_stability NA when quarterly metadata missing",
     is.na(t2_legacy$fcf_stability))


# ---- .decumulate_cfo unit tests ----

test("decumulate: Q1 passes through unchanged (already 3mo)", {
  q <- data.table(fiscal_year = 2024L, period_type = "Q1",
                  cfo_period_days = 90L,
                  operating_cashflow = 100, total_assets = 1000)
  d <- .decumulate_cfo(q)
  d$cfo_std == 100 && d$cfo_kind == "3mo"
})

test("decumulate: Q2_std = Q2_YTD - Q1_YTD", {
  q <- data.table(
    fiscal_year = c(2024L, 2024L),
    period_type = c("Q1", "Q2"),
    cfo_period_days = c(90L, 181L),
    operating_cashflow = c(100, 250),
    total_assets = c(1000, 1000)
  )
  d <- .decumulate_cfo(q)
  d[period_type == "Q2", cfo_std] == 150
})

test("decumulate: Q3_std = Q3_YTD - Q2_YTD", {
  q <- data.table(
    fiscal_year = rep(2024L, 3),
    period_type = c("Q1", "Q2", "Q3"),
    cfo_period_days = c(90L, 181L, 272L),
    operating_cashflow = c(100, 250, 450),
    total_assets = rep(1000, 3)
  )
  d <- .decumulate_cfo(q)
  d[period_type == "Q3", cfo_std] == 200
})

test("decumulate: Q2 with missing Q1 sibling -> NA", {
  q <- data.table(
    fiscal_year = 2024L, period_type = "Q2",
    cfo_period_days = 181L, operating_cashflow = 250,
    total_assets = 1000
  )
  d <- .decumulate_cfo(q)
  is.na(d$cfo_std)
})

test("decumulate: mismatched duration (TTM under fp=Q2) dropped", {
  q <- data.table(
    fiscal_year = c(2024L, 2024L),
    period_type = c("Q1", "Q2"),
    cfo_period_days = c(90L, 365L),   # Q2 is wrong duration
    operating_cashflow = c(100, 500),
    total_assets = c(1000, 1000)
  )
  d <- .decumulate_cfo(q)
  d[period_type == "Q1", cfo_std] == 100 &&
    is.na(d[period_type == "Q2", cfo_std])
})

test("decumulate: Q4_std = Q4_YTD - Q3_YTD (non-calendar fiscal filer)", {
  # Tickers like WMT/HD file fp=Q4 with a ~360-day period -- i.e. the annual
  # value labelled as Q4. The de-cumulator should derive Q4 standalone as
  # Q4_YTD - Q3_YTD, matching the last 3 months of the fiscal year.
  q <- data.table(
    fiscal_year = rep(2024L, 4),
    period_type = c("Q1", "Q2", "Q3", "Q4"),
    cfo_period_days = c(90L, 181L, 272L, 363L),
    operating_cashflow = c(100, 250, 450, 700),   # YTD cumulative
    total_assets = rep(1000, 4)
  )
  d <- .decumulate_cfo(q)
  d[period_type == "Q4", cfo_std] == 250  # 700 - 450
})

test("decumulate: stubs across fiscal_year boundary don't contaminate", {
  # Fiscal year 2024 has Q1/Q2; fiscal year 2025 has Q2 only -> 2025-Q2 is NA
  q <- data.table(
    fiscal_year = c(2024L, 2024L, 2025L),
    period_type = c("Q1", "Q2", "Q2"),
    cfo_period_days = c(90L, 181L, 181L),
    operating_cashflow = c(100, 250, 300),
    total_assets = rep(1000, 3)
  )
  d <- .decumulate_cfo(q)
  d[fiscal_year == 2024 & period_type == "Q2", cfo_std] == 150 &&
    is.na(d[fiscal_year == 2025 & period_type == "Q2", cfo_std])
})

test("decumulate: NULL / empty input returns input", {
  is.null(.decumulate_cfo(NULL)) &&
    nrow(.decumulate_cfo(data.table())) == 0
})

test("decumulate: missing required columns returns input unchanged", {
  q <- data.table(fiscal_year = 1L, period_type = "Q1",
                  operating_cashflow = 100)  # missing cfo_period_days
  d <- .decumulate_cfo(q)
  !("cfo_std" %in% names(d))
})

test("decumulate: 16-week Q1 (KR/AAP retail calendar) classified as 3mo", {
  # Kroger and Advance Auto Parts use a 16/12/12/12 fiscal calendar where
  # Q1 runs ~111 days. The 3mo band must include 111-120 to classify these
  # rows as valid standalone quarters.
  q <- data.table(fiscal_year = 2024L, period_type = "Q1",
                  cfo_period_days = 111L,
                  operating_cashflow = 100, total_assets = 1000)
  d <- .decumulate_cfo(q)
  d$cfo_std == 100 && d$cfo_kind == "3mo"
})


# ---- FCF Stability end-to-end correctness ----

test("fcf_stability: value equals sd of true standalone series", {
  # Build a deterministic standalone series, cumulate to YTD, feed in.
  set.seed(777)
  fy <- rep(2020:2025, each = 3)
  pt <- rep(c("Q1", "Q2", "Q3"), 6)
  dy <- rep(c(90L, 181L, 272L), 6)
  std_true <- rnorm(18, 1000, 100)
  ytd <- ave(std_true, fy, FUN = cumsum)
  q <- data.table(fiscal_year = fy, period_type = pt, cfo_period_days = dy,
                  operating_cashflow = ytd, total_assets = rep(50000, 18))
  t <- .compute_tier2(curr_row, prior_row, quarterly_hist = q)
  expected <- sd(std_true[(length(std_true)-15):length(std_true)] / 50000)
  abs(t$fcf_stability - expected) < 1e-10
})


# ---- Dedup duration-match tie-break (fetcher) ----
# Loaded here because the helper lives in R/fundamental_fetcher.R
source("R/fundamental_fetcher.R")

test("dedup: prefers YTD row over 12-month TTM at same period_end/fp", {
  dt <- data.table(
    ticker = "T", cik = "0", concept = "operating_cashflow",
    tag = "NetCashProvidedByUsedInOperatingActivities",
    value = c(60, 120),                                   # YTD, TTM
    period_end  = as.Date(c("2024-06-30", "2024-06-30")),
    period_start = as.Date(c("2024-01-01", "2023-07-01")), # 181d, 365d
    filed = as.Date(c("2024-08-01", "2024-08-01")),
    form = c("10-Q", "10-Q"), accession = c("A1", "A1"),
    fiscal_year = c(2024L, 2024L), fiscal_qtr = c("Q2", "Q2"), unit = "USD"
  )
  out <- dedup_fundamentals(dt)
  nrow(out) == 1 && out$value == 60
})

test("dedup: single row with mismatched duration still retained", {
  # If only the TTM row exists, dedup must keep it (no alternative).
  dt <- data.table(
    ticker = "T", cik = "0", concept = "operating_cashflow",
    tag = "NetCashProvidedByUsedInOperatingActivities",
    value = 120,
    period_end  = as.Date("2024-06-30"),
    period_start = as.Date("2023-07-01"),
    filed = as.Date("2024-08-01"),
    form = "10-Q", accession = "A1",
    fiscal_year = 2024L, fiscal_qtr = "Q2", unit = "USD"
  )
  out <- dedup_fundamentals(dt)
  nrow(out) == 1 && out$value == 120
})

test("dedup: instant tag (NA period_start) unaffected by tie-break", {
  # Balance-sheet tags have NA period_start. Ordering falls through to
  # the existing accession chain; later accession (amendment) wins.
  dt <- data.table(
    ticker = "T", cik = "0", concept = "total_assets", tag = "Assets",
    value = c(100, 101),
    period_end  = as.Date(c("2024-06-30", "2024-06-30")),
    period_start = as.Date(c(NA, NA)),
    filed = as.Date(c("2024-08-01", "2024-08-01")),
    form = c("10-Q", "10-Q"), accession = c("A1", "A2"),
    fiscal_year = c(2024L, 2024L), fiscal_qtr = c("Q2", "Q2"), unit = "USD"
  )
  out <- dedup_fundamentals(dt)
  nrow(out) == 1 && out$value == 101
})

test("dedup: .duration_match_rank returns 0 for NA inputs", {
  identical(.duration_match_rank(c(NA_integer_, 181L), c("Q2", NA)),
            c(0L, 0L))
})

test("dedup: .duration_match_rank flags out-of-band duration", {
  # Q2 band is [160, 200]. 365 is out, 181 is in.
  identical(.duration_match_rank(c(365L, 181L), c("Q2", "Q2")),
            c(1L, 0L))
})

test("dedup: .duration_match_rank accepts 16-week Q1 (111-120 days)", {
  # KR/AAP retail calendar: Q1 runs ~111 days. Q1 band must accept these.
  identical(.duration_match_rank(c(90L, 111L, 119L, 125L),
                                  rep("Q1", 4)),
            c(0L, 0L, 0L, 1L))
})


# ============================================================================
# UNIT TESTS: Z-scoring
# ============================================================================
message("\n=== Z-Scoring ===")

# Create a small cross-section
set.seed(42)
n <- 20
ztest_dt <- data.table(
  pe_trailing = rnorm(n, 20, 5),
  gross_margin = rnorm(n, 0.4, 0.1),
  gpa = rnorm(n, 0.3, 0.05),
  inventory_turnover = rnorm(n, 5, 1),
  sector = c(rep("Technology", 15), rep("Financial", 5))
)
# Set financial-NA indicators to NA for financials
ztest_dt[sector == "Financial", gpa := NA_real_]
ztest_dt[sector == "Financial", inventory_turnover := NA_real_]

zscored <- zscore_cross_section(ztest_dt)

test("zscore: output is data.table",
     is.data.table(zscored))

test("zscore: sector column removed",
     !("sector" %in% names(zscored)))

test("zscore: pe_trailing mean near 0", {
  m <- mean(zscored$pe_trailing, na.rm = TRUE)
  abs(m) < 0.1
})

test("zscore: pe_trailing sd near 1", {
  s <- sd(zscored$pe_trailing, na.rm = TRUE)
  abs(s - 1) < 0.2
})

test("zscore: financial rows remain NA for gpa",
     all(is.na(zscored$gpa[16:20])))

test("zscore: z-scores bounded to [-3, 3]", {
  all_vals <- unlist(zscored[, .SD, .SDcols = names(zscored)])
  all(is.na(all_vals) | (all_vals >= -3 & all_vals <= 3))
})

test("zscore: handles NA sector without error", {
  dt_na <- data.table(
    pe_trailing = c(20, 25, 30),
    gpa = c(0.3, 0.4, 0.5),
    sector = c("Technology", NA, "Financial")
  )
  z <- zscore_cross_section(dt_na)
  is.data.table(z) && nrow(z) == 3
})


# ============================================================================
# UNIT TESTS: Public API
# ============================================================================
message("\n=== Public API ===")

test("get_indicator_names returns 57 names",
     length(get_indicator_names()) == 57)

test("get_indicator_names has no duplicates",
     !anyDuplicated(get_indicator_names()))

# compute_ticker_indicators with synthetic long data
synth_long <- rbindlist(list(
  .make_long(c("revenue", "cogs", "operating_income", "net_income",
               "total_assets", "stockholders_equity", "eps_diluted",
               "operating_cashflow", "capex", "shares_outstanding",
               "current_assets", "current_liabilities",
               "long_term_debt", "short_term_debt", "cash",
               "interest_expense", "depreciation", "inventory",
               "accounts_receivable", "sga", "rnd",
               "buybacks", "dividends_paid",
               "accounts_payable", "accrued_liabilities",
               "deferred_revenue", "prepaid_expenses"),
             c(10000, 4000, 3000, 2000,
               50000, 20000, 3.8,
               3500, 1200, 1000,
               15000, 8000,
               5000, 1000, 4000,
               200, 800, 3000,
               2000, 2000, 500,
               -800, -600,
               1500, 500,
               300, 200)),
  .make_long(c("revenue", "cogs", "operating_income", "net_income",
               "total_assets", "stockholders_equity", "eps_diluted",
               "operating_cashflow", "capex", "shares_outstanding",
               "current_assets", "current_liabilities",
               "long_term_debt", "short_term_debt", "cash",
               "interest_expense", "depreciation", "inventory",
               "accounts_receivable", "sga",
               "accounts_payable", "accrued_liabilities",
               "deferred_revenue", "prepaid_expenses"),
             c(9000, 3700, 2700, 1800,
               48000, 18000, 3.4,
               3200, 1100, 1010,
               14000, 7500,
               5500, 900, 3500,
               220, 750, 2800,
               1800, 1900,
               1400, 450,
               250, 180),
             fy = 2023L, pe = as.Date("2023-12-31"),
             filed = as.Date("2024-02-15"))
))

result <- compute_ticker_indicators(synth_long, 150, "Technology")

test("compute_ticker: returns named numeric vector",
     is.numeric(result) && !is.null(names(result)))

test("compute_ticker: correct length (57)",
     length(result) == 57)

test("compute_ticker: pe_trailing = 150/3.8",
     abs(result[["pe_trailing"]] - 150/3.8) < 0.01)

test("compute_ticker: market_cap = 150 * 1000",
     result[["market_cap"]] == 150000)

test("compute_ticker: gross_margin = 0.6",
     abs(result[["gross_margin"]] - 0.6) < 0.001)

test("compute_ticker: revenue_growth_yoy computed",
     !is.na(result[["revenue_growth_yoy"]]))

test("compute_ticker: f_score in [0, 9]",
     result[["f_score"]] >= 0 && result[["f_score"]] <= 9)

# Financial sector test
fin_result <- compute_ticker_indicators(synth_long, 150, "Financial")

test("compute_ticker: financial gpa is NA",
     is.na(fin_result[["gpa"]]))

test("compute_ticker: financial inventory_turnover is NA",
     is.na(fin_result[["inventory_turnover"]]))

test("compute_ticker: financial cash_based_op is NA",
     is.na(fin_result[["cash_based_op"]]))

test("compute_ticker: financial capex_depreciation is NA",
     is.na(fin_result[["capex_depreciation"]]))

test("compute_ticker: financial inventory_sales_change is NA",
     is.na(fin_result[["inventory_sales_change"]]))

test("compute_ticker: financial roe is NOT NA (valid for financials)",
     !is.na(fin_result[["roe"]]))

# Failure handling
test("compute_ticker: NULL input returns all NA", {
  na_result <- compute_ticker_indicators(NULL, 100, "Technology")
  all(is.na(na_result))
})


# ============================================================================
# UNIT TESTS: compute_cross_section
# ============================================================================
message("\n=== compute_cross_section ===")

# Create 3 synthetic tickers
r1 <- compute_ticker_indicators(synth_long, 150, "Technology")
r2 <- compute_ticker_indicators(synth_long, 200, "Healthcare")
r3 <- compute_ticker_indicators(synth_long, 100, "Financial")

cs <- compute_cross_section(
  indicator_list = list(r1, r2, r3),
  tickers = c("TECH1", "HC1", "FIN1"),
  sectors = c("Technology", "Healthcare", "Financial")
)

test("cross_section: returns list with raw and zscored",
     is.list(cs) && all(c("raw", "zscored") %in% names(cs)))

test("cross_section: raw has 3 rows",
     nrow(cs$raw) == 3)

test("cross_section: zscored has 3 rows",
     nrow(cs$zscored) == 3)

test("cross_section: raw has ticker column",
     "ticker" %in% names(cs$raw))

test("cross_section: zscored has ticker column",
     "ticker" %in% names(cs$zscored))

test("cross_section: raw values preserved",
     cs$raw[ticker == "TECH1", pe_trailing] == r1[["pe_trailing"]])


# ============================================================================
# INTEGRATION TESTS: Real cached data
# ============================================================================
message("\n=== Integration Tests (Real Data) ===")

# Find AAPL parquet
aapl_files <- list.files("cache/fundamentals", pattern = "AAPL\\.parquet$",
                         full.names = TRUE)

if (length(aapl_files) > 0) {
  aapl_dt <- as.data.table(arrow::read_parquet(aapl_files[1]))
  message(sprintf("  Loaded AAPL: %d rows, concepts: %s",
                  nrow(aapl_dt),
                  paste(unique(aapl_dt$concept), collapse = ", ")))

  # Compute indicators (use approximate price)
  aapl_result <- compute_ticker_indicators(aapl_dt, 175, "Technology")

  test("AAPL: pe_trailing is positive",
       !is.na(aapl_result[["pe_trailing"]]) && aapl_result[["pe_trailing"]] > 0)

  test("AAPL: gross_margin between 0.3 and 0.6",
       !is.na(aapl_result[["gross_margin"]]) &&
         aapl_result[["gross_margin"]] > 0.3 &&
         aapl_result[["gross_margin"]] < 0.6)

  test("AAPL: roe is positive",
       !is.na(aapl_result[["roe"]]) && aapl_result[["roe"]] > 0)

  test("AAPL: market_cap is large (> 1 trillion proxy)",
       !is.na(aapl_result[["market_cap"]]) &&
         aapl_result[["market_cap"]] > 1e12)

  test("AAPL: revenue_growth_yoy is finite",
       !is.na(aapl_result[["revenue_growth_yoy"]]) &&
         is.finite(aapl_result[["revenue_growth_yoy"]]))

  test("AAPL: f_score in [0, 9]",
       aapl_result[["f_score"]] >= 0 && aapl_result[["f_score"]] <= 9)

  test("AAPL: gpa is not NA (non-financial)",
       !is.na(aapl_result[["gpa"]]))

  # Count non-NA indicators
  n_computed <- sum(!is.na(aapl_result))
  message(sprintf("  AAPL: %d/%d indicators computed (%.0f%%)",
                  n_computed, length(aapl_result),
                  100 * n_computed / length(aapl_result)))

} else {
  message("  SKIP  AAPL parquet not found in cache/fundamentals/")
}

# Find JPM parquet (financial sector)
jpm_files <- list.files("cache/fundamentals", pattern = "JPM\\.parquet$",
                        full.names = TRUE)

if (length(jpm_files) > 0) {
  jpm_dt <- as.data.table(arrow::read_parquet(jpm_files[1]))
  message(sprintf("  Loaded JPM: %d rows", nrow(jpm_dt)))

  jpm_result <- compute_ticker_indicators(jpm_dt, 200, "Financial")

  test("JPM: gpa is NA (financial)",
       is.na(jpm_result[["gpa"]]))

  test("JPM: inventory_turnover is NA (financial)",
       is.na(jpm_result[["inventory_turnover"]]))

  test("JPM: cash_based_op is NA (financial)",
       is.na(jpm_result[["cash_based_op"]]))

  test("JPM: roe is not NA",
       !is.na(jpm_result[["roe"]]))

  test("JPM: pe_trailing computed",
       !is.na(jpm_result[["pe_trailing"]]))

  test("JPM: f_score is NA or in [0, 9]", {
    v <- jpm_result[["f_score"]]
    is.na(v) || (v >= 0 && v <= 9)
  })

  n_computed <- sum(!is.na(jpm_result))
  message(sprintf("  JPM: %d/%d indicators computed (%.0f%%)",
                  n_computed, length(jpm_result),
                  100 * n_computed / length(jpm_result)))

} else {
  message("  SKIP  JPM parquet not found in cache/fundamentals/")
}


# ============================================================================
# INTEGRATION TESTS: Multi-ticker cross-section
# ============================================================================
message("\n=== Cross-Section Integration ===")

# Load up to 20 tickers for cross-section test
fund_files <- list.files("cache/fundamentals", full.names = TRUE, pattern = "\\.parquet$")

if (length(fund_files) >= 5) {
  # Use first 20 (or fewer)
  test_files <- head(fund_files, 20)
  n_test <- length(test_files)

  # Need sector data
  sector_file <- "cache/lookups/sector_industry.parquet"
  if (file.exists(sector_file)) {
    sector_dt <- as.data.table(arrow::read_parquet(sector_file))

    ind_list <- list()
    tickers_used <- character()
    sectors_used <- character()

    for (f in test_files) {
      dt <- tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
      if (is.null(dt) || nrow(dt) == 0) next

      tk <- dt$ticker[1]
      sec <- sector_dt[ticker == tk, sector]
      if (length(sec) == 0) sec <- "Unknown"
      sec <- sec[1]

      res <- compute_ticker_indicators(dt, 100, sec)
      ind_list[[length(ind_list) + 1]] <- res
      tickers_used <- c(tickers_used, tk)
      sectors_used <- c(sectors_used, sec)
    }

    if (length(ind_list) >= 3) {
      cs <- compute_cross_section(ind_list, tickers_used, sectors_used)

      test("cross_section_real: raw has correct rows",
           nrow(cs$raw) == length(ind_list))

      test("cross_section_real: zscored has correct rows",
           nrow(cs$zscored) == length(ind_list))

      test("cross_section_real: all indicator columns present", {
        all(.INDICATOR_NAMES %in% names(cs$raw))
      })

      test("cross_section_real: z-scores bounded [-3, 3]", {
        vals <- unlist(cs$zscored[, .SD, .SDcols = .INDICATOR_NAMES])
        all(is.na(vals) | (vals >= -3 & vals <= 3))
      })

      # At least some indicators should be non-NA for most tickers
      non_na_pct <- mean(!is.na(as.matrix(
        cs$raw[, .SD, .SDcols = .INDICATOR_NAMES])))
      message(sprintf("  Cross-section: %d tickers, %.0f%% non-NA values",
                      length(ind_list), non_na_pct * 100))

      test("cross_section_real: >30% of values are non-NA",
           non_na_pct > 0.3)

    } else {
      message("  SKIP  Not enough tickers loaded for cross-section test")
    }
  } else {
    message("  SKIP  sector_industry.parquet not found")
  }
} else {
  message("  SKIP  Not enough parquet files in cache/fundamentals/")
}


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== RESULTS: %d passed, %d failed ===\n", .n_pass, .n_fail))
if (.n_fail > 0) quit(status = 1)
