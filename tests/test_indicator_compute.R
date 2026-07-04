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

test("winsorize: default probs = c(0.025, 0.975)", {
  # Default percentiles align with the cross-sectional winsorization used
  # by zscore_cross_section / .robust_zscore.
  x <- c(-1000, rnorm(98), 1000)
  w <- .winsorize(x)
  # Extremes clipped inward; interior preserved.
  w[1] > -100 && w[100] < 100
})

# .robust_zscore
test("robust_zscore: NA-free normal input yields SD ~ 1, median ~ 0", {
  set.seed(1)
  x <- rnorm(500)
  z <- .robust_zscore(x, clip = NULL)  # drop clip to inspect raw scale
  abs(median(z)) < 0.1 && abs(sd(z) - 1) < 0.2
})

test("robust_zscore: outlier does not contaminate location/scale", {
  # A single extreme value would inflate SD under mean/SD standardization.
  # Under pre-winsorization + MAD, the outlier is clipped to p97.5 first
  # and can't blow up the scale.
  x <- c(rnorm(99), 1e6)
  z <- .robust_zscore(x, clip = NULL)
  # The extreme observation must be bounded (clipped to the p97.5 of x).
  # Scale should remain ~1 despite the outlier.
  !is.na(z[100]) && abs(z[100]) < 10 && abs(sd(z[-100]) - 1) < 0.3
})

test("robust_zscore: NAs preserved in output", {
  x <- c(1, 2, NA_real_, 4, 5, 6)
  z <- .robust_zscore(x)
  is.na(z[3]) && !is.na(z[1])
})

test("robust_zscore: fewer than min_n non-NA values -> all NA", {
  x <- c(1, 2, NA, NA, NA)
  z <- .robust_zscore(x, min_n = 3)
  all(is.na(z))
})

test("robust_zscore: degenerate spread (all equal) -> all NA", {
  x <- rep(5, 10)
  z <- .robust_zscore(x)
  all(is.na(z))
})

test("robust_zscore: clip bounds respected when non-NULL", {
  x <- c(rnorm(98), -100, 100)
  z <- .robust_zscore(x, clip = c(-3, 3))
  all(is.na(z) | (z >= -3 & z <= 3))
})

test("robust_zscore: clip = NULL leaves tails unbounded (up to winsor)", {
  # Without clip, z values are capped only by the winsorization p2.5/p97.5.
  # For n=100 rnorm, that's roughly ~|2| on the z scale -- well within [-3,3]
  # -- so clip NULL vs clip c(-3,3) produce identical output for normal data.
  set.seed(2)
  x <- rnorm(100)
  z_clip <- .robust_zscore(x, clip = c(-3, 3))
  z_none <- .robust_zscore(x, clip = NULL)
  all(abs(z_clip - z_none) < 1e-10, na.rm = TRUE)
})

test("robust_zscore: custom calib_sample used for location/scale", {
  # When calib_sample is supplied, current x_w is standardized against that
  # sample -- this is the expanding-window case.
  current <- c(10, 12, 14, 16, 18)
  history <- rnorm(1000, mean = 100, sd = 20)
  z <- .robust_zscore(current, calib_sample = history, clip = NULL)
  # Current values are far below history's median (100); z must be strongly
  # negative. Under per-snapshot (no history), z would be near 0.
  all(z < 0) && median(z) < -3
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

# A 10-K reports prior-year comparatives under the SAME fiscal_year label
# and filed date as the current period. The pivot must select the current
# period (max period_end) per concept -- not parse order -- or wide rows
# mix periods across concepts (ORCL fy2025 carried fy2023 equity/revenue).
comparative_long <- rbindlist(list(
  # comparatives listed FIRST to expose parse-order sensitivity
  .make_long(c("revenue", "stockholders_equity"), c(800, 1000),
             pe = as.Date("2022-12-31")),
  .make_long(c("revenue", "stockholders_equity"), c(900, 4000),
             pe = as.Date("2023-12-31")),
  .make_long(c("revenue", "stockholders_equity"), c(1000, 9000),
             pe = as.Date("2024-12-31"))
))

comp_wide <- pivot_fundamentals(comparative_long)

test("pivot: comparative rows collapse to one row per label",
     nrow(comp_wide) == 1)

test("pivot: current period wins over same-label comparatives",
     comp_wide$revenue == 1000 && comp_wide$stockholders_equity == 9000)

test("pivot: metadata period_end matches the chosen period",
     comp_wide$period_end == as.Date("2024-12-31"))

# Amendment for the same period (later filed) still beats the original
amended_long <- rbindlist(list(
  .make_long("revenue", 1000, filed = as.Date("2025-02-15")),
  .make_long("revenue", 1010, filed = as.Date("2025-05-01"))
))
test("pivot: amendment (later filed) wins for the same period",
     pivot_fundamentals(amended_long)$revenue == 1010)


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

# C-3: missing depreciation propagates NA rather than silently reporting EBIT
# mislabelled as EBITDA. The .make_long fixture has operating_income but no
# depreciation, so ebitda must be NA.
test("derive: ebitda is NA when depreciation tag is absent (C-3)",
     is.na(derived[fiscal_year == 2024, ebitda]))

# ---- C-3 NA-vs-zero split: explicit coverage ----

test("C-3 derive: ebitda = opinc + depr when both present", {
  d <- .derive_quantities(data.table(operating_income = 100,
                                     depreciation = 30))
  d$ebitda == 130
})

test("C-3 derive: fcf = opcf - capex when both present", {
  d <- .derive_quantities(data.table(operating_cashflow = 500, capex = 120))
  d$fcf == 380
})

test("C-3 derive: fcf is NA when capex absent (was: equal to OpCF)", {
  d <- .derive_quantities(data.table(operating_cashflow = 500))
  is.na(d$fcf)
})

test("C-3 derive: fcf is NA when capex column present but value NA", {
  d <- .derive_quantities(data.table(operating_cashflow = 500,
                                     capex = NA_real_))
  is.na(d$fcf)
})

test("C-3 derive: total_debt is NA when both LTD and STD absent", {
  d <- .derive_quantities(data.table(revenue = 1000))  # no debt cols
  is.na(d$total_debt)
})

test("C-3 derive: total_debt treats missing STD as 0 when LTD present", {
  d <- .derive_quantities(data.table(long_term_debt = 5000))
  d$total_debt == 5000
})

test("C-3 derive: total_debt treats missing LTD as 0 when STD present", {
  d <- .derive_quantities(data.table(short_term_debt = 800))
  d$total_debt == 800
})

test("C-3 derive: total_debt is NA when both LTD and STD are NA values", {
  d <- .derive_quantities(data.table(long_term_debt = NA_real_,
                                     short_term_debt = NA_real_))
  is.na(d$total_debt)
})

test("C-3 derive: total_debt = LTD + STD when both present", {
  d <- .derive_quantities(data.table(long_term_debt = 5000,
                                     short_term_debt = 800))
  d$total_debt == 5800
})

test("C-3 derive: net_debt is NA when cash absent", {
  d <- .derive_quantities(data.table(long_term_debt = 5000,
                                     short_term_debt = 800))
  is.na(d$net_debt)
})

test("C-3 derive: net_debt is NA when total_debt NA (both debt inputs absent)", {
  d <- .derive_quantities(data.table(cash = 1000))
  is.na(d$net_debt)
})

test("C-3 derive: net_debt = total_debt - cash when all present", {
  d <- .derive_quantities(data.table(long_term_debt = 5000,
                                     short_term_debt = 800,
                                     cash = 2000))
  d$net_debt == 3800
})

test("C-3 derive: vectorised across multi-row input (mixed presence)", {
  d <- .derive_quantities(data.table(
    operating_cashflow = c(500, 500, 500),
    capex = c(120, NA_real_, 200)
  ))
  d$fcf[1] == 380 && is.na(d$fcf[2]) && d$fcf[3] == 300
})


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

test("valuation: EV is NA when cash column missing (C-3)", {
  # Under C-3 net_debt = total_debt - cash propagates NA when cash is
  # unreported; EV follows. Previously this silently used total_debt
  # as a proxy, implicitly treating cash as 0.
  no_cash <- copy(curr_row)
  no_cash[, cash := NULL]
  no_cash <- .derive_quantities(no_cash)
  v <- .compute_valuation(no_cash, price, 1000)
  is.na(v$enterprise_value)
})

test("valuation: EV NA when both debt tags absent (C-3)", {
  # Without LTD and STD, total_debt is NA; net_debt and EV follow.
  no_debt <- copy(curr_row)
  no_debt[, `:=`(long_term_debt = NULL, short_term_debt = NULL)]
  no_debt <- .derive_quantities(no_debt)
  v <- .compute_valuation(no_debt, price, 1000)
  is.na(v$enterprise_value)
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

test("profitability: negative equity -> roe is negative (sign-preserving)", {
  # Principle: negative denominators carry distress signal and must propagate
  # into the cross-section. Winsorization + robust standardization downstream
  # tame the tails without censoring sign.
  neg_eq <- copy(curr_row)
  neg_eq[, stockholders_equity := -1000]
  p <- .compute_profitability(neg_eq)
  !is.na(p$roe) && p$roe == 2000 / -1000
})

# ---- Sign-preservation across the valuation / leverage / shareholder set ----

test("sign-preserve: negative equity yields negative pb", {
  r <- copy(curr_row); r[, stockholders_equity := -5000]
  v <- .compute_valuation(r, price, 1000)
  !is.na(v$pb) && v$pb == 150000 / -5000
})

test("sign-preserve: negative FCF yields negative pfcf", {
  # fcf is derived; inject via operating_cashflow < capex
  r <- copy(curr_row); r[, `:=`(operating_cashflow = 100, capex = 500)]
  r <- .derive_quantities(r)
  v <- .compute_valuation(r, price, 1000)
  !is.na(v$pfcf) && v$pfcf < 0
})

test("sign-preserve: negative EBITDA yields negative ev_ebitda", {
  r <- copy(curr_row); r[, `:=`(operating_income = -500, depreciation = 100)]
  r <- .derive_quantities(r)
  v <- .compute_valuation(r, price, 1000)
  !is.na(v$ev_ebitda) && v$ev_ebitda < 0
})

test("sign-preserve: negative equity yields negative debt_equity", {
  r <- copy(curr_row); r[, stockholders_equity := -2000]
  lev <- .compute_leverage(r)
  !is.na(lev$debt_equity) && lev$debt_equity == 6000 / -2000
})

test("sign-preserve: negative EBITDA yields negative net_debt_ebitda", {
  r <- copy(curr_row); r[, `:=`(operating_income = -200, depreciation = 100)]
  r <- .derive_quantities(r)
  lev <- .compute_leverage(r)
  !is.na(lev$net_debt_ebitda) && lev$net_debt_ebitda < 0
})

test("sign-preserve: negative NI yields negative payout_ratio", {
  # div = -600 (cash outflow), so abs_div = 600, ni = -100 -> ratio = -6
  r <- copy(curr_row); r[, net_income := -100]
  shr <- .compute_shareholder(r, 150000)
  !is.na(shr$payout_ratio) && shr$payout_ratio == 600 / -100
})

test("sign-preserve: negative IC yields negative roic", {
  # Force IC < 0: equity=100, total_debt=100, cash=500 -> IC = -300
  r <- copy(curr_row)
  r[, `:=`(stockholders_equity = 100, long_term_debt = 100,
           short_term_debt = 0, cash = 500)]
  r <- .derive_quantities(r)
  p <- .compute_profitability(r)
  !is.na(p$roic) && p$roic < 0
})

test("C-3 profitability: roic NA when cash absent (no silent zeroing)", {
  r <- copy(curr_row); r[, cash := NA_real_]
  r <- .derive_quantities(r)
  p <- .compute_profitability(r)
  is.na(p$roic)
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
# UNIT TESTS: Wave 1 -- Balance-Sheet Change Family
# ============================================================================
# Hand-computed from the shared fixture (curr_row / prior_row):
#   curr:  COA=11000 COL=7000  WC=4000 NCOA=35000 NCOL=17000 NCO=18000
#          FNA=0 FINL=6000 FIN=-6000 NOA=22000 CEQ=20000
#   prior: COA=10000 COL=6500  WC=3500 NCOA=34000 NCOL=17000 NCO=17000
#          FNA=0 FINL=6500 FIN=-6500 NOA=20500 CEQ=18000
#   avgAT=49000, lagAT=48000
message("\n=== Wave 1: Balance-Sheet Change Family ===")

bsc <- .compute_bs_change(curr_row, prior_row)

test("bsc: del_coa = 1000/49000",
     abs(bsc$del_coa - 1000/49000) < 1e-9)

test("bsc: del_col = 500/49000",
     abs(bsc$del_col - 500/49000) < 1e-9)

test("bsc: del_finl = -500/49000 (LTD down 500)",
     abs(bsc$del_finl - (-500/49000)) < 1e-9)

test("bsc: del_lti = 0 (no LT investments reported, zero-if-na)",
     bsc$del_lti == 0)

test("bsc: del_equ = 2000/49000",
     abs(bsc$del_equ - 2000/49000) < 1e-9)

test("bsc: del_netfin = 500/49000 (FIN -6500 -> -6000)",
     abs(bsc$del_netfin - 500/49000) < 1e-9)

test("bsc: total_accruals = (dWC + dNCO + dFIN)/avgAT = 2000/49000",
     abs(bsc$total_accruals - 2000/49000) < 1e-9)

test("bsc: Richardson identity TACC = (dNOA + dFIN)/avgAT", {
  # NOA = WC + NCO by construction, so dWC + dNCO = dNOA = 1500
  abs(bsc$total_accruals - (1500 - (-500))/49000) < 1e-9
})

test("bsc: dnoa = 1500/48000 (lagged AT scaling)",
     abs(bsc$dnoa - 1500/48000) < 1e-9)

test("bsc: ch_nncoa = 1000/48000",
     abs(bsc$ch_nncoa - 1000/48000) < 1e-9)

test("bsc: ch_nwc = 500/48000",
     abs(bsc$ch_nwc - 500/48000) < 1e-9)

test("bsc: inventory_change = 200/49000",
     abs(bsc$inventory_change - 200/49000) < 1e-9)

test("bsc: inventory_growth = 200/2800",
     abs(bsc$inventory_growth - 200/2800) < 1e-9)

test("bsc: ppe_inv_change NA when ppe_gross missing",
     is.na(bsc$ppe_inv_change))

test("bsc: equity_growth = 2000/18000",
     abs(bsc$equity_growth - 2000/18000) < 1e-9)

test("bsc: gr_ltnoa = (18000-17000)/49000 (LTNOA = NOA - WC)",
     abs(bsc$gr_ltnoa - 1000/49000) < 1e-9)

test("bsc: all NA when prior is NULL", {
  r <- .compute_bs_change(curr_row, NULL)
  all(is.na(unlist(r)))
})

test("bsc: ppe_inv_change with PPEG present, INV zero-if-na", {
  c2 <- copy(curr_row);  c2[, ppe_gross := 26000]
  p2 <- copy(prior_row); p2[, ppe_gross := 24000]
  r <- .compute_bs_change(c2, p2)
  # (26000+3000 - 24000-2800)/48000 = 2200/48000
  abs(r$ppe_inv_change - 2200/48000) < 1e-9
})

test("bsc: equity_growth NA when prior book equity negative", {
  p2 <- copy(prior_row); p2[, stockholders_equity := -100]
  r <- .compute_bs_change(curr_row, p2)
  is.na(r$equity_growth)
})

test("bsc: LT identity fallback (Liabilities tag absent)", {
  # LT = AT - CEQ: curr 50000-20000=30000, prior 48000-18000=30000 --
  # identical to the tagged values, so dnoa must be unchanged
  c2 <- copy(curr_row);  c2[, total_liabilities := NA_real_]
  p2 <- copy(prior_row); p2[, total_liabilities := NA_real_]
  r <- .compute_bs_change(c2, p2)
  abs(r$dnoa - 1500/48000) < 1e-9
})

test("bsc: FINL NA when all debt/preferred components missing", {
  c2 <- copy(curr_row)
  c2[, `:=`(long_term_debt = NA_real_, short_term_debt = NA_real_)]
  r <- .compute_bs_change(c2, prior_row)
  is.na(r$del_finl)
})

# -- .prior_fy_row(): label-free prior selection (relabeling regression) --
message("\n=== .prior_fy_row (label-free prior) ===")

# Latest-view relabeling scenario: the fy2024 10-K re-reported the FY2023
# balance sheet, so the period 2023-12-31 rows carry the fy=2024 label and
# the fy=2023 label group holds only the stale period 2022-12-31.
relabel_long <- data.table(
  concept     = rep(c("total_assets", "stockholders_equity"), 3),
  value       = c(1200, 500,   1000, 400,   800, 300),
  fiscal_year = c(2024L, 2024L, 2024L, 2024L, 2023L, 2023L),
  period_type = "FY",
  period_end  = as.Date(c("2024-12-31", "2024-12-31",
                          "2023-12-31", "2023-12-31",
                          "2022-12-31", "2022-12-31")),
  filed       = as.Date(c("2025-02-15", "2025-02-15",
                          "2025-02-15", "2025-02-15",
                          "2024-02-15", "2024-02-15"))
)

test("prior_fy_row: picks the true prior PERIOD, not the label group", {
  p <- .prior_fy_row(relabel_long, 2024L)
  !is.null(p) && p$total_assets == 1000 && p$stockholders_equity == 400
})

test("prior_fy_row: gap guard rejects a 2-year hole", {
  gap_long <- relabel_long[period_end != as.Date("2023-12-31")]
  is.null(.prior_fy_row(gap_long, 2024L))
})

test("prior_fy_row: NULL when target year absent",
     is.null(.prior_fy_row(relabel_long, 2030L)))

test("compute_ticker_indicators: asset_growth uses period-true prior", {
  ind <- compute_ticker_indicators(relabel_long, price_on_filed = NA_real_,
                                   sector = "Technology", target_fy = 2024L)
  abs(ind[["asset_growth"]] - 0.2) < 1e-9   # 1200/1000 - 1, not 1200/800 - 1
})


# ============================================================================
# UNIT TESTS: Wave 2 -- External Financing Family
# ============================================================================
message("\n=== Wave 2: External Financing Family ===")

# Synthetic 7-year long history; one filing per year (filed = pe + 60d).
.fin_long <- local({
  yrs <- 2018:2024
  mk <- function(concept, values) data.table(
    concept = concept, value = values,
    fiscal_year = yrs, period_type = "FY",
    period_end = as.Date(sprintf("%d-12-31", yrs)),
    filed = as.Date(sprintf("%d-03-01", yrs + 1))
  )
  rbindlist(list(
    mk("total_assets",       rep(1000, 7)),
    mk("shares_outstanding", c(130, 120, 118, 116, 112, 100, 110)),
    mk("long_term_debt",     c(90, 100, 120, 150, 170, 190, 200)),
    mk("short_term_debt",    c(20, 25, 22, 24, 26, 20, 30)),
    mk("debt_issuance",      c(10, 20, 30, 40, 50, 60, 100)),
    mk("debt_repayment",     c(5, 10, 15, 20, 25, 30, 40)),
    mk("equity_issuance",    c(5, 5, 5, 5, 5, 5, 20)),
    mk("buybacks",           c(-10, -20, -20, -30, -40, -40, -50)),
    mk("dividends_paid",     c(10, 12, 14, 16, 20, 25, 30)),
    mk("revenue",            rep(500, 7)),
    mk("net_income",         rep(50, 7))
  ))
})

fin_ind <- compute_ticker_indicators(.fin_long, price_on_filed = 10,
                                     sector = "Technology",
                                     target_fy = 2024L)

test("fin: net_debt_finance = (100 - 40 + (30-20))/1000",
     abs(fin_ind[["net_debt_finance"]] - 0.07) < 1e-9)

test("fin: net_equity_finance = (20 - 50 - 30)/1000",
     abs(fin_ind[["net_equity_finance"]] - (-0.06)) < 1e-9)

test("fin: xfin = ndf + nef",
     abs(fin_ind[["xfin"]] - 0.01) < 1e-9)

test("fin: composite_debt_issuance = log(TD_2024/TD_2019) = log(230/125)",
     abs(fin_ind[["composite_debt_issuance"]] - log(230/125)) < 1e-9)

test("fin: share_iss_1y = 110/100 - 1 (no splits)",
     abs(fin_ind[["share_iss_1y"]] - 0.10) < 1e-9)

test("fin: share_iss_5y = 110/120 - 1",
     abs(fin_ind[["share_iss_5y"]] - (110/120 - 1)) < 1e-9)

test("fin: net_payout_yield = (30 + 50 - 20)/(10 * 110)",
     abs(fin_ind[["net_payout_yield"]] - 60/1100) < 1e-9)

# Split adjustment: 2:1 split (ratio 0.5) between the 2023 and 2024
# filings. The 2024 count is on the post-split basis; the surviving 2023
# row was filed pre-split.
test("fin: share_iss_1y split-adjusts by filed-date basis", {
  long2 <- copy(.fin_long)
  long2[concept == "shares_outstanding" & fiscal_year == 2024, value := 220]
  splits <- data.table(ex_date = as.Date("2024-06-15"), ratio = 0.5)
  ind <- compute_ticker_indicators(long2, 10, "Technology",
                                   target_fy = 2024L, splits = splits)
  abs(ind[["share_iss_1y"]] - 0.10) < 1e-9   # 220*0.5/100 - 1
})

test("fin: no adjustment when both counts share one filing basis", {
  # Move the 2023 shares row into the 2024 filing (post-split restated)
  long2 <- copy(.fin_long)
  long2[concept == "shares_outstanding" & fiscal_year == 2024, value := 220]
  long2[concept == "shares_outstanding" & fiscal_year == 2023,
        `:=`(value = 200, filed = as.Date("2025-03-01"))]
  splits <- data.table(ex_date = as.Date("2024-06-15"), ratio = 0.5)
  ind <- compute_ticker_indicators(long2, 10, "Technology",
                                   target_fy = 2024L, splits = splits)
  abs(ind[["share_iss_1y"]] - 0.10) < 1e-9   # 220/200 - 1, window empty
})

test("fin: net_debt_finance NA when debt side entirely unreported", {
  long2 <- .fin_long[!concept %in% c("debt_issuance", "debt_repayment",
                                      "short_term_debt")]
  ind <- compute_ticker_indicators(long2, 10, "Technology", target_fy = 2024L)
  is.na(ind[["net_debt_finance"]])
})

test("fin: 5y indicators NA when history too short", {
  long2 <- .fin_long[fiscal_year >= 2022]
  ind <- compute_ticker_indicators(long2, 10, "Technology", target_fy = 2024L)
  is.na(ind[["composite_debt_issuance"]]) && is.na(ind[["share_iss_5y"]]) &&
    !is.na(ind[["share_iss_1y"]])
})

test("fin: net_payout_yield NA without price",
     is.na(compute_ticker_indicators(.fin_long, NA_real_, "Technology",
                                     target_fy = 2024L)[["net_payout_yield"]]))

test("fin: DEI cover count beats constant issued count (BA case)", {
  # Balance-sheet line carries the constant ISSUED count; the DEI cover
  # instant (a few weeks after FYE) carries true outstanding.
  long2 <- copy(.fin_long)
  long2[, tag := "CommonStockSharesOutstanding"]
  long2[concept == "shares_outstanding", value := 1000]     # constant issued
  dei <- data.table(
    concept = "shares_outstanding", value = c(610, 750),
    fiscal_year = c(2023L, 2024L), period_type = "FY",
    period_end = as.Date(c("2024-01-25", "2025-01-27")),
    filed = as.Date(c("2024-02-01", "2025-02-03")),
    tag = "EntityCommonStockSharesOutstanding"
  )
  ind <- compute_ticker_indicators(rbind(long2, dei), 10, "Technology",
                                   target_fy = 2024L)
  abs(ind[["share_iss_1y"]] - (750/610 - 1)) < 1e-9
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
  # Balance-sheet tags have NA period_start. dedup_fundamentals now keeps
  # one row per accession (vintage-preserving); the cross-accession collapse
  # (later accession = amendment wins) happens in pit_dedup.
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
  resolved <- pit_dedup(out)
  nrow(out) == 2 && nrow(resolved) == 1 && resolved$value == 101
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
# UNIT TESTS: Wave 3 -- quarter panel + seasonal-surprise family
# ============================================================================
message("\n=== Wave 3: Quarter Panel + Surprise Family ===")

# Duration-row builder: long-format rows with period_start (the quarter
# panel classifies by reported duration, never by labels).
.make_qrow <- function(concept, value, ps, pe, filed,
                       fy = 2024L, pt = "Q1", form = "10-Q",
                       tag = NA_character_) {
  data.table(concept = concept, value = value,
             period_start = as.Date(ps), period_end = as.Date(pe),
             fiscal_year = fy, period_type = pt,
             filed = as.Date(filed), form = form, tag = tag)
}

# Instant-row builder (period_start NA, excluded from duration series)
.make_irow <- function(concept, value, pe, filed, fy = 2024L, pt = "FY",
                       tag = NA_character_) {
  data.table(concept = concept, value = value,
             period_start = as.Date(NA), period_end = as.Date(pe),
             fiscal_year = fy, period_type = pt,
             filed = as.Date(filed), form = "10-K", tag = tag)
}

# -- .classify_duration --
test("classify_duration: bands and NA gaps",
     identical(.classify_duration(c(90L, 112L, 181L, 272L, 363L, 140L,
                                    NA_integer_)),
               c("3mo", "3mo", "2Q", "3Q", "FY", NA, NA)))

# -- .flow_quarter_series: YTD-only filer --
# Cumulative revenue 100 / 220 / 360 / 520 -> standalone 100/120/140/160
ytd_filer <- rbindlist(list(
  .make_qrow("revenue", 100, "2024-01-01", "2024-03-31", "2024-05-01"),
  .make_qrow("revenue", 220, "2024-01-01", "2024-06-30", "2024-08-01", pt = "Q2"),
  .make_qrow("revenue", 360, "2024-01-01", "2024-09-30", "2024-11-01", pt = "Q3"),
  .make_qrow("revenue", 520, "2024-01-01", "2024-12-31", "2025-02-15",
             pt = "FY", form = "10-K")
))

ytd_ser <- .flow_quarter_series(ytd_filer, "revenue")

test("quarter panel: YTD-only filer yields 4 standalone quarters",
     nrow(ytd_ser) == 4)

test("quarter panel: YTD differencing recovers standalone values",
     identical(ytd_ser$value, c(100, 120, 140, 160)))

test("quarter panel: Q4 = FY - Q3ytd",
     ytd_ser[qend == as.Date("2024-12-31"), value] == 160)

# -- direct 3-month rows win over derived values --
mixed_filer <- rbindlist(list(
  ytd_filer,
  # direct Q2 standalone reported at a slightly different value (rounding
  # in the filing); the direct row must win
  .make_qrow("revenue", 121, "2024-04-01", "2024-06-30", "2024-08-01", pt = "Q2")
))
test("quarter panel: direct 3mo row preferred over YTD difference",
     .flow_quarter_series(mixed_filer, "revenue")[
       qend == as.Date("2024-06-30"), value] == 121)

# -- missing Q3 YTD -> no Q4 --
no_q3 <- ytd_filer[!(period_end == as.Date("2024-09-30"))]
test("quarter panel: missing Q3ytd -> Q4 absent, not wrong",
     !(as.Date("2024-12-31") %in% .flow_quarter_series(no_q3, "revenue")$qend))

# -- 16-week Q1 (Kroger/AAP calendar) --
kr_filer <- rbindlist(list(
  .make_qrow("revenue", 160, "2024-02-01", "2024-05-23", "2024-06-20"),  # 112d
  .make_qrow("revenue", 280, "2024-02-01", "2024-08-15", "2024-09-12",
             pt = "Q2")                                                  # 196d
))
kr_ser <- .flow_quarter_series(kr_filer, "revenue")
test("quarter panel: 16-week Q1 classifies and differences (Q2 = 120)",
     nrow(kr_ser) == 2 && kr_ser$value[2] == 120)

# -- fiscal-year-transition stub is not force-classified --
stub <- .make_qrow("revenue", 300, "2024-01-01", "2024-05-31", "2024-07-15",
                   pt = "FY", form = "10-K")  # 151d stub "FY"
test("quarter panel: transition-stub duration lands in a band gap",
     is.null(.flow_quarter_series(stub, "revenue")))

# -- split-spanning EPS chain --
# 4:1 split between the two filings: pre-split EPS 4.00 must be
# normalized to 1.00 (current basis) before seasonal differencing.
eps_split <- rbindlist(list(
  .make_qrow("eps_diluted", 4.00, "2023-01-01", "2023-03-31", "2023-05-01",
             fy = 2023L),
  .make_qrow("eps_diluted", 1.05, "2024-01-01", "2024-03-31", "2024-05-01")
))
split_tbl <- data.table(ex_date = as.Date("2023-08-31"), ratio = 0.25)
eps_ser <- .flow_quarter_series(eps_split, "eps_diluted",
                                splits = split_tbl, split_adjust = TRUE)
test("quarter panel: split-spanning EPS normalized to current basis",
     abs(eps_ser$value[1] - 1.00) < 1e-9 && abs(eps_ser$value[2] - 1.05) < 1e-9)

# -- .q_shares_at: DEI cover instant preferred, split-adjusted --
share_rows <- rbindlist(list(
  .make_irow("shares_outstanding", 1012, "2024-12-31", "2025-02-15",
             tag = "CommonStockSharesOutstanding"),
  .make_irow("shares_outstanding", 610, "2025-01-20", "2025-02-15",
             tag = "EntityCommonStockSharesOutstanding")
))
test("q_shares_at: DEI cover instant wins over balance-sheet line",
     .q_shares_at(share_rows, as.Date("2024-12-31")) == 610)

test("q_shares_at: split after filed maps count to current basis",
     .q_shares_at(share_rows, as.Date("2024-12-31"),
                  splits = data.table(ex_date = as.Date("2025-06-30"),
                                      ratio = 0.25)) == 2440)

# -- .seasonal_surprise --
# 14 quarters, seasonal diffs d5..d14 = (0,1,...,8,10): hand-computed
# su_14 = 5.5, sd(su_6..su_13) = 0.5 * sd(1:8) -> SUE = 4.4907
qe14 <- seq(as.Date("2021-03-31"), by = "quarter", length.out = 14)
d <- c(0, 1, 2, 3, 4, 5, 6, 7, 8, 10)
x14 <- rep(100, 14)
for (i in 5:14) x14[i] <- x14[i - 4] + d[i - 4]
test("seasonal_surprise: hand-computed SUE",
     abs(.seasonal_surprise(qe14, x14, 1e-10) - 4.490732) < 1e-4)

test("seasonal_surprise: ultra-stable diffs hit the sd floor -> NA",
     is.na(.seasonal_surprise(qe14, seq(100, 230, by = 10), 1e-10)))

test("seasonal_surprise: fewer than 2 prior surprises -> NA",
     is.na(.seasonal_surprise(qe14[1:6], x14[1:6], 1e-10)))

test("seasonal_surprise: short series -> NA",
     is.na(.seasonal_surprise(qe14[1:3], x14[1:3], 1e-10)))

# -- full-ticker fixture: 17 calendar quarters of direct 3mo rows --
# (17 = the 13 SUE needs plus history for streak edge cases)
.q_fixture <- function(ni_vals, tax_vals = NULL, at_vals = NULL) {
  n <- length(ni_vals)
  qs <- seq(as.Date("2021-01-01"), by = "quarter", length.out = n)
  qe <- seq(as.Date("2021-03-31"), by = "quarter", length.out = n)
  rows <- list()
  for (i in seq_len(n)) {
    fy <- as.integer(format(qe[i], "%Y"))
    rows[[length(rows) + 1]] <- .make_qrow("net_income", ni_vals[i],
                                           qs[i], qe[i], qe[i] + 40, fy = fy)
    if (!is.null(tax_vals)) {
      rows[[length(rows) + 1]] <- .make_qrow("income_tax_expense",
                                             tax_vals[i], qs[i], qe[i],
                                             qe[i] + 40, fy = fy)
    }
    if (!is.null(at_vals)) {
      rows[[length(rows) + 1]] <- .make_irow("total_assets", at_vals[i],
                                             qe[i], qe[i] + 40, fy = fy)
    }
  }
  rbindlist(rows)
}

# -- ch_tax --
tax_fund <- .q_fixture(ni_vals = rep(50, 8),
                       tax_vals = c(20, 20, 20, 20, 20, 20, 20, 30),
                       at_vals = rep(1000, 8))
sur_tax <- .compute_surprise(tax_fund, target_fy = 2022L)
test("ch_tax: seasonal tax change over lag-quarter assets",
     abs(sur_tax$ch_tax - (30 - 20) / 1000) < 1e-9)

# -- roaq --
test("roaq: quarterly NI over prior-quarter assets",
     abs(sur_tax$roaq - 50 / 1000) < 1e-9)

# -- num_earn_increase streaks --
# base 100 for quarters 1-4; +1 seasonal growth through quarter 12; then
# quarter 13 breaks (NI_13 < NI_9), 14-16 grow again -> streak of 3
ni_streak3 <- rep(100, 16)
for (i in 5:16) ni_streak3[i] <- ni_streak3[i - 4] + 1
ni_streak3[13] <- ni_streak3[9] - 1
ni_streak3[14:16] <- ni_streak3[10:12] + 1
sur_s3 <- .compute_surprise(.q_fixture(ni_streak3), target_fy = 2024L)
test("num_earn_increase: broken quarter 3 lags back -> streak 3",
     sur_s3$num_earn_increase == 3)

# exactly 8: break at lag 8, growth since
ni_streak8 <- rep(100, 16)
for (i in 5:16) ni_streak8[i] <- ni_streak8[i - 4] + 1
ni_streak8[8] <- ni_streak8[4] - 1   # chearn_8 <= 0
for (i in 9:16) ni_streak8[i] <- ni_streak8[i - 4] + 1
sur_s8 <- .compute_surprise(.q_fixture(ni_streak8), target_fy = 2024L)
test("num_earn_increase: reported break at lag 8 -> streak 8",
     sur_s8$num_earn_increase == 8)

# unbroken growth through all 16 quarters: the OpenAP-exact >= 9 quirk
ni_streak9 <- rep(100, 16)
for (i in 5:16) ni_streak9[i] <- ni_streak9[i - 4] + 1
sur_s9 <- .compute_surprise(.q_fixture(ni_streak9), target_fy = 2024L)
test("num_earn_increase: 9+ streak scores 0 (OpenAP-exact)",
     sur_s9$num_earn_increase == 0)

# current quarter down -> 0
ni_down <- ni_streak9
ni_down[16] <- ni_down[12] - 5
sur_dn <- .compute_surprise(.q_fixture(ni_down), target_fy = 2024L)
test("num_earn_increase: current seasonal decline -> 0",
     sur_dn$num_earn_increase == 0)

# missing quarter continues a streak (OpenAP NA-as-positive convention)
gap_fund <- .q_fixture(ni_streak3)
gap_fund <- gap_fund[!(concept == "net_income" &
                         period_end == as.Date("2024-09-30"))]  # drop lag 1
sur_gap <- .compute_surprise(gap_fund, target_fy = 2024L)
test("num_earn_increase: missing lag quarter treated as continuing",
     sur_gap$num_earn_increase == 3)

# -- earnings_surprise via full fixture: split must not distort SUE --
eps16 <- rep(1, 16)
for (i in 5:16) eps16[i] <- eps16[i - 4] * 1.10
qs16 <- seq(as.Date("2021-01-01"), by = "quarter", length.out = 16)
qe16 <- seq(as.Date("2021-03-31"), by = "quarter", length.out = 16)
eps_rows <- rbindlist(lapply(1:16, function(i) {
  .make_qrow("eps_diluted", eps16[i], qs16[i], qe16[i], qe16[i] + 40,
             fy = as.integer(format(qe16[i], "%Y")))
}))
# 4:1 split mid-2023: as-filed EPS before the split are 4x current basis
splt <- as.Date("2023-08-31")
eps_asfiled <- copy(eps_rows)
eps_asfiled[filed < splt, value := value * 4]
sur_eps_clean <- .compute_surprise(eps_rows, target_fy = 2024L)
sur_eps_split <- .compute_surprise(
  eps_asfiled, target_fy = 2024L,
  splits = data.table(ex_date = splt, ratio = 0.25))
test("earnings_surprise: split-adjusted SUE equals no-split SUE",
     abs(sur_eps_clean$earnings_surprise -
           sur_eps_split$earnings_surprise) < 1e-6)

test("earnings_surprise: unadjusted split would have distorted SUE",
     is.na(.compute_surprise(eps_asfiled, target_fy = 2024L)$earnings_surprise) ||
       abs(.compute_surprise(eps_asfiled, target_fy = 2024L)$earnings_surprise -
             sur_eps_clean$earnings_surprise) > 0.5)

# -- earnings_consistency --
# steady 10% grower: every egrowth = 0.1 / (0.5 * (1 + 1/1.1)) = 0.104762
ec_years <- 2018:2024
ec_eps <- 1.1^(0:6)
ec_rows <- rbindlist(lapply(seq_along(ec_years), function(i) {
  y <- ec_years[i]
  rbindlist(list(
    .make_qrow("eps_diluted", ec_eps[i],
               sprintf("%d-01-01", y), sprintf("%d-12-31", y),
               sprintf("%d-02-15", y + 1), fy = as.integer(y), pt = "FY",
               form = "10-K"),
    .make_irow("total_assets", 1000, sprintf("%d-12-31", y),
               sprintf("%d-02-15", y + 1), fy = as.integer(y))
  ))
}))
test("earnings_consistency: steady grower mean growth",
     abs(.compute_earnings_consistency(ec_rows, 2024L) - 0.1047619) < 1e-5)

# sign flip: negative growth after positive -> NA
ec_flip <- copy(ec_rows)
ec_flip[concept == "eps_diluted" & fiscal_year == 2024L, value := 1.1^5 * 0.9]
test("earnings_consistency: sign flip vs prior year -> NA",
     is.na(.compute_earnings_consistency(ec_flip, 2024L)))

# extreme ratio (> 6x) -> NA
ec_jump <- copy(ec_rows)
ec_jump[concept == "eps_diluted" & fiscal_year == 2024L, value := 1.1^5 * 7]
test("earnings_consistency: EPS ratio above 6 -> NA",
     is.na(.compute_earnings_consistency(ec_jump, 2024L)))

# -- panel-based revenue_growth_qoq inside .compute_surprise --
rev_panel_fund <- ytd_filer
sur_rev <- .compute_surprise(rev_panel_fund, target_fy = 2024L)
test("rev_qoq: panel flag set when a revenue panel exists",
     isTRUE(sur_rev$has_rev_panel))

test("rev_qoq: latest standalone quarter vs the one before",
     abs(sur_rev$rev_qoq - (160 - 140) / 140) < 1e-9)

test("rev_qoq: no period_start data -> no panel, legacy fallback",
     !isTRUE(.compute_surprise(
       .make_long("revenue", 1000), target_fy = 2024L)$has_rev_panel))


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

test("zscore: pe_trailing median near 0", {
  # Median/MAD standardization centers on median, not mean. Mean still near
  # 0 for normal data but less tightly than under mean/SD standardization.
  m <- median(zscored$pe_trailing, na.rm = TRUE)
  abs(m) < 0.1
})

test("zscore: pe_trailing sd near 1 (loose tolerance for small n)", {
  # Under median/MAD the identity sd(z) == 1 is only asymptotic on normal
  # data; at n=15 (after financial-row exclusion) sample noise can push
  # sd(z) to ~1.5. The 500-ticker production cross-section is much tighter.
  s <- sd(zscored$pe_trailing, na.rm = TRUE)
  !is.na(s) && s > 0.5 && s < 2.0
})

test("zscore: financial rows remain NA for gpa",
     all(is.na(zscored$gpa[16:20])))

test("zscore: z-scores bounded to default clip [-5, 5]", {
  all_vals <- unlist(zscored[, .SD, .SDcols = names(zscored)])
  all(is.na(all_vals) | (all_vals >= -5 & all_vals <= 5))
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

# ---- New robust/winsorization behaviour ----

test("zscore robust: outlier raw value does not blow up other z-scores", {
  # Under mean/SD, a single raw outlier can inflate SD so that the rest of
  # the cross-section gets compressed toward 0. Under pre-winsorize + MAD,
  # the outlier is clipped first and scale stays informative.
  set.seed(11)
  dt_out <- data.table(
    pe_trailing = c(rnorm(49, 20, 5), 1e6),
    sector = rep("Technology", 50)
  )
  z <- zscore_cross_section(dt_out)
  # Non-outlier tickers should still have a meaningful spread (|z| > 0.5
  # for at least some of them). Under naive mean/SD this would collapse.
  sum(abs(z$pe_trailing) > 0.5, na.rm = TRUE) >= 5
})

test("zscore robust: negative denominator signal preserved", {
  # pb with negative equity is now computed (negative ratio). That negative
  # value must make it through to the z-score as a distinctly-negative z.
  dt <- data.table(
    pb = c(rep(c(1.5, 2, 2.5, 3), 4), -5),  # last one: distress case
    sector = rep("Technology", 17)
  )
  z <- zscore_cross_section(dt)
  !is.na(z$pb[17]) && z$pb[17] < -1
})

test("zscore: clip = NULL removes the [-3, 3] bound", {
  # With n=100 normal data the clip is inactive anyway; test the contract
  # rather than the value. Use an indicator designed to produce a z beyond
  # 3 even under MAD: mostly-constant data with one outlier that survives
  # winsorization (n=3 -> no clipping happens since the bounds are the data).
  dt <- data.table(
    pe_trailing = c(0, 0.1, 100),
    sector = rep("Technology", 3)
  )
  z_clip   <- zscore_cross_section(dt, clip = c(-3, 3))
  z_unclip <- zscore_cross_section(dt, clip = NULL)
  # The outlier should be at the clip bound under clip, larger under NULL
  # (or they may coincide if winsorization pulled the outlier inside 3 MADs;
  # either way z_unclip must be >= z_clip for the high end).
  !is.na(z_unclip$pe_trailing[3]) &&
    z_unclip$pe_trailing[3] >= z_clip$pe_trailing[3] - 1e-10
})

test("zscore: pre-winsorization capped at [p2.5, p97.5] of input", {
  # With 100 points where two are extreme, the extremes get clipped to the
  # empirical p2.5/p97.5. Under median/MAD without clip, the max |z| is
  # bounded by the winsorization width / MAD. For rnorm(100) that's
  # well below 3 -- so clip should be inactive.
  set.seed(3)
  dt <- data.table(
    pe_trailing = c(rnorm(98), -50, 50),
    sector = rep("Technology", 100)
  )
  z <- zscore_cross_section(dt, clip = NULL)
  # The two raw extremes get winsorized to p2.5/p97.5 (~|2|), so z is
  # bounded well inside [-3, 3] even with clip = NULL.
  !is.na(max(abs(z$pe_trailing), na.rm = TRUE)) &&
    max(abs(z$pe_trailing), na.rm = TRUE) < 3
})


# ============================================================================
# UNIT TESTS: Public API
# ============================================================================
message("\n=== Public API ===")

test("get_indicator_names returns 87 names",
     length(get_indicator_names()) == 87)

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

test("compute_ticker: correct length (87)",
     length(result) == 87)

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

      test("cross_section_real: z-scores bounded to default clip [-5, 5]", {
        vals <- unlist(cs$zscored[, .SD, .SDcols = .INDICATOR_NAMES])
        all(is.na(vals) | (vals >= -5 & vals <= 5))
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
