# ============================================================================
# indicator_compute.R  --  Fundamental Indicator Computation (Module 4)
# ============================================================================
# Pure computation layer: no I/O. Receives data.tables and price scalars,
# returns named vectors and data.tables.
#
# Computes 81 output fields from EDGAR XBRL data + market prices:
#   - 36 baseline indicators (valuation, profitability, growth, leverage,
#     efficiency, cash flow quality, shareholder return, size)
#   - 2 size extras (shares outstanding, public float)
#   - 6 Tier 1 research indicators (GP/A, Asset Growth, Sloan Accrual,
#     Pct Accruals, NOA, Piotroski F-Score with 9 binary components)
#   - 6 Tier 2 research indicators (Cash-Based OP, FCF Stability,
#     SGA Efficiency, CAPEX/DA, DSO Change, Inventory/Sales Change)
#   - 15 Wave 1 balance-sheet change indicators (Richardson 2005
#     decomposition, dNOA, Soliman changes, inventory/PPE investment,
#     equity growth, LTNOA growth) -- see docs/research/09
#   - 7 Wave 2 external financing indicators (Bradshaw 2006 net debt/
#     equity financing + XFIN, composite debt issuance, split-adjusted
#     share issuance 1y/5y, net payout yield) -- see docs/research/09
#
# Public API:
#   compute_ticker_indicators(fund_dt, price, sector, target_fy, target_period)
#   compute_cross_section(indicator_list, tickers, sectors)
#   get_indicator_names()
#
# Dependencies: data.table
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
})


# =============================================================================
# SECTION 1: CONSTANTS AND HELPERS
# =============================================================================

# Indicators that are meaningless for financial sector (banks lack COGS,
# inventory, standard operating income structure)
.FINANCIAL_NA_INDICATORS <- c(
  "gpa", "inventory_turnover", "cash_based_op",
  "capex_depreciation", "inventory_sales_change",
  # Wave 1 inventory/PPE-investment indicators (banks hold no inventory;
  # their PPE is immaterial to the investment anomaly)
  "inventory_change", "inventory_growth", "ppe_inv_change"
)

# XBRL tags that should not contribute to a concept when read from cache.
# InterestIncomeExpenseNet is net of interest income and can be negative
# for cash-rich firms; it is not a valid proxy for gross interest expense
# and would flip the sign of interest_coverage. Caches built before this
# alias was removed still contain these rows; drop them on read.
.DEPRECATED_CONCEPT_TAGS <- list(
  interest_expense = c("InterestIncomeExpenseNet")
)

# Canonical indicator names in output order (81 entries):
# 36 baseline + 2 size (shares_outstanding, public_float) + 5 tier1 +
# 10 piotroski (9 components + composite) + 6 tier2 + 15 wave1 + 7 wave2
.INDICATOR_NAMES <- c(
  # Valuation (8)
  "pe_trailing", "peg", "pb", "ps", "pfcf",
  "ev_ebitda", "ev_revenue", "earnings_yield",
  # Profitability (6)
  "gross_margin", "operating_margin", "net_margin",
  "roe", "roa", "roic",
  # Growth (5)
  "revenue_growth_yoy", "revenue_growth_qoq",
  "eps_growth_yoy", "opinc_growth_yoy", "ebitda_growth",
  # Leverage (5)
  "debt_equity", "net_debt_ebitda", "interest_coverage",
  "current_ratio", "quick_ratio",
  # Efficiency (3)
  "asset_turnover", "inventory_turnover", "receivables_turnover",
  # Cash Flow Quality (3)
  "fcf_ni", "opcf_ni", "capex_revenue",
  # Shareholder Return (3)
  "dividend_yield", "payout_ratio", "buyback_yield",
  # Size (5)
  "market_cap", "enterprise_value", "revenue_raw",
  "shares_outstanding", "public_float",
  # Tier 1 Research (6 + 10 Piotroski)
  "gpa", "asset_growth", "sloan_accrual", "pct_accruals",
  "net_operating_assets",
  "f_roa", "f_droa", "f_cfo", "f_accrual",
  "f_dlever", "f_dliquid", "f_eq_off",
  "f_dmargin", "f_dturn", "f_score",
  # Tier 2 Research (6)
  "cash_based_op", "fcf_stability", "sga_efficiency",
  "capex_depreciation", "dso_change", "inventory_sales_change",
  # Wave 1: balance-sheet change family (15) -- OpenAP expansion,
  # docs/research/09_openap_indicator_expansion.md
  "del_coa", "del_col", "del_finl", "del_lti", "del_equ",
  "del_netfin", "total_accruals",
  "dnoa", "ch_nncoa", "ch_nwc",
  "inventory_change", "inventory_growth", "ppe_inv_change",
  "equity_growth", "gr_ltnoa",
  # Wave 2: external financing family (7) -- OpenAP expansion
  "net_debt_finance", "net_equity_finance", "xfin",
  "composite_debt_issuance", "share_iss_1y", "share_iss_5y",
  "net_payout_yield"
)

# Family-to-indicator mapping (canonical groupings, 10 families)
.INDICATOR_FAMILIES <- list(
  valuation         = c("pe_trailing", "peg", "pb", "ps", "pfcf",
                         "ev_ebitda", "ev_revenue", "earnings_yield"),
  profitability     = c("gross_margin", "operating_margin", "net_margin",
                         "roe", "roa", "roic"),
  growth            = c("revenue_growth_yoy", "revenue_growth_qoq",
                         "eps_growth_yoy", "opinc_growth_yoy", "ebitda_growth"),
  leverage          = c("debt_equity", "net_debt_ebitda", "interest_coverage",
                         "current_ratio", "quick_ratio"),
  efficiency        = c("asset_turnover", "inventory_turnover",
                         "receivables_turnover"),
  cashflow_quality  = c("fcf_ni", "opcf_ni", "capex_revenue"),
  shareholder       = c("dividend_yield", "payout_ratio", "buyback_yield"),
  size              = c("market_cap", "enterprise_value", "revenue_raw",
                         "shares_outstanding", "public_float"),
  piotroski         = c("f_roa", "f_droa", "f_cfo", "f_accrual",
                         "f_dlever", "f_dliquid", "f_eq_off",
                         "f_dmargin", "f_dturn", "f_score"),
  research          = c("gpa", "asset_growth", "sloan_accrual", "pct_accruals",
                         "net_operating_assets", "cash_based_op", "fcf_stability",
                         "sga_efficiency", "capex_depreciation", "dso_change",
                         "inventory_sales_change"),
  balance_sheet_change = c("del_coa", "del_col", "del_finl", "del_lti",
                            "del_equ", "del_netfin", "total_accruals",
                            "dnoa", "ch_nncoa", "ch_nwc",
                            "inventory_change", "inventory_growth",
                            "ppe_inv_change", "equity_growth", "gr_ltnoa"),
  financing         = c("net_debt_finance", "net_equity_finance", "xfin",
                         "composite_debt_issuance", "share_iss_1y",
                         "share_iss_5y", "net_payout_yield")
)


#' Return indicator names belonging to one or more families.
#'
#' @param families Character vector of family slugs, e.g. "valuation" or
#'   c("valuation", "growth"). NULL returns all 81 indicator names.
#'   Matching is case-insensitive.
#' @return Character vector of indicator names, in canonical order.
#'
#' @examples
#'   indicator_names("valuation")
#'   indicator_names(c("valuation", "growth"))
#'   indicator_names()  # all 81
indicator_names <- function(families = NULL) {
  if (is.null(families)) return(.INDICATOR_NAMES)

  families <- tolower(families)
  unknown  <- setdiff(families, names(.INDICATOR_FAMILIES))
  if (length(unknown) > 0L) {
    stop("indicator_names: unknown family: ",
         paste(shQuote(unknown), collapse = ", "),
         ". Valid: ", paste(names(.INDICATOR_FAMILIES), collapse = ", "),
         call. = FALSE)
  }

  # Preserve canonical order across selected families
  selected <- unlist(.INDICATOR_FAMILIES[families], use.names = FALSE)
  .INDICATOR_NAMES[.INDICATOR_NAMES %in% selected]
}


# -- Assertion helper (same pattern as other modules) --
.assert_output <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}


# -- Safe division: NA when denominator is zero, NA, or near-zero --
.safe_divide <- function(num, denom, min_abs_denom = 1e-9) {
  if (is.na(num) || is.na(denom) || abs(denom) < min_abs_denom) {
    return(NA_real_)
  }
  num / denom
}


# -- Safe growth rate: (current - prior) / |prior|, NA-safe --
.safe_growth <- function(current, prior) {
  .safe_divide(current - prior, abs(prior))
}


# -- Winsorize to percentile bounds --
.winsorize <- function(x, probs = c(0.025, 0.975)) {
  if (all(is.na(x))) return(x)
  q <- quantile(x, probs, na.rm = TRUE)
  pmin(pmax(x, q[1]), q[2])
}


# -- Robust cross-sectional standardization --
#
# Pipeline (see docs/research/06_indicator_verification_report.md §12):
#   1. Winsorize x cross-sectionally at [probs[1], probs[2]].
#   2. Standardize using median / MAD of a calibration sample.
#      For per-snapshot use, calib_sample defaults to the winsorized x
#      itself. For expanding-window standardization, the caller supplies
#      the pooled historical + current winsorized values.
#
# Design rationale:
#   - Pre-winsorize before computing location / scale so fat-tailed outliers
#     cannot contaminate the standardization statistics.
#   - Median / MAD (scaled by 1.4826 to estimate sigma under a Gaussian
#     null) is robust to the remaining within-band skew. Under a normal
#     distribution it coincides with mean / SD, so values computed on
#     well-behaved indicators are unchanged in practice.
#   - clip default is c(-5, 5): the per-indicator / pooled tail diagnostic
#     (tools/diag_zscore_tails.R) showed p99.9 = 23 unclipped and 4.16:1
#     positive-heavy asymmetry. Clipping at +/-3 censors 1.8% of negative
#     (mostly distress) values along with 7.6% of positive (mostly size /
#     valuation / growth) values -- net-negative for factor discrimination.
#     c(-5, 5) preserves the negative tail essentially fully while still
#     bounding the scale so one indicator can't dominate a factor blend.
#     Long-term fix is log-transforming size / valuation indicators before
#     winsorization (verification report Section 14 item #11); once that
#     lands, this clip can likely be dropped (pass clip = NULL).
#
# Returns a numeric vector the same length as x. Original NAs are
# preserved; if x has fewer than min_n non-NA observations or the
# calibration spread is degenerate, the whole indicator returns all-NA.
.robust_zscore <- function(x,
                           calib_sample = NULL,
                           probs = c(0.025, 0.975),
                           min_n = 3,
                           clip = c(-5, 5)) {
  if (sum(!is.na(x)) < min_n) return(rep(NA_real_, length(x)))

  # 1. Per-snapshot winsorization using current-cross-section percentiles.
  q <- quantile(x, probs, na.rm = TRUE)
  x_w <- pmin(pmax(x, q[1]), q[2])

  # 2. Calibration sample. Defaults to winsorized x (per-snapshot); caller
  # may pass a pooled expanding-window sample.
  if (is.null(calib_sample)) {
    s <- x_w[!is.na(x_w)]
  } else {
    s <- calib_sample[!is.na(calib_sample)]
  }
  if (length(s) < min_n) return(rep(NA_real_, length(x)))

  med <- median(s)
  mad_v <- mad(s, center = med, constant = 1.4826)
  if (is.na(mad_v) || mad_v < 1e-12) {
    return(rep(NA_real_, length(x)))
  }

  z <- (x_w - med) / mad_v
  if (!is.null(clip)) {
    z <- pmin(pmax(z, clip[1]), clip[2])
  }
  z[is.na(x)] <- NA_real_
  z
}


# -- Safe column accessor: returns NA if column missing or value is NA --
.col <- function(row, col_name) {
  if (!(col_name %in% names(row))) return(NA_real_)
  val <- row[[col_name]]
  if (length(val) == 0) return(NA_real_)
  as.numeric(val[1])
}


# -- De-cumulate YTD quarterly operating_cashflow into standalone-quarter values.
# Input: q_rows, the wide-format quarterly slice with columns
#   operating_cashflow, cfo_period_days, fiscal_year, period_type, total_assets.
# Adds columns cfo_kind and cfo_std. cfo_std is the 3-month-standalone CFO
# value; NA for rows whose duration does not match the fp label (stubs /
# dedup mismatches) or whose prior-YTD sibling is missing from the history.
# See docs/research/07_cfo_cumulation_issue.md for the rationale.
.decumulate_cfo <- function(q_rows) {
  if (is.null(q_rows) || nrow(q_rows) == 0) return(q_rows)
  req <- c("operating_cashflow", "cfo_period_days",
           "period_type", "fiscal_year")
  if (!all(req %in% names(q_rows))) return(q_rows)

  dt <- copy(q_rows)

  # Q1-standalone upper bound is 120 (not 110) to accommodate 16-week Q1
  # used by Kroger/AAP fiscal calendars (Q1=16wk, Q2-Q4=12wk each).
  dt[, cfo_kind := fifelse(is.na(cfo_period_days), NA_character_,
          fifelse(cfo_period_days >= 60  & cfo_period_days <= 120, "3mo",
          fifelse(cfo_period_days >= 160 & cfo_period_days <= 200, "2Q",
          fifelse(cfo_period_days >= 250 & cfo_period_days <= 290, "3Q",
          fifelse(cfo_period_days >= 330 & cfo_period_days <= 380, "FY",
                  NA_character_)))))]

  expected_map <- c(Q1 = "3mo", Q2 = "2Q", Q3 = "3Q", Q4 = "FY")
  dt[, expected_kind := expected_map[period_type]]

  # Only rows whose duration matches the fp label are usable as YTD anchors
  dt[, cfo_usable := fifelse(
    !is.na(cfo_kind) & !is.na(expected_kind) & cfo_kind == expected_kind,
    operating_cashflow, NA_real_)]

  # Q1 YTD = Q1 standalone. Q2/Q3/Q4 need prior YTD in same fiscal_year.
  setorder(dt, fiscal_year, period_type)
  dt[, cfo_std := NA_real_]
  dt[period_type == "Q1", cfo_std := cfo_usable]

  prior_map <- c(Q2 = "Q1", Q3 = "Q2", Q4 = "Q3")
  for (pt in names(prior_map)) {
    prior_pt <- prior_map[[pt]]
    curr_idx <- which(dt$period_type == pt & !is.na(dt$cfo_usable))
    for (i in curr_idx) {
      fy <- dt$fiscal_year[i]
      prior_i <- which(dt$fiscal_year == fy &
                       dt$period_type == prior_pt &
                       !is.na(dt$cfo_usable))
      if (length(prior_i) == 1L) {
        dt$cfo_std[i] <- dt$cfo_usable[i] - dt$cfo_usable[prior_i]
      }
    }
  }

  dt
}


# -- Drop rows sourced from deprecated XBRL tags.
# Applied at compute-time to cached fundamentals that may still contain
# rows from tags that have been removed from the current alias map.
.filter_deprecated_tags <- function(fund_dt) {
  if (is.null(fund_dt) || nrow(fund_dt) == 0) return(fund_dt)
  if (!all(c("concept", "tag") %in% names(fund_dt))) return(fund_dt)
  dt <- fund_dt
  for (cn in names(.DEPRECATED_CONCEPT_TAGS)) {
    bad_tags <- .DEPRECATED_CONCEPT_TAGS[[cn]]
    dt <- dt[!(concept == cn & tag %in% bad_tags)]
  }
  dt
}


# =============================================================================
# SECTION 2: DATA PREPARATION -- PIVOT LONG TO WIDE
# =============================================================================

#' Pivot long-format fundamental data to wide format
#'
#' Converts per-ticker long-format data (one row per concept x period) to
#' wide format (one row per fiscal_year x period_type, one column per concept).
#'
#' @param fund_dt data.table. Long-format fundamental data from cached parquet.
#'   Must have columns: concept, value, fiscal_year, period_type, period_end, filed.
#' @return data.table in wide format sorted by fiscal_year and period_type.
#'   Returns NULL if input is empty.
pivot_fundamentals <- function(fund_dt) {

  if (is.null(fund_dt) || nrow(fund_dt) == 0) return(NULL)

  # Filter to classified periods only
  dt <- fund_dt[!is.na(period_type) & !is.na(fiscal_year)]
  if (nrow(dt) == 0) return(NULL)

  # For each (fiscal_year, period_type, concept), keep the row with the
  # LATEST PERIOD END, then latest filed date. Period end must rank first:
  # a 10-K reports prior-year comparatives under the SAME fiscal_year label
  # and the SAME filed date as the current period, so ordering by filed
  # alone left the survivor to parse order -- wide rows could mix periods
  # across concepts (e.g. ORCL fy2025 row carrying fy2023 equity/revenue).
  # The current period is always the group's max period_end; filed breaks
  # ties between an original and its amendment for the same period.
  setorder(dt, fiscal_year, period_type, concept, -period_end, -filed)
  dt <- dt[!duplicated(dt[, .(fiscal_year, period_type, concept)])]

  # Pivot: one row per (fiscal_year, period_type), one column per concept
  # Keep the latest period_end and filed date per group for metadata
  meta <- dt[, .(period_end = max(period_end, na.rm = TRUE),
                 filed = max(filed, na.rm = TRUE)),
             by = .(fiscal_year, period_type)]

  # Carry period_days of the operating_cashflow row separately. Needed by
  # .compute_tier2 to distinguish 3mo-standalone from YTD-cumulative rows,
  # because US 10-Q filings report CFO on a YTD basis (Q2 = 6mo, Q3 = 9mo).
  # Synthetic/test inputs may lack period_start; emit NA in that case.
  cfo_src <- dt[concept == "operating_cashflow"]
  if ("period_start" %in% names(cfo_src)) {
    cfo_meta <- cfo_src[, .(cfo_period_days =
                             as.integer(period_end[1] - period_start[1])),
                        by = .(fiscal_year, period_type)]
  } else {
    cfo_meta <- unique(cfo_src[, .(fiscal_year, period_type)])
    cfo_meta[, cfo_period_days := NA_integer_]
  }

  wide <- dcast(dt, fiscal_year + period_type ~ concept,
                value.var = "value", fun.aggregate = function(x) x[1])

  # Merge metadata back
  wide <- merge(wide, meta, by = c("fiscal_year", "period_type"), all.x = TRUE)
  wide <- merge(wide, cfo_meta, by = c("fiscal_year", "period_type"), all.x = TRUE)

  setorder(wide, fiscal_year, period_type)
  wide
}


# =============================================================================
# SECTION 3: DERIVED QUANTITIES
# =============================================================================

#' Add derived financial quantities to a wide-format row
#'
#' Computes intermediate values reused across multiple indicator groups.
#' Modifies data.table in place for efficiency.
#'
#' @param wide_dt data.table. Wide-format row(s) from pivot_fundamentals().
#' @return Same data.table with additional columns added.
.derive_quantities <- function(wide_dt) {
  if (is.null(wide_dt) || nrow(wide_dt) == 0) return(wide_dt)

  # Raw column getter: NA-vector if column missing, else the column itself
  # with NAs preserved (no fill). C-3: callers decide per-quantity whether
  # a missing input should propagate NA or be treated as a true zero.
  .raw <- function(col_name) {
    if (col_name %in% names(wide_dt)) {
      as.numeric(wide_dt[[col_name]])
    } else {
      rep(NA_real_, nrow(wide_dt))
    }
  }

  # Gross profit = revenue - cogs
  if (all(c("revenue", "cogs") %in% names(wide_dt))) {
    wide_dt[, gross_profit := revenue - cogs]
  }

  # EBITDA = operating_income + D&A. If D&A is absent, the result would be
  # EBIT mislabelled as EBITDA; propagate NA so downstream (ev_ebitda,
  # net_debt_ebitda, ebitda_growth) is NA rather than silently wrong.
  if ("operating_income" %in% names(wide_dt)) {
    wide_dt[, ebitda := operating_income + .raw("depreciation")]
  }

  # FCF = operating_cashflow - capex. If capex is absent, FCF would equal
  # OpCF (overstated); propagate NA.
  if ("operating_cashflow" %in% names(wide_dt)) {
    wide_dt[, fcf := operating_cashflow - .raw("capex")]
  }

  # Total debt = LTD + STD.
  # Both absent -> NA: indistinguishable from "firm has no debt" vs. "tags
  # not captured", so don't silently report zero debt.
  # One present, one absent -> treat absent as 0: matches EDGAR filing
  # practice where firms populate only the tag they actually use.
  ltd <- .raw("long_term_debt")
  std <- .raw("short_term_debt")
  both_na <- is.na(ltd) & is.na(std)
  ltd0 <- fifelse(is.na(ltd), 0.0, ltd)
  std0 <- fifelse(is.na(std), 0.0, std)
  wide_dt[, total_debt := fifelse(both_na, NA_real_, ltd0 + std0)]

  # Net debt = total_debt - cash. NA in either input propagates; cash is
  # universally reported by going concerns, so absence is data coverage,
  # not "no cash".
  wide_dt[, net_debt := total_debt - .raw("cash")]

  wide_dt
}


# =============================================================================
# SECTION 4: BASELINE INDICATORS
# =============================================================================

# -- 4.1 Valuation (8 indicators) --
.compute_valuation <- function(curr, price, shares) {

  market_cap <- if (!is.na(price) && !is.na(shares)) price * shares else NA_real_
  # EV = market_cap + net_debt. C-3: net_debt is NA when cash is unreported
  # or when both debt tags are absent; propagate rather than silently
  # substituting total_debt (which would treat cash as 0).
  ev <- if (!is.na(market_cap)) {
    nd <- .col(curr, "net_debt")
    if (!is.na(nd)) market_cap + nd else NA_real_
  } else NA_real_

  eps <- .col(curr, "eps_diluted")
  rev <- .col(curr, "revenue")
  equity <- .col(curr, "stockholders_equity")
  fcf_val <- .col(curr, "fcf")
  ebitda_val <- .col(curr, "ebitda")

  # Sign-guard-free: negative denominators (negative book value, negative FCF,
  # negative EBITDA) produce negative multiples that encode distress. The
  # cross-sectional winsorization + robust standardization downstream tame
  # the tails; sign-censoring here would destroy information.
  list(
    pe_trailing    = .safe_divide(price, eps, min_abs_denom = 0.01),
    pb             = .safe_divide(market_cap, equity),
    ps             = .safe_divide(market_cap, rev),
    pfcf           = .safe_divide(market_cap, fcf_val),
    ev_ebitda      = .safe_divide(ev, ebitda_val),
    ev_revenue     = .safe_divide(ev, rev),
    earnings_yield = .safe_divide(eps, price, min_abs_denom = 0.01),
    market_cap     = market_cap,
    enterprise_value = ev
  )
}


# -- 4.2 Profitability (6 indicators) --
.compute_profitability <- function(curr) {

  rev    <- .col(curr, "revenue")
  gp     <- .col(curr, "gross_profit")
  opinc  <- .col(curr, "operating_income")
  ni     <- .col(curr, "net_income")
  equity <- .col(curr, "stockholders_equity")
  assets <- .col(curr, "total_assets")
  td     <- .col(curr, "total_debt")
  cash_v <- .col(curr, "cash")

  # Invested capital = equity + debt - cash. NA in any component propagates
  # (C-3: missing cash is a data-coverage gap, not implicit zero).
  ic <- equity + td - cash_v

  # Sign-guard-free: negative denominators (negative equity, negative IC)
  # produce negative ratios that carry distress signal into the cross-section.
  # Winsorization handles the fat tails; sign-censoring destroys information.
  list(
    gross_margin     = .safe_divide(gp, rev),
    operating_margin = .safe_divide(opinc, rev),
    net_margin       = .safe_divide(ni, rev),
    roe              = .safe_divide(ni, equity),
    roa              = .safe_divide(ni, assets),
    roic             = .safe_divide(ni, ic)
  )
}


# -- 4.3 Growth (5 indicators) --
.compute_growth <- function(curr, prior, curr_q = NULL, prior_q = NULL) {

  out <- list(
    revenue_growth_yoy = .safe_growth(.col(curr, "revenue"), .col(prior, "revenue")),
    eps_growth_yoy     = .safe_growth(.col(curr, "eps_diluted"), .col(prior, "eps_diluted")),
    opinc_growth_yoy   = .safe_growth(.col(curr, "operating_income"),
                                      .col(prior, "operating_income")),
    ebitda_growth      = .safe_growth(.col(curr, "ebitda"), .col(prior, "ebitda"))
  )

  # QoQ revenue growth: requires quarterly rows
  out$revenue_growth_qoq <- if (!is.null(curr_q) && !is.null(prior_q)) {
    .safe_growth(.col(curr_q, "revenue"), .col(prior_q, "revenue"))
  } else NA_real_

  out
}


# -- 4.4 Leverage (5 indicators) --
.compute_leverage <- function(curr) {

  equity  <- .col(curr, "stockholders_equity")
  td      <- .col(curr, "total_debt")
  ebitda  <- .col(curr, "ebitda")
  nd      <- .col(curr, "net_debt")
  intexp  <- .col(curr, "interest_expense")
  opinc   <- .col(curr, "operating_income")
  ca      <- .col(curr, "current_assets")
  cl      <- .col(curr, "current_liabilities")
  inv     <- .col(curr, "inventory")

  # Sign-guard-free: negative equity / negative EBITDA flip the sign of the
  # leverage ratio and thereby signal distress. Winsorization handles tails.
  list(
    debt_equity       = .safe_divide(td, equity),
    net_debt_ebitda   = .safe_divide(nd, ebitda),
    interest_coverage = .safe_divide(opinc, intexp, min_abs_denom = 1),
    current_ratio     = .safe_divide(ca, cl),
    quick_ratio       = .safe_divide(
      if (!is.na(ca)) ca - (if (is.na(inv)) 0 else inv) else NA_real_, cl)
  )
}


# -- 4.5 Efficiency (3 indicators) --
.compute_efficiency <- function(curr) {

  rev    <- .col(curr, "revenue")
  assets <- .col(curr, "total_assets")
  cogs_v <- .col(curr, "cogs")
  inv    <- .col(curr, "inventory")
  ar     <- .col(curr, "accounts_receivable")

  list(
    asset_turnover       = .safe_divide(rev, assets),
    inventory_turnover   = .safe_divide(cogs_v, inv),
    receivables_turnover = .safe_divide(rev, ar)
  )
}


# -- 4.6 Cash Flow Quality (3 indicators) --
.compute_cashflow_quality <- function(curr) {

  fcf_v  <- .col(curr, "fcf")
  opcf   <- .col(curr, "operating_cashflow")
  ni     <- .col(curr, "net_income")
  capex_v <- .col(curr, "capex")
  rev    <- .col(curr, "revenue")

  list(
    fcf_ni       = .safe_divide(fcf_v, ni, min_abs_denom = 1),
    opcf_ni      = .safe_divide(opcf, ni, min_abs_denom = 1),
    capex_revenue = .safe_divide(capex_v, rev)
  )
}


# -- 4.7 Shareholder Return (3 indicators) --
.compute_shareholder <- function(curr, market_cap) {

  div  <- .col(curr, "dividends_paid")
  buy  <- .col(curr, "buybacks")
  ni   <- .col(curr, "net_income")

  # dividends_paid and buybacks are typically negative (cash outflows)
  abs_div <- if (!is.na(div)) abs(div) else NA_real_
  abs_buy <- if (!is.na(buy)) abs(buy) else NA_real_

  # payout_ratio: allow negative when NI < 0 (firm paying dividends despite
  # losses -- a distress signal). Winsorization caps the tails.
  list(
    dividend_yield = .safe_divide(abs_div, market_cap),
    payout_ratio   = .safe_divide(abs_div, ni),
    buyback_yield  = .safe_divide(abs_buy, market_cap)
  )
}


# =============================================================================
# SECTION 5: TIER 1 RESEARCH INDICATORS
# =============================================================================

.compute_tier1 <- function(curr, prior) {

  ni     <- .col(curr, "net_income")
  cfo    <- .col(curr, "operating_cashflow")
  assets <- .col(curr, "total_assets")
  p_assets <- .col(prior, "total_assets")
  gp     <- .col(curr, "gross_profit")
  cash_v <- .col(curr, "cash")
  liab   <- .col(curr, "total_liabilities")
  td     <- .col(curr, "total_debt")

  avg_assets <- if (!is.na(assets) && !is.na(p_assets)) {
    (assets + p_assets) / 2
  } else NA_real_

  # -- GP/A --
  gpa <- .safe_divide(gp, assets)

  # -- Asset Growth --
  asset_growth <- .safe_growth(assets, p_assets)

  # -- Sloan Accrual Ratio (CF version) --
  accrual_num <- if (!is.na(ni) && !is.na(cfo)) ni - cfo else NA_real_
  sloan_accrual <- .safe_divide(accrual_num, avg_assets)

  # -- Percent Accruals --
  pct_accruals <- .safe_divide(accrual_num, abs(ni), min_abs_denom = 1)

  # -- Net Operating Assets --
  op_assets <- if (!is.na(assets) && !is.na(cash_v)) assets - cash_v else NA_real_
  op_liab <- if (!is.na(liab) && !is.na(td)) liab - td else NA_real_
  noa_num <- if (!is.na(op_assets) && !is.na(op_liab)) op_assets - op_liab else NA_real_
  noa <- .safe_divide(noa_num, p_assets)

  # -- Piotroski F-Score (9 binary components) --
  pio <- .compute_piotroski(curr, prior)

  c(list(
    gpa                = gpa,
    asset_growth       = asset_growth,
    sloan_accrual      = sloan_accrual,
    pct_accruals       = pct_accruals,
    net_operating_assets = noa
  ), pio)
}


.compute_piotroski <- function(curr, prior) {

  # Current period values
  ni     <- .col(curr, "net_income")
  cfo    <- .col(curr, "operating_cashflow")
  assets <- .col(curr, "total_assets")
  ltd    <- .col(curr, "long_term_debt")
  ca     <- .col(curr, "current_assets")
  cl     <- .col(curr, "current_liabilities")
  shares <- .col(curr, "shares_outstanding")
  gp     <- .col(curr, "gross_profit")
  rev    <- .col(curr, "revenue")

  # Prior period values
  p_ni     <- .col(prior, "net_income")
  p_cfo    <- .col(prior, "operating_cashflow")
  p_assets <- .col(prior, "total_assets")
  p_ltd    <- .col(prior, "long_term_debt")
  p_ca     <- .col(prior, "current_assets")
  p_cl     <- .col(prior, "current_liabilities")
  p_shares <- .col(prior, "shares_outstanding")
  p_gp     <- .col(prior, "gross_profit")
  p_rev    <- .col(prior, "revenue")

  # Each component returns NA_integer_ when required inputs are missing,
  # 0L/1L otherwise. Treating missing inputs as 0 would silently bias
  # f_score downward for firms with limited prior-year data.
  .bin <- function(cond) {
    if (is.na(cond)) NA_integer_ else if (cond) 1L else 0L
  }

  # ROA = NI / Assets
  roa   <- .safe_divide(ni, assets)
  p_roa <- .safe_divide(p_ni, p_assets)

  # Profitability signals
  f_roa     <- .bin(roa > 0)
  f_droa    <- .bin(roa > p_roa)
  f_cfo     <- .bin(.safe_divide(cfo, assets) > 0)
  f_accrual <- .bin(cfo > ni)

  # Leverage / Liquidity signals
  lt_ratio   <- .safe_divide(ltd, assets)
  p_lt_ratio <- .safe_divide(p_ltd, p_assets)
  f_dlever   <- .bin(lt_ratio < p_lt_ratio)

  cr   <- .safe_divide(ca, cl)
  p_cr <- .safe_divide(p_ca, p_cl)
  f_dliquid <- .bin(cr > p_cr)

  f_eq_off <- .bin(shares <= p_shares)

  # Operating efficiency signals
  gm   <- .safe_divide(gp, rev)
  p_gm <- .safe_divide(p_gp, p_rev)
  f_dmargin <- .bin(gm > p_gm)

  at   <- .safe_divide(rev, assets)
  p_at <- .safe_divide(p_rev, p_assets)
  f_dturn <- .bin(at > p_at)

  # Composite: NA if any component is NA. Academic formulation requires
  # all nine signals to be defined; downstream consumers can impute or
  # filter as needed.
  components <- c(f_roa, f_droa, f_cfo, f_accrual, f_dlever,
                  f_dliquid, f_eq_off, f_dmargin, f_dturn)
  f_score <- if (anyNA(components)) NA_integer_ else sum(components)

  list(
    f_roa = f_roa, f_droa = f_droa, f_cfo = f_cfo, f_accrual = f_accrual,
    f_dlever = f_dlever, f_dliquid = f_dliquid, f_eq_off = f_eq_off,
    f_dmargin = f_dmargin, f_dturn = f_dturn, f_score = f_score
  )
}


# =============================================================================
# SECTION 6: TIER 2 RESEARCH INDICATORS
# =============================================================================

.compute_tier2 <- function(curr, prior, quarterly_hist = NULL) {

  # -- Cash-Based Operating Profitability --
  # (Rev - COGS - SGA + R&D - dAR - dINV - dPrepaid
  #  + dDeferredRev + dAP + dAccruedLiab) / Total Assets
  rev    <- .col(curr, "revenue")
  cogs_v <- .col(curr, "cogs")
  sga    <- .col(curr, "sga")
  rnd    <- .col(curr, "rnd")
  assets <- .col(curr, "total_assets")

  # Start with Rev - COGS - SGA + R&D
  cbop_base <- NA_real_
  if (!is.na(rev) && !is.na(cogs_v) && !is.na(sga)) {
    cbop_base <- rev - cogs_v - sga + (if (is.na(rnd)) 0 else rnd)

    # Working capital adjustments (partial computation: skip missing items)
    wc_items <- list(
      list(curr = "accounts_receivable", sign = -1),  # -dAR
      list(curr = "inventory",           sign = -1),  # -dINV
      list(curr = "prepaid_expenses",    sign = -1),  # -dPrepaid
      list(curr = "deferred_revenue",    sign =  1),  # +dDeferredRev
      list(curr = "accounts_payable",    sign =  1),  # +dAP
      list(curr = "accrued_liabilities", sign =  1)   # +dAccruedLiab
    )

    for (item in wc_items) {
      c_val <- .col(curr, item$curr)
      p_val <- .col(prior, item$curr)
      if (!is.na(c_val) && !is.na(p_val)) {
        cbop_base <- cbop_base + item$sign * (c_val - p_val)
      }
    }
  }
  cash_based_op <- .safe_divide(cbop_base, assets)

  # -- FCF Stability: SD(standalone-quarter CFO / Assets) over up to 16 quarters --
  # US 10-Q filings report operating_cashflow on a YTD cumulative basis:
  # Q1 ~= 3 months, Q2 ~= 6 months, Q3 ~= 9 months. We must de-cumulate
  # before computing volatility, else the series is dominated by the
  # intra-year build-up staircase (see docs/research/07_cfo_cumulation_issue.md).
  fcf_stability <- NA_real_
  required_q_cols <- c("operating_cashflow", "total_assets",
                       "cfo_period_days", "fiscal_year", "period_type")
  if (!is.null(quarterly_hist) && nrow(quarterly_hist) > 0 &&
      all(required_q_cols %in% names(quarterly_hist))) {
    q_std <- .decumulate_cfo(quarterly_hist)
    setorder(q_std, fiscal_year, period_type)
    valid <- !is.na(q_std$cfo_std) &
             !is.na(q_std$total_assets) & q_std$total_assets > 0
    q_tail <- tail(q_std[valid], 16)
    if (nrow(q_tail) >= 8) {
      fcf_stability <- sd(q_tail$cfo_std / q_tail$total_assets)
    }
  }

  # -- SGA Efficiency Signal: %delta(SGA) - %delta(Revenue) --
  sga_eff <- NA_real_
  sga_g <- .safe_growth(.col(curr, "sga"), .col(prior, "sga"))
  rev_g <- .safe_growth(.col(curr, "revenue"), .col(prior, "revenue"))
  if (!is.na(sga_g) && !is.na(rev_g)) {
    sga_eff <- sga_g - rev_g
  }

  # -- CAPEX / Depreciation --
  capex_v <- .col(curr, "capex")
  depr    <- .col(curr, "depreciation")
  capex_depr <- .safe_divide(capex_v, depr)
  # Cap at 5x to avoid extreme ratios
  if (!is.na(capex_depr) && capex_depr > 5) capex_depr <- 5

  # -- DSO Change: delta(AR/Revenue * 365) --
  dso_change <- NA_real_
  ar_c <- .col(curr, "accounts_receivable")
  ar_p <- .col(prior, "accounts_receivable")
  rev_c <- .col(curr, "revenue")
  rev_p <- .col(prior, "revenue")
  dso_c <- .safe_divide(ar_c, rev_c)
  dso_p <- .safe_divide(ar_p, rev_p)
  if (!is.na(dso_c) && !is.na(dso_p)) {
    dso_change <- (dso_c - dso_p) * 365
  }

  # -- Inventory/Sales Change: delta(Inventory/COGS) --
  inv_sales_change <- NA_real_
  inv_c  <- .col(curr, "inventory")
  inv_p  <- .col(prior, "inventory")
  cogs_c <- .col(curr, "cogs")
  cogs_p <- .col(prior, "cogs")
  is_c <- .safe_divide(inv_c, cogs_c)
  is_p <- .safe_divide(inv_p, cogs_p)
  if (!is.na(is_c) && !is.na(is_p)) {
    inv_sales_change <- is_c - is_p
  }

  list(
    cash_based_op        = cash_based_op,
    fcf_stability        = fcf_stability,
    sga_efficiency       = sga_eff,
    capex_depreciation   = capex_depr,
    dso_change           = dso_change,
    inventory_sales_change = inv_sales_change
  )
}


# =============================================================================
# SECTION 6a: PRIOR-YEAR ROW CONSTRUCTION (label-free)
# =============================================================================

# Build the prior-year wide row for a target fiscal year from long-format
# fundamentals, keyed by period_end instead of the fiscal_year label.
#
# Anchor: the current year's balance-sheet date is the max FY period_end of
# total_assets rows labeled target_fy (the target year's own rows keep
# their original label because no newer annual filing exists in the view
# when target_fy is the newest year; for older target years callers pass
# an as-of view dated at that year's filing, with the same property).
# The prior date is the max FY total_assets period_end strictly before the
# anchor; all FY rows at that date -- whatever filing re-reported them
# last, whatever label they now carry -- form the prior row.
#
# Returns a one-row data.table shaped like a pivot_fundamentals row (with
# derived quantities), or NULL when no prior year exists at annual spacing
# (gap outside [270, 500] days).
.prior_fy_row <- function(fund_dt, target_fy) {
  .lag_fy_row(fund_dt, target_fy, lag_years = 1L)
}

# Generalized k-year lag row. Selects the FY total_assets period_end
# closest to (anchor - k * 365.25 days); rejects matches more than 135
# days off annual spacing (53-week calendars and fiscal-year-end shifts
# stay well inside that band; a missing year does not). Closest-match
# also skips fiscal-year-transition stub periods that a plain
# max(period_end < anchor) would trip on.
.lag_fy_row <- function(fund_dt, target_fy, lag_years = 1L) {
  pe <- .lag_fy_pe(fund_dt, target_fy, lag_years)
  if (is.null(pe)) return(NULL)
  .fy_row_at_period(fund_dt, pe, label_fy = target_fy - lag_years)
}

# The balance-sheet date lag_years before the target year's own date.
# lag_years = 0 returns the target year's anchor date itself.
.lag_fy_pe <- function(fund_dt, target_fy, lag_years = 1L) {

  at_fy <- fund_dt[concept == "total_assets" & !is.na(period_end) &
                     !is.na(period_type) & period_type == "FY"]
  if (nrow(at_fy) == 0) return(NULL)

  anchor <- at_fy[fiscal_year == target_fy, suppressWarnings(max(period_end))]
  if (!length(anchor) || is.na(anchor) || is.infinite(as.numeric(anchor))) {
    return(NULL)
  }
  if (lag_years == 0L) return(anchor)

  target_date <- anchor - round(lag_years * 365.25)
  cands <- at_fy[period_end < anchor, unique(period_end)]
  if (!length(cands)) return(NULL)

  off <- abs(as.numeric(cands - target_date))
  if (min(off) > 135) return(NULL)
  cands[which.min(off)]
}

# Assemble a one-row wide slice (with derived quantities) from all FY rows
# at one balance-sheet date, label-free.
.fy_row_at_period <- function(fund_dt, pe, label_fy) {

  rows <- fund_dt[period_type == "FY" & period_end == pe & !is.na(value)]
  if (nrow(rows) == 0) return(NULL)

  # One value per concept (dedup guarantees uniqueness per (concept,
  # period_end, FY); duplicated labels for the same period cannot occur
  # after pit_dedup, but keep first defensively)
  rows <- rows[!duplicated(concept)]
  wide <- dcast(rows, period_end ~ concept, value.var = "value")
  filed_val <- rows[!is.na(filed), if (.N) max(filed) else as.Date(NA)]
  wide[, `:=`(fiscal_year = as.integer(label_fy), period_type = "FY",
              filed = filed_val)]
  .derive_quantities(wide)
}


# =============================================================================
# SECTION 6b: WAVE 1 -- BALANCE-SHEET CHANGE FAMILY
# =============================================================================
# 15 indicators from the OpenAP expansion (docs/research/09, Wave 1).
# All are year-over-year changes in balance-sheet aggregates, annual only.
#
# Notation: AT = total_assets, ACT = current_assets, LCT = current_liabilities,
# CHE = cash, LT = total_liabilities, LTD/STD = long/short-term debt,
# CEQ = stockholders_equity, INV = inventory, PPEG = ppe_gross,
# STI/LTI = st/lt_investments, PS = preferred_stock,
# avgAT = (AT_t + AT_{t-1})/2.
#
# Building blocks (Richardson, Sloan, Soliman, Tuna 2005 JAE):
#   COA  = ACT - CHE                 current operating assets
#   COL  = LCT - STD                 current operating liabilities
#   WC   = COA - COL                 net working capital
#   NCOA = AT - ACT - LTI            noncurrent operating assets
#   NCOL = LT - LCT - LTD            noncurrent operating liabilities
#   NCO  = NCOA - NCOL               net noncurrent operating assets
#   FNA  = STI + LTI                 financial assets
#   FINL = LTD + STD + PS            financial liabilities
#   FIN  = FNA - FINL                net financial assets
#
# Missing-input policy (see docs/INDICATORS.md, Phase 0 concepts): the
# absence-means-zero concepts (STI, LTI, PS, STD) enter as 0 when missing;
# FINL additionally requires at least one debt/preferred component reported
# (all three missing -> NA, same convention as derived total_debt). Core
# balance-sheet lines (AT, ACT, LCT, LT, CHE, CEQ) propagate NA.

# Sum of the non-NA components; NA when ALL components are NA.
.sum_available <- function(...) {
  v <- c(...)
  if (all(is.na(v))) NA_real_ else sum(v, na.rm = TRUE)
}

# Treat NA as a true zero (documented absence-means-zero concepts only)
.zero_if_na <- function(x) if (is.na(x)) 0.0 else x

# Richardson building blocks for one wide row. Returns a list of scalars
# (NA where core inputs are missing).
.bs_blocks <- function(row) {
  at   <- .col(row, "total_assets")
  act  <- .col(row, "current_assets")
  lct  <- .col(row, "current_liabilities")
  che  <- .col(row, "cash")
  lt   <- .col(row, "total_liabilities")
  ltd  <- .col(row, "long_term_debt")
  std  <- .col(row, "short_term_debt")
  ceq  <- .col(row, "stockholders_equity")
  sti  <- .col(row, "st_investments")
  lti  <- .col(row, "lt_investments")
  ps   <- .col(row, "preferred_stock")
  td   <- .col(row, "total_debt")
  mi   <- .col(row, "minority_interest")

  # The Liabilities total tag is absent for ~25% of filers (they tag only
  # LiabilitiesAndStockholdersEquity). Derive it from the balance-sheet
  # identity: LT = AT - CEQ - noncontrolling interest. This is exactly how
  # Hirshleifer et al. (2004) build operating liabilities without LT.
  if (is.na(lt) && !is.na(at) && !is.na(ceq)) {
    lt <- at - ceq - .zero_if_na(mi)
  }

  coa  <- if (!is.na(act) && !is.na(che)) act - che else NA_real_
  col_ <- if (!is.na(lct)) lct - .zero_if_na(std) else NA_real_
  wc   <- if (!is.na(coa) && !is.na(col_)) coa - col_ else NA_real_
  ncoa <- if (!is.na(at) && !is.na(act)) at - act - .zero_if_na(lti) else NA_real_
  ncol_ <- if (!is.na(lt) && !is.na(lct)) lt - lct - .zero_if_na(ltd) else NA_real_
  nco  <- if (!is.na(ncoa) && !is.na(ncol_)) ncoa - ncol_ else NA_real_
  fna  <- .zero_if_na(sti) + .zero_if_na(lti)
  finl <- .sum_available(ltd, std, ps)
  fin  <- if (!is.na(finl)) fna - finl else NA_real_

  # NOA exactly as Tier 1 net_operating_assets numerator:
  # operating assets (AT - CHE) minus operating liabilities (LT - total debt)
  noa <- if (!is.na(at) && !is.na(che) && !is.na(lt) && !is.na(td)) {
    (at - che) - (lt - td)
  } else NA_real_

  list(at = at, ceq = ceq, coa = coa, col = col_, wc = wc,
       ncoa = ncoa, ncol = ncol_, nco = nco,
       fna = fna, finl = finl, fin = fin, noa = noa)
}

.compute_bs_change <- function(curr, prior) {

  out <- list(
    del_coa = NA_real_, del_col = NA_real_, del_finl = NA_real_,
    del_lti = NA_real_, del_equ = NA_real_, del_netfin = NA_real_,
    total_accruals = NA_real_,
    dnoa = NA_real_, ch_nncoa = NA_real_, ch_nwc = NA_real_,
    inventory_change = NA_real_, inventory_growth = NA_real_,
    ppe_inv_change = NA_real_, equity_growth = NA_real_,
    gr_ltnoa = NA_real_
  )
  if (is.null(prior)) return(out)

  b_t <- .bs_blocks(curr)
  b_p <- .bs_blocks(prior)

  avg_at <- if (!is.na(b_t$at) && !is.na(b_p$at)) {
    (b_t$at + b_p$at) / 2
  } else NA_real_
  lag_at <- b_p$at

  d <- function(x_t, x_p) {
    if (is.na(x_t) || is.na(x_p)) NA_real_ else x_t - x_p
  }

  # -- Richardson, Sloan, Soliman, Tuna (2005 JAE): balance-sheet
  #    decomposition changes, scaled by average total assets --
  out$del_coa    <- .safe_divide(d(b_t$coa,  b_p$coa),  avg_at)
  out$del_col    <- .safe_divide(d(b_t$col,  b_p$col),  avg_at)
  out$del_finl   <- .safe_divide(d(b_t$finl, b_p$finl), avg_at)
  out$del_equ    <- .safe_divide(d(b_t$ceq,  b_p$ceq),  avg_at)
  out$del_netfin <- .safe_divide(d(b_t$fin,  b_p$fin),  avg_at)

  # DelLTI = change in long-term investments (IVAO). Zero-if-na components:
  # a firm holding no LT investments has a true 0 change.
  lti_t <- .zero_if_na(.col(curr,  "lt_investments"))
  lti_p <- .zero_if_na(.col(prior, "lt_investments"))
  out$del_lti <- .safe_divide(lti_t - lti_p, avg_at)

  # TotalAccruals = dWC + dNCO + dFIN over avgAT (Richardson eq. 5)
  d_wc  <- d(b_t$wc,  b_p$wc)
  d_nco <- d(b_t$nco, b_p$nco)
  d_fin <- d(b_t$fin, b_p$fin)
  ta_num <- if (is.na(d_wc) || is.na(d_nco) || is.na(d_fin)) {
    NA_real_
  } else d_wc + d_nco + d_fin
  out$total_accruals <- .safe_divide(ta_num, avg_at)

  # -- Hirshleifer, Hou, Teoh, Zhang (2004 JAE): change in net operating
  #    assets over lagged total assets --
  out$dnoa <- .safe_divide(d(b_t$noa, b_p$noa), lag_at)

  # -- Soliman (2008 TAR): changes in net noncurrent operating assets and
  #    net working capital, over lagged total assets --
  out$ch_nncoa <- .safe_divide(d_nco, lag_at)
  out$ch_nwc   <- .safe_divide(d_wc,  lag_at)

  # -- Inventory investment --
  inv_t <- .col(curr,  "inventory")
  inv_p <- .col(prior, "inventory")
  # Thomas & Zhang (2002 RAS): dINV over average total assets
  out$inventory_change <- .safe_divide(d(inv_t, inv_p), avg_at)
  # Belo & Lin (2012 RFS): inventory growth rate
  out$inventory_growth <- .safe_growth(inv_t, inv_p)

  # -- Lyandres, Sun, Zhang (2008 RFS): (dPPEgross + dINV) over lagged
  #    total assets. INV zero-if-na so no-inventory firms still measure
  #    the PPE component; PPEG is required.
  ppeg_t <- .col(curr,  "ppe_gross")
  ppeg_p <- .col(prior, "ppe_gross")
  ppe_inv_num <- if (is.na(ppeg_t) || is.na(ppeg_p)) NA_real_ else {
    (ppeg_t + .zero_if_na(inv_t)) - (ppeg_p + .zero_if_na(inv_p))
  }
  out$ppe_inv_change <- .safe_divide(ppe_inv_num, lag_at)

  # -- Lockwood & Prombutr (2010 JFR): growth in book equity. Requires
  #    positive book equity in both years (a sign flip through zero is a
  #    distress event, not growth).
  if (!is.na(b_t$ceq) && !is.na(b_p$ceq) && b_t$ceq > 0 && b_p$ceq > 0) {
    out$equity_growth <- .safe_growth(b_t$ceq, b_p$ceq)
  }

  # -- Fairfield, Whisenant, Yohn (2003 TAR): growth in long-term net
  #    operating assets over avgAT, with LTNOA = NOA - WC (their
  #    decomposition of NOA into working capital + long-term component).
  ltnoa_t <- if (!is.na(b_t$noa) && !is.na(b_t$wc)) b_t$noa - b_t$wc else NA_real_
  ltnoa_p <- if (!is.na(b_p$noa) && !is.na(b_p$wc)) b_p$noa - b_p$wc else NA_real_
  out$gr_ltnoa <- .safe_divide(d(ltnoa_t, ltnoa_p), avg_at)

  out
}


# =============================================================================
# SECTION 6c: WAVE 2 -- EXTERNAL FINANCING FAMILY
# =============================================================================
# 7 indicators from the OpenAP expansion (docs/research/09, Wave 2).
# Annual. Sign conventions: Proceeds*/Repayments* cash-flow tags are
# positive by construction; buybacks/dividends enter as |absolute| so the
# result is robust to either reporting sign (same convention as the
# shareholder-return group).
#
# Split adjustment: a share count's split basis is the split state at its
# FILED date -- comparatives re-reported by later filings arrive
# retroactively restated (GAAP), so period_end says nothing about basis.
# share_iss_* maps the current count onto the older row's basis by the
# product of split ratios (quantmod convention: an n:1 split has ratio
# 1/n) with ex_date between the two rows' filed dates. When both values
# survive from the same filing the window is empty and no adjustment is
# applied -- which is correct, because that filing reported both on one
# basis. splits = NULL leaves counts unadjusted -- callers supply
# cache/splits data (see load_ticker_splits).

# Product of split ratios with ex_date in (from, to]; 1 when no splits
# data or no splits in the window.
.split_ratio_between <- function(splits, from, to) {
  if (is.null(splits) || !NROW(splits) || is.na(from) || is.na(to)) return(1)
  ex <- as.Date(splits$ex_date); rt <- as.numeric(splits$ratio)
  sel <- !is.na(ex) & !is.na(rt) & rt > 0 & ex > as.Date(from) & ex <= as.Date(to)
  if (!any(sel)) 1 else prod(rt[sel])
}

# FY share count for the fiscal year ending at pe, with the row's own
# filed date (the basis date). Candidates are FY-labeled share rows dated
# in [pe, pe + 120d]: the balance-sheet date itself plus the 10-K
# cover-page instant a few weeks later. The DEI cover tag
# (EntityCommonStockSharesOutstanding) is preferred when present: it is
# the true point-in-time outstanding count, whereas some filers populate
# the balance-sheet CommonStockSharesOutstanding line with the constant
# ISSUED count and vary treasury stock instead (BA: 1,012.26M for years
# on end while outstanding moved 610M -> 750M through the 2024 offering).
# Returns list(v, filed) or NULL.
.fy_shares_at <- function(fund_dt, pe) {
  if (is.null(pe)) return(NULL)
  win <- fund_dt[concept == "shares_outstanding" & period_type == "FY" &
                   !is.na(value) & !is.na(period_end) &
                   period_end >= pe & period_end <= pe + 120]
  if (nrow(win) == 0) return(NULL)
  if ("tag" %in% names(win)) {
    dei <- win[tag == "EntityCommonStockSharesOutstanding"]
    if (nrow(dei)) win <- dei
  }
  r <- win[order(period_end)][1]
  list(v = r$value[1], filed = r$filed[1])
}

.compute_financing <- function(fund_dt, target_fy, curr, prior, lag5,
                               market_cap, splits = NULL) {

  out <- list(
    net_debt_finance = NA_real_, net_equity_finance = NA_real_,
    xfin = NA_real_, composite_debt_issuance = NA_real_,
    share_iss_1y = NA_real_, share_iss_5y = NA_real_,
    net_payout_yield = NA_real_
  )

  iss  <- .col(curr, "equity_issuance")
  bb   <- .col(curr, "buybacks")
  dv   <- .col(curr, "dividends_paid")
  abs_bb <- if (is.na(bb)) NA_real_ else abs(bb)
  abs_dv <- if (is.na(dv)) NA_real_ else abs(dv)

  # -- Boudoukh et al. (2007 JF): net payout yield. Current-year data
  #    only; price-sensitive via market cap. Components zero-if-na
  #    (absence = none paid/issued) but at least one must be reported.
  if (!is.na(market_cap) && !all(is.na(c(abs_dv, abs_bb, iss)))) {
    payout <- .zero_if_na(abs_dv) + .zero_if_na(abs_bb) - .zero_if_na(iss)
    out$net_payout_yield <- .safe_divide(payout, market_cap)
  }

  if (!is.null(prior)) {

    at_t <- .col(curr, "total_assets")
    at_p <- .col(prior, "total_assets")
    avg_at <- if (!is.na(at_t) && !is.na(at_p)) (at_t + at_p) / 2 else NA_real_

    # -- Bradshaw, Richardson, Sloan (2006 JAE): net debt financing =
    #    (LT debt issuance - LT debt repayment + change in short-term
    #    debt) / avgAT. Components zero-if-na; NA when nothing at all is
    #    reported on the debt-financing side.
    dltis <- .col(curr, "debt_issuance")
    dltr  <- .col(curr, "debt_repayment")
    std_t <- .col(curr, "short_term_debt")
    std_p <- .col(prior, "short_term_debt")
    if (!all(is.na(c(dltis, dltr, std_t, std_p)))) {
      d_std <- .zero_if_na(std_t) - .zero_if_na(std_p)
      ndf_num <- .zero_if_na(dltis) - .zero_if_na(dltr) + d_std
      out$net_debt_finance <- .safe_divide(ndf_num, avg_at)
    }

    # -- Bradshaw et al. (2006): net equity financing = (stock issuance
    #    - repurchases - dividends) / avgAT.
    if (!all(is.na(c(iss, abs_bb, abs_dv)))) {
      nef_num <- .zero_if_na(iss) - .zero_if_na(abs_bb) - .zero_if_na(abs_dv)
      out$net_equity_finance <- .safe_divide(nef_num, avg_at)
    }

    # -- XFIN = net external financing (Bradshaw et al. 2006)
    if (!is.na(out$net_debt_finance) && !is.na(out$net_equity_finance)) {
      out$xfin <- out$net_debt_finance + out$net_equity_finance
    }

    # -- Pontiff & Woodgate (2008 JF): 1-year split-adjusted share
    #    issuance. Counts and their basis (filed) dates come from the
    #    long data at the anchored balance-sheet dates.
    sh_t <- .fy_shares_at(fund_dt, .lag_fy_pe(fund_dt, target_fy, 0L))
    sh_p <- .fy_shares_at(fund_dt, .lag_fy_pe(fund_dt, target_fy, 1L))
    if (!is.null(sh_t) && !is.null(sh_p)) {
      adj <- .split_ratio_between(splits, sh_p$filed, sh_t$filed)
      out$share_iss_1y <- .safe_growth(sh_t$v * adj, sh_p$v)
    }
  }

  if (!is.null(lag5)) {

    # -- Lyandres, Sun, Zhang (2008 RFS): composite debt issuance =
    #    log(total debt_t / total debt_{t-5}), both must be positive.
    td_t <- .col(curr, "total_debt")
    td_5 <- .col(lag5, "total_debt")
    if (!is.na(td_t) && !is.na(td_5) && td_t > 0 && td_5 > 0) {
      out$composite_debt_issuance <- log(td_t / td_5)
    }

    # -- Daniel & Titman (2006 JF): 5-year split-adjusted share issuance
    sh_t <- .fy_shares_at(fund_dt, .lag_fy_pe(fund_dt, target_fy, 0L))
    sh_5 <- .fy_shares_at(fund_dt, .lag_fy_pe(fund_dt, target_fy, 5L))
    if (!is.null(sh_t) && !is.null(sh_5)) {
      adj <- .split_ratio_between(splits, sh_5$filed, sh_t$filed)
      out$share_iss_5y <- .safe_growth(sh_t$v * adj, sh_5$v)
    }
  }

  out
}


# =============================================================================
# SECTION 7: CROSS-SECTIONAL Z-SCORING
# =============================================================================

#' Z-score indicator values cross-sectionally
#'
#' Per-snapshot robust standardization. Winsorizes raw values cross-
#' sectionally at the [p2.5, p97.5] percentiles of the current snapshot,
#' then standardizes using median / MAD of the winsorized cross-section.
#' Financial-sector-invalid indicators (gpa, inventory_turnover,
#' cash_based_op, capex_depreciation, inventory_sales_change) are
#' computed excluding financial rows and then NA-masked for financials.
#'
#' @param indicator_dt data.table. Rows = tickers, columns = indicator values.
#'   Must also have 'sector' column for financial sector handling.
#' @param clip Numeric length-2 or NULL. Post-standardization clip bounds.
#'   Defaults to c(-5, 5) -- see .robust_zscore docstring and
#'   tools/diag_zscore_tails.R for the tail-diagnostic evidence that
#'   drove the choice of 5 over 3.
#' @return data.table with z-scored values (same structure minus sector column).
zscore_cross_section <- function(indicator_dt, clip = c(-5, 5)) {

  if (is.null(indicator_dt) || nrow(indicator_dt) == 0) return(indicator_dt)

  dt <- copy(indicator_dt)
  is_financial <- dt[["sector"]] %in% "Financial"

  ind_cols <- setdiff(names(dt), c("ticker", "sector"))

  for (col_name in ind_cols) {
    vals <- dt[[col_name]]
    financial_masked <- col_name %in% .FINANCIAL_NA_INDICATORS

    # For financial-NA indicators, exclude financials from the winsorization
    # and standardization sample, then NA-mask financial rows on output.
    if (financial_masked) {
      vals_in <- vals
      vals_in[is_financial] <- NA_real_
      z <- .robust_zscore(vals_in, clip = clip)
      z[is_financial] <- NA_real_
    } else {
      z <- .robust_zscore(vals, clip = clip)
    }
    dt[, (col_name) := z]
  }

  dt[, sector := NULL]
  dt
}


# =============================================================================
# SECTION 8: PUBLIC API
# =============================================================================

#' Get canonical indicator names
#'
#' @return Character vector of all indicator names in output order.
get_indicator_names <- function() {
  .INDICATOR_NAMES
}


#' Compute all indicators for a single ticker at a point in time
#'
#' @param fund_dt data.table. Long-format fundamental data for one ticker
#'   (from cached parquet). Must have columns: concept, value, fiscal_year,
#'   period_type, period_end, filed.
#' @param price_on_filed Numeric scalar. Closing price on the filing date.
#' @param sector Character scalar. Sector classification for this ticker.
#' @param target_fy Integer or NULL. Target fiscal year. If NULL, uses the
#'   most recent fiscal year available.
#' @param target_period Character. Period type to target ("FY" for annual).
#' @param splits data.table or NULL. Split events (ex_date, ratio) for this
#'   ticker, as returned by load_ticker_splits(). Used to split-adjust the
#'   share-issuance indicators; NULL leaves share counts unadjusted.
#' @return Named numeric vector of indicator values. NA for indicators that
#'   cannot be computed. Returns all-NA vector on total failure.
compute_ticker_indicators <- function(fund_dt, price_on_filed, sector,
                                      target_fy = NULL,
                                      target_period = "FY",
                                      splits = NULL) {

  # All-NA fallback
  all_na <- setNames(rep(NA_real_, length(.INDICATOR_NAMES)), .INDICATOR_NAMES)

  result <- tryCatch({

    # Drop rows sourced from deprecated XBRL tags (handles pre-existing caches)
    fund_dt <- .filter_deprecated_tags(fund_dt)

    # Pivot to wide format
    wide <- pivot_fundamentals(fund_dt)
    if (is.null(wide)) return(all_na)

    # Add derived quantities
    wide <- .derive_quantities(wide)

    # Select target fiscal year
    fy_rows <- wide[period_type == target_period]
    if (nrow(fy_rows) == 0) return(all_na)

    if (is.null(target_fy)) {
      target_fy <- max(fy_rows$fiscal_year)
    }

    curr_idx <- which(fy_rows$fiscal_year == target_fy)
    if (length(curr_idx) == 0) return(all_na)
    curr <- fy_rows[curr_idx[1]]

    # Prior year for growth / change indicators, constructed from the LONG
    # data by PERIOD END, not by fiscal_year label. The target year's 10-K
    # re-reports the prior year as comparatives carrying the TARGET year's
    # label, so in any view that includes that 10-K the "fiscal_year - 1"
    # label group has lost its current-period rows and lagged indicators
    # would difference against a period two years back. (concept,
    # period_end, FY) is label-free and immune to relabeling. A gap guard
    # rejects non-annual spacing (fiscal-year transition stubs, missing
    # years) rather than differencing across the gap.
    prior <- .prior_fy_row(fund_dt, target_fy)

    # Quarterly rows for QoQ growth and FCF Stability
    q_rows <- wide[period_type %in% c("Q1", "Q2", "Q3", "Q4")]
    q_rows <- q_rows[order(fiscal_year, period_type)]

    # Most recent quarter and the immediately preceding quarter for QoQ
    curr_q <- NULL
    prior_q <- NULL
    if (nrow(q_rows) >= 2) {
      # Find quarters for target_fy
      target_q <- q_rows[fiscal_year == target_fy]
      if (nrow(target_q) > 0) {
        curr_q <- target_q[nrow(target_q)]  # latest quarter

        # Prior quarter: immediately preceding quarter (e.g., Q3->Q2, Q1->prior Q4)
        curr_q_idx <- which(q_rows$fiscal_year == curr_q$fiscal_year &
                              q_rows$period_type == curr_q$period_type)
        if (length(curr_q_idx) > 0 && curr_q_idx[1] > 1) {
          prior_q <- q_rows[curr_q_idx[1] - 1L]
        }
      }
    }

    # Quarterly history for FCF Stability. Pass the full quarterly series
    # so .decumulate_cfo can find each row's YTD sibling within the same
    # fiscal year. Tail-to-16 happens after de-cumulation.
    quarterly_hist <- if (nrow(q_rows) >= 8) q_rows else NULL

    # Extract key values
    shares <- .col(curr, "shares_outstanding")
    price  <- price_on_filed

    # -- Compute all indicator groups --
    val  <- .compute_valuation(curr, price, shares)
    prof <- .compute_profitability(curr)
    grow <- .compute_growth(curr, prior, curr_q, prior_q)
    lev  <- .compute_leverage(curr)
    eff  <- .compute_efficiency(curr)
    cfq  <- .compute_cashflow_quality(curr)
    shr  <- .compute_shareholder(curr, val$market_cap)
    t1   <- .compute_tier1(curr, prior)
    t2   <- .compute_tier2(curr, prior, quarterly_hist)
    bsc  <- .compute_bs_change(curr, prior)
    lag5 <- .lag_fy_row(fund_dt, target_fy, lag_years = 5L)
    fin  <- .compute_financing(fund_dt, target_fy, curr, prior, lag5,
                               val$market_cap, splits)

    # PEG ratio: depends on growth (computed after growth)
    peg <- NA_real_
    pe <- val$pe_trailing
    eps_g <- grow$eps_growth_yoy
    if (!is.na(pe) && !is.na(eps_g) && eps_g > 0) {
      peg <- .safe_divide(pe, eps_g * 100)
    }

    # Assemble output vector in canonical order
    out <- c(
      # Valuation (8)
      pe_trailing      = val$pe_trailing,
      peg              = peg,
      pb               = val$pb,
      ps               = val$ps,
      pfcf             = val$pfcf,
      ev_ebitda        = val$ev_ebitda,
      ev_revenue       = val$ev_revenue,
      earnings_yield   = val$earnings_yield,
      # Profitability (6)
      gross_margin     = prof$gross_margin,
      operating_margin = prof$operating_margin,
      net_margin       = prof$net_margin,
      roe              = prof$roe,
      roa              = prof$roa,
      roic             = prof$roic,
      # Growth (5)
      revenue_growth_yoy = grow$revenue_growth_yoy,
      revenue_growth_qoq = grow$revenue_growth_qoq,
      eps_growth_yoy     = grow$eps_growth_yoy,
      opinc_growth_yoy   = grow$opinc_growth_yoy,
      ebitda_growth      = grow$ebitda_growth,
      # Leverage (5)
      debt_equity       = lev$debt_equity,
      net_debt_ebitda   = lev$net_debt_ebitda,
      interest_coverage = lev$interest_coverage,
      current_ratio     = lev$current_ratio,
      quick_ratio       = lev$quick_ratio,
      # Efficiency (3)
      asset_turnover       = eff$asset_turnover,
      inventory_turnover   = eff$inventory_turnover,
      receivables_turnover = eff$receivables_turnover,
      # Cash Flow Quality (3)
      fcf_ni       = cfq$fcf_ni,
      opcf_ni      = cfq$opcf_ni,
      capex_revenue = cfq$capex_revenue,
      # Shareholder Return (3)
      dividend_yield = shr$dividend_yield,
      payout_ratio   = shr$payout_ratio,
      buyback_yield  = shr$buyback_yield,
      # Size (5)
      market_cap         = val$market_cap,
      enterprise_value   = val$enterprise_value,
      revenue_raw        = .col(curr, "revenue"),
      shares_outstanding = .col(curr, "shares_outstanding"),
      public_float       = .col(curr, "public_float"),
      # Tier 1 (5 + 10 Piotroski)
      gpa                  = t1$gpa,
      asset_growth         = t1$asset_growth,
      sloan_accrual        = t1$sloan_accrual,
      pct_accruals         = t1$pct_accruals,
      net_operating_assets = t1$net_operating_assets,
      f_roa     = t1$f_roa,
      f_droa    = t1$f_droa,
      f_cfo     = t1$f_cfo,
      f_accrual = t1$f_accrual,
      f_dlever  = t1$f_dlever,
      f_dliquid = t1$f_dliquid,
      f_eq_off  = t1$f_eq_off,
      f_dmargin = t1$f_dmargin,
      f_dturn   = t1$f_dturn,
      f_score   = t1$f_score,
      # Tier 2 (6)
      cash_based_op          = t2$cash_based_op,
      fcf_stability          = t2$fcf_stability,
      sga_efficiency         = t2$sga_efficiency,
      capex_depreciation     = t2$capex_depreciation,
      dso_change             = t2$dso_change,
      inventory_sales_change = t2$inventory_sales_change,
      # Wave 1: balance-sheet change family (15)
      del_coa          = bsc$del_coa,
      del_col          = bsc$del_col,
      del_finl         = bsc$del_finl,
      del_lti          = bsc$del_lti,
      del_equ          = bsc$del_equ,
      del_netfin       = bsc$del_netfin,
      total_accruals   = bsc$total_accruals,
      dnoa             = bsc$dnoa,
      ch_nncoa         = bsc$ch_nncoa,
      ch_nwc           = bsc$ch_nwc,
      inventory_change = bsc$inventory_change,
      inventory_growth = bsc$inventory_growth,
      ppe_inv_change   = bsc$ppe_inv_change,
      equity_growth    = bsc$equity_growth,
      gr_ltnoa         = bsc$gr_ltnoa,
      # Wave 2: external financing family (7)
      net_debt_finance        = fin$net_debt_finance,
      net_equity_finance      = fin$net_equity_finance,
      xfin                    = fin$xfin,
      composite_debt_issuance = fin$composite_debt_issuance,
      share_iss_1y            = fin$share_iss_1y,
      share_iss_5y            = fin$share_iss_5y,
      net_payout_yield        = fin$net_payout_yield
    )

    # Financial sector: NA-out meaningless indicators
    if (!is.na(sector) && sector == "Financial") {
      out[.FINANCIAL_NA_INDICATORS] <- NA_real_
    }

    out

  }, error = function(e) {
    warning(sprintf("compute_ticker_indicators: failed: %s", e$message),
            call. = FALSE)
    all_na
  })

  .assert_output(result, "compute_ticker_indicators", list(
    "is numeric vector"   = is.numeric,
    "has names"           = function(x) !is.null(names(x)),
    "correct length"      = function(x) length(x) == length(.INDICATOR_NAMES)
  ))

  result
}


#' Compute cross-sectional indicator matrix with z-scoring
#'
#' @param indicator_list Named list of named numeric vectors (one per ticker),
#'   as returned by compute_ticker_indicators().
#' @param tickers Character vector. Ticker symbols (same order as indicator_list).
#' @param sectors Character vector. Sector per ticker (same order).
#' @return List with $raw (data.table) and $zscored (data.table).
#'   Both have ticker column + indicator columns.
compute_cross_section <- function(indicator_list, tickers, sectors,
                                  clip = c(-5, 5)) {

  stopifnot(length(indicator_list) == length(tickers))
  stopifnot(length(indicator_list) == length(sectors))

  # Stack named vectors into data.table
  raw_dt <- rbindlist(lapply(indicator_list, as.list), fill = TRUE)
  raw_dt[, ticker := tickers]
  raw_dt[, sector := sectors]

  # Reorder columns: ticker first, then indicators
  setcolorder(raw_dt, c("ticker", "sector", .INDICATOR_NAMES))

  # Z-score
  zscore_input <- copy(raw_dt)
  zscore_input[, ticker := NULL]
  zscored_dt <- zscore_cross_section(zscore_input, clip = clip)
  zscored_dt[, ticker := tickers]
  setcolorder(zscored_dt, c("ticker", .INDICATOR_NAMES))

  # Remove sector from raw output
  raw_out <- copy(raw_dt)
  raw_out[, sector := NULL]

  .assert_output(raw_out, "compute_cross_section$raw", list(
    "is data.table"    = is.data.table,
    "has ticker col"   = function(x) "ticker" %in% names(x),
    "nrow matches"     = function(x) nrow(x) == length(tickers)
  ))

  list(raw = raw_out, zscored = zscored_dt)
}
