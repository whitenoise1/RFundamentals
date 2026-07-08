# ============================================================================
# indicator_compute.R  --  Fundamental Indicator Computation (Module 4)
# ============================================================================
# Pure computation layer: no I/O. Receives data.tables and price scalars,
# returns named vectors and data.tables.
#
# Computes 130 output fields from EDGAR XBRL data + market prices:
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
#   - 6 Wave 3 seasonal-surprise indicators (ChTax, earnings-increase
#     streak, revenue/earnings surprise, earnings consistency, roaq)
#     built on a label-free standalone-quarter panel -- see docs/research/09
#   - 29 Wave 4 levels and investment indicators (ME-scaled levels, tax,
#     tangibility, capex growth family, AB98 constructions, R&D capital)
#     -- see docs/research/09 Wave 4 preflight, RESEARCH_INDICATORS.md W4.*
#   - 14 Wave 5 composite scores (Mohanram G-score components, Ohlson
#     O-Score, Altman Z-Score, Herfindahl industry concentration, KZ and
#     WW constraint indices) -- see docs/research/09 Wave 5 preflight,
#     RESEARCH_INDICATORS.md W5.*
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
  "inventory_change", "inventory_growth", "ppe_inv_change",
  # Wave 4: COGS/inventory/NOA-turnover constructions are undefined for
  # banks; net_debt_price follows OpenAP's SIC 6000-6999 exclusion (a
  # bank's debt is operating raw material, not net financing)
  "tang", "op_leverage", "ch_asset_turnover", "gr_sale_to_gr_inv",
  "net_debt_price",
  # Wave 5: Ohlson/Altman coefficients presume industrials; OpenAP sets
  # both to missing for SIC > 5999
  "o_score", "z_score"
)

# Sector -> indicators NA map (Wave 5 generalization of the financial
# mask). OpenAP's OScore/ZScore exclusion is SIC 4000-4999 and > 5999:
# transport/utilities plus financials-and-beyond. Finviz sectors cannot
# isolate transports (inside Industrials), so the mapping is Financial +
# Utilities + Real Estate (documented deviation, docs/research/09 Wave 5
# item 9). Herf's permanent SIC-49 exclusion maps to Utilities.
.SECTOR_NA_INDICATORS <- list(
  "Financial"   = .FINANCIAL_NA_INDICATORS,
  "Utilities"   = c("o_score", "z_score", "herf"),
  "Real Estate" = c("o_score", "z_score")
)

# Reverse map (indicator -> masking sectors), built once so per-column
# mask lookups in the z-scoring loops are O(1) for unmasked columns.
.SECTOR_NA_COL_MAP <- local({
  m <- list()
  for (sec in names(.SECTOR_NA_INDICATORS)) {
    for (cn in .SECTOR_NA_INDICATORS[[sec]]) m[[cn]] <- c(m[[cn]], sec)
  }
  m
})

# TRUE for cross-section rows whose sector NA-masks this indicator.
.sector_na_mask <- function(col_name, sectors) {
  secs <- .SECTOR_NA_COL_MAP[[col_name]]
  if (is.null(secs)) rep(FALSE, length(sectors)) else sectors %in% secs
}

# FALSE for benchmark-group labels that cannot anchor a cross-sectional
# comparison: NA or the "Unknown" placeholder assemblers use for failed
# finviz lookups.
.benchmark_group_ok <- function(label) {
  !is.na(label) && label != "Unknown"
}

# Indicators finalized by sector demeaning at the cross-section stage.
# compute_ticker_indicators emits the firm-level ingredient (for
# ch_inv_ia: the AB98 two-year-average-base capex growth); the
# cross-section functions subtract the equal-weighted sector mean --
# the compute module never sees the cross-section (pure, per-ticker).
.SECTOR_DEMEAN_INDICATORS <- c("ch_inv_ia")

# Wave 5 columns finalized at the cross-section stage. The per-ticker
# vector (and the per-ticker timeseries layers) carry these as NA plus
# explicitly named ing_* ingredient columns; the finalize pass
# (industry_adjust_cross_section) populates a final column ONLY when its
# ingredients are present, so re-running the pass on finalized data is a
# no-op rather than a silent re-binarization. ms_accrual (CFO > NI) is
# genuinely per-ticker and is emitted final; ms_score is assembled from
# the finalized components. Any cross-section assembly point must run
# industry_adjust_cross_section or NA these columns (same rule as
# .SECTOR_DEMEAN_INDICATORS).
.MS_MEDIAN_GT <- c("ms_roa", "ms_cfroa", "ms_rd", "ms_capex", "ms_adv")
.MS_MEDIAN_LT <- c("ms_roa_vol", "ms_rev_vol")
.MS_COMPONENTS <- c("ms_roa", "ms_cfroa", "ms_accrual", "ms_roa_vol",
                    "ms_rev_vol", "ms_rd", "ms_capex", "ms_adv")

# Ingredient carrier per median-binarized MS component (final -> ing_).
.MS_INGREDIENT_OF <- setNames(
  paste0("ing_", c(.MS_MEDIAN_GT, .MS_MEDIAN_LT)),
  c(.MS_MEDIAN_GT, .MS_MEDIAN_LT)
)

# Revenue columns consumed by the Herfindahl 3-year average (current +
# two lagged FYs). Declared per consumer so the generic ingredient
# registry can grow without silently feeding herf non-revenue columns.
.HERF_REV_COLS <- c("revenue_raw", "ing_rev_lag1", "ing_rev_lag2")

.CROSS_SECTION_FINALIZED <- c(.SECTOR_DEMEAN_INDICATORS,
                              c(.MS_MEDIAN_GT, .MS_MEDIAN_LT),
                              "ms_score", "herf", "ww_index")

# Hidden ingredient columns appended to the per-ticker vector after the
# canonical indicators; consumed by the finalize pass and dropped from
# cross-section outputs. Per-ticker timeseries layers persist them so
# the daily cross-section reader can finalize.
.CS_INGREDIENT_COLS <- c(unname(.MS_INGREDIENT_OF), "ing_ww_partial",
                         "ing_rev_lag1", "ing_rev_lag2")

# XBRL tags that should not contribute to a concept when read from cache.
# InterestIncomeExpenseNet is net of interest income and can be negative
# for cash-rich firms; it is not a valid proxy for gross interest expense
# and would flip the sign of interest_coverage. Caches built before this
# alias was removed still contain these rows; drop them on read.
.DEPRECATED_CONCEPT_TAGS <- list(
  interest_expense = c("InterestIncomeExpenseNet")
)

# Canonical indicator names in output order (130 entries):
# 36 baseline + 2 size (shares_outstanding, public_float) + 5 tier1 +
# 10 piotroski (9 components + composite) + 6 tier2 + 15 wave1 + 7 wave2 +
# 6 wave3 + 29 wave4 (19 levels + 10 investment) + 14 wave5 (9 mohanram +
# 2 distress + 3 structure)
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
  "net_payout_yield",
  # Wave 3: quarterly seasonal-surprise family (6) -- OpenAP expansion
  "ch_tax", "num_earn_increase", "revenue_surprise",
  "earnings_surprise", "earnings_consistency", "roaq",
  # Wave 4: levels family (19) -- OpenAP expansion
  "rd_me", "ebm", "bpebm", "cf_me", "cfp",
  "net_debt_price", "tang", "tax_to_book", "effective_tax_rate",
  "am", "book_leverage", "leverage_mkt", "cash_prod", "oper_prof",
  "cash_assets", "op_leverage", "payout_yield", "salecash", "depr_rate",
  # Wave 4: investment family (10) -- OpenAP expansion
  "pchdepr", "grcapx", "grcapx3y", "pct_tot_acc",
  "ch_asset_turnover", "gr_sale_to_gr_inv", "surprise_rd",
  "investment", "rd_cap", "ch_inv_ia",
  # Wave 5: mohanram family (8 components + composite) -- OpenAP expansion
  "ms_roa", "ms_cfroa", "ms_accrual", "ms_roa_vol", "ms_rev_vol",
  "ms_rd", "ms_capex", "ms_adv", "ms_score",
  # Wave 5: distress family (2) -- OpenAP expansion
  "o_score", "z_score",
  # Wave 5: structure family (3) -- OpenAP expansion
  "herf", "kz_index", "ww_index"
)

# Family-to-indicator mapping (canonical groupings, 18 families)
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
                         "share_iss_5y", "net_payout_yield"),
  surprise          = c("ch_tax", "num_earn_increase", "revenue_surprise",
                         "earnings_surprise", "earnings_consistency", "roaq"),
  levels            = c("rd_me", "ebm", "bpebm", "cf_me", "cfp",
                         "net_debt_price", "tang", "tax_to_book",
                         "effective_tax_rate", "am", "book_leverage",
                         "leverage_mkt", "cash_prod", "oper_prof",
                         "cash_assets", "op_leverage", "payout_yield",
                         "salecash", "depr_rate"),
  investment        = c("pchdepr", "grcapx", "grcapx3y", "pct_tot_acc",
                         "ch_asset_turnover", "gr_sale_to_gr_inv",
                         "surprise_rd", "investment", "rd_cap", "ch_inv_ia"),
  mohanram          = c("ms_roa", "ms_cfroa", "ms_accrual", "ms_roa_vol",
                         "ms_rev_vol", "ms_rd", "ms_capex", "ms_adv",
                         "ms_score"),
  distress          = c("o_score", "z_score"),
  structure         = c("herf", "kz_index", "ww_index")
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


# -- Classify a reported duration (days) into a cumulation kind.
# Strict bands with NA gaps: a duration that fits no band (fiscal-year
# transition stubs, dedup mismatches) is NA rather than force-classified.
# Q1-standalone upper bound is 120 (not 110) to accommodate 16-week Q1
# used by Kroger/AAP fiscal calendars (Q1=16wk, Q2-Q4=12wk each).
# Shared by .decumulate_cfo and the Wave 3 quarter-panel builders; the
# looser catch-all bands in ttm_eps.R stay separate on purpose (the TTM
# identity is robust to a missing middle quarter, differencing is not).
.classify_duration <- function(days) {
  fifelse(is.na(days), NA_character_,
  fifelse(days >= 60  & days <= 120, "3mo",
  fifelse(days >= 160 & days <= 200, "2Q",
  fifelse(days >= 250 & days <= 290, "3Q",
  fifelse(days >= 330 & days <= 380, "FY",
          NA_character_)))))
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

  dt[, cfo_kind := .classify_duration(cfo_period_days)]

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

# Richardson total-accruals numerator dWC + dNCO + dFIN (all three
# required). Shared by total_accruals (scaled by avgAT, Wave 1) and
# pct_tot_acc (scaled by |NI|, Wave 4) so the pair cannot drift.
.ta_numerator <- function(b_t, b_p) {
  d_wc  <- b_t$wc  - b_p$wc
  d_nco <- b_t$nco - b_p$nco
  d_fin <- b_t$fin - b_p$fin
  if (is.na(d_wc) || is.na(d_nco) || is.na(d_fin)) {
    NA_real_
  } else d_wc + d_nco + d_fin
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
  out$total_accruals <- .safe_divide(.ta_numerator(b_t, b_p), avg_at)

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

# Preference order for share-count rows, applied by .fy_shares_at and
# .q_shares_at. The Wave M synthetic whole-company count outranks the DEI
# cover count, which outranks everything else: the DEI cover is the true
# point-in-time outstanding count, whereas some filers populate the
# balance-sheet CommonStockSharesOutstanding line with the constant
# ISSUED count and vary treasury stock instead (BA: 1,012.26M for years
# on end while outstanding moved 610M -> 750M through the 2024 offering).
.SHARES_TAG_PREF <- c("MulticlassSharesOutstanding",
                      "EntityCommonStockSharesOutstanding",
                      "MulticlassSharesOutstandingBS")

.prefer_share_tag <- function(win) {
  if (!"tag" %in% names(win)) return(win)
  for (pref in .SHARES_TAG_PREF) {
    sel <- win[tag == pref]
    if (nrow(sel)) return(sel)
  }
  win
}

# FY share count for the fiscal year ending at pe, with the row's own
# filed date (the basis date). Candidates are FY-labeled share rows dated
# in [pe, pe + 120d]: the balance-sheet date itself plus the 10-K
# cover-page instant a few weeks later.
# Returns list(v, filed) or NULL.
.fy_shares_at <- function(fund_dt, pe) {
  if (is.null(pe)) return(NULL)
  win <- fund_dt[concept == "shares_outstanding" & period_type == "FY" &
                   !is.na(value) & !is.na(period_end) &
                   period_end >= pe & period_end <= pe + 120]
  if (nrow(win) == 0) return(NULL)
  win <- .prefer_share_tag(win)
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
# SECTION 6d: WAVE 3 -- QUARTERLY SEASONAL-SURPRISE FAMILY
# =============================================================================
# 6 indicators from the OpenAP expansion (docs/research/09, Wave 3), built
# on a label-free standalone-quarter panel. Constructions verified against
# the OpenAP pyCode predictors (github.com/OpenSourceAP/CrossSection,
# Signals/pyCode/Predictors) on 2026-07-03:
#   ch_tax               ChTax, Thomas & Zhang (2011 JAR)
#   num_earn_increase    NumEarnIncrease, Loh & Warachka (2012 MS)
#   revenue_surprise     RevenueSurprise, Jegadeesh & Livnat (2006 JFE)
#   earnings_surprise    EarningsSurprise (SUE), Foster, Olsen, Shevlin (1984)
#   earnings_consistency EarningsConsistency, Alwathainani (2009 BAR)
#   roaq                 roaq, Balakrishnan, Bartov, Faurel (2010 JAE)
#
# Panel construction never trusts (fiscal_year, period_type) labels: a
# later 10-K re-reports prior periods under its own label, so label-keyed
# quarter selection differences the wrong periods (the Wave 1 lesson).
# Rows are grouped by period_start and classified by reported duration
# (.classify_duration); lags are matched by period_end distance with gap
# guards. PIT correctness is inherited from the caller's pit_dedup as-of
# view. Financials are computed, not masked: taxes, net income and EPS
# are well-defined for banks (bank revenue tags are sparse, so
# revenue_surprise degrades to NA there -- documented, not forced).

# Product of split ratios with ex_date after `filed`: maps an as-filed
# per-share/count value onto the CURRENT split basis (GAAP restates
# per-share figures for splits in every filing, so a value's basis is the
# split state at its filed date). Scalar; quantmod convention (4:1 split
# has ratio 0.25). Same rule as ttm_eps.R's .split_factor, local so this
# module stays self-contained.
.split_to_current <- function(filed, splits = NULL) {
  if (is.null(splits) || !NROW(splits) || is.na(filed)) return(1)
  ex <- as.Date(splits$ex_date); rt <- as.numeric(splits$ratio)
  sel <- !is.na(ex) & !is.na(rt) & rt > 0 & ex > as.Date(filed)
  if (!any(sel)) 1 else prod(rt[sel])
}

# Index of the element of `dates` nearest to `target`, provided it is
# within `tol` days; NA_integer_ otherwise.
.near_idx <- function(dates, target, tol) {
  if (!length(dates) || is.na(target)) return(NA_integer_)
  off <- abs(as.numeric(dates - target))
  i <- which.min(off)
  if (!length(i) || off[i] > tol) return(NA_integer_)
  i
}

# Duration rows of one concept, duration-classified and vintage-deduped.
# One row per (period_start, kind); the original report wins (non-8-K,
# then earliest filed -- same PIT rule as build_ttm_eps_series). With
# split_adjust = TRUE values are normalized to the current split basis
# BEFORE any differencing, so a chain spanning a split stays coherent.
# Returns NULL when the input has no usable duration rows.
.dedup_period_rows <- function(fund_dt, cn, splits = NULL,
                               split_adjust = FALSE) {
  need <- c("concept", "value", "period_start", "period_end", "filed")
  if (is.null(fund_dt) || !all(need %in% names(fund_dt))) return(NULL)
  x <- fund_dt[concept == cn & !is.na(period_start) & !is.na(period_end) &
                 !is.na(value)]
  if (nrow(x) == 0) return(NULL)
  x[, `:=`(ps = as.Date(period_start), pe = as.Date(period_end),
           fd = as.Date(filed))]
  if (split_adjust) {
    x[, value := value *
        vapply(fd, .split_to_current, numeric(1), splits = splits)]
  }
  x[, kind := .classify_duration(as.integer(pe - ps))]
  x <- x[!is.na(kind)]
  if (nrow(x) == 0) return(NULL)
  x[, is8k := if ("form" %in% names(x)) {
    as.integer(!is.na(form) & form == "8-K")
  } else 0L]
  setorder(x, ps, kind, is8k, fd, na.last = TRUE)
  x[, .SD[1], by = .(ps, kind)]
}

# Standalone-quarter series for one flow concept: data.table(qend, filed,
# value) ordered by qend, or NULL. Per quarter, a directly reported
# 3-month row wins; otherwise the value is recovered by differencing
# consecutive YTD rows sharing a period_start (Q2 = 2Q - Q1, Q3 = 3Q - 2Q,
# Q4 = FY - 3Q). The derived quarter's implied duration must itself look
# like a quarter (60-131 d): the bands admit pathological pairings at
# their edges, and differencing across a fiscal-year-change stub must
# yield nothing rather than a wrong value.
.flow_quarter_series <- function(fund_dt, cn, splits = NULL,
                                 split_adjust = FALSE) {
  x <- .dedup_period_rows(fund_dt, cn, splits, split_adjust)
  if (is.null(x)) return(NULL)

  parts <- list(
    x[kind == "3mo", .(qend = pe, filed = fd, value = value, direct = 1L)]
  )
  ytd_pairs <- list(c("2Q", "3mo"), c("3Q", "2Q"), c("FY", "3Q"))
  for (p in ytd_pairs) {
    hi <- x[kind == p[1], .(ps, pe, fd, value)]
    lo <- x[kind == p[2], .(ps, pe, fd, value)]
    if (nrow(hi) == 0 || nrow(lo) == 0) next
    m <- merge(hi, lo, by = "ps", suffixes = c("_hi", "_lo"))
    m <- m[as.integer(pe_hi - pe_lo) >= 60 & as.integer(pe_hi - pe_lo) <= 131]
    if (nrow(m) == 0) next
    parts[[length(parts) + 1]] <-
      m[, .(qend = pe_hi, filed = pmax(fd_hi, fd_lo),
            value = value_hi - value_lo, direct = 0L)]
  }
  ser <- rbindlist(parts)
  if (nrow(ser) == 0) return(NULL)
  setorder(ser, qend, -direct, filed, na.last = TRUE)
  ser <- ser[!duplicated(qend)]
  ser[, direct := NULL]
  setorder(ser, qend)
  ser
}

# Instant series for one balance-sheet concept: data.table(qend, value)
# with one row per period_end (original report wins), ordered.
.instant_series <- function(fund_dt, cn) {
  need <- c("concept", "value", "period_end", "filed")
  if (is.null(fund_dt) || !all(need %in% names(fund_dt))) return(NULL)
  x <- fund_dt[concept == cn & !is.na(period_end) & !is.na(value)]
  if (nrow(x) == 0) return(NULL)
  x[, `:=`(pe = as.Date(period_end), fd = as.Date(filed))]
  x[, is8k := if ("form" %in% names(x)) {
    as.integer(!is.na(form) & form == "8-K")
  } else 0L]
  setorder(x, pe, is8k, fd, na.last = TRUE)
  x <- x[!duplicated(pe)]
  x[, .(qend = pe, value = as.numeric(value))]
}

# Split-adjusted share count for the quarter ending at qend, on the
# CURRENT basis. Candidates are share rows dated in [qend, qend + 75]:
# the balance-sheet instant plus the filing's cover instant a few weeks
# later, but never the next quarter's date (>= qend + 84). The DEI cover
# tag is preferred for the same reason as .fy_shares_at (BA-style filers
# hold the balance-sheet line at the constant issued count).
.q_shares_at <- function(fund_dt, qend, splits = NULL) {
  if (is.null(qend) || is.na(qend)) return(NA_real_)
  win <- fund_dt[concept == "shares_outstanding" & !is.na(value) &
                   !is.na(period_end) &
                   period_end >= qend & period_end <= qend + 75]
  if (nrow(win) == 0) return(NA_real_)
  win <- .prefer_share_tag(win)
  # filed breaks ties between an original report and a later re-report of
  # the same instant (original wins; its filed date is the split basis)
  r <- win[order(period_end, filed)][1]
  # counts move OPPOSITE to per-share values across a split: an as-filed
  # pre-split count is n times too small on the current basis, so divide
  # by the ratio product (EPS multiplies by it)
  as.numeric(r$value[1]) / .split_to_current(as.Date(r$filed[1]), splits)
}

# Lag index within an ordered quarter series: the quarter nearest to
# `nominal_days` before quarter i, within `tol`, strictly earlier than i.
# Nominal quarter spacing 91.3 d absorbs 52/53-week calendars and 16-week
# quarters at tol = 40; the seasonal (4-quarter) lag at 365/35 enforces
# the documented [330, 400] gap band.
.q_lag_idx <- function(qend, i, nominal_days, tol) {
  j <- .near_idx(qend, qend[i] - nominal_days, tol)
  if (is.na(j) || j >= i) NA_integer_ else j
}

# Standardized seasonal surprise (Foster, Olsen, Shevlin 1984 form as
# reproduced by OpenAP) at the LATEST quarter of the series:
#   diff_t  = x_t - x_{t-4q}
#   drift_t = mean(diff over the prior 8 quarters, available-case)
#   su_t    = diff_t - drift_t
#   signal  = su_t / sd(su over the prior 8 quarters, >= 2 required)
# Available-case convention per OpenAP: no minimum-count rule beyond what
# the mean (>= 1) and sd (>= 2) need. sd at or below sd_floor -> NA
# (ultra-stable seasonal diffs would otherwise explode the ratio).
.seasonal_surprise <- function(qend, v, sd_floor) {
  n <- length(v)
  if (n < 4) return(NA_real_)

  dif <- rep(NA_real_, n)
  for (i in seq_len(n)) {
    j <- .q_lag_idx(qend, i, 365, 35)
    if (!is.na(j) && !is.na(v[i]) && !is.na(v[j])) dif[i] <- v[i] - v[j]
  }

  su <- rep(NA_real_, n)
  for (i in seq_len(n)) {
    if (is.na(dif[i])) next
    lags <- vapply(1:8, function(m) {
      j <- .q_lag_idx(qend, i, round(m * 91.3), 40)
      if (is.na(j)) NA_real_ else dif[j]
    }, numeric(1))
    if (all(is.na(lags))) next
    su[i] <- dif[i] - mean(lags, na.rm = TRUE)
  }

  if (is.na(su[n])) return(NA_real_)
  sd_lags <- vapply(1:8, function(m) {
    j <- .q_lag_idx(qend, n, round(m * 91.3), 40)
    if (is.na(j)) NA_real_ else su[j]
  }, numeric(1))
  if (sum(!is.na(sd_lags)) < 2) return(NA_real_)
  s <- stats::sd(sd_lags, na.rm = TRUE)
  if (is.na(s) || s <= sd_floor) return(NA_real_)
  su[n] / s
}

# Earnings consistency (Alwathainani 2009, OpenAP CLG-CHG form). Annual:
# egrowth_t = (e_t - e_{t-1}) / (0.5 * (|e_{t-1}| + |e_{t-2}|)); the
# signal is the mean of egrowth over the current + 4 prior years
# (available-case), NA'd when current or prior-year EPS is missing, the
# raw EPS ratio exceeds 6 in magnitude, or the current growth's sign
# flips against last year's (positive after negative; negative after
# positive-or-missing). Anchored to the target year's balance-sheet date
# so it moves with the annual indicators, not the latest quarter.
.compute_earnings_consistency <- function(fund_dt, target_fy, splits = NULL) {
  x <- .dedup_period_rows(fund_dt, "eps_diluted", splits, split_adjust = TRUE)
  if (is.null(x)) return(NA_real_)
  a <- x[kind == "FY", .(pe, value)]
  if (nrow(a) < 3) return(NA_real_)
  setorder(a, pe)

  anchor <- .lag_fy_pe(fund_dt, target_fy, 0L)
  if (is.null(anchor)) return(NA_real_)
  ti <- .near_idx(a$pe, as.Date(anchor), 10)
  if (is.na(ti)) return(NA_real_)

  lag_eps <- function(i, m) {
    j <- .near_idx(a$pe, a$pe[i] - round(m * 365.25), 35)
    if (is.na(j) || j >= i) NA_real_ else a$value[j]
  }
  egrowth <- function(i) {
    if (is.na(i)) return(NA_real_)
    e0 <- a$value[i]; e1 <- lag_eps(i, 1); e2 <- lag_eps(i, 2)
    if (is.na(e0) || is.na(e1) || is.na(e2)) return(NA_real_)
    den <- 0.5 * (abs(e1) + abs(e2))
    if (den < 1e-9) return(NA_real_)
    (e0 - e1) / den
  }

  idx <- vapply(0:4, function(m) {
    if (m == 0) return(ti)
    j <- .near_idx(a$pe, a$pe[ti] - round(m * 365.25), 35)
    if (is.na(j) || j >= ti) NA_integer_ else j
  }, integer(1))
  egs <- vapply(idx, egrowth, numeric(1))

  e0 <- a$value[ti]; e1 <- lag_eps(ti, 1)
  if (is.na(e1)) return(NA_real_)
  if (isTRUE(abs(e0 / e1) > 6)) return(NA_real_)
  eg0 <- egs[1]; eg1 <- egs[2]
  if (isTRUE(eg0 > 0 & eg1 < 0)) return(NA_real_)
  if (!is.na(eg0) && eg0 < 0 && (is.na(eg1) || eg1 > 0)) return(NA_real_)

  if (all(is.na(egs))) return(NA_real_)
  mean(egs, na.rm = TRUE)
}

# Quarter panels shared by .compute_surprise and .compute_ms_ingredients
# within one compute_ticker_indicators call -- the de-cumulation scan is
# the most expensive per-ticker step, so it must run once.
.quarter_panels <- function(fund_dt) {
  list(
    ni  = .flow_quarter_series(fund_dt, "net_income"),
    rev = .flow_quarter_series(fund_dt, "revenue"),
    at  = .instant_series(fund_dt, "total_assets")
  )
}

.compute_surprise <- function(fund_dt, target_fy, splits = NULL,
                              panels = NULL) {

  out <- list(
    ch_tax = NA_real_, num_earn_increase = NA_real_,
    revenue_surprise = NA_real_, earnings_surprise = NA_real_,
    earnings_consistency = NA_real_, roaq = NA_real_,
    # panel-based QoQ revenue growth (Wave 1 follow-up): reported
    # separately so the caller can fall back to the legacy label-based
    # selection only when no revenue panel exists at all (synthetic
    # inputs without period_start).
    has_rev_panel = FALSE, rev_qoq = NA_real_
  )

  if (is.null(panels)) panels <- .quarter_panels(fund_dt)
  ni  <- panels$ni
  rev <- panels$rev
  at  <- panels$at
  tax <- .flow_quarter_series(fund_dt, "income_tax_expense")
  eps <- .flow_quarter_series(fund_dt, "eps_diluted", splits,
                              split_adjust = TRUE)

  # -- Thomas & Zhang (2011): seasonal change in quarterly tax expense
  #    over total assets at the lag quarter's balance-sheet date.
  if (!is.null(tax) && nrow(tax) >= 2 && !is.null(at)) {
    i <- nrow(tax)
    j <- .q_lag_idx(tax$qend, i, 365, 35)
    if (!is.na(j)) {
      k <- .near_idx(at$qend, tax$qend[j], 10)
      if (!is.na(k)) {
        out$ch_tax <- .safe_divide(tax$value[i] - tax$value[j], at$value[k])
      }
    }
  }

  # -- Balakrishnan, Bartov, Faurel (2010): quarterly NI over prior
  #    quarter's total assets.
  if (!is.null(ni) && nrow(ni) >= 2 && !is.null(at)) {
    i <- nrow(ni)
    j <- .q_lag_idx(ni$qend, i, 91, 40)
    if (!is.na(j)) {
      k <- .near_idx(at$qend, ni$qend[j], 10)
      if (!is.na(k)) out$roaq <- .safe_divide(ni$value[i], at$value[k])
    }
  }

  # -- Loh & Warachka (2012) streak, OpenAP-exact: chearn_q = NI_q -
  #    NI_{q-4}; streak = k when the current and first k-1 lagged chearn
  #    are positive OR MISSING and lag k is reported non-positive.
  #    Faithful quirks: a missing quarter continues a streak, and a
  #    streak of 9+ (no reported non-positive lag within 8) scores 0.
  if (!is.null(ni) && nrow(ni) >= 1) {
    q <- ni$qend; v <- ni$value; n <- nrow(ni)
    chearn_at <- function(i) {
      j <- .q_lag_idx(q, i, 365, 35)
      if (is.na(j)) NA_real_ else v[i] - v[j]
    }
    ch <- vapply(0:8, function(m) {
      i <- if (m == 0) n else .q_lag_idx(q, n, round(m * 91.3), 40)
      if (is.na(i)) NA_real_ else chearn_at(i)
    }, numeric(1))
    pos <- is.na(ch) | ch > 0
    nincr <- 0L
    for (k in 1:8) {
      if (!all(pos[1:k])) break
      if (!is.na(ch[k + 1]) && ch[k + 1] <= 0) { nincr <- k; break }
    }
    out$num_earn_increase <- as.numeric(nincr)
  }

  # -- Foster, Olsen, Shevlin (1984): SUE on split-adjusted diluted EPS
  #    (OpenAP uses basic EPS ex-extraordinaries; diluted is what we
  #    cache -- documented deviation).
  if (!is.null(eps)) {
    out$earnings_surprise <- .seasonal_surprise(eps$qend, eps$value, 1e-10)
  }

  # -- Jegadeesh & Livnat (2006): revenue surprise per share, shares on
  #    the current split basis so the per-share series is coherent.
  if (!is.null(rev)) {
    shr <- vapply(rev$qend, .q_shares_at, numeric(1),
                  fund_dt = fund_dt, splits = splits)
    revps <- fifelse(is.na(shr) | shr <= 0, NA_real_, rev$value / shr)
    out$revenue_surprise <- .seasonal_surprise(rev$qend, revps, 1e-8)
  }

  # -- Alwathainani (2009): annual EPS growth consistency.
  out$earnings_consistency <-
    .compute_earnings_consistency(fund_dt, target_fy, splits)

  # -- Panel-based QoQ revenue growth (latest standalone quarter vs the
  #    one before it, matched by period_end -- not by labels).
  if (!is.null(rev)) {
    out$has_rev_panel <- TRUE
    i <- nrow(rev)
    j <- .q_lag_idx(rev$qend, i, 91, 40)
    if (!is.na(j)) out$rev_qoq <- .safe_growth(rev$value[i], rev$value[j])
  }

  out
}


# =============================================================================
# SECTION 6e: WAVE 4 -- LEVELS FAMILY
# =============================================================================
# 19 indicators from the OpenAP expansion (docs/research/09, Wave 4).
# Constructions verified against the OpenAP Stata code
# (Signals/LegacyStataCode) on 2026-07-03; canonical formulas, guards and
# deviations in RESEARCH_INDICATORS.md W4.1-W4.13. All current-year
# ratios; ME = market cap on the filing date (our PIT rule).
#
# che_eq = cash + st_investments approximates Compustat CHE (which
# includes short-term investments). STI is absence-means-zero; cash NA
# propagates (C-3: missing cash is a coverage gap, not zero).
# OpenAP backtest-only sample filters (size terciles, B/M quintiles,
# manufacturing-only, positive-payout, seasoning) are NOT applied --
# this is a database, not a backtest.

# Top statutory federal tax rate for the fiscal year ending at pe:
# 0.35 through 2017-12-31, 0.21 from 2018-01-01, day-weighted blend for
# fiscal years straddling the boundary (TCJA sec. 15). fy_start is the
# true fiscal-year start when the caller knows it (prior FY period_end
# + 1 -- covers 52/53-week calendars); the 365-day fallback misblends
# only short transition periods (10-KT) straddling the boundary, whose
# prior row the gap guard rejects anyway. XBRL history starts ~2009,
# earlier regimes are moot. OpenAP's legacy table stops at 0.35 -- the
# TCJA regime is a deliberate correction (docs/research/09, item 9).
.statutory_rate <- function(pe, fy_start = NULL) {
  if (is.null(pe) || is.na(pe)) return(NA_real_)
  pe  <- as.Date(pe)
  cut <- as.Date("2017-12-31")
  if (pe <= cut) return(0.35)
  fs_ok <- !is.null(fy_start) && !is.na(fy_start)
  if (fs_ok) {
    fy_start <- as.Date(fy_start)
    n <- as.integer(pe - fy_start) + 1L
    if (n < 150L || n > 400L) fs_ok <- FALSE
  }
  if (!fs_ok) fy_start <- pe - 364L
  if (fy_start > cut) return(0.21)
  d_old <- as.integer(cut - fy_start) + 1L
  n_day <- as.integer(pe - fy_start) + 1L
  (0.35 * d_old + 0.21 * (n_day - d_old)) / n_day
}

# Shared resolution of the Compustat-style level components used by BOTH
# the compute path (.compute_levels) and the timeseries stub extraction
# (.extract_stubs in timeseries_builder.R). One source of truth: the
# filing-date and daily-recompute paths must not drift (Wave 4 review).
# Policies: che = cash + STI-zero-if-na (cash required, C-3); LT via the
# Wave 1 identity fallback AT - CEQ - NCI; FINL = LTD + STD + PS with
# >= 1 component reported (Wave 1 convention).
.level_components <- function(row) {
  at   <- .col(row, "total_assets")
  ceq  <- .col(row, "stockholders_equity")
  ni   <- .col(row, "net_income")
  cash <- .col(row, "cash")
  che  <- if (is.na(cash)) NA_real_ else {
    cash + .zero_if_na(.col(row, "st_investments"))
  }
  lt <- .col(row, "total_liabilities")
  if (is.na(lt) && !is.na(at) && !is.na(ceq)) {
    lt <- at - ceq - .zero_if_na(.col(row, "minority_interest"))
  }
  td   <- .col(row, "total_debt")
  finl <- .sum_available(.col(row, "long_term_debt"),
                         .col(row, "short_term_debt"),
                         .col(row, "preferred_stock"))
  list(
    at = at, ceq = ceq, ni = ni, che = che, lt = lt, td = td,
    finl    = finl,
    ncash   = if (!is.na(che) && !is.na(td)) che - td else NA_real_,
    ndp_num = if (!is.na(finl) && !is.na(che)) finl - che else NA_real_,
    cf_num  = if (is.na(ni)) NA_real_ else {
      ni + .zero_if_na(.col(row, "depreciation"))
    }
  )
}

.compute_levels <- function(curr, market_cap, fy_start = NULL, lc = NULL) {

  out <- list(
    rd_me = NA_real_, ebm = NA_real_, bpebm = NA_real_,
    cf_me = NA_real_, cfp = NA_real_, net_debt_price = NA_real_,
    tang = NA_real_, tax_to_book = NA_real_,
    effective_tax_rate = NA_real_, am = NA_real_,
    book_leverage = NA_real_, leverage_mkt = NA_real_,
    cash_prod = NA_real_, oper_prof = NA_real_, cash_assets = NA_real_,
    op_leverage = NA_real_, payout_yield = NA_real_,
    salecash = NA_real_, depr_rate = NA_real_
  )

  if (is.null(lc)) lc <- .level_components(curr)
  at     <- lc$at
  ceq    <- lc$ceq
  ni     <- lc$ni
  che_eq <- lc$che
  lt     <- lc$lt
  rev    <- .col(curr, "revenue")
  cogs_v <- .col(curr, "cogs")
  sga    <- .col(curr, "sga")
  rnd    <- .col(curr, "rnd")
  dep    <- .col(curr, "depreciation")
  cfo    <- .col(curr, "operating_cashflow")
  intexp <- .col(curr, "interest_expense")
  tax    <- .col(curr, "income_tax_expense")
  pretax <- .col(curr, "pretax_income")
  rec    <- .col(curr, "accounts_receivable")
  inv    <- .col(curr, "inventory")
  ppeg   <- .col(curr, "ppe_gross")
  ppen   <- .col(curr, "ppe_net")
  dv     <- .col(curr, "dividends_paid")
  bb     <- .col(curr, "buybacks")

  me <- market_cap

  # -- Chan, Lakonishok, Sougiannis (2001 JF): R&D to market. Missing
  #    R&D stays NA (OpenAP drops missing xrd): only R&D reporters carry
  #    a value, unlike rd_cap's zero-fill.
  out$rd_me <- .safe_divide(rnd, me)

  # -- Penman, Richardson, Tuna (2007 JAR): enterprise book-to-market
  #    and its leverage component. net_cash = che_eq - total debt
  #    (OpenAP's dc/dvpa/tstkp adjustments are sparse Compustat items
  #    outside XBRL scope; treated as 0 -- W4.2).
  ncash <- lc$ncash
  if (!is.na(ceq) && !is.na(me) && !is.na(ncash)) {
    out$ebm <- .safe_divide(ceq + ncash, me + ncash)
    bp <- .safe_divide(ceq, me)
    if (!is.na(bp) && !is.na(out$ebm)) out$bpebm <- bp - out$ebm
  }

  # -- Lakonishok, Shleifer, Vishny (1994 JF): (NI + D&A) / ME.
  #    Depreciation zero-if-na (OpenAP fills dp), NI required.
  out$cf_me <- .safe_divide(lc$cf_num, me)

  # -- Desai, Rajgopal, Venkatachalam (2004 TAR): CFO / ME (OpenAP's
  #    oancf branch; the pre-1988 accrual fallback is moot in XBRL era).
  out$cfp <- .safe_divide(cfo, me)

  # -- Penman et al. (2007): net debt to price. FINL = LTD + STD + PS
  #    (>= 1 component reported, Wave 1 convention). Financial NA via
  #    the mask (OpenAP drops SIC 6000-6999); the B/M-quintile filter is
  #    a backtest restriction, not applied.
  out$net_debt_price <- .safe_divide(lc$ndp_num, me)

  # -- Hahn & Lee (2009 JF), Almeida-Campello weights: asset
  #    tangibility. GROSS PPE required; REC/INV zero-if-na (OpenAP fill
  #    list). Manufacturing-only in OpenAP; we compute for all
  #    non-financials (W4.5).
  if (!is.na(che_eq) && !is.na(ppeg)) {
    tang_num <- che_eq + 0.715 * .zero_if_na(rec) +
      0.547 * .zero_if_na(inv) + 0.535 * ppeg
    out$tang <- .safe_divide(tang_num, at)
  }

  # -- Lev & Nissim (2004 TAR): taxable income to book income, total
  #    tax expense grossed up by the statutory rate (the current
  #    federal/foreign split is not fetched -- W4.6 deviation). OpenAP
  #    guard: 1 when taxes are positive but book income is not.
  pe_date <- if ("period_end" %in% names(curr)) {
    curr$period_end[1]
  } else as.Date(NA)
  tr <- .statutory_rate(pe_date, fy_start)
  if (!is.na(tax) && !is.na(ni) && !is.na(tr)) {
    if (tax > 0 && ni <= 0) {
      out$tax_to_book <- 1
    } else {
      out$tax_to_book <- .safe_divide(tax / tr, ni, min_abs_denom = 1)
    }
  }

  # -- Plain effective tax rate level (ours; the AB98 interacted ETR is
  #    an OpenAP placebo and price-entangled -- W4.6).
  out$effective_tax_rate <- .safe_divide(tax, pretax)

  # -- Fama & French (1992 JF): assets to market.
  out$am <- .safe_divide(at, me)

  # -- Fama & French (1992): book leverage = AT / BE, BE > 0 (negative
  #    book equity is a distress flag, not leverage -- W4.7).
  if (!is.na(ceq) && ceq > 0) {
    out$book_leverage <- .safe_divide(at, ceq)
  }

  # -- Bhandari (1988 JF): market leverage = total LIABILITIES / ME
  #    (OpenAP uses lt, not total debt -- preflight correction 2).
  out$leverage_mkt <- .safe_divide(lt, me)

  # -- Chandrashekar & Rao (2009 WP): cash productivity. OpenAP omits
  #    the paper's + DLTT term (preflight correction 3). min_abs_denom
  #    = 1: near-zero cash -> NA rather than an absurd ratio.
  if (!is.na(me) && !is.na(at)) {
    out$cash_prod <- .safe_divide(me - at, che_eq, min_abs_denom = 1)
  }

  # -- Fama & French (2006 JFE): operating profitability to book
  #    equity. All four numerator inputs required (OpenAP, stricter
  #    than FF); BE > 0 (W4.9).
  if (!is.na(rev) && !is.na(cogs_v) && !is.na(sga) && !is.na(intexp) &&
      !is.na(ceq) && ceq > 0) {
    out$oper_prof <- .safe_divide(rev - cogs_v - sga - intexp, ceq)
  }

  # -- Palazzo (2012 JFE): cash to assets. Quarterly cheq/atq in
  #    OpenAP; filing-frequency here (W4.10 deviation).
  out$cash_assets <- .safe_divide(che_eq, at)

  # -- Novy-Marx (2011 RoF): operating leverage. SGA zero-if-na
  #    (explicit in OpenAP), COGS required. Financial NA via the mask.
  if (!is.na(cogs_v)) {
    out$op_leverage <- .safe_divide(cogs_v + .zero_if_na(sga), at)
  }

  # -- Boudoukh et al. (2007 JF): gross payout yield. Complement of
  #    net_payout_yield without the issuance leg; components zero-if-na,
  #    at least one reported. Zero-payout firms keep 0 (OpenAP drops
  #    them -- backtest filter, W4.12).
  abs_dv <- if (is.na(dv)) NA_real_ else abs(dv)
  abs_bb <- if (is.na(bb)) NA_real_ else abs(bb)
  if (!all(is.na(c(abs_dv, abs_bb)))) {
    out$payout_yield <- .safe_divide(.zero_if_na(abs_dv) +
                                       .zero_if_na(abs_bb), me)
  }

  # -- Placebo levels (OpenAP Table 4; database completeness -- W4.13).
  out$salecash  <- .safe_divide(rev, che_eq, min_abs_denom = 1)
  out$depr_rate <- .safe_divide(dep, ppen)

  out
}


# =============================================================================
# SECTION 6f: WAVE 4 -- INVESTMENT FAMILY
# =============================================================================
# 10 indicators from the OpenAP expansion (docs/research/09, Wave 4).
# Lagged and industry-adjusted constructions; lag rows come from
# .lag_fy_row (label-free, gap-guarded). Canonical formulas in
# RESEARCH_INDICATORS.md W4.14-W4.22.
#
# ch_inv_ia is emitted as the FIRM-LEVEL ingredient (AB98 capex growth);
# the sector demean happens in industry_adjust_cross_section at the
# cross-section stage (.SECTOR_DEMEAN_INDICATORS).

# Abarbanell & Bushee (1998) growth: base = mean of the two prior
# years, falling back to simple 1-year growth when the 2-year version
# is unavailable (OpenAP rule). No positivity guard on the base
# (division by ~0 -> NA via .safe_divide).
.ab98_growth <- function(x0, x1, x2) {
  if (is.na(x0)) return(NA_real_)
  if (!is.na(x1) && !is.na(x2)) {
    base <- (x1 + x2) / 2
    g <- .safe_divide(x0 - base, base)
    if (!is.na(g)) return(g)
  }
  .safe_divide(x0 - x1, x1)
}

.compute_investment <- function(curr, prior, lag2, lag3, lag4) {

  out <- list(
    pchdepr = NA_real_, grcapx = NA_real_, grcapx3y = NA_real_,
    pct_tot_acc = NA_real_, ch_asset_turnover = NA_real_,
    gr_sale_to_gr_inv = NA_real_, surprise_rd = NA_real_,
    investment = NA_real_, rd_cap = NA_real_, ch_inv_ia = NA_real_
  )

  capx_t <- .col(curr,  "capex")
  capx_1 <- .col(prior, "capex")
  capx_2 <- .col(lag2,  "capex")
  capx_3 <- .col(lag3,  "capex")
  rev_t  <- .col(curr,  "revenue")
  rev_1  <- .col(prior, "revenue")
  rev_2  <- .col(lag2,  "revenue")
  inv_t  <- .col(curr,  "inventory")
  inv_1  <- .col(prior, "inventory")
  inv_2  <- .col(lag2,  "inventory")
  rnd_t  <- .col(curr,  "rnd")
  rnd_1  <- .col(prior, "rnd")
  at_t   <- .col(curr,  "total_assets")
  at_1   <- .col(prior, "total_assets")
  ni     <- .col(curr,  "net_income")

  # -- Placebo: percent change in depreciation rate (dp / net PPE).
  dr_t <- .safe_divide(.col(curr,  "depreciation"), .col(curr,  "ppe_net"))
  dr_p <- .safe_divide(.col(prior, "depreciation"), .col(prior, "ppe_net"))
  if (!is.na(dr_t) && !is.na(dr_p)) {
    out$pchdepr <- .safe_growth(dr_t, dr_p)
  }

  # -- Anderson & Garcia-Feijoo (2006 JF): capex growth. grcapx3y is
  #    current capex over the prior-3-year average (preflight
  #    correction 1), NOT capx_t / capx_{t-3} - 1. Positive
  #    denominators required (a negative capex base is a data artifact,
  #    not disinvestment).
  if (!is.na(capx_t) && !is.na(capx_2) && capx_2 > 0) {
    out$grcapx <- (capx_t - capx_2) / capx_2
  }
  if (!is.na(capx_t) && !is.na(capx_1) && !is.na(capx_2) &&
      !is.na(capx_3) && (capx_1 + capx_2 + capx_3) > 0) {
    out$grcapx3y <- 3 * capx_t / (capx_1 + capx_2 + capx_3)
  }

  if (!is.null(prior)) {

    b_t <- .bs_blocks(curr)
    b_p <- .bs_blocks(prior)

    # -- Hafzalla, Lundholm, Van Winkle (2011 TAR): percent total
    #    accruals. Numerator = unscaled Wave 1 Richardson decomposition
    #    via the shared helper (the OpenAP CF-statement version needs
    #    fincf/ivncf, not fetched -- W4.16). |NI| floor of 1 dollar as
    #    in pct_accruals.
    ta_num <- .ta_numerator(b_t, b_p)
    if (!is.na(ta_num) && !is.na(ni)) {
      out$pct_tot_acc <- .safe_divide(ta_num, abs(ni), min_abs_denom = 1)
    }

    # -- Soliman (2008 TAR): change in NOA turnover, simple difference
    #    of levels. NOA per the Tier 1 Hirshleifer construction (OpenAP's
    #    granular aco/lco/lo items are not fetched -- W4.17). Negative
    #    turnover -> NA before differencing (OpenAP guard).
    noa_2 <- if (!is.null(lag2)) .bs_blocks(lag2)$noa else NA_real_
    ato_t <- if (!is.na(b_t$noa) && !is.na(b_p$noa)) {
      .safe_divide(rev_t, (b_t$noa + b_p$noa) / 2)
    } else NA_real_
    ato_p <- if (!is.na(b_p$noa) && !is.na(noa_2)) {
      .safe_divide(rev_1, (b_p$noa + noa_2) / 2)
    } else NA_real_
    if (!is.na(ato_t) && ato_t < 0) ato_t <- NA_real_
    if (!is.na(ato_p) && ato_p < 0) ato_p <- NA_real_
    if (!is.na(ato_t) && !is.na(ato_p)) {
      out$ch_asset_turnover <- ato_t - ato_p
    }

    # -- Abarbanell & Bushee (1998 TAR): sales growth minus inventory
    #    growth, both on the 2-year-average base with 1-year fallback.
    gs <- .ab98_growth(rev_t, rev_1, rev_2)
    gi <- .ab98_growth(inv_t, inv_1, inv_2)
    if (!is.na(gs) && !is.na(gi)) {
      out$gr_sale_to_gr_inv <- gs - gi
    }

    # -- Eberhart, Maxwell, Siddique (2004 JF): unexpected R&D
    #    increase. Thresholds strictly > 5% on raw R&D growth and R&D
    #    intensity growth. Deviations from OpenAP's Stata missing
    #    semantics (W4.19): revenue and assets are REQUIRED (missing
    #    would vacuously pass their conditions in Stata), and a zero
    #    R&D base is NA (growth from nothing is not measurable), not a
    #    vacuous pass.
    if (!is.na(rnd_t) && !is.na(rnd_1)) {
      if (rnd_1 <= 0 || is.na(rev_t) || is.na(at_t) || is.na(at_1) ||
          at_t <= 0 || at_1 <= 0) {
        out$surprise_rd <- NA_real_
      } else {
        hit <- (rnd_t / rev_t > 0) &&
               (rnd_t / at_t  > 0) &&
               (rnd_t / rnd_1 > 1.05) &&
               ((rnd_t / at_t) / (rnd_1 / at_1) > 1.05)
        out$surprise_rd <- as.numeric(hit)
      }
    }

  }

  # -- Titman, Wei, Xie (2004 JFQA): capex intensity vs own history.
  #    Benchmark mean INCLUDES the current year (OpenAP asrol), >= 2 of
  #    3 years required, NA when revenue < $10M (preflight item 11).
  #    Values are raw dollars, Compustat's floor of 10 ($MM) is 1e7.
  #    Deliberately OUTSIDE the prior gate: a missing t-1 row with a
  #    valid t-2 row still satisfies the 2-of-3 rule (OpenAP's asrol
  #    min(24) computes in that state too).
  if (!is.na(rev_t) && rev_t >= 1e7) {
    ci0 <- .safe_divide(capx_t, rev_t)
    ci1 <- .safe_divide(capx_1, rev_1)
    ci2 <- .safe_divide(capx_2, rev_2)
    cis <- c(ci0, ci1, ci2)
    if (!is.na(ci0) && sum(!is.na(cis)) >= 2) {
      out$investment <- .safe_divide(ci0, mean(cis, na.rm = TRUE))
    }
  }

  # -- Li (2011 RFS): capitalized R&D with 20% straight-line
  #    amortization, over current assets. R&D zero-if-na WITHIN an
  #    existing year (non-R&D firms get 0, unlike rd_me), but all five
  #    fiscal-year rows must exist (OpenAP: a missing lagged
  #    observation makes the signal missing). Small-cap-only predictor
  #    in OpenAP -- database quantity here (W4.21).
  lag_rows <- list(curr, prior, lag2, lag3, lag4)
  if (!any(vapply(lag_rows, is.null, logical(1)))) {
    w <- c(1, 0.8, 0.6, 0.4, 0.2)
    rd_stock <- sum(w * vapply(lag_rows, function(r) {
      .zero_if_na(.col(r, "rnd"))
    }, numeric(1)))
    out$rd_cap <- .safe_divide(rd_stock, at_t)
  }

  # -- Abarbanell & Bushee (1998): firm-level capex growth on the same
  #    AB98 base; finalized to ch_inv_ia = pchcapx - sector mean by
  #    industry_adjust_cross_section (the OpenAP capx <- delta-PPE
  #    imputation is not adopted; capex coverage is high in XBRL).
  out$ch_inv_ia <- .ab98_growth(capx_t, capx_1, capx_2)

  out
}


# =============================================================================
# SECTION 6g: WAVE 5 -- COMPOSITE SCORES
# =============================================================================
# 14 indicators from the OpenAP expansion (docs/research/09, Wave 5).
# Constructions verified against the OpenAP Stata/pyCode on 2026-07-03;
# canonical formulas, guards and deviations in RESEARCH_INDICATORS.md
# W5.1-W5.14. Traded-signal discretizations (MS clamp to {1..6}, OScore
# top-decile binary) are NOT applied -- continuous database quantities.
#
# Split of work: o_score, z_score, kz_index and the WW firm-level terms
# are per-ticker; MS sector-median binarization, the Herfindahl HHI and
# the WW industry sales-growth term need the cross-section and are
# finalized in industry_adjust_cross_section.

# Shared price-free prefixes of the two price-sensitive composites, used
# by BOTH the compute path and the timeseries stub extraction
# (.extract_stubs in timeseries_builder.R) -- one source of truth, the
# Wave 4 .level_components rule.
#   z_prefix:  1.2*WC/AT + 1.4*RE/AT + 3.3*EBIT/AT + REV/AT, where
#              EBIT = NI + interest-zero-if-na + tax (ni + xint + txt,
#              the OpenAP proxy; interest absence = no debt, tax
#              required). Completed by + 0.6*ME/LT.
#   kz_prefix: -1.002*(NI+DP)/PPENT + 0.283*(AT - CEQ)/AT
#              + 3.139*DEBT/(DEBT+CEQ) - 39.368*|DIV|/PPENT
#              - 1.315*CHE/PPENT, txdb omitted (not fetched, W5.11).
#              Completed by + 0.283*ME/AT.
.composite_prefixes <- function(row, lc = NULL) {
  if (is.null(lc)) lc <- .level_components(row)
  at  <- lc$at
  act <- .col(row, "current_assets")
  lct <- .col(row, "current_liabilities")

  z_prefix <- NA_real_
  re  <- .col(row, "retained_earnings")
  rev <- .col(row, "revenue")
  tax <- .col(row, "income_tax_expense")
  if (!is.na(at) && at > 0 && !is.na(act) && !is.na(lct) &&
      !is.na(re) && !is.na(lc$ni) && !is.na(tax) && !is.na(rev)) {
    ebit <- lc$ni + .zero_if_na(.col(row, "interest_expense")) + tax
    z_prefix <- (1.2 * (act - lct) + 1.4 * re + 3.3 * ebit + rev) / at
  }

  kz_prefix <- NA_real_
  ppent <- .col(row, "ppe_net")
  dv    <- abs(.zero_if_na(.col(row, "dividends_paid")))
  if (!is.na(ppent) && abs(ppent) >= 1 && !is.na(lc$cf_num) &&
      !is.na(lc$che) && !is.na(lc$td) && !is.na(lc$ceq) &&
      !is.na(at) && at > 0) {
    lever <- .safe_divide(lc$td, lc$td + lc$ceq)
    if (!is.na(lever)) {
      kz_prefix <- -1.002 * lc$cf_num / ppent +
        0.283 * (at - lc$ceq) / at +
        3.139 * lever -
        39.368 * dv / ppent -
        1.315 * lc$che / ppent
    }
  }

  list(z_prefix = z_prefix, kz_prefix = kz_prefix,
       at = at, lt = lc$lt, act = act, lct = lct, ni = lc$ni,
       cf_num = lc$cf_num)
}

# Ohlson (1980) O-Score as replicated by OpenAP via Dichev (1998) and
# Altman (1968) Z-Score (OpenAP Placebo). Deviations (W5.6-W5.8): FFO =
# operating cash flow (the post-1987 fopt fallback, our whole sample);
# the GNP deflator in log(AT) is omitted (constant within a snapshot,
# cross-sectionally exact); INTWO is the replicated sum-negative form,
# not loss-in-both-years; continuous scores stored, higher o_score =
# more distressed. ME = market cap on the filing date.
.compute_distress <- function(curr, prior, market_cap, pfx) {

  out <- list(o_score = NA_real_, z_score = NA_real_)

  at   <- pfx$at
  lt   <- pfx$lt
  act  <- pfx$act
  lct  <- pfx$lct
  ni   <- pfx$ni
  ni_p <- if (is.null(prior)) NA_real_ else .col(prior, "net_income")
  cfo  <- .col(curr, "operating_cashflow")

  if (!is.na(at) && at > 0 && !is.na(lt) && !is.na(act) && !is.na(lct) &&
      !is.na(ni) && !is.na(ni_p) && !is.na(cfo)) {
    chin  <- .safe_divide(ni - ni_p, abs(ni) + abs(ni_p))
    ffolt <- .safe_divide(cfo, lt)
    lctact <- .safe_divide(lct, act)
    if (!is.na(chin) && !is.na(ffolt) && !is.na(lctact)) {
      # log size in Compustat millions so raw levels match the
      # literature; the unit choice (like the omitted GNP deflator) is
      # a constant within a snapshot -- cross-sectionally exact
      out$o_score <- -1.32 - 0.407 * log(at / 1e6) + 6.03 * lt / at -
        1.43 * (act - lct) / at + 0.076 * lctact -
        1.72 * as.numeric(lt > at) - 2.37 * ni / at - 1.83 * ffolt +
        0.285 * as.numeric(ni + ni_p < 0) - 0.521 * chin
    }
  }

  # z_prefix carries the four accounting terms; the leverage term is
  # 0.6 * ME / total LIABILITIES (OpenAP uses lt, not debt -- W5.8).
  me_lt <- .safe_divide(market_cap, pfx$lt)
  if (!is.na(pfx$z_prefix) && !is.na(me_lt)) {
    out$z_score <- pfx$z_prefix + 0.6 * me_lt
  }

  out
}

# Kaplan-Zingales index (Lamont, Polk, Saa-Requejo 2001) and the
# firm-level part of the Whited-Wu (2006) index -- both OpenAP Placebos,
# stored for database completeness (W5.11-W5.13). kz_index needs ME (the
# Tobin-q term); ww is completed at the cross-section stage by
# + 0.102 * (industry sales growth)/4. The /4 factors approximate
# quarterly rates (WW was estimated on quarterly data). OpenAP's 1-month
# firm sales-growth lag on the monthly-expanded panel is a port artifact
# -- proper FY-over-FY growth is used instead (W5.12).
.compute_kz_ww <- function(curr, prior, market_cap, pfx) {

  out <- list(kz_index = NA_real_, ww_partial = NA_real_)

  me_at <- .safe_divide(market_cap, pfx$at)
  if (!is.na(pfx$kz_prefix) && !is.na(me_at)) {
    out$kz_index <- pfx$kz_prefix + 0.283 * me_at
  }

  at    <- pfx$at
  rev   <- .col(curr, "revenue")
  rev_p <- if (is.null(prior)) NA_real_ else .col(prior, "revenue")
  if (!is.na(at) && at > 0 && !is.na(pfx$cf_num)) {
    g_firm <- .safe_growth(rev, rev_p)
    if (!is.na(g_firm)) {
      dv <- .col(curr, "dividends_paid")
      divpos <- as.numeric(!is.na(dv) && abs(dv) > 0)
      dltt <- .zero_if_na(.col(curr, "long_term_debt"))
      # log size in Compustat millions (same rationale as o_score)
      out$ww_partial <- -0.091 * pfx$cf_num / (4 * at) - 0.062 * divpos +
        0.021 * dltt / at - 0.044 * log(at / 1e6) - 0.035 * g_firm / 4
    }
  }

  out
}

# Mohanram (2005) G-score ingredients. Level components (m1-m3, m6-m8)
# are FY filing-frequency values per repo convention; the volatility
# components (m4, m5) follow OpenAP's quarterly construction: sd of
# quarterly ROA (NI_q / AT_q, contemporaneous assets) and sd of the
# quarterly seasonal sales ratio (rev_q / rev_{q-4}) over a 4-year
# window ending at the target FY's balance-sheet date, minimum 6
# observations (OpenAP: 48-month window, min 18 monthly obs ~ 6
# quarters). R&D and advertising are zero-filled (absence = immaterial
# spend, the OpenAP fill list); the Stata missing=+infinity artifact is
# NOT replicated -- missing profitability inputs give NA components
# (W5.1-W5.5). Binarization vs sector medians happens at the
# cross-section stage; ms_accrual needs no median and is emitted as the
# final binary.
.compute_ms_ingredients <- function(fund_dt, curr, prior, panels = NULL) {

  out <- list(ms_roa = NA_real_, ms_cfroa = NA_real_, ms_accrual = NA_real_,
              ms_roa_vol = NA_real_, ms_rev_vol = NA_real_,
              ms_rd = NA_real_, ms_capex = NA_real_, ms_adv = NA_real_)

  at_t <- .col(curr, "total_assets")
  at_p <- if (is.null(prior)) NA_real_ else .col(prior, "total_assets")
  ni   <- .col(curr, "net_income")
  cfo  <- .col(curr, "operating_cashflow")

  if (!is.na(at_t) && !is.na(at_p)) {
    avgat <- (at_t + at_p) / 2
    out$ms_roa   <- .safe_divide(ni, avgat)
    out$ms_cfroa <- .safe_divide(cfo, avgat)
    out$ms_rd    <- .safe_divide(.zero_if_na(.col(curr, "rnd")), at_p)
    out$ms_capex <- .safe_divide(.col(curr, "capex"), at_p)
    out$ms_adv   <- .safe_divide(.zero_if_na(.col(curr, "advertising")), at_p)
  }
  if (!is.na(cfo) && !is.na(ni)) {
    out$ms_accrual <- as.numeric(cfo > ni)
  }

  # Volatility components on the label-free quarter panel (Wave 3
  # machinery, built once by the caller), anchored at the target FY's
  # period_end so the annual indicator moves with annual filings.
  fy_pe <- if ("period_end" %in% names(curr)) {
    as.Date(curr$period_end[1])
  } else as.Date(NA)
  if (is.na(fy_pe)) return(out)
  if (is.null(panels)) panels <- .quarter_panels(fund_dt)
  ni_q  <- panels$ni
  rev_q <- panels$rev
  at_q  <- panels$at

  # Trailing-16-quarter window: the oldest admitted quarter ends ~15
  # quarters (1369 d) before fy_pe; 1415 tolerates 52/53-week drift
  # while excluding the 17th quarter back (~1461 d).
  win_lo <- fy_pe - 1415L

  # sd of the values indexed by `idx` (in-window quarters), min 6 obs.
  win_sd <- function(vals) {
    if (sum(!is.na(vals)) >= 6) stats::sd(vals, na.rm = TRUE) else NA_real_
  }

  if (!is.null(ni_q) && !is.null(at_q)) {
    idx <- which(ni_q$qend > win_lo & ni_q$qend <= fy_pe)
    out$ms_roa_vol <- win_sd(vapply(idx, function(i) {
      k <- .near_idx(at_q$qend, ni_q$qend[i], 10)
      if (is.na(k)) NA_real_ else .safe_divide(ni_q$value[i], at_q$value[k])
    }, numeric(1)))
  }

  if (!is.null(rev_q)) {
    idx <- which(rev_q$qend > win_lo & rev_q$qend <= fy_pe)
    out$ms_rev_vol <- win_sd(vapply(idx, function(i) {
      j <- .q_lag_idx(rev_q$qend, i, 365, 35)
      if (is.na(j)) NA_real_ else {
        .safe_divide(rev_q$value[i], rev_q$value[j])
      }
    }, numeric(1)))
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
#' Sector-masked indicators (.SECTOR_NA_INDICATORS: the financial mask
#' plus the Wave 5 Utilities / Real Estate exclusions) are computed
#' excluding masked rows and then NA-masked on output.
#'
#' @param indicator_dt data.table. Rows = tickers, columns = indicator values.
#'   Must also have 'sector' column for sector mask handling.
#' @param clip Numeric length-2 or NULL. Post-standardization clip bounds.
#'   Defaults to c(-5, 5) -- see .robust_zscore docstring and
#'   tools/diag_zscore_tails.R for the tail-diagnostic evidence that
#'   drove the choice of 5 over 3.
#' @return data.table with z-scored values (same structure minus sector column).
zscore_cross_section <- function(indicator_dt, clip = c(-5, 5)) {

  if (is.null(indicator_dt) || nrow(indicator_dt) == 0) return(indicator_dt)

  dt <- copy(indicator_dt)
  sectors_vec <- dt[["sector"]]

  ind_cols <- setdiff(names(dt), c("ticker", "sector"))

  for (col_name in ind_cols) {
    vals <- dt[[col_name]]
    masked <- .sector_na_mask(col_name, sectors_vec)

    # For sector-masked indicators, exclude masked rows from the
    # winsorization and standardization sample, then NA-mask on output.
    if (any(masked)) {
      vals_in <- vals
      vals_in[masked] <- NA_real_
      z <- .robust_zscore(vals_in, clip = clip)
      z[masked] <- NA_real_
    } else {
      z <- .robust_zscore(vals, clip = clip)
    }
    dt[, (col_name) := z]
  }

  dt[, sector := NULL]

  .assert_output(dt, "zscore_cross_section", list(
    "is data.table"       = is.data.table,
    "row count preserved" = function(x) nrow(x) == nrow(indicator_dt)
  ))
  dt
}


#' Finalize cross-section-stage indicators on an assembled cross-section
#'
#' compute_ticker_indicators emits explicit ing_* ingredient columns for
#' the indicators in .CROSS_SECTION_FINALIZED (which are per-ticker NA);
#' this pass completes them against the assembled cross-section, per
#' snapshot:
#'   - ch_inv_ia: subtracts the equal-weighted within-sector mean of the
#'     AB98 capex growth (Wave 4; the one non-idempotent block, since
#'     its ingredient travels under the final name).
#'   - ms_* / ms_score: binarizes the ing_ms_* ingredients against
#'     within-sector medians (>= 3 non-NA contributors) -- ms_roa,
#'     ms_cfroa, ms_rd, ms_capex, ms_adv score 1 strictly ABOVE the
#'     median, ms_roa_vol and ms_rev_vol strictly BELOW; ms_accrual
#'     arrives as the final binary. ms_score = sum of the 8 components,
#'     NA if any NA (f_score convention).
#'   - herf: within-industry HHI of revenue shares, averaged over the
#'     current + up to 2 lagged fiscal years (.HERF_REV_COLS); a year
#'     contributes only with >= max(2, half the current-year count)
#'     contributing firms -- singletons and sparsely-lagged years NA.
#'   - ww_index: ing_ww_partial + 0.102 * (industry sales growth)/4,
#'     industry growth from aggregate revenue of the >= 2 firms with
#'     both current and lag-1 revenue.
#' Benchmark groups labeled "Unknown" or NA give NA. Each block runs
#' only when its ingredients are present, so a second pass over
#' finalized data is a no-op (except ch_inv_ia, above); the sector-NA
#' sweep at the end re-applies .SECTOR_NA_INDICATORS to every finalized
#' column. OpenAP benchmarks within 2/3/4-digit SIC over the full CRSP
#' universe; ours is the finviz sector (medians, demean) / industry
#' (herf, ww) map over ~500 large caps -- the large-cap-relative caveat
#' is documented (docs/research/09, Wave 5 risk 1).
#' Called by compute_cross_section and the timeseries cross-section
#' reader BEFORE z-scoring, so raw and z-scored outputs both carry the
#' finalized values. Per-ticker layers (fund/daily parquet) store NA
#' finals plus the ing_* ingredients by design. Without an industry
#' column, herf and ww_index degrade to NA. Hidden ingredient columns
#' (.CS_INGREDIENT_COLS) are dropped after use.
#'
#' @param dt data.table with a sector column (industry column needed for
#'   herf / ww_index) and indicator columns. Modified in place; also
#'   returned.
industry_adjust_cross_section <- function(dt) {
  if (is.null(dt) || nrow(dt) == 0 || !"sector" %in% names(dt)) return(dt)
  n_in <- nrow(dt)

  # -- ch_inv_ia: within-sector demean (Wave 4). Unknown/NA sector
  #    groups cannot anchor the demean -> NA (Wave 5 review fix; the
  #    ingredient-under-final-name design makes this block, unlike the
  #    ingredient-guarded ones below, non-idempotent by construction).
  for (cn in intersect(.SECTOR_DEMEAN_INDICATORS, names(dt))) {
    dt[, (cn) := {
      v <- .SD[[1]]
      if (!.benchmark_group_ok(.BY[[1]])) {
        rep(NA_real_, length(v))
      } else {
        m <- mean(v, na.rm = TRUE)
        if (is.finite(m)) v - m else v
      }
    }, by = sector, .SDcols = cn]
  }

  # -- Mohanram components: within-sector median binarization, one
  #    grouped pass over all ingredient columns. Runs only when the
  #    ingredients are present (fresh cross-section); on already-
  #    finalized data the ing_ columns are gone and the binaries are
  #    left untouched.
  ms_final <- names(.MS_INGREDIENT_OF)
  ms_ing   <- unname(.MS_INGREDIENT_OF)
  if (all(c(ms_ing, ms_final) %in% names(dt))) {
    dt[, (ms_final) := {
      grp_ok <- .benchmark_group_ok(.BY[[1]])
      lapply(seq_along(ms_ing), function(k) {
        v <- .SD[[ms_ing[k]]]
        n_ok <- sum(!is.na(v))
        if (!grp_ok || n_ok < 3L) return(rep(NA_real_, length(v)))
        med <- median(v, na.rm = TRUE)
        if (ms_final[k] %in% .MS_MEDIAN_LT) {
          as.numeric(v < med)
        } else {
          as.numeric(v > med)
        }
      })
    }, by = sector, .SDcols = ms_ing]

    # ms_score: sum of the 8 components, NA if any NA (f_score
    # convention) -- assembled only in the same pass that binarized.
    comp <- as.matrix(dt[, .MS_COMPONENTS, with = FALSE])
    dt[, ms_score := as.numeric(rowSums(comp))]
  }

  # -- herf: within-industry HHI of revenue shares, 3-year average.
  #    A lag year contributes only when its contributor count is at
  #    least 2 AND at least half the current year's (sparse lagged
  #    coverage -- pre-Wave-5 layers, recent listings -- would otherwise
  #    inflate concentration several-fold).
  has_ind <- "industry" %in% names(dt)
  if (has_ind && all(.HERF_REV_COLS %in% names(dt))) {
    dt[, herf := {
      if (!.benchmark_group_ok(.BY[[1]])) {
        rep(NA_real_, .N)
      } else {
        n_curr <- sum(!is.na(.SD[["revenue_raw"]]) &
                        .SD[["revenue_raw"]] > 0)
        hhi <- vapply(.HERF_REV_COLS, function(rc) {
          v <- .SD[[rc]]
          v <- v[!is.na(v) & v > 0]
          if (length(v) < max(2L, ceiling(n_curr / 2))) return(NA_real_)
          sum((v / sum(v))^2)
        }, numeric(1))
        h <- if (all(is.na(hhi))) NA_real_ else mean(hhi, na.rm = TRUE)
        rep(h, .N)
      }
    }, by = industry, .SDcols = .HERF_REV_COLS]
  } else if (!has_ind && "herf" %in% names(dt) &&
             any(.CS_INGREDIENT_COLS %in% names(dt))) {
    # Fresh cross-section without an industry map: cannot finalize.
    dt[, herf := NA_real_]
  }

  # -- ww_index: firm terms + 0.102 * (industry sales growth)/4.
  #    Industry growth needs >= 2 firms with both current and lag-1
  #    revenue (one firm's idiosyncratic growth must not set the
  #    industry term).
  ww_need <- c("ing_ww_partial", "revenue_raw", "ing_rev_lag1")
  if (has_ind && all(ww_need %in% names(dt))) {
    dt[, ww_index := {
      r0 <- .SD[["revenue_raw"]]
      r1 <- .SD[["ing_rev_lag1"]]
      both <- !is.na(r0) & !is.na(r1)
      g <- if (.benchmark_group_ok(.BY[[1]]) && sum(both) >= 2L &&
                 sum(r1[both]) > 0) {
        sum(r0[both]) / sum(r1[both]) - 1
      } else NA_real_
      .SD[["ing_ww_partial"]] + 0.102 * g / 4
    }, by = industry, .SDcols = ww_need]
  } else if (!has_ind && "ww_index" %in% names(dt) &&
             any(.CS_INGREDIENT_COLS %in% names(dt))) {
    dt[, ww_index := NA_real_]
  }

  # -- Sector NA sweep over every cross-section-finalized column
  #    (values created here bypass the compute-time per-ticker mask) --
  for (cn in intersect(.CROSS_SECTION_FINALIZED, names(dt))) {
    masked <- .sector_na_mask(cn, dt[["sector"]])
    if (any(masked)) dt[masked, (cn) := NA_real_]
  }

  # -- Drop hidden ingredient columns --
  drop_cols <- intersect(.CS_INGREDIENT_COLS, names(dt))
  if (length(drop_cols)) dt[, (drop_cols) := NULL]

  .assert_output(dt, "industry_adjust_cross_section", list(
    "is data.table"       = is.data.table,
    "row count preserved" = function(x) nrow(x) == n_in,
    "ingredients dropped" = function(x) {
      !any(.CS_INGREDIENT_COLS %in% names(x))
    }
  ))
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

  # All-NA fallback (canonical indicators + hidden cross-section ingredients)
  out_names <- c(.INDICATOR_NAMES, .CS_INGREDIENT_COLS)
  all_na <- setNames(rep(NA_real_, length(out_names)), out_names)

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
    qp   <- .quarter_panels(fund_dt)
    sur  <- .compute_surprise(fund_dt, target_fy, splits, panels = qp)
    # lag3/lag4 feed only grcapx3y and rd_cap, both of which also need
    # the prior row -- skip the fund_dt rescans when prior is absent.
    lag2 <- .lag_fy_row(fund_dt, target_fy, lag_years = 2L)
    lag3 <- if (is.null(prior)) NULL else {
      .lag_fy_row(fund_dt, target_fy, lag_years = 3L)
    }
    lag4 <- if (is.null(prior)) NULL else {
      .lag_fy_row(fund_dt, target_fy, lag_years = 4L)
    }
    lc_curr <- .level_components(curr)
    lvl  <- .compute_levels(curr, val$market_cap,
                            fy_start = if (!is.null(prior)) {
                              prior$period_end[1] + 1L
                            } else NULL,
                            lc = lc_curr)
    invt <- .compute_investment(curr, prior, lag2, lag3, lag4)
    pfx  <- .composite_prefixes(curr, lc = lc_curr)
    dis  <- .compute_distress(curr, prior, val$market_cap, pfx)
    kzww <- .compute_kz_ww(curr, prior, val$market_cap, pfx)
    msi  <- .compute_ms_ingredients(fund_dt, curr, prior, panels = qp)

    # Wave 1 follow-up: when a standalone-quarter revenue panel exists,
    # its period_end-matched QoQ replaces the label-based selection above
    # (labels mis-pair quarters once a later filing re-reports them).
    if (isTRUE(sur$has_rev_panel)) {
      grow$revenue_growth_qoq <- sur$rev_qoq
    }

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
      net_payout_yield        = fin$net_payout_yield,
      # Wave 3: quarterly seasonal-surprise family (6)
      ch_tax               = sur$ch_tax,
      num_earn_increase    = sur$num_earn_increase,
      revenue_surprise     = sur$revenue_surprise,
      earnings_surprise    = sur$earnings_surprise,
      earnings_consistency = sur$earnings_consistency,
      roaq                 = sur$roaq,
      # Wave 4: levels family (19)
      rd_me              = lvl$rd_me,
      ebm                = lvl$ebm,
      bpebm              = lvl$bpebm,
      cf_me              = lvl$cf_me,
      cfp                = lvl$cfp,
      net_debt_price     = lvl$net_debt_price,
      tang               = lvl$tang,
      tax_to_book        = lvl$tax_to_book,
      effective_tax_rate = lvl$effective_tax_rate,
      am                 = lvl$am,
      book_leverage      = lvl$book_leverage,
      leverage_mkt       = lvl$leverage_mkt,
      cash_prod          = lvl$cash_prod,
      oper_prof          = lvl$oper_prof,
      cash_assets        = lvl$cash_assets,
      op_leverage        = lvl$op_leverage,
      payout_yield       = lvl$payout_yield,
      salecash           = lvl$salecash,
      depr_rate          = lvl$depr_rate,
      # Wave 4: investment family (10; ch_inv_ia holds the firm-level
      # ingredient until industry_adjust_cross_section finalizes it)
      pchdepr            = invt$pchdepr,
      grcapx             = invt$grcapx,
      grcapx3y           = invt$grcapx3y,
      pct_tot_acc        = invt$pct_tot_acc,
      ch_asset_turnover  = invt$ch_asset_turnover,
      gr_sale_to_gr_inv  = invt$gr_sale_to_gr_inv,
      surprise_rd        = invt$surprise_rd,
      investment         = invt$investment,
      rd_cap             = invt$rd_cap,
      ch_inv_ia          = invt$ch_inv_ia,
      # Wave 5: mohanram family. The median-binarized components and
      # ms_score are cross-section-only: per-ticker they are NA and
      # their raw ingredients travel in the ing_ms_* columns below.
      # ms_accrual (CFO > NI) needs no cross-section and is final here.
      ms_roa     = NA_real_,
      ms_cfroa   = NA_real_,
      ms_accrual = msi$ms_accrual,
      ms_roa_vol = NA_real_,
      ms_rev_vol = NA_real_,
      ms_rd      = NA_real_,
      ms_capex   = NA_real_,
      ms_adv     = NA_real_,
      ms_score   = NA_real_,
      # Wave 5: distress family
      o_score = dis$o_score,
      z_score = dis$z_score,
      # Wave 5: structure family (herf and the ww_index completion are
      # cross-section-only)
      herf     = NA_real_,
      kz_index = kzww$kz_index,
      ww_index = NA_real_,
      # Hidden ingredients consumed by the cross-section finalize pass
      # (.CS_INGREDIENT_COLS order: MS levels/volatilities, WW firm
      # terms, lagged revenues for herf / industry growth)
      ing_ms_roa     = msi$ms_roa,
      ing_ms_cfroa   = msi$ms_cfroa,
      ing_ms_rd      = msi$ms_rd,
      ing_ms_capex   = msi$ms_capex,
      ing_ms_adv     = msi$ms_adv,
      ing_ms_roa_vol = msi$ms_roa_vol,
      ing_ms_rev_vol = msi$ms_rev_vol,
      ing_ww_partial = kzww$ww_partial,
      ing_rev_lag1 = if (is.null(prior)) NA_real_ else {
        .col(prior, "revenue")
      },
      ing_rev_lag2 = if (is.null(lag2)) NA_real_ else {
        .col(lag2, "revenue")
      }
    )

    # Sector NA masks (Financial and, since Wave 5, Utilities and Real
    # Estate for the Ohlson/Altman family)
    if (!is.na(sector) && sector %in% names(.SECTOR_NA_INDICATORS)) {
      out[.SECTOR_NA_INDICATORS[[sector]]] <- NA_real_
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
    "correct length"      = function(x) {
      length(x) == length(.INDICATOR_NAMES) + length(.CS_INGREDIENT_COLS)
    }
  ))

  result
}


#' Compute cross-sectional indicator matrix with z-scoring
#'
#' @param indicator_list Named list of named numeric vectors (one per ticker),
#'   as returned by compute_ticker_indicators().
#' @param tickers Character vector. Ticker symbols (same order as indicator_list).
#' @param sectors Character vector. Sector per ticker (same order).
#' @param industries Character vector or NULL. Finviz industry per ticker
#'   (same order). Needed by the herf / ww_index finalization; when NULL
#'   those indicators degrade to NA.
#' @return List with $raw (data.table) and $zscored (data.table).
#'   Both have ticker column + indicator columns.
compute_cross_section <- function(indicator_list, tickers, sectors,
                                  industries = NULL,
                                  clip = c(-5, 5)) {

  stopifnot(length(indicator_list) == length(tickers))
  stopifnot(length(indicator_list) == length(sectors))
  stopifnot(is.null(industries) || length(industries) == length(tickers))

  # Stack named vectors into data.table
  raw_dt <- rbindlist(lapply(indicator_list, as.list), fill = TRUE)
  raw_dt[, ticker := tickers]
  raw_dt[, sector := sectors]
  if (!is.null(industries)) raw_dt[, industry := industries]

  # Reorder columns: ticker first, then indicators (hidden ingredient
  # columns, if present, trail and are consumed by the finalize pass)
  setcolorder(raw_dt, c("ticker", "sector", .INDICATOR_NAMES))

  # Finalize cross-section-stage indicators (ch_inv_ia demean, Mohanram
  # binarization, herf, ww_index) before z-scoring so both raw and
  # z-scored outputs carry the finalized values.
  industry_adjust_cross_section(raw_dt)
  if (!is.null(industries)) raw_dt[, industry := NULL]

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
