# ============================================================================
# indicator_compute.R  --  Fundamental Indicator Computation (Module 4)
# ============================================================================
# Pure computation layer: no I/O. Receives data.tables and price scalars,
# returns named vectors and data.tables.
#
# Computes 57 output fields from EDGAR XBRL data + market prices:
#   - 36 baseline indicators (valuation, profitability, growth, leverage,
#     efficiency, cash flow quality, shareholder return, size)
#   - 6 Tier 1 research indicators (GP/A, Asset Growth, Sloan Accrual,
#     Pct Accruals, NOA, Piotroski F-Score with 9 binary components)
#   - 6 Tier 2 research indicators (Cash-Based OP, FCF Stability,
#     SGA Efficiency, CAPEX/DA, DSO Change, Inventory/Sales Change)
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
  "capex_depreciation", "inventory_sales_change"
)

# Canonical indicator names in output order (57 entries):
# 36 baseline + 5 tier1 + 10 piotroski (9 components + composite) + 6 tier2
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
  # Size (3)
  "market_cap", "enterprise_value", "revenue_raw",
  # Tier 1 Research (6 + 10 Piotroski)
  "gpa", "asset_growth", "sloan_accrual", "pct_accruals",
  "net_operating_assets",
  "f_roa", "f_droa", "f_cfo", "f_accrual",
  "f_dlever", "f_dliquid", "f_eq_off",
  "f_dmargin", "f_dturn", "f_score",
  # Tier 2 Research (6)
  "cash_based_op", "fcf_stability", "sga_efficiency",
  "capex_depreciation", "dso_change", "inventory_sales_change"
)


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
.winsorize <- function(x, probs = c(0.01, 0.99)) {
  if (all(is.na(x))) return(x)
  q <- quantile(x, probs, na.rm = TRUE)
  pmin(pmax(x, q[1]), q[2])
}


# -- Safe column accessor: returns NA if column missing or value is NA --
.col <- function(row, col_name) {
  if (!(col_name %in% names(row))) return(NA_real_)
  val <- row[[col_name]]
  if (length(val) == 0) return(NA_real_)
  as.numeric(val[1])
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

  # For each (fiscal_year, period_type, concept), keep the row with the latest
  # filed date (most up-to-date value for that period)
  setorder(dt, fiscal_year, period_type, concept, -filed)
  dt <- dt[!duplicated(dt[, .(fiscal_year, period_type, concept)])]

  # Pivot: one row per (fiscal_year, period_type), one column per concept
  # Keep the latest period_end and filed date per group for metadata
  meta <- dt[, .(period_end = max(period_end, na.rm = TRUE),
                 filed = max(filed, na.rm = TRUE)),
             by = .(fiscal_year, period_type)]

  wide <- dcast(dt, fiscal_year + period_type ~ concept,
                value.var = "value", fun.aggregate = function(x) x[1])

  # Merge metadata back
  wide <- merge(wide, meta, by = c("fiscal_year", "period_type"), all.x = TRUE)

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

  # Use fcoalesce for NA -> 0 where appropriate
  .co <- function(col_name) {
    if (col_name %in% names(wide_dt)) {
      nafill(wide_dt[[col_name]], fill = 0)
    } else {
      rep(0, nrow(wide_dt))
    }
  }

  # Gross profit
  if (all(c("revenue", "cogs") %in% names(wide_dt))) {
    wide_dt[, gross_profit := revenue - cogs]
  }

  # EBITDA = operating income + D&A
  if ("operating_income" %in% names(wide_dt)) {
    wide_dt[, ebitda := operating_income + .co("depreciation")]
  }

  # Free cash flow = operating CF - capex
  if ("operating_cashflow" %in% names(wide_dt)) {
    wide_dt[, fcf := operating_cashflow - .co("capex")]
  }

  # Total debt = LT debt + ST debt (missing = 0)
  wide_dt[, total_debt := .co("long_term_debt") + .co("short_term_debt")]

  # Net debt = total debt - cash
  wide_dt[, net_debt := total_debt - .co("cash")]

  wide_dt
}


# =============================================================================
# SECTION 4: BASELINE INDICATORS
# =============================================================================

# -- 4.1 Valuation (8 indicators) --
.compute_valuation <- function(curr, price, shares) {

  market_cap <- if (!is.na(price) && !is.na(shares)) price * shares else NA_real_
  # Use net_debt (from .derive_quantities) which handles NA cash -> 0
  ev <- if (!is.na(market_cap)) {
    nd <- .col(curr, "net_debt")
    if (!is.na(nd)) market_cap + nd else market_cap + .col(curr, "total_debt")
  } else NA_real_

  eps <- .col(curr, "eps_diluted")
  rev <- .col(curr, "revenue")
  equity <- .col(curr, "stockholders_equity")
  fcf_val <- .col(curr, "fcf")
  ebitda_val <- .col(curr, "ebitda")

  list(
    pe_trailing    = .safe_divide(price, eps, min_abs_denom = 0.01),
    pb             = if (!is.na(equity) && equity > 0) .safe_divide(market_cap, equity) else NA_real_,
    ps             = .safe_divide(market_cap, rev),
    pfcf           = if (!is.na(fcf_val) && fcf_val > 0) .safe_divide(market_cap, fcf_val) else NA_real_,
    ev_ebitda      = if (!is.na(ebitda_val) && ebitda_val > 0) .safe_divide(ev, ebitda_val) else NA_real_,
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

  # Invested capital = equity + debt - cash
  ic <- if (!is.na(equity) && !is.na(td)) {
    equity + td - if (is.na(cash_v)) 0 else cash_v
  } else NA_real_

  list(
    gross_margin     = .safe_divide(gp, rev),
    operating_margin = .safe_divide(opinc, rev),
    net_margin       = .safe_divide(ni, rev),
    roe              = if (!is.na(equity) && equity > 0) .safe_divide(ni, equity) else NA_real_,
    roa              = .safe_divide(ni, assets),
    roic             = if (!is.na(ic) && ic > 0) .safe_divide(ni, ic) else NA_real_
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

  list(
    debt_equity       = if (!is.na(equity) && equity > 0) .safe_divide(td, equity) else NA_real_,
    net_debt_ebitda   = if (!is.na(ebitda) && ebitda > 0) .safe_divide(nd, ebitda) else NA_real_,
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

  list(
    dividend_yield = .safe_divide(abs_div, market_cap),
    payout_ratio   = if (!is.na(ni) && ni > 0) .safe_divide(abs_div, ni) else NA_real_,
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

  # ROA = NI / Assets
  roa   <- .safe_divide(ni, assets)
  p_roa <- .safe_divide(p_ni, p_assets)

  # Profitability signals
  f_roa     <- if (!is.na(roa) && roa > 0) 1L else 0L
  f_droa    <- if (!is.na(roa) && !is.na(p_roa) && roa > p_roa) 1L else 0L
  f_cfo     <- if (!is.na(cfo) && !is.na(assets) && assets > 0 && cfo / assets > 0) 1L else 0L
  f_accrual <- if (!is.na(cfo) && !is.na(ni) && cfo > ni) 1L else 0L

  # Leverage / Liquidity signals
  lt_ratio   <- .safe_divide(ltd, assets)
  p_lt_ratio <- .safe_divide(p_ltd, p_assets)
  f_dlever   <- if (!is.na(lt_ratio) && !is.na(p_lt_ratio) && lt_ratio < p_lt_ratio) 1L else 0L

  cr   <- .safe_divide(ca, cl)
  p_cr <- .safe_divide(p_ca, p_cl)
  f_dliquid <- if (!is.na(cr) && !is.na(p_cr) && cr > p_cr) 1L else 0L

  f_eq_off <- if (!is.na(shares) && !is.na(p_shares) && shares <= p_shares) 1L else 0L

  # Operating efficiency signals
  gm   <- .safe_divide(gp, rev)
  p_gm <- .safe_divide(p_gp, p_rev)
  f_dmargin <- if (!is.na(gm) && !is.na(p_gm) && gm > p_gm) 1L else 0L

  at   <- .safe_divide(rev, assets)
  p_at <- .safe_divide(p_rev, p_assets)
  f_dturn <- if (!is.na(at) && !is.na(p_at) && at > p_at) 1L else 0L

  # Composite
  components <- c(f_roa, f_droa, f_cfo, f_accrual, f_dlever,
                  f_dliquid, f_eq_off, f_dmargin, f_dturn)
  f_score <- sum(components)

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

  # -- FCF Stability: SD(CFO/Assets) over trailing quarters --
  fcf_stability <- NA_real_
  if (!is.null(quarterly_hist) && nrow(quarterly_hist) >= 8) {
    cfo_vals <- quarterly_hist[["operating_cashflow"]]
    asset_vals <- quarterly_hist[["total_assets"]]
    if (!is.null(cfo_vals) && !is.null(asset_vals)) {
      ratio <- cfo_vals / asset_vals
      valid <- !is.na(ratio)
      if (sum(valid) >= 8) {
        fcf_stability <- sd(ratio[valid])
      }
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
# SECTION 7: CROSS-SECTIONAL Z-SCORING
# =============================================================================

#' Z-score indicator values cross-sectionally
#'
#' @param indicator_dt data.table. Rows = tickers, columns = indicator values.
#'   Must also have 'sector' column for financial sector handling.
#' @return data.table with z-scored values (same structure minus sector column).
zscore_cross_section <- function(indicator_dt) {

  if (is.null(indicator_dt) || nrow(indicator_dt) == 0) return(indicator_dt)

  dt <- copy(indicator_dt)
  is_financial <- dt[["sector"]] %in% "Financial"

  # Z-score each indicator column
  ind_cols <- setdiff(names(dt), c("ticker", "sector"))

  for (col_name in ind_cols) {
    vals <- dt[[col_name]]

    if (col_name %in% .FINANCIAL_NA_INDICATORS) {
      # For financial-NA indicators: compute z-scores excluding financials
      use <- !is_financial & !is.na(vals)
    } else {
      use <- !is.na(vals)
    }

    if (sum(use) < 3) {
      # Not enough data for meaningful z-score
      dt[, (col_name) := NA_real_]
      next
    }

    mu <- mean(vals[use])
    s  <- sd(vals[use])

    if (is.na(s) || s < 1e-12) {
      dt[, (col_name) := NA_real_]
      next
    }

    z <- (vals - mu) / s
    # Winsorize z-scores at [-3, 3]
    z <- pmin(pmax(z, -3), 3)
    # Keep NA for original NAs
    z[is.na(vals)] <- NA_real_
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
#' @return Named numeric vector of indicator values. NA for indicators that
#'   cannot be computed. Returns all-NA vector on total failure.
compute_ticker_indicators <- function(fund_dt, price_on_filed, sector,
                                      target_fy = NULL,
                                      target_period = "FY") {

  # All-NA fallback
  all_na <- setNames(rep(NA_real_, length(.INDICATOR_NAMES)), .INDICATOR_NAMES)

  result <- tryCatch({

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

    # Prior fiscal year for growth / change indicators
    prior_fy <- target_fy - 1L
    prior_idx <- which(fy_rows$fiscal_year == prior_fy)
    prior <- if (length(prior_idx) > 0) fy_rows[prior_idx[1]] else NULL

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

    # Quarterly history for FCF Stability (up to 16 quarters)
    quarterly_hist <- NULL
    if (nrow(q_rows) >= 8) {
      quarterly_hist <- tail(q_rows, 16)
    }

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
      # Size (3)
      market_cap       = val$market_cap,
      enterprise_value = val$enterprise_value,
      revenue_raw      = .col(curr, "revenue"),
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
      inventory_sales_change = t2$inventory_sales_change
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
compute_cross_section <- function(indicator_list, tickers, sectors) {

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
  zscored_dt <- zscore_cross_section(zscore_input)
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
