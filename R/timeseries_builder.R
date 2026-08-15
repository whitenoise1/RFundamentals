# ============================================================================
# timeseries_builder.R  --  Daily Time Series Builder (Module 7)
# ============================================================================
# Builds and maintains per-ticker daily time series of fundamental indicators.
#
# Architecture (three-layer storage per ticker):
#   1. Fundamentals layer ({ticker}_fund.parquet): sparse, one row per fiscal
#      year. Stores fundamental-only indicators and accounting stubs for
#      price-sensitive recomputation. Updates only on new filings.
#      With quarterly = TRUE the layer is mixed-cadence: additional rows
#      per (fiscal_year, Q1-Q3), marked by a `quarter` column ("FY" on
#      annual rows), carrying the natively quarterly indicators
#      (.Q_NATIVE_INDICATORS), the TTM-quarterly indicators
#      (.Q_TTM_INDICATORS, annual ratio formulas recomputed on
#      trailing-twelve-month flows), instant stock stubs, TTM flow
#      stubs, and standalone-quarter q3m_<concept> ingredient columns
#      (.Q3M_CONCEPTS; also stamped on FY rows of quarterly builds).
#   2. Per-share layer ({ticker}_pershare.parquet): sparse, one row per
#      FILING (10-Q cadence). The authoritative as-of-filing share count
#      (dei cover / Wave M synthetic), stamped by filed date. Fixes D1:
#      the fund layer was annual-only, so daily market cap was computed
#      from a share count up to ~1yr stale and never split-adjusted.
#   3. Daily layer ({ticker}_daily.parquet): dense, one row per trading day.
#      Stores every canonical indicator (get_indicator_names()) plus the
#      hidden cross-section ingredient columns. Price-sensitive indicators
#      recomputed daily from stubs + closing price -- per-share stubs
#      (shares, eps) mapped onto each day's as-traded split basis via
#      .split_factor, so mc = price x shares is exact across splits;
#      fundamental-only carried forward; cross-section-finalized columns
#      (ms_*, herf, ww_index, ch_inv_ia) are per-ticker NA/ingredients
#      until a cross-section reader finalizes them. On mixed-cadence
#      fund layers the daily rows also carry pass-B provenance stamps
#      (period_end_q, quarter_q) and the layer's q3m_* columns.
#
# Storage:
#   cache/timeseries/{ticker}_fund.parquet
#   cache/timeseries/{ticker}_pershare.parquet
#   cache/timeseries/{ticker}_daily.parquet
#
# Public API:
#   build_ticker_fundamentals(ticker, cik, sector, ...)
#   build_ticker_pershare(ticker, cik, ...)
#   update_ticker_daily(ticker, through_date, ...)
#   build_timeseries(start_date, end_date, ...)   -- historical build
#   update_all_daily(through_date, ...)            -- daily incremental
#   load_daily_cross_section(target_date, ...)     -- cross-section reader
#   load_ticker_timeseries(ticker, ...)            -- per-ticker reader
#
# Dependencies: data.table, arrow, quantmod, xts, zoo
# Requires: indicator_compute.R, fundamental_fetcher.R, pit_assembler.R
#           (already sourced)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(quantmod)
  library(xts)
  library(zoo)
})


# Google Drive sync conflict-copy guard (RF-3). Guarded so co-sourcing with
# fundamental_fetcher.R (which also defines it) is a no-op. Drops "foo (1).parquet"
# copies that broad "\\.parquet$" globs would otherwise read as real data.
if (!exists(".drop_drive_conflict_paths", mode = "function")) {
  .drop_drive_conflict_paths <- function(paths) {
    if (!length(paths)) return(paths)
    paths[!grepl(" \\([0-9]{1,3}\\)\\.[^.]+$", basename(paths))]
  }
}


# =============================================================================
# SECTION 1: CONSTANTS
# =============================================================================

.TS_DIR <- "cache/timeseries"

# Price-sensitive indicators: change with market price
.PRICE_SENSITIVE_INDICATORS <- c(
  "pe_trailing", "peg", "pb", "ps", "pfcf",
  "ev_ebitda", "ev_revenue", "earnings_yield",
  "dividend_yield", "buyback_yield",
  "market_cap", "enterprise_value",
  "net_payout_yield",
  # Wave 4 ME-scaled levels
  "rd_me", "ebm", "bpebm", "cf_me", "cfp", "net_debt_price",
  "am", "leverage_mkt", "cash_prod", "payout_yield",
  # Wave 5 ME-completed composites (Altman leverage term, KZ Tobin-q)
  "z_score", "kz_index"
)

# Accounting stubs: raw items stored in the fundamentals layer that enable
# recomputation of price-sensitive indicators without re-reading XBRL data.
# Prefixed with "stub_" to avoid column name collisions with indicators.
# Wave 4 stubs are pre-resolved numerators/components (identity fallbacks
# and zero-if-na policies applied at extraction, mirroring .compute_levels):
#   stub_rnd      R&D expense (NA = non-reporter, rd_me stays NA)
#   stub_assets   total assets
#   stub_liabilities  total liabilities (Wave 1 identity fallback applied)
#   stub_cf_num   NI + depreciation-zero-if-na (LSV cash flow numerator)
#   stub_cfo      operating cash flow
#   stub_che      cash + st_investments (Compustat CHE equivalent)
#   stub_net_cash che - total_debt (Penman EBM net cash)
#   stub_ndp_num  FINL - che (net_debt_price numerator; NA for financials)
# Wave 5 stubs are the price-free composite prefixes from
# .composite_prefixes (indicator_compute.R Section 6g):
#   stub_z_prefix  Altman terms ex the 0.6*ME/LT leverage term (NA for
#                  sectors whose mask covers z_score)
#   stub_kz_prefix KZ terms ex the 0.283*ME/AT Tobin-q piece
.STUB_NAMES <- c(
  "stub_shares", "stub_eps", "stub_equity",
  "stub_revenue", "stub_fcf", "stub_ebitda",
  "stub_total_debt", "stub_net_debt", "stub_cash",
  "stub_dividends", "stub_buybacks", "stub_equity_issuance",
  "stub_rnd", "stub_assets", "stub_liabilities", "stub_cf_num",
  "stub_cfo", "stub_che", "stub_net_cash", "stub_ndp_num",
  "stub_z_prefix", "stub_kz_prefix"
)

# Fundamental-only indicators: change only when a new filing appears
.FUNDAMENTAL_INDICATORS <- setdiff(get_indicator_names(), .PRICE_SENSITIVE_INDICATORS)

# Stub feeding each sector-masked price-sensitive indicator: when a
# ticker's sector NA-masks the indicator, its stub is NA'd at fund-layer
# build so the daily recompute cannot resurrect the value.
.MASKED_STUB_OF <- c(
  net_debt_price = "stub_ndp_num",
  z_score        = "stub_z_prefix"
)

# Quarterly-kept indicator columns (mixed-cadence extension), split by
# provenance. The split exists because of a wrong-basis trap: the
# compute_ticker_indicators output at target_period = "Q*" evaluates
# every annual-bound formula on the wide pivot's quarterly row, where
# 10-Q cash-flow items are YTD-CUMULATIVE -- a flow-over-stock ratio
# copied from that output would mix nine months of flows with a
# quarter-end instant, a plausible wrong number. So:
#
#   .Q_NATIVE_INDICATORS: computed INSIDE compute_ticker_indicators from
#     the label-free standalone-quarter panels (Wave 3 machinery,
#     indicator_compute.R Section 6d), natively quarterly, and COPIED
#     from its output on a quarterly row.
#   .Q_TTM_INDICATORS: annual ratio formulas re-based to trailing-
#     twelve-month flows (.q_ttm_bundle) against as-of instants,
#     computed by .compute_ttm_indicators_q and NEVER copied from the
#     compute_ticker_indicators output (see the trap above).
#
# Every indicator outside the union is an annual-bound formula with no
# approved TTM construction (lagged rows, deltas, YoY terms) and stays
# NA on quarterly rows -- absent, never plausible. In the daily layer
# the union (.Q_KEPT_INDICATORS) maps from pass B (all fund rows,
# latest period wins); everything else maps from pass A (annual rows
# only).
.Q_NATIVE_INDICATORS <- c(
  "roaq", "revenue_surprise", "earnings_surprise", "revenue_growth_qoq",
  "ch_tax", "num_earn_increase", "fcf_stability",
  "ms_roa_vol", "ms_rev_vol", "ing_ms_roa_vol", "ing_ms_rev_vol"
)

.Q_TTM_INDICATORS <- c(
  "gross_margin", "operating_margin", "net_margin", "roe", "roa", "roic",
  "asset_turnover", "inventory_turnover", "receivables_turnover",
  "fcf_ni", "opcf_ni", "capex_revenue", "payout_ratio",
  "debt_equity", "net_debt_ebitda", "interest_coverage",
  "current_ratio", "quick_ratio",
  "gpa", "pct_accruals", "effective_tax_rate",
  "oper_prof", "book_leverage", "cash_assets", "salecash", "depr_rate"
)

.Q_KEPT_INDICATORS <- c(.Q_NATIVE_INDICATORS, .Q_TTM_INDICATORS)

# Standalone-quarter flow concepts served on quarterly-built fund rows
# as q3m_<concept> columns: the anchor quarter's OWN 3-month value from
# the Wave 3 standalone-quarter machinery (.flow_quarter_series), never
# the wide pivot's duration-ambiguous quarterly cell. On FY rows the
# anchor is the standalone Q4 recovered from the FY - 3Q pairing. Raw
# ingredients for downstream consumers, not indicators; the daily
# mapper serves them from pass B on mixed-cadence layers only.
.Q3M_CONCEPTS <- c(
  "revenue", "cogs", "sga", "net_income", "operating_cashflow", "capex",
  "operating_income", "depreciation", "dividends_paid", "buybacks",
  "income_tax_expense", "pretax_income", "interest_expense", "rnd"
)


# =============================================================================
# SECTION 2: PRIVATE HELPERS
# =============================================================================

.assert_output_ts <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}


# Stub column at fund_idx, or NA vector when the fund layer predates the
# stub (same degrade-to-NA convention as the Wave 2 iss fallback).
.stub_or_na <- function(fund_dt, stub, fund_idx) {
  if (stub %in% names(fund_dt)) {
    as.numeric(fund_dt[[stub]][fund_idx])
  } else NA_real_
}


# Filing forms whose share counts feed the per-share layer. 8-K
# comparatives and registration statements are excluded: their counts
# are not the cover-page as-of-filing number.
.PERSHARE_FORMS <- c("10-Q", "10-Q/A", "10-K", "10-K/A",
                     "20-F", "20-F/A", "40-F", "40-F/A")

#' Extract the per-filing share-count series from raw XBRL vintages
#'
#' One row per filed date: the authoritative as-of-filing outstanding
#' count. Within a filing, tags rank per .SHARES_TAG_PREF (Wave M
#' synthetic whole-company count, then the DEI cover instant, then
#' balance-sheet counts -- indicator_compute.R Section 6b rationale),
#' and within the winning tag the LATEST period_end wins: a filing's
#' cover count is dated days before filing, restated comparatives are
#' dated quarters earlier. Values are as-filed, so their split basis is
#' the split state at the filed date (same GAAP-restatement convention
#' as .split_factor documents for EPS).
#'
#' @param fund_v data.table. Raw vintage rows (get_fundamentals vintages=TRUE).
#' @return data.table(filed_date, period_end, shares) ordered by filed_date,
#'   or NULL when no usable share rows exist.
.extract_pershare_series <- function(fund_v) {
  if (is.null(fund_v) || nrow(fund_v) == 0) return(NULL)
  sh <- fund_v[concept == "shares_outstanding" & !is.na(value) & value > 0 &
                 !is.na(filed) & !is.na(period_end) &
                 form %in% .PERSHARE_FORMS]
  if (nrow(sh) == 0) return(NULL)

  sh <- copy(sh)
  sh[, filed := as.Date(filed)]
  sh[, period_end := as.Date(period_end)]

  rows <- vector("list", length(unique(sh$filed)))
  n <- 0L
  for (fd in sort(unique(sh$filed))) {
    grp <- .prefer_share_tag(sh[filed == fd])
    r <- grp[order(-period_end)][1]
    n <- n + 1L
    rows[[n]] <- data.table(filed_date = as.Date(fd, origin = "1970-01-01"),
                            period_end = r$period_end,
                            shares     = as.numeric(r$value))
  }
  out <- rbindlist(rows[seq_len(n)])
  setorder(out, filed_date)
  out
}


#' Extract accounting stubs from pivoted wide-format data for a fiscal year
#'
#' @param wide_dt data.table. Output of pivot_fundamentals + .derive_quantities.
#' @param target_fy Integer. Target fiscal year.
#' @return Named list of stub values, or NULL.
.extract_stubs <- function(wide_dt, target_fy) {

  fy_rows <- wide_dt[period_type == "FY"]
  curr_idx <- which(fy_rows$fiscal_year == target_fy)
  if (length(curr_idx) == 0) return(NULL)
  curr <- fy_rows[curr_idx[1]]

  # Wave 4 components come from .level_components (indicator_compute.R
  # Section 6e), Wave 5 prefixes from .composite_prefixes (Section 6g)
  # -- the SAME resolvers the compute path uses, so the filing-date and
  # daily-recompute values cannot drift.
  lc  <- .level_components(curr)
  pfx <- .composite_prefixes(curr, lc = lc)

  list(
    stub_shares     = .col(curr, "shares_outstanding"),
    stub_eps        = .col(curr, "eps_diluted"),
    stub_equity     = lc$ceq,
    stub_revenue    = .col(curr, "revenue"),
    stub_fcf        = .col(curr, "fcf"),
    stub_ebitda     = .col(curr, "ebitda"),
    stub_total_debt = lc$td,
    stub_net_debt   = .col(curr, "net_debt"),
    stub_cash       = .col(curr, "cash"),
    stub_dividends  = .col(curr, "dividends_paid"),
    stub_buybacks   = .col(curr, "buybacks"),
    stub_equity_issuance = .col(curr, "equity_issuance"),
    stub_rnd        = .col(curr, "rnd"),
    stub_assets     = lc$at,
    stub_liabilities = lc$lt,
    stub_cf_num     = lc$cf_num,
    stub_cfo        = .col(curr, "operating_cashflow"),
    stub_che        = lc$che,
    stub_net_cash   = lc$ncash,
    stub_ndp_num    = lc$ndp_num,
    stub_z_prefix   = pfx$z_prefix,
    stub_kz_prefix  = pfx$kz_prefix
  )
}


# Trailing-twelve-month value of one flow concept under a PIT as-of view:
# the sum of the 4 standalone quarters ending at q_pe, built from the
# Wave 3 standalone-quarter machinery (.flow_quarter_series de-cumulates
# YTD 10-Q cash-flow items with duration guards). NEVER the wide pivot's
# quarterly rows: those are duration-ambiguous (10-Q cash-flow items are
# YTD-cumulative). The 3 lagged quarters resolve by nominal spacing with
# the surprise-family tolerances; any missing quarter yields NA --
# absence, never a fabricated partial sum.
.q_ttm_flow <- function(fund_asof, cn, q_pe, splits = NULL,
                        split_adjust = FALSE) {
  ser <- tryCatch(.flow_quarter_series(fund_asof, cn, splits, split_adjust),
                  error = function(e) NULL)
  if (is.null(ser) || nrow(ser) < 4) return(NA_real_)
  i <- .near_idx(ser$qend, q_pe, 10)
  if (is.na(i)) return(NA_real_)
  idx <- c(i, vapply(1:3, function(m) {
    .q_lag_idx(ser$qend, i, round(m * 91.3), 40)
  }, integer(1)))
  if (anyNA(idx)) return(NA_real_)
  v <- ser$value[idx]
  if (anyNA(v)) return(NA_real_)
  sum(v)
}


#' TTM and standalone-quarter values for all .Q3M_CONCEPTS in one pass
#'
#' One .flow_quarter_series scan per concept, from which BOTH sides
#' derive: ttm_<concept> reproduces .q_ttm_flow exactly (same anchor
#' tolerance 10 d, same lag resolution round(m * 91.3) at tol 40, NA
#' when the series is missing/short, the anchor is missing, any lag is
#' missing, or any value is NA); q3m_<concept> is the anchor quarter's
#' own 3-month value (NA when no anchor resolves). Exists so the
#' quarterly-row build does not re-scan the vintage table once per
#' consumer (.extract_stubs_q + .compute_ttm_indicators_q + the q3m
#' columns all read one bundle). Dollar aggregates are split-invariant,
#' so no split adjustment (matching .extract_stubs_q's .q_ttm_flow
#' calls; eps stays on its own dual path in .q_ttm_eps).
#'
#' @param fund_asof data.table. pit_dedup view as of the row's filing.
#' @param q_pe Date. The anchor quarter's period_end.
#' @return Named list: ttm_<concept> and q3m_<concept> per .Q3M_CONCEPTS.
.q_ttm_bundle <- function(fund_asof, q_pe) {
  out <- setNames(
    as.list(rep(NA_real_, 2L * length(.Q3M_CONCEPTS))),
    c(paste0("ttm_", .Q3M_CONCEPTS), paste0("q3m_", .Q3M_CONCEPTS)))
  for (cn in .Q3M_CONCEPTS) {
    ser <- tryCatch(.flow_quarter_series(fund_asof, cn, NULL, FALSE),
                    error = function(e) NULL)
    if (is.null(ser) || nrow(ser) == 0) next
    i <- .near_idx(ser$qend, q_pe, 10)
    if (is.na(i)) next
    out[[paste0("q3m_", cn)]] <- as.numeric(ser$value[i])
    if (nrow(ser) < 4) next
    idx <- c(i, vapply(1:3, function(m) {
      .q_lag_idx(ser$qend, i, round(m * 91.3), 40)
    }, integer(1)))
    if (anyNA(idx)) next
    v <- ser$value[idx]
    if (anyNA(v)) next
    out[[paste0("ttm_", cn)]] <- sum(v)
  }
  out
}


# TTM diluted EPS for the quarter ending at q_pe, expressed on the
# Q-FILING-DATE split basis (the stub convention: update_ticker_daily
# maps stub_eps from its filed-date basis onto each day's as-traded
# basis via .split_factor). Prefers the ttm_eps.R identity
# (YTD + FY_prev - YTD_prev, robust to a missing middle quarter),
# computed on the PIT as-of view; falls back to summing 4 standalone
# split-adjusted quarters from the Wave 3 panels. Both machineries
# express values on the CURRENT split basis, so the result is converted
# back to the q_filed basis via .split_to_current.
.q_ttm_eps <- function(fund_asof, q_pe, q_filed, splits = NULL) {
  v <- NA_real_
  if (exists("build_ttm_eps_series", mode = "function")) {
    ttm <- tryCatch(build_ttm_eps_series(fund_asof, splits),
                    error = function(e) NULL)
    if (!is.null(ttm) && nrow(ttm) > 0) {
      i <- .near_idx(ttm$qend, q_pe, 10)
      if (!is.na(i)) v <- as.numeric(ttm$eps_ttm[i])
    }
  }
  if (is.na(v)) {
    v <- .q_ttm_flow(fund_asof, "eps_diluted", q_pe, splits,
                     split_adjust = TRUE)
  }
  if (is.na(v)) return(NA_real_)
  v / .split_to_current(q_filed, splits)
}


#' Extract accounting stubs for one quarterly row (mixed-cadence extension)
#'
#' Stock stubs are the Q-row instant values as-of the quarter's original
#' filing date (balance-sheet levels are correct-basis at any period end;
#' .level_components applies the same identity fallbacks as the FY path,
#' so filing-date and daily-recompute values cannot drift). Flow stubs
#' are trailing-twelve-month sums of standalone quarters (.q_ttm_flow /
#' .q_ttm_eps); an unrecoverable TTM is NA. Flow stubs with no quarterly
#' TTM construction in the approved design (equity_issuance, rnd,
#' cf_num) and the Wave 5 composite prefixes (annual mixed flow/stock
#' terms) stay NA on quarterly rows: absence, never an annual value
#' re-labelled as quarterly.
#'
#' @param qrow data.table. The quarter's wide pivot row (1 row).
#' @param fund_asof data.table. pit_dedup view as of q_filed.
#' @param q_pe Date. The quarter's period_end.
#' @param q_filed Date. The quarter's original filing date.
#' @param splits data.table or NULL. Split events.
#' @param ttm Named list or NULL. Precomputed .q_ttm_bundle for this
#'   quarter; NULL falls back to per-flow .q_ttm_flow calls (identical
#'   results, one vintage scan per flow).
#' @param lc Named list or NULL. Precomputed .level_components(qrow);
#'   NULL resolves it here.
#' @return Named list covering .STUB_NAMES.
.extract_stubs_q <- function(qrow, fund_asof, q_pe, q_filed, splits = NULL,
                             ttm = NULL, lc = NULL) {

  if (is.null(lc)) lc <- .level_components(qrow)

  if (is.null(ttm)) {
    cfo_ttm   <- .q_ttm_flow(fund_asof, "operating_cashflow", q_pe)
    capex_ttm <- .q_ttm_flow(fund_asof, "capex", q_pe)
    opinc_ttm <- .q_ttm_flow(fund_asof, "operating_income", q_pe)
    dep_ttm   <- .q_ttm_flow(fund_asof, "depreciation", q_pe)
    rev_ttm   <- .q_ttm_flow(fund_asof, "revenue", q_pe)
    div_ttm   <- .q_ttm_flow(fund_asof, "dividends_paid", q_pe)
    buy_ttm   <- .q_ttm_flow(fund_asof, "buybacks", q_pe)
  } else {
    cfo_ttm   <- ttm$ttm_operating_cashflow
    capex_ttm <- ttm$ttm_capex
    opinc_ttm <- ttm$ttm_operating_income
    dep_ttm   <- ttm$ttm_depreciation
    rev_ttm   <- ttm$ttm_revenue
    div_ttm   <- ttm$ttm_dividends_paid
    buy_ttm   <- ttm$ttm_buybacks
  }

  # Same NA policies as .derive_quantities: FCF without capex would be
  # overstated OpCF; EBITDA without D&A would be EBIT mislabelled.
  fcf_ttm <- if (is.na(cfo_ttm) || is.na(capex_ttm)) NA_real_ else
    cfo_ttm - capex_ttm
  ebitda_ttm <- if (is.na(opinc_ttm) || is.na(dep_ttm)) NA_real_ else
    opinc_ttm + dep_ttm

  list(
    stub_shares     = .col(qrow, "shares_outstanding"),
    stub_eps        = .q_ttm_eps(fund_asof, q_pe, q_filed, splits),
    stub_equity     = lc$ceq,
    stub_revenue    = rev_ttm,
    stub_fcf        = fcf_ttm,
    stub_ebitda     = ebitda_ttm,
    stub_total_debt = lc$td,
    stub_net_debt   = .col(qrow, "net_debt"),
    stub_cash       = .col(qrow, "cash"),
    stub_dividends  = div_ttm,
    stub_buybacks   = buy_ttm,
    stub_equity_issuance = NA_real_,
    stub_rnd        = NA_real_,
    stub_assets     = lc$at,
    stub_liabilities = lc$lt,
    stub_cf_num     = NA_real_,
    stub_cfo        = cfo_ttm,
    stub_che        = lc$che,
    stub_net_cash   = lc$ncash,
    stub_ndp_num    = lc$ndp_num,
    stub_z_prefix   = NA_real_,
    stub_kz_prefix  = NA_real_
  )
}


#' Compute the TTM-quarterly indicators for one quarterly row
#'
#' Re-bases the annual ratio formulas in .Q_TTM_INDICATORS onto
#' trailing-twelve-month flows (the .q_ttm_bundle) against the Q-row's
#' as-of instants. Each formula MIRRORS its FY definition in
#' indicator_compute.R (.compute_profitability, .compute_leverage,
#' .compute_efficiency, .compute_cashflow_quality, .compute_shareholder,
#' .compute_tier1, .compute_levels) -- same .safe_divide floors, same
#' positivity guards (ceq > 0 for book_leverage / oper_prof), same
#' required-vs-zero-if-na term policies, same derived-quantity NA
#' policies (gross profit needs both terms; FCF without capex and
#' EBITDA without D&A propagate NA, as in .derive_quantities). Values
#' NEVER come from the compute_ticker_indicators output at a quarterly
#' target_period: its flow cells are YTD-cumulative there (the
#' wrong-basis trap documented at the constants).
#'
#' @param qrow data.table. The quarter's wide pivot row (1 row).
#' @param ttm Named list. Output of .q_ttm_bundle for this quarter.
#' @param lc Named list or NULL. Precomputed .level_components(qrow);
#'   NULL resolves it here.
#' @return Named list covering .Q_TTM_INDICATORS (NA where inputs are
#'   unavailable).
.compute_ttm_indicators_q <- function(qrow, ttm, lc = NULL) {

  if (is.null(lc)) lc <- .level_components(qrow)

  out <- setNames(as.list(rep(NA_real_, length(.Q_TTM_INDICATORS))),
                  .Q_TTM_INDICATORS)

  rev    <- ttm$ttm_revenue
  cogs_v <- ttm$ttm_cogs
  sga    <- ttm$ttm_sga
  ni     <- ttm$ttm_net_income
  cfo    <- ttm$ttm_operating_cashflow
  capex_v <- ttm$ttm_capex
  opinc  <- ttm$ttm_operating_income
  dep    <- ttm$ttm_depreciation
  div    <- ttm$ttm_dividends_paid
  tax    <- ttm$ttm_income_tax_expense
  pretax <- ttm$ttm_pretax_income
  intexp <- ttm$ttm_interest_expense

  # Derived TTM quantities, same NA policies as .derive_quantities:
  # gross profit needs both terms; EBITDA without D&A would be EBIT
  # mislabelled; FCF without capex would be overstated OpCF.
  gp <- if (is.na(rev) || is.na(cogs_v)) NA_real_ else rev - cogs_v
  ebitda_v <- if (is.na(opinc) || is.na(dep)) NA_real_ else opinc + dep
  fcf_v <- if (is.na(cfo) || is.na(capex_v)) NA_real_ else cfo - capex_v

  # -- Profitability (.compute_profitability): sign-guard-free, invested
  #    capital = equity + debt - cash with NA in any component propagating
  ic <- lc$ceq + lc$td - .col(qrow, "cash")
  out$gross_margin     <- .safe_divide(gp, rev)
  out$operating_margin <- .safe_divide(opinc, rev)
  out$net_margin       <- .safe_divide(ni, rev)
  out$roe              <- .safe_divide(ni, lc$ceq)
  out$roa              <- .safe_divide(ni, lc$at)
  out$roic             <- .safe_divide(ni, ic)

  # -- Efficiency (.compute_efficiency) --
  out$asset_turnover       <- .safe_divide(rev, lc$at)
  out$inventory_turnover   <- .safe_divide(cogs_v, .col(qrow, "inventory"))
  out$receivables_turnover <- .safe_divide(rev, .col(qrow, "accounts_receivable"))

  # -- Cash flow quality (.compute_cashflow_quality) --
  out$fcf_ni        <- .safe_divide(fcf_v, ni, min_abs_denom = 1)
  out$opcf_ni       <- .safe_divide(cfo, ni, min_abs_denom = 1)
  out$capex_revenue <- .safe_divide(capex_v, rev)

  # -- Shareholder return (.compute_shareholder): payout allowed
  #    negative when NI < 0 (distress signal) --
  abs_div <- if (!is.na(div)) abs(div) else NA_real_
  out$payout_ratio <- .safe_divide(abs_div, ni)

  # -- Leverage (.compute_leverage): net_debt is the Q-row instant
  #    derived quantity (total_debt - cash, NA propagates) --
  ca  <- .col(qrow, "current_assets")
  cl  <- .col(qrow, "current_liabilities")
  inv <- .col(qrow, "inventory")
  out$debt_equity       <- .safe_divide(lc$td, lc$ceq)
  out$net_debt_ebitda   <- .safe_divide(.col(qrow, "net_debt"), ebitda_v)
  out$interest_coverage <- .safe_divide(opinc, intexp, min_abs_denom = 1)
  out$current_ratio     <- .safe_divide(ca, cl)
  out$quick_ratio       <- .safe_divide(
    if (!is.na(ca)) ca - (if (is.na(inv)) 0 else inv) else NA_real_, cl)

  # -- Tier 1 (.compute_tier1): gpa and pct_accruals only (the lagged
  #    constructions have no TTM basis and stay outside the set) --
  out$gpa <- .safe_divide(gp, lc$at)
  accrual_num <- if (!is.na(ni) && !is.na(cfo)) ni - cfo else NA_real_
  out$pct_accruals <- .safe_divide(accrual_num, abs(ni), min_abs_denom = 1)

  # -- Levels (.compute_levels) --
  out$effective_tax_rate <- .safe_divide(tax, pretax)
  if (!is.na(lc$ceq) && lc$ceq > 0) {
    out$book_leverage <- .safe_divide(lc$at, lc$ceq)
  }
  # FF operating profitability: all four numerator inputs required,
  # BE > 0 (W4.9)
  if (!is.na(rev) && !is.na(cogs_v) && !is.na(sga) && !is.na(intexp) &&
      !is.na(lc$ceq) && lc$ceq > 0) {
    out$oper_prof <- .safe_divide(rev - cogs_v - sga - intexp, lc$ceq)
  }
  out$cash_assets <- .safe_divide(lc$che, lc$at)
  out$salecash    <- .safe_divide(rev, lc$che, min_abs_denom = 1)
  out$depr_rate   <- .safe_divide(dep, .col(qrow, "ppe_net"))

  out
}


#' Build quarterly fund-layer rows for one ticker (mixed-cadence extension)
#'
#' Enumerates (fiscal_year, Q1/Q2/Q3) from the vintage table; each
#' quarter is computed as-of its ORIGINAL filing date: min(filed) over
#' the quarter's plain 10-Q rows (fallback: min over all its rows when
#' no plain 10-Q exists). Amendments filed later can neither pull the
#' stamp earlier nor leak into the as-of view (pit_dedup as_of cuts
#' them). Indicator columns stored: the natively quarterly set
#' (.Q_NATIVE_INDICATORS, copied from the compute output) and the
#' TTM-quarterly set (.Q_TTM_INDICATORS, recomputed by
#' .compute_ttm_indicators_q from one .q_ttm_bundle -- never copied
#' from the compute output, whose flow cells are YTD-cumulative at a
#' quarterly target_period); every other indicator column is NA. Stubs
#' per .extract_stubs_q; standalone-quarter q3m_<concept> ingredient
#' columns from the same bundle. A quarter whose pivot row is missing
#' or too sparse yields no row -- degradation is absence.
#'
#' @param fund_v data.table. Raw XBRL vintages (get_fundamentals vintages=TRUE).
#' @param sector Character. Sector classification.
#' @param splits data.table or NULL. Split events.
#' @return data.table of quarterly rows, or NULL.
.build_quarterly_rows <- function(fund_v, sector, splits = NULL) {

  qv <- fund_v[period_type %in% c("Q1", "Q2", "Q3") & !is.na(filed)]
  if (nrow(qv) == 0) return(NULL)

  keys <- unique(qv[, .(fiscal_year, period_type)])
  keys <- keys[!is.na(fiscal_year)]
  if (nrow(keys) == 0) return(NULL)
  setorder(keys, fiscal_year, period_type)

  ind_cols <- c(.FUNDAMENTAL_INDICATORS, .CS_INGREDIENT_COLS)
  results  <- vector("list", nrow(keys))
  n_ok <- 0L

  for (r in seq_len(nrow(keys))) {
    fy <- keys$fiscal_year[r]
    qq <- keys$period_type[r]

    grp  <- qv[fiscal_year == fy & period_type == qq]
    orig <- grp[form == "10-Q"]
    q_filed <- as.Date(min(if (nrow(orig)) orig$filed else grp$filed))

    # Point-in-time view as of the quarter's original filing date
    fund_asof <- pit_dedup(fund_v, as_of = q_filed)

    wide <- pivot_fundamentals(fund_asof)
    if (is.null(wide)) next
    wide <- .derive_quantities(wide)

    qrow <- wide[period_type == qq & fiscal_year == fy]
    if (nrow(qrow) == 0) next
    qrow <- qrow[1]
    # The pivot's period_end metadata is max() over the group's concepts
    # and is pulled PAST the quarter end by instant facts dated at the
    # cover page (dei shares are stamped ~3 weeks after quarter end).
    # The quarter's true period_end comes from its duration rows; the
    # polluted metadata date is only a last-resort fallback.
    dur <- fund_asof[fiscal_year == fy & period_type == qq &
                       !is.na(period_start) & !is.na(period_end)]
    q_pe <- if (nrow(dur) > 0) as.Date(max(dur$period_end)) else
      as.Date(qrow$period_end[1])
    if (is.na(q_pe)) next

    # Same minimum-density rule as the FY loop
    data_cols <- setdiff(names(qrow),
                         c("fiscal_year", "period_type", "period_end", "filed"))
    n_concepts <- sum(!is.na(as.matrix(qrow[1, ..data_cols])))
    if (n_concepts < 5) next

    indicators <- tryCatch(
      compute_ticker_indicators(fund_asof, price_on_filed = NA_real_,
                                sector = sector, target_fy = fy,
                                target_period = qq, splits = splits),
      error = function(e) NULL
    )
    if (is.null(indicators)) next

    # One TTM/standalone-quarter scan and one instant resolution,
    # shared by the TTM indicators, the stubs, and the q3m columns
    ttm <- .q_ttm_bundle(fund_asof, q_pe)
    lc  <- .level_components(qrow)

    # Copy ONLY the natively quarterly outputs from the compute path;
    # all other indicator columns stay NA on a quarterly row
    # (annual-bound formulas are wrong-basis at Q and must be absent,
    # not plausible).
    q_ind <- setNames(as.list(rep(NA_real_, length(ind_cols))), ind_cols)
    for (nm in intersect(.Q_NATIVE_INDICATORS, names(indicators))) {
      q_ind[[nm]] <- unname(indicators[[nm]])
    }

    # TTM-quarterly indicators are recomputed on the TTM basis, never
    # copied from the compute output (wrong-basis trap -- see the
    # constants)
    ttm_ind <- .compute_ttm_indicators_q(qrow, ttm, lc)
    for (nm in intersect(.Q_TTM_INDICATORS, names(q_ind))) {
      q_ind[[nm]] <- ttm_ind[[nm]]
    }

    stubs <- .extract_stubs_q(qrow, fund_asof, q_pe, q_filed, splits,
                              ttm = ttm, lc = lc)

    # Same masked-stub rule as the FY loop
    if (!is.na(sector) && sector %in% names(.SECTOR_NA_INDICATORS)) {
      masked_here <- intersect(names(.MASKED_STUB_OF),
                               .SECTOR_NA_INDICATORS[[sector]])
      for (ind in masked_here) stubs[[.MASKED_STUB_OF[[ind]]]] <- NA_real_
    }

    n_ok <- n_ok + 1L
    results[[n_ok]] <- c(
      list(fiscal_year = as.integer(fy),
           quarter     = qq,
           filed_date  = q_filed,
           period_end  = q_pe),
      q_ind,
      stubs,
      ttm[paste0("q3m_", .Q3M_CONCEPTS)]
    )
  }

  if (n_ok == 0) return(NULL)
  rbindlist(results[seq_len(n_ok)], fill = TRUE)
}


#' Vectorized computation of price-sensitive indicators
#'
#' Given vectors of accounting stubs and prices (one element per trading day),
#' computes all 25 price-sensitive indicators. Replicates the logic from
#' .compute_valuation, .compute_shareholder, .compute_levels and the
#' Wave 5 composite completions in indicator_compute.R. Wave 4/5 stub
#' params default to NA so older fund layers degrade to NA columns
#' instead of erroring.
#'
#' @param p Numeric vector. Closing prices.
#' @param shares Numeric vector. Shares outstanding.
#' @param eps Numeric vector. Diluted EPS.
#' @param equity Numeric vector. Stockholders' equity.
#' @param rev Numeric vector. Revenue.
#' @param fcf_v Numeric vector. Free cash flow.
#' @param ebitda Numeric vector. EBITDA.
#' @param td Numeric vector. Total debt.
#' @param nd Numeric vector. Net debt.
#' @param div Numeric vector. Dividends paid (negative = outflow).
#' @param buy Numeric vector. Buybacks (negative = outflow).
#' @param eps_g Numeric vector. EPS growth YoY (for PEG).
#' @param iss Numeric vector. Equity issuance (Wave 2 stub).
#' @param rnd Numeric vector. R&D expense (stub_rnd).
#' @param at_v Numeric vector. Total assets (stub_assets).
#' @param lt_v Numeric vector. Total liabilities (stub_liabilities).
#' @param cf_num Numeric vector. NI + depreciation (stub_cf_num).
#' @param cfo Numeric vector. Operating cash flow (stub_cfo).
#' @param che Numeric vector. Cash + ST investments (stub_che).
#' @param ncash Numeric vector. che - total debt (stub_net_cash).
#' @param ndp_num Numeric vector. FINL - che (stub_ndp_num).
#' @param z_prefix Numeric vector. Altman price-free terms (stub_z_prefix).
#' @param kz_prefix Numeric vector. KZ price-free terms (stub_kz_prefix).
#' @return data.table with 25 price-sensitive indicator columns.
.compute_price_sensitive_vec <- function(p, shares, eps, equity, rev,
                                         fcf_v, ebitda, td, nd,
                                         div, buy, eps_g,
                                         iss = NA_real_,
                                         rnd = NA_real_, at_v = NA_real_,
                                         lt_v = NA_real_, cf_num = NA_real_,
                                         cfo = NA_real_, che = NA_real_,
                                         ncash = NA_real_,
                                         ndp_num = NA_real_,
                                         z_prefix = NA_real_,
                                         kz_prefix = NA_real_) {

  # Market cap & enterprise value
  mc <- ifelse(!is.na(p) & !is.na(shares), p * shares, NA_real_)
  ev <- ifelse(!is.na(mc),
               ifelse(!is.na(nd), mc + nd,
                      ifelse(!is.na(td), mc + td, NA_real_)),
               NA_real_)

  # P/E (guard: |EPS| >= 0.01)
  pe <- ifelse(!is.na(p) & !is.na(eps) & abs(eps) >= 0.01,
               p / eps, NA_real_)

  # PEG = P/E / (EPS growth * 100), only when growth > 0
  peg <- ifelse(!is.na(pe) & !is.na(eps_g) & eps_g > 0 &
                  abs(eps_g * 100) >= 1e-9,
                pe / (eps_g * 100), NA_real_)

  # P/B (equity must be positive)
  pb <- ifelse(!is.na(mc) & !is.na(equity) & equity > 0,
               mc / equity, NA_real_)

  # P/S
  ps <- ifelse(!is.na(mc) & !is.na(rev) & abs(rev) >= 1e-9,
               mc / rev, NA_real_)

  # P/FCF (FCF must be positive)
  pfcf <- ifelse(!is.na(mc) & !is.na(fcf_v) & fcf_v > 0,
                 mc / fcf_v, NA_real_)

  # EV/EBITDA (EBITDA must be positive)
  ev_ebitda <- ifelse(!is.na(ev) & !is.na(ebitda) & ebitda > 0,
                      ev / ebitda, NA_real_)

  # EV/Revenue
  ev_revenue <- ifelse(!is.na(ev) & !is.na(rev) & abs(rev) >= 1e-9,
                       ev / rev, NA_real_)

  # Earnings yield = EPS / price (guard: |price| >= 0.01)
  ey <- ifelse(!is.na(eps) & !is.na(p) & abs(p) >= 0.01,
               eps / p, NA_real_)

  # Dividend yield = |dividends_paid| / market_cap
  abs_div <- ifelse(!is.na(div), abs(div), NA_real_)
  div_yield <- ifelse(!is.na(abs_div) & !is.na(mc) & abs(mc) >= 1e-9,
                      abs_div / mc, NA_real_)

  # Buyback yield = |buybacks| / market_cap
  abs_buy <- ifelse(!is.na(buy), abs(buy), NA_real_)
  bb_yield <- ifelse(!is.na(abs_buy) & !is.na(mc) & abs(mc) >= 1e-9,
                     abs_buy / mc, NA_real_)

  # Gross payout components, shared by net_payout_yield (Wave 2) and
  # payout_yield (Wave 4) -- one zero-fill site for the sign policy.
  any_gross <- !is.na(abs_div) | !is.na(abs_buy)
  gpy_num <- ifelse(is.na(abs_div), 0, abs_div) +
    ifelse(is.na(abs_buy), 0, abs_buy)

  # Net payout yield = (|dividends| + |buybacks| - equity issuance) /
  # market_cap (Boudoukh et al. 2007). Components zero-if-na; NA when
  # none of the three is reported.
  any_payout <- any_gross | !is.na(iss)
  npy_num <- gpy_num - ifelse(is.na(iss), 0, iss)
  npy <- ifelse(any_payout & !is.na(mc) & abs(mc) >= 1e-9,
                npy_num / mc, NA_real_)

  # -- Wave 4 ME-scaled levels (indicator_compute.R Section 6e) --
  mc_ok <- !is.na(mc) & abs(mc) >= 1e-9

  # R&D to market (missing R&D stays NA)
  rd_me <- ifelse(mc_ok & !is.na(rnd), rnd / mc, NA_real_)

  # Penman enterprise book-to-market + leverage component
  ebm <- ifelse(mc_ok & !is.na(equity) & !is.na(ncash) &
                  abs(mc + ncash) >= 1e-9,
                (equity + ncash) / (mc + ncash), NA_real_)
  bp <- ifelse(mc_ok & !is.na(equity), equity / mc, NA_real_)
  bpebm <- ifelse(!is.na(bp) & !is.na(ebm), bp - ebm, NA_real_)

  # LSV cash flow to price; Desai CFO to price
  cf_me <- ifelse(mc_ok & !is.na(cf_num), cf_num / mc, NA_real_)
  cfp   <- ifelse(mc_ok & !is.na(cfo), cfo / mc, NA_real_)

  # Penman net debt to price (stub NA'd for financials at extraction)
  net_debt_price <- ifelse(mc_ok & !is.na(ndp_num), ndp_num / mc, NA_real_)

  # FF assets to market; Bhandari market leverage
  am_v <- ifelse(mc_ok & !is.na(at_v), at_v / mc, NA_real_)
  lev_mkt <- ifelse(mc_ok & !is.na(lt_v), lt_v / mc, NA_real_)

  # Cash productivity (near-zero cash -> NA, same floor as Section 6e)
  cash_prod <- ifelse(!is.na(mc) & !is.na(at_v) & !is.na(che) &
                        abs(che) >= 1,
                      (mc - at_v) / che, NA_real_)

  # Gross payout yield (components zero-if-na, >= 1 reported; any_gross
  # and gpy_num are computed once above, next to net payout yield)
  payout_yield <- ifelse(any_gross & mc_ok, gpy_num / mc, NA_real_)

  # -- Wave 5 composite completions (indicator_compute.R Section 6g) --
  # Altman: prefix + 0.6*ME/LT; KZ: prefix + 0.283*ME/AT
  z_score <- ifelse(mc_ok & !is.na(z_prefix) & !is.na(lt_v) &
                      abs(lt_v) >= 1e-9,
                    z_prefix + 0.6 * mc / lt_v, NA_real_)
  kz_index <- ifelse(mc_ok & !is.na(kz_prefix) & !is.na(at_v) &
                       abs(at_v) >= 1e-9,
                     kz_prefix + 0.283 * mc / at_v, NA_real_)

  data.table(
    pe_trailing      = pe,
    peg              = peg,
    pb               = pb,
    ps               = ps,
    pfcf             = pfcf,
    ev_ebitda        = ev_ebitda,
    ev_revenue       = ev_revenue,
    earnings_yield   = ey,
    dividend_yield   = div_yield,
    buyback_yield    = bb_yield,
    market_cap       = mc,
    enterprise_value = ev,
    net_payout_yield = npy,
    rd_me            = rd_me,
    ebm              = ebm,
    bpebm            = bpebm,
    cf_me            = cf_me,
    cfp              = cfp,
    net_debt_price   = net_debt_price,
    am               = am_v,
    leverage_mkt     = lev_mkt,
    cash_prod        = cash_prod,
    payout_yield     = payout_yield,
    z_score          = z_score,
    kz_index         = kz_index
  )
}


# =============================================================================
# SECTION 3: build_ticker_fundamentals()
# =============================================================================
#' Build the fundamentals layer for a single ticker
#'
#' Reads cached XBRL data, computes fundamental-only indicators and extracts
#' accounting stubs for each available fiscal year. Saves to parquet.
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit CIK.
#' @param sector Character. Sector classification.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param ts_dir Character. Time series output directory.
#' @param force Logical. Rebuild even if file exists.
#' @param quarterly Logical. FALSE (default) reproduces the annual-only
#'   layer byte-identically (no quarter column, gated downstream). TRUE
#'   appends quarterly Q1-Q3 rows after the FY loop and stamps
#'   standalone-quarter q3m_<concept> columns on FY rows too; the new
#'   `quarter` column ("FY"/"Q1"/"Q2"/"Q3") doubles as the cadence
#'   marker (no separate cadence column -- documented choice, cadence
#'   is quarter == "FY" vs not). NOTE: an existing file is returned
#'   as-is unless force = TRUE, whatever cadence it was built with.
#' @return data.table (the fundamentals layer), or NULL on failure.
build_ticker_fundamentals <- function(ticker, cik, sector,
                                      fund_dir = "cache/fundamentals",
                                      ts_dir   = .TS_DIR,
                                      force    = FALSE,
                                      quarterly = FALSE) {

  if (!dir.exists(ts_dir)) dir.create(ts_dir, recursive = TRUE)

  out_path <- file.path(ts_dir, sprintf("%s_fund.parquet", ticker))
  if (file.exists(out_path) && !force) {
    # legacy layers predate the per-share layer -- backfill it so the
    # daily recompute gets 10-Q-cadence share counts without a forced
    # fund rebuild
    ps_path <- file.path(ts_dir, sprintf("%s_pershare.parquet", ticker))
    if (!file.exists(ps_path)) {
      tryCatch(build_ticker_pershare(ticker, cik, fund_dir, ts_dir),
               error = function(e) NULL)
    }
    return(as.data.table(arrow::read_parquet(out_path)))
  }

  # Load raw XBRL vintages (one row per concept x period x filing), so each
  # fiscal year can be computed from the data that was public when its own
  # annual report was filed. Old collapsed caches degrade gracefully: the
  # vintage table then has one row per period and the as-of resolution is a
  # no-op.
  fund_v <- tryCatch(
    get_fundamentals(ticker, cik, cache_dir = fund_dir, vintages = TRUE),
    error = function(e) NULL
  )
  if (is.null(fund_v) || nrow(fund_v) == 0) return(NULL)

  # Enumerate fiscal years from the latest view
  fy_all <- pit_dedup(fund_v)[period_type == "FY"]
  if (nrow(fy_all) == 0) return(NULL)

  fiscal_years <- sort(unique(fy_all$fiscal_year))
  annual_forms <- c("10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A")

  # Split events for share-issuance adjustment (loaded once per ticker;
  # guarded so the builder works when ttm_eps.R is not sourced)
  splits <- if (exists("load_ticker_splits")) {
    tryCatch(load_ticker_splits(ticker, fetch = FALSE), error = function(e) NULL)
  } else NULL

  results <- vector("list", length(fiscal_years))
  n_ok <- 0L

  for (fy in fiscal_years) {
    # Original availability date: earliest annual-form filing that reports
    # this fiscal year. Amendments and later re-reports come afterwards and
    # must not push the stamp forward.
    fy_grp <- fund_v[fiscal_year == fy & period_type == "FY" & !is.na(filed)]
    if (nrow(fy_grp) == 0) next
    ann <- fy_grp[form %in% annual_forms]
    filed_date <- as.Date(min(if (nrow(ann)) ann$filed else fy_grp$filed))

    # Point-in-time view as of that filing date; prior years resolve to the
    # vintages that were public then.
    fund_asof <- pit_dedup(fund_v, as_of = filed_date)

    wide <- pivot_fundamentals(fund_asof)
    if (is.null(wide)) next
    wide <- .derive_quantities(wide)

    curr <- wide[period_type == "FY" & fiscal_year == fy]
    if (nrow(curr) == 0) next

    period_end <- as.Date(max(curr$period_end, na.rm = TRUE))

    # Require minimum data density
    data_cols <- setdiff(names(curr),
                         c("fiscal_year", "period_type", "period_end", "filed"))
    n_concepts <- sum(!is.na(as.matrix(curr[1, ..data_cols])))
    if (n_concepts < 5) next

    # Compute indicators with price=NA (price-sensitive ones become NA)
    indicators <- tryCatch(
      compute_ticker_indicators(fund_asof, price_on_filed = NA_real_,
                                sector = sector, target_fy = fy,
                                splits = splits),
      error = function(e) NULL
    )
    if (is.null(indicators)) next

    # Extract fundamental-only indicator values, plus the hidden
    # cross-section ingredient columns (lagged revenues) that the daily
    # cross-section reader needs to finalize herf / ww_index.
    fund_only <- as.list(indicators[c(.FUNDAMENTAL_INDICATORS,
                                      .CS_INGREDIENT_COLS)])

    # Extract accounting stubs from the as-of pivoted data
    stubs <- .extract_stubs(wide, fy)
    if (is.null(stubs)) next

    # Sector-masked price-sensitive indicators are recomputed daily from
    # stubs, out of reach of the compute-path mask -- NA the stubs at
    # the source, driven by the declarative indicator -> stub map so a
    # future masked price-sensitive indicator cannot silently miss it.
    if (!is.na(sector) && sector %in% names(.SECTOR_NA_INDICATORS)) {
      masked_here <- intersect(names(.MASKED_STUB_OF),
                               .SECTOR_NA_INDICATORS[[sector]])
      for (ind in masked_here) stubs[[.MASKED_STUB_OF[[ind]]]] <- NA_real_
    }

    # Mixed-cadence extension: standalone-quarter (q3m) ingredient
    # columns on the FY row, as-of the FY filing. The anchor at the FY
    # period_end is the standalone Q4 that .flow_quarter_series
    # recovers from the FY - 3Q pairing. Gated: with quarterly = FALSE
    # nothing is computed and no column exists (c() drops the NULL), so
    # the annual-only path stays byte-identical.
    q3m <- if (isTRUE(quarterly)) {
      .q_ttm_bundle(fund_asof, period_end)[paste0("q3m_", .Q3M_CONCEPTS)]
    } else NULL

    n_ok <- n_ok + 1L
    results[[n_ok]] <- c(
      list(fiscal_year = as.integer(fy),
           filed_date  = filed_date,
           period_end  = period_end),
      fund_only,
      stubs,
      q3m
    )
  }

  if (n_ok == 0) return(NULL)

  dt <- rbindlist(results[seq_len(n_ok)], fill = TRUE)
  setorder(dt, fiscal_year)

  # Mixed-cadence extension: quarterly rows AFTER the FY loop. The
  # quarterly = FALSE path is untouched (no quarter column, same
  # ordering) and stays byte-identical to the annual-only builder.
  if (isTRUE(quarterly)) {
    dt[, quarter := "FY"]
    q_dt <- tryCatch(.build_quarterly_rows(fund_v, sector, splits),
                     error = function(e) {
                       warning(sprintf(
                         "build_ticker_fundamentals: quarterly rows failed: %s -- %s",
                         ticker, e$message), call. = FALSE)
                       NULL
                     })
    if (!is.null(q_dt)) {
      dt <- rbindlist(list(dt, q_dt), fill = TRUE)
      setorder(dt, fiscal_year, quarter)   # "FY" < "Q1" < "Q2" < "Q3"
    }
  }

  .assert_output_ts(dt, "build_ticker_fundamentals", list(
    "is data.table"      = is.data.table,
    "has fiscal_year"    = function(x) "fiscal_year" %in% names(x),
    "has filed_date"     = function(x) "filed_date" %in% names(x),
    "has stubs"          = function(x) all(.STUB_NAMES %in% names(x)),
    "at least one row"   = function(x) nrow(x) > 0
  ))

  arrow::write_parquet(dt, out_path)

  # Per-share layer (D1): quarterly-cadence share counts from the same
  # vintage table, one row per filing. Extraction failure degrades to
  # the FY-stub fallback in update_ticker_daily, never blocks the build.
  ps <- tryCatch(.extract_pershare_series(fund_v), error = function(e) NULL)
  if (!is.null(ps)) {
    arrow::write_parquet(ps, file.path(ts_dir,
                                       sprintf("%s_pershare.parquet", ticker)))
  }

  dt
}


# =============================================================================
# SECTION 3b: build_ticker_pershare()
# =============================================================================
#' Build (or rebuild) the per-share layer for a single ticker
#'
#' Standalone entry point for layers whose fund parquet already exists
#' (build_ticker_fundamentals writes the per-share layer inline when it
#' builds fresh). Reads the cached XBRL vintages only -- no network.
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit CIK (NULL lets get_fundamentals resolve).
#' @param fund_dir Character. Fundamentals cache directory.
#' @param ts_dir Character. Time series output directory.
#' @param force Logical. Rebuild even if file exists.
#' @return data.table (the per-share layer), or NULL when no share rows.
build_ticker_pershare <- function(ticker, cik = NULL,
                                  fund_dir = "cache/fundamentals",
                                  ts_dir   = .TS_DIR,
                                  force    = FALSE) {

  out_path <- file.path(ts_dir, sprintf("%s_pershare.parquet", ticker))
  if (file.exists(out_path) && !force) {
    return(as.data.table(arrow::read_parquet(out_path)))
  }

  fund_v <- tryCatch(
    get_fundamentals(ticker, cik, cache_dir = fund_dir, vintages = TRUE),
    error = function(e) NULL
  )
  ps <- .extract_pershare_series(fund_v)
  if (is.null(ps)) return(NULL)

  .assert_output_ts(ps, "build_ticker_pershare", list(
    "is data.table"   = is.data.table,
    "has filed_date"  = function(x) "filed_date" %in% names(x),
    "has shares"      = function(x) "shares" %in% names(x),
    "positive shares" = function(x) all(x$shares > 0),
    "sorted by filed" = function(x) !is.unsorted(x$filed_date)
  ))

  if (!dir.exists(ts_dir)) dir.create(ts_dir, recursive = TRUE)
  arrow::write_parquet(ps, out_path)
  ps
}


# =============================================================================
# SECTION 4: update_ticker_daily()
# =============================================================================
#' Update daily time series for a single ticker
#'
#' Reads the fundamentals layer, identifies new trading days since the last
#' stored date, and appends rows. Uses vectorized computation with a
#' two-pass point-in-time mapping:
#'   PASS A (annual rows only): all indicator columns outside
#'     .Q_KEPT_INDICATORS, plus fiscal_year and filed_date -- so
#'     filed_date keeps meaning "latest ANNUAL filing".
#'   PASS B (all rows, latest period_end wins where filed <= day): the
#'     quarterly-kept indicator columns (.Q_KEPT_INDICATORS = native +
#'     TTM-quarterly), every stub feeding
#'     .compute_price_sensitive_vec (valuation ratios refresh
#'     quarterly on mixed-cadence layers), and the daily column
#'     filed_date_q = the filed_date of the pass-B row in effect.
#' Mixed-cadence fund layers (a `quarter` column present) additionally
#' get pass-B provenance stamps -- period_end_q and quarter_q, the
#' period_end and quarter label of the pass-B row in effect -- and the
#' standalone-quarter q3m_<concept> ingredient columns the layer
#' carries, mapped from the same pass-B row. Both are gated on the
#' `quarter` column: legacy and annual-only layers produce exactly
#' today's columns (their output stays byte-identical).
#' On annual-only fund layers pass B degrades to pass A's rows and the
#' output matches the single-pass builder except the added filed_date_q
#' column (= filed_date).
#'
#' @param ticker Character. Ticker symbol.
#' @param through_date Date. Update through this date (default today).
#' @param start_date Date. Earliest date to compute (default 2010-01-04).
#' @param price_dir Character. Price cache directory.
#' @param ts_dir Character. Time series directory.
#' @param refresh_price Logical. Delete and re-fetch price cache for this ticker.
#' @return data.table (full daily layer), or NULL on failure.
update_ticker_daily <- function(ticker,
                                through_date  = Sys.Date(),
                                start_date    = as.Date("2010-01-04"),
                                price_dir     = "cache/prices",
                                ts_dir        = .TS_DIR,
                                refresh_price = FALSE) {

  through_date <- as.Date(through_date)
  start_date   <- as.Date(start_date)

  # -- Load fundamentals layer --
  fund_path <- file.path(ts_dir, sprintf("%s_fund.parquet", ticker))
  if (!file.exists(fund_path)) return(NULL)

  fund_dt <- as.data.table(arrow::read_parquet(fund_path))
  if (nrow(fund_dt) == 0) return(NULL)
  fund_dt[, filed_date := as.Date(filed_date)]

  # -- Load existing daily layer --
  daily_path <- file.path(ts_dir, sprintf("%s_daily.parquet", ticker))
  existing_dt <- NULL
  last_date <- start_date - 1L

  if (file.exists(daily_path)) {
    existing_dt <- tryCatch(
      as.data.table(arrow::read_parquet(daily_path)),
      error = function(e) NULL
    )
    if (!is.null(existing_dt) && nrow(existing_dt) > 0) {
      existing_dt[, date := as.Date(date)]
      last_date <- max(existing_dt$date)
    }
  }

  if (last_date >= through_date) return(existing_dt)

  # -- Handle price refresh (both providers' cache files) --
  if (refresh_price) {
    escaped_tk <- gsub("\\.", "\\\\.", ticker)
    cache_files <- list.files(price_dir,
                              pattern = sprintf("^%s_(yahoo|tiingo)_", escaped_tk),
                              full.names = TRUE)
    for (f in cache_files) file.remove(f)
  }

  # -- Load price data --
  price_xts <- tryCatch(
    fetch_ticker_prices(ticker, cache_dir = price_dir),
    error = function(e) NULL
  )
  if (is.null(price_xts) || nrow(price_xts) == 0) return(existing_dt)

  # Extract the as-traded close for all trading days at once: Close
  # (column 4, split-adjusted to today's basis) divided by the post-date
  # split factor. Adjusted (column 6) additionally removes dividends and
  # is never a valid level against as-reported fundamentals. Without a
  # splits cache this falls back to Close: exact for never-split tickers.
  splits <- if (exists("load_ticker_splits")) {
    tryCatch(load_ticker_splits(ticker, fetch = FALSE), error = function(e) NULL)
  } else NULL

  nc <- ncol(price_xts)
  price_col <- if (nc >= 4) 4 else 1
  all_prices <- as.numeric(coredata(price_xts[, price_col]))
  all_dates  <- as.Date(index(price_xts))
  all_prices <- all_prices / .split_factor(all_dates, splits)

  # -- Filter to new dates --
  new_mask <- all_dates > last_date & all_dates <= through_date
  if (sum(new_mask) == 0) return(existing_dt)

  new_dates  <- all_dates[new_mask]
  new_prices <- all_prices[new_mask]

  # -- Point-in-time lookup, PASS A (annual rows only): find latest fiscal
  # year filed on or before each day.
  # Must find max(fiscal_year) WHERE filed_date <= d, NOT just latest filing.
  # Simple findInterval on filed_date is wrong when prior-year amendments
  # produce non-monotonic filed_dates (e.g., FY2022 amended after FY2023 filed).
  # Fix: sort by fiscal_year DESC, iterate ~15 rows, assign greedily.
  # Mixed-cadence layers carry a `quarter` column; annual rows are its
  # "FY" rows. Legacy layers (no column) are annual-only already.
  has_q  <- "quarter" %in% names(fund_dt)
  ann_dt <- if (has_q) fund_dt[quarter == "FY"] else fund_dt
  if (nrow(ann_dt) == 0) return(existing_dt)
  setorder(ann_dt, -fiscal_year)
  n_days <- length(new_dates)
  fund_idx <- rep(NA_integer_, n_days)

  for (k in seq_len(nrow(ann_dt))) {
    eligible <- is.na(fund_idx) & new_dates >= ann_dt$filed_date[k]
    fund_idx[eligible] <- k
  }

  # PASS B (all rows, annual + quarterly), keyed on period_end DESC with
  # the same greedy amendment-safe iteration: latest reported period wins
  # where filed <= day. Feeds the quarterly-kept indicator columns, every
  # stub entering .compute_price_sensitive_vec, and filed_date_q. On
  # annual-only layers pass B IS pass A (same rows, same order -- one row
  # per fiscal year makes -fiscal_year and -period_end identical sorts),
  # so the mapping is reused rather than recomputed.
  if (has_q) {
    pb_dt <- copy(fund_dt)
    pb_dt[, period_end := as.Date(period_end)]
    setorder(pb_dt, -period_end)
    fund_idx_b <- rep(NA_integer_, n_days)
    for (k in seq_len(nrow(pb_dt))) {
      eligible <- is.na(fund_idx_b) & new_dates >= pb_dt$filed_date[k]
      fund_idx_b[eligible] <- k
    }
  } else {
    pb_dt <- ann_dt
    fund_idx_b <- fund_idx
  }

  # Remove dates before any annual filing or with NA price
  valid <- !is.na(fund_idx) & !is.na(new_prices)
  if (sum(valid) == 0) return(existing_dt)

  new_dates  <- new_dates[valid]
  new_prices <- new_prices[valid]
  fund_idx   <- fund_idx[valid]
  fund_idx_b <- fund_idx_b[valid]

  # -- Per-share stubs on the day's split basis (D1 fix) --
  # Shares: most recent per-share layer observation with filed <= day
  # (10-Q cadence), falling back to the pass-B row's stub (the FY stub
  # on legacy layers; the latest filed Q/FY row on mixed-cadence
  # layers). EPS: the pass-B row's stub (annual EPS on FY rows; TTM EPS
  # on quarterly rows, where pe_trailing becomes an honest trailing
  # P/E). Both are as-filed values whose split basis is the split state
  # at their filed date; map them onto each day's as-traded basis so
  # mc = price x shares and pe = price / eps hold exactly across
  # splits. .split_factor(t) is the product of ratios of splits AFTER
  # t, so basis f -> basis d is x S(d)/S(f) for counts and x S(f)/S(d)
  # for per-share amounts. This basis mapping applies to pass-B stubs
  # identically on both layer generations.
  n_new <- length(new_dates)
  sh_val   <- rep(NA_real_, n_new)
  sh_basis <- rep(as.Date(NA), n_new)

  pershare_path <- file.path(ts_dir, sprintf("%s_pershare.parquet", ticker))
  if (file.exists(pershare_path)) {
    ps <- tryCatch(as.data.table(arrow::read_parquet(pershare_path)),
                   error = function(e) NULL)
    if (!is.null(ps) && nrow(ps) > 0) {
      ps[, filed_date := as.Date(filed_date)]
      setorder(ps, -filed_date)
      for (k in seq_len(nrow(ps))) {
        elig <- is.na(sh_val) & new_dates >= ps$filed_date[k]
        if (!any(elig)) next
        sh_val[elig]   <- ps$shares[k]
        sh_basis[elig] <- ps$filed_date[k]
      }
    }
  }

  # Pass-B-stub fallback, and staleness guard: when the stub in effect
  # is NEWER than the last per-share observation (a stalled per-share
  # series -- share tags dropped from later vintages), the stub count
  # is the fresher basis and must win. Without this, one extraction gap
  # freezes the share count forever.
  fund_filed <- pb_dt$filed_date[fund_idx_b]
  fb <- is.na(sh_val) | (!is.na(fund_filed) & sh_basis < fund_filed &
                           !is.na(as.numeric(pb_dt$stub_shares[fund_idx_b])))
  if (any(fb)) {
    sh_val[fb]   <- as.numeric(pb_dt$stub_shares[fund_idx_b])[fb]
    sh_basis[fb] <- fund_filed[fb]
  }

  sf_day    <- .split_factor(new_dates, splits)
  shares_adj <- sh_val * sf_day / .split_factor(sh_basis, splits)

  eps_basis <- pb_dt$filed_date[fund_idx_b]
  eps_adj   <- as.numeric(pb_dt$stub_eps[fund_idx_b]) *
    .split_factor(eps_basis, splits) / sf_day

  # -- Build result data.table --
  # fiscal_year / filed_date come from pass A (latest ANNUAL filing);
  # filed_date_q is the pass-B row's filing date (= filed_date on
  # annual-only layers, steps quarterly on mixed-cadence layers).
  result_dt <- data.table(
    date         = new_dates,
    price        = new_prices,
    fiscal_year  = ann_dt$fiscal_year[fund_idx],
    filed_date   = ann_dt$filed_date[fund_idx],
    filed_date_q = pb_dt$filed_date[fund_idx_b]
  )

  # Pass-B provenance stamps, mixed-cadence layers only: which reported
  # period (and cadence) the pass-B row in effect carries. Gated on
  # has_q so legacy and annual-only daily outputs stay byte-identical.
  if (has_q) {
    set(result_dt, j = "period_end_q",
        value = as.Date(pb_dt$period_end[fund_idx_b]))
    set(result_dt, j = "quarter_q",
        value = as.character(pb_dt$quarter[fund_idx_b]))
  }

  # -- Map fundamental-only indicators (plus hidden cross-section
  #    ingredients for the herf / ww_index finalization) from fund layer:
  #    quarterly-kept columns from the pass-B row, all others from the
  #    pass-A (annual) row --
  for (col in c(.FUNDAMENTAL_INDICATORS, .CS_INGREDIENT_COLS)) {
    q_col <- col %in% .Q_KEPT_INDICATORS
    src <- if (q_col) pb_dt else ann_dt
    idx <- if (q_col) fund_idx_b else fund_idx
    if (col %in% names(src)) {
      set(result_dt, j = col, value = as.numeric(src[[col]][idx]))
    } else {
      set(result_dt, j = col, value = NA_real_)
    }
  }

  # -- Standalone-quarter (q3m_*) ingredient columns from the pass-B
  #    row: only the columns the fund layer carries (quarterly builds),
  #    and only on mixed-cadence layers -- legacy and annual-only
  #    layers create no q3m column at all --
  if (has_q) {
    for (col in grep("^q3m_", names(pb_dt), value = TRUE)) {
      set(result_dt, j = col, value = as.numeric(pb_dt[[col]][fund_idx_b]))
    }
  }

  # -- Vectorized price-sensitive indicator computation --
  # ALL stubs come from the pass-B row (annual row on legacy layers;
  # latest filed Q/FY row on mixed-cadence layers, so valuation ratios
  # refresh quarterly). shares/eps arrive on the day's split basis (see
  # above); every other stub is an aggregate dollar amount and
  # split-invariant. Stubs a quarterly row leaves NA (equity_issuance,
  # rnd, cf_num, z/kz prefixes) NA their dependents on those days.
  price_dt <- .compute_price_sensitive_vec(
    p       = new_prices,
    shares  = shares_adj,
    eps     = eps_adj,
    equity  = as.numeric(pb_dt$stub_equity[fund_idx_b]),
    rev     = as.numeric(pb_dt$stub_revenue[fund_idx_b]),
    fcf_v   = as.numeric(pb_dt$stub_fcf[fund_idx_b]),
    ebitda  = as.numeric(pb_dt$stub_ebitda[fund_idx_b]),
    td      = as.numeric(pb_dt$stub_total_debt[fund_idx_b]),
    nd      = as.numeric(pb_dt$stub_net_debt[fund_idx_b]),
    div     = as.numeric(pb_dt$stub_dividends[fund_idx_b]),
    buy     = as.numeric(pb_dt$stub_buybacks[fund_idx_b]),
    eps_g   = result_dt$eps_growth_yoy,  # from fundamental indicators
    # pre-Wave-2 / pre-Wave-4 fund layers lack these stubs -> NA columns
    iss     = .stub_or_na(pb_dt, "stub_equity_issuance", fund_idx_b),
    rnd     = .stub_or_na(pb_dt, "stub_rnd", fund_idx_b),
    at_v    = .stub_or_na(pb_dt, "stub_assets", fund_idx_b),
    lt_v    = .stub_or_na(pb_dt, "stub_liabilities", fund_idx_b),
    cf_num  = .stub_or_na(pb_dt, "stub_cf_num", fund_idx_b),
    cfo     = .stub_or_na(pb_dt, "stub_cfo", fund_idx_b),
    che     = .stub_or_na(pb_dt, "stub_che", fund_idx_b),
    ncash   = .stub_or_na(pb_dt, "stub_net_cash", fund_idx_b),
    ndp_num = .stub_or_na(pb_dt, "stub_ndp_num", fund_idx_b),
    z_prefix  = .stub_or_na(pb_dt, "stub_z_prefix", fund_idx_b),
    kz_prefix = .stub_or_na(pb_dt, "stub_kz_prefix", fund_idx_b)
  )

  # Bind price-sensitive columns into result
  for (col in names(price_dt)) {
    set(result_dt, j = col, value = price_dt[[col]])
  }

  # -- Combine with existing and save --
  if (!is.null(existing_dt) && nrow(existing_dt) > 0) {
    combined <- rbindlist(list(existing_dt, result_dt), fill = TRUE)
  } else {
    combined <- result_dt
  }

  setorder(combined, date)
  arrow::write_parquet(combined, daily_path)

  combined
}


# =============================================================================
# SECTION 5: build_timeseries()  --  Historical build
# =============================================================================
#' Build complete daily time series for all tickers
#'
#' Initial historical build. For each ticker: builds the fundamentals layer
#' (if missing) then builds the daily layer from start_date through end_date.
#' Resumable: skips existing fundamentals layers, appends only new dates to
#' daily layers.
#'
#' @param start_date Character. Earliest date (default "2010-01-04").
#' @param end_date Date. Latest date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_dir Character. Price cache directory.
#' @param ts_dir Character. Time series output directory.
#' @param tickers Character vector or NULL. Specific tickers to process.
#'   NULL = all tickers with cached fundamentals.
#' @return Invisible list of stats.
build_timeseries <- function(start_date  = "2010-01-04",
                             end_date    = Sys.Date(),
                             master_path = "cache/lookups/constituent_master.parquet",
                             sector_path = "cache/lookups/sector_industry.parquet",
                             fund_dir    = "cache/fundamentals",
                             price_dir   = "cache/prices",
                             ts_dir      = .TS_DIR,
                             tickers     = NULL) {

  t0 <- Sys.time()
  message("build_timeseries: starting historical build...")

  if (!exists("load_ticker_splits")) {
    warning(paste0("build_timeseries: ttm_eps.R not sourced -- daily prices ",
                   "fall back to split-adjusted Close (mis-scaled before a ",
                   "ticker's splits) and no eps_ttm augment will run"),
            call. = FALSE)
  }

  # Load lookups. Sectors resolve to the build-time view (SCD date-join,
  # D2/B): fund layers are built once with one sector, so the latest
  # classification as of the build horizon is the correct label.
  master  <- as.data.table(arrow::read_parquet(master_path))
  sectors <- .sector_asof(as.data.table(arrow::read_parquet(sector_path)),
                          as.Date(end_date))

  # Determine tickers to process: every roster ticker whose CIK has a
  # fundamentals cache file. The filename ticker suffix alone misses
  # shared-CIK listings (the fetch dedups by CIK, so 0001437107_DISCA
  # also serves DISCK and WBD; get_fundamentals resolves the alias).
  if (is.null(tickers)) {
    fund_files <- .drop_drive_conflict_paths(
      list.files(fund_dir, pattern = "\\.parquet$"))
    file_tks   <- gsub("^\\d+_(.+)\\.parquet$", "\\1", fund_files)
    file_ciks  <- gsub("^(\\d+)_.+\\.parquet$", "\\1", fund_files)
    tickers <- union(file_tks, master[cik %in% file_ciks, ticker])
  }

  # Merge metadata
  ticker_dt <- data.table(ticker = tickers)
  ticker_dt <- merge(ticker_dt, master[, .(ticker, cik)],
                     by = "ticker", all.x = TRUE)
  ticker_dt <- merge(ticker_dt, sectors[, .(ticker, sector, industry)],
                     by = "ticker", all.x = TRUE)
  ticker_dt[is.na(sector), sector := "Unknown"]
  ticker_dt[is.na(industry), industry := "Unknown"]

  n <- nrow(ticker_dt)
  message(sprintf("  tickers: %d, range: %s to %s", n, start_date, end_date))

  n_fund_built <- 0L; n_fund_cached <- 0L
  n_daily_ok <- 0L; n_fail <- 0L

  for (i in seq_len(n)) {
    tk  <- ticker_dt$ticker[i]
    cik <- ticker_dt$cik[i]
    sec <- ticker_dt$sector[i]

    if (i %% 50 == 0 || i == 1) {
      message(sprintf("  [%d/%d] %s", i, n, tk))
    }

    # Step 1: Build fundamentals layer (skip if exists)
    fund_path <- file.path(ts_dir, sprintf("%s_fund.parquet", tk))
    if (file.exists(fund_path)) {
      n_fund_cached <- n_fund_cached + 1L
    } else {
      fund_result <- tryCatch(
        build_ticker_fundamentals(tk, cik, sec, fund_dir, ts_dir),
        error = function(e) {
          warning(sprintf("  fund failed: %s -- %s", tk, e$message),
                  call. = FALSE)
          NULL
        }
      )
      if (!is.null(fund_result)) {
        n_fund_built <- n_fund_built + 1L
      } else {
        n_fail <- n_fail + 1L
        next
      }
    }

    # Step 2: Update daily layer
    daily_result <- tryCatch(
      update_ticker_daily(tk, through_date = end_date,
                          start_date = as.Date(start_date),
                          price_dir = price_dir, ts_dir = ts_dir),
      error = function(e) {
        warning(sprintf("  daily failed: %s -- %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )

    if (!is.null(daily_result)) {
      n_daily_ok <- n_daily_ok + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)

  message(sprintf(paste0(
    "\nbuild_timeseries: done in %.1f min\n",
    "  fund layers: %d built, %d cached\n",
    "  daily layers: %d ok, %d failed (of %d)"),
    elapsed, n_fund_built, n_fund_cached, n_daily_ok, n_fail, n))

  # -- Augment daily layer with split-adjusted trailing-TTM EPS + P/E -----------
  # Existence-gated (no-op unless ttm_eps.R is sourced), mirroring the pipeline's
  # build_all_features hook. Runs once, after every _daily parquet is (re)built,
  # so the analytics ingest's eps_ttm/pe_ttm columns survive a full rebuild rather
  # than needing a manual augment_daily_ttm() call. Split factors are cached
  # (cache/splits), so re-runs hit no network. See ttm_eps.R.
  if (exists("augment_daily_ttm", mode = "function")) {
    message("\n  augmenting daily layer: eps_ttm / pe_ttm (split-adjusted)...")
    tryCatch(augment_daily_ttm(fund_dir = fund_dir, ts_dir = ts_dir),
             error = function(e)
               warning(sprintf("augment_daily_ttm failed: %s", e$message), call. = FALSE))
  }

  invisible(list(
    n_fund_built  = n_fund_built,
    n_fund_cached = n_fund_cached,
    n_daily_ok    = n_daily_ok,
    n_fail        = n_fail,
    elapsed_min   = elapsed
  ))
}


# =============================================================================
# SECTION 6: update_all_daily()  --  Daily incremental
# =============================================================================
#' Incremental daily update for all tickers
#'
#' For each ticker with a fundamentals layer, updates the daily layer
#' through the specified date. Set refresh_prices=TRUE (default) to
#' re-download Yahoo price data so the latest close is available.
#'
#' @param through_date Date. Update through this date (default today).
#' @param ts_dir Character. Time series directory.
#' @param price_dir Character. Price cache directory.
#' @param refresh_prices Logical. Re-download price data for all tickers.
#' @param master_path Character. Path to constituent_master.parquet (for
#'   new tickers that might need fundamentals built).
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @return Invisible list of stats.
update_all_daily <- function(through_date   = Sys.Date(),
                             ts_dir         = .TS_DIR,
                             price_dir      = "cache/prices",
                             refresh_prices = TRUE,
                             master_path    = "cache/lookups/constituent_master.parquet",
                             sector_path    = "cache/lookups/sector_industry.parquet",
                             fund_dir       = "cache/fundamentals") {

  t0 <- Sys.time()
  through_date <- as.Date(through_date)

  # Find tickers with fundamentals layers
  fund_files <- list.files(ts_dir, pattern = "_fund\\.parquet$")
  tickers <- gsub("_fund\\.parquet$", "", fund_files)

  n <- length(tickers)
  message(sprintf("update_all_daily: %d tickers through %s (refresh_prices=%s)",
                  n, through_date, refresh_prices))

  n_updated <- 0L; n_fail <- 0L

  for (i in seq_along(tickers)) {
    tk <- tickers[i]

    if (i %% 100 == 0 || i == 1) {
      message(sprintf("  [%d/%d] %s", i, n, tk))
    }

    result <- tryCatch(
      update_ticker_daily(tk, through_date = through_date,
                          price_dir = price_dir, ts_dir = ts_dir,
                          refresh_price = refresh_prices),
      error = function(e) {
        warning(sprintf("  failed: %s -- %s", tk, e$message), call. = FALSE)
        NULL
      }
    )

    if (!is.null(result)) {
      n_updated <- n_updated + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("update_all_daily: done in %.1f min -- %d updated, %d failed",
                  elapsed, n_updated, n_fail))

  # -- Re-augment daily layer with eps_ttm / pe_ttm (split-adjusted) ------------
  # Existence-gated; keeps the analytics columns fresh after an incremental run.
  # All-ticker (augment_daily_ttm is not per-ticker scoped); cheap on network
  # because split factors are cached, the cost is re-reading/writing the _daily
  # parquets. NOTE: a NEW split is only picked up once cache/splits/{tk}.parquet
  # is refreshed (delete it, or it is fetched fresh for tickers with no cache).
  if (exists("augment_daily_ttm", mode = "function")) {
    message("  augmenting daily layer: eps_ttm / pe_ttm (split-adjusted)...")
    tryCatch(augment_daily_ttm(fund_dir = fund_dir, ts_dir = ts_dir),
             error = function(e)
               warning(sprintf("augment_daily_ttm failed: %s", e$message), call. = FALSE))
  }

  invisible(list(
    n_updated   = n_updated,
    n_fail      = n_fail,
    elapsed_min = elapsed
  ))
}


# =============================================================================
# SECTION 7: load_daily_cross_section()  --  Cross-section reader
# =============================================================================
#' Load cross-sectional data for a specific date
#'
#' Reads individual ticker daily files and assembles a cross-section
#' (one row per ticker, all indicators) for the given date.
#'
#' @param target_date Date or character. The date to load.
#' @param ts_dir Character. Time series directory.
#' @param sector_path Character. Path to sector_industry.parquet (for metadata).
#' @param zscore Logical. Also compute and return z-scored version (default TRUE).
#' @return If zscore=FALSE: data.table. If zscore=TRUE: list with $raw, $zscored.
#'   NULL if no data found.
load_daily_cross_section <- function(target_date,
                                     ts_dir      = .TS_DIR,
                                     sector_path = "cache/lookups/sector_industry.parquet",
                                     zscore      = TRUE) {

  target_date <- as.Date(target_date)

  daily_files <- list.files(ts_dir, pattern = "_daily\\.parquet$",
                            full.names = TRUE)
  if (length(daily_files) == 0) {
    warning("load_daily_cross_section: no daily files found")
    return(NULL)
  }

  rows <- vector("list", length(daily_files))
  n_ok <- 0L

  for (f in daily_files) {
    tk <- gsub("_daily\\.parquet$", "", basename(f))

    dt <- tryCatch({
      d <- as.data.table(arrow::read_parquet(f))
      d[, date := as.Date(date)]
      d[date == target_date]
    }, error = function(e) NULL)

    if (!is.null(dt) && nrow(dt) > 0) {
      dt[, ticker := tk]
      n_ok <- n_ok + 1L
      rows[[n_ok]] <- dt[1]
    }
  }

  if (n_ok == 0) return(NULL)

  cs <- rbindlist(rows[seq_len(n_ok)], fill = TRUE)

  # Add sector/industry metadata, resolved as of the target date (SCD
  # date-join, fix D2/B)
  if (file.exists(sector_path)) {
    sec_dt <- .sector_asof(as.data.table(arrow::read_parquet(sector_path)),
                           target_date)
    cs <- merge(cs, sec_dt[, .(ticker, sector, industry)],
                by = "ticker", all.x = TRUE)
    cs[is.na(sector), sector := "Unknown"]
    cs[is.na(industry), industry := "Unknown"]

    # Finalize cross-section-stage indicators (ch_inv_ia demean,
    # Mohanram binarization, herf, ww_index): the per-ticker daily
    # layers store firm-level ingredients; the cross-section is where
    # sector medians / industry aggregates exist. Runs before z-scoring
    # so both raw and z-scored outputs carry the finalized values.
    # Also drops the hidden ingredient columns.
    industry_adjust_cross_section(cs)

    # Sector NA mask on the raw cross-section. Fundamental indicators
    # are already NA'd per ticker at compute time, but price-sensitive
    # sector-masked indicators (net_debt_price, z_score) are recomputed
    # daily from stubs frozen at fund-layer build -- masking here with
    # the CURRENT sector also covers tickers built before their sector
    # resolved.
    for (cn in intersect(names(.SECTOR_NA_COL_MAP), names(cs))) {
      m <- .sector_na_mask(cn, cs$sector)
      if (any(m)) set(cs, i = which(m), j = cn, value = NA_real_)
    }
  } else {
    # Without the sector lookup the finalize pass cannot run; a value
    # under a cross-section-finalized name must be the finalized
    # quantity, so degrade to NA rather than serving raw ingredients.
    warning("load_daily_cross_section: sector lookup missing; ",
            "cross-section-finalized indicators set to NA", call. = FALSE)
    for (cn in intersect(.CROSS_SECTION_FINALIZED, names(cs))) {
      set(cs, j = cn, value = NA_real_)
    }
    ing_cols <- intersect(.CS_INGREDIENT_COLS, names(cs))
    if (length(ing_cols)) cs[, (ing_cols) := NULL]
  }

  # Order columns: metadata, then indicators in canonical order
  ind_names  <- get_indicator_names()
  meta_cols  <- c("date", "ticker", "sector", "industry", "price",
                  "fiscal_year", "filed_date")
  present_meta <- intersect(meta_cols, names(cs))
  present_ind  <- intersect(ind_names, names(cs))
  other_cols   <- setdiff(names(cs), c(present_meta, present_ind))
  setcolorder(cs, c(present_meta, present_ind, other_cols))

  .assert_output_ts(cs, "load_daily_cross_section", list(
    "is data.table"     = is.data.table,
    "has ticker col"    = function(x) "ticker" %in% names(x),
    "no ingredient cols" = function(x) {
      !any(.CS_INGREDIENT_COLS %in% names(x))
    }
  ))

  if (!zscore) return(cs)

  # Z-score using the existing cross-sectional z-scoring function
  if ("sector" %in% names(cs) && length(present_ind) > 0) {
    zscore_input <- cs[, c("ticker", "sector", present_ind), with = FALSE]
    zscored_dt <- zscore_cross_section(zscore_input)
    setcolorder(zscored_dt, c("ticker", present_ind))

    return(list(raw = cs, zscored = zscored_dt))
  }

  cs
}


# =============================================================================
# SECTION 8: load_ticker_timeseries()  --  Per-ticker reader
# =============================================================================
#' Load daily time series for a single ticker
#'
#' @param ticker Character. Ticker symbol.
#' @param ts_dir Character. Time series directory.
#' @param from Date or character. Start date filter (optional).
#' @param to Date or character. End date filter (optional).
#' @return data.table, or NULL if file doesn't exist.
load_ticker_timeseries <- function(ticker, ts_dir = .TS_DIR,
                                   from = NULL, to = NULL) {

  path <- file.path(ts_dir, sprintf("%s_daily.parquet", ticker))
  if (!file.exists(path)) return(NULL)

  dt <- tryCatch(
    as.data.table(arrow::read_parquet(path)),
    error = function(e) NULL
  )
  if (is.null(dt) || nrow(dt) == 0) return(NULL)

  dt[, date := as.Date(date)]

  if (!is.null(from)) dt <- dt[date >= as.Date(from)]
  if (!is.null(to))   dt <- dt[date <= as.Date(to)]

  dt
}


# =============================================================================
# SECTION 9: list_timeseries_tickers()  --  Discovery utility
# =============================================================================
#' List tickers that have daily time series available
#'
#' @param ts_dir Character. Time series directory.
#' @return Character vector of ticker symbols, or character(0).
list_timeseries_tickers <- function(ts_dir = .TS_DIR) {
  if (!dir.exists(ts_dir)) return(character(0))
  files <- list.files(ts_dir, pattern = "_daily\\.parquet$")
  sort(gsub("_daily\\.parquet$", "", files))
}
