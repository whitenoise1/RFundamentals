# Independent Verification Report -- 57 Fundamental Indicators

Reviewer: independent replication against academic references
Target file: `R/indicator_compute.R`
Doc references: `docs/INDICATORS.md`, `docs/RESEARCH_INDICATORS.md`
Review date: 2026-04-19
Last updated: 2026-04-20 (C-1 rollout complete: universe sweep + 2 new
non-conformer classes resolved, 13 tickers rebuilt, alias map expanded)
Scope: formula correctness (constitution) and code replication (implementation fidelity)

---

## Executive Summary

Independent verification covered all 57 output fields in `.INDICATOR_NAMES`
(36 baseline + 5 Tier 1 ratios + 10 Piotroski + 6 Tier 2). Each indicator
was checked on three axes:

1. **Constitution** -- academic/textbook formula vs. formula in `INDICATORS.md`
2. **Replication** -- formula in doc vs. the R code that computes it
3. **Operational robustness** -- guards, sign conventions, PIT rules, NA paths

### Verdict counts (original, 2026-04-19)

| Verdict | Count | Meaning |
|---|---|---|
| PASS | 39 | Formula matches academic source; code matches formula; no material issue |
| PASS-WITH-NOTE | 13 | Implementation is defensible but deviates from textbook (e.g., point-in-time denominator instead of average). Documented and internally consistent |
| WARN | 5 | Methodological or operational concern that could bias the signal |
| FAIL | 0 | No indicator is wrong outright |

### Post-fix status (2026-04-20)

Of the 5 WARN items, **4 are now RESOLVED in code** (C-1, C-2, C-3, C-5).
The only remaining WARN is ROIC's NI-instead-of-NOPAT numerator.

Item #6 from §14 (pe_trailing sign guard) was re-framed at user direction:
the asymmetry is resolved by removing positive-denominator guards across
the indicator set (the sign-preservation principle) rather than NA-ing out
negative P/E. Smart winsorization (cross-sectional p2.5/p97.5) and
expanding-window median/MAD standardization tame the fat tails without
censoring the distress signal.

| Finding | Original | Current | Notes |
|---------|----------|---------|-------|
| C-1 fcf_stability YTD cumulation | WARN | **RESOLVED**        | 13 tickers rebuilt + universe sweep done + 2 new non-conformer classes (Kroger 16-week Q1, discontinued-ops CFO alias) fixed. See `07_cfo_cumulation_issue.md` §6 |
| C-2 Piotroski NA handling        | WARN | **RESOLVED**        | Implemented + tested |
| C-3 derive_quantities nafill(0)  | WARN | **RESOLVED**        | NA-vs-zero split implemented in `.derive_quantities` + EV fallback dropped + `ic` NA-propagates cash (2026-04-20) |
| C-4 avg vs end-of-period denom   | NOTE | NOTE (unchanged)    | Intentional |
| C-5 interest_coverage alias      | WARN | **RESOLVED**        | Alias dropped + cached-row filter |
| ROIC = NI / IC (not NOPAT)       | WARN | WARN (unchanged)    | Not yet addressed |
| §14 #6 pe_trailing sign guard    | NOTE | **RESOLVED** (reframed) | All positive-denominator guards removed across the indicator set. Negative multiples (P/B, P/FCF, EV/EBITDA, ROE, ROIC, debt/equity, net_debt/EBITDA, payout_ratio) now propagate as distress signals. Fat-tail protection moved to a cross-sectional p2.5/p97.5 winsorization + expanding-window median/MAD z-score |

### Cross-cutting findings (details in Section 12)

- **C-1 (FULLY RESOLVED, 2026-04-20)** `fcf_stability` uses quarterly `operating_cashflow`. US 10-Q filings report CFO on a year-to-date (YTD) cumulative basis, not stand-alone-quarter basis. If `fundamental_fetcher.R` does not de-cumulate, the SD calculation is dominated by the YTD growth within each fiscal year, not true CFO volatility. **Fix:** compute-time YTD de-cumulator (`.decumulate_cfo` in `R/indicator_compute.R`) + fetcher dedup duration-match tie-break (`R/fundamental_fetcher.R`). Full diagnostic + fix documented in `docs/research/07_cfo_cumulation_issue.md`. Rollout: 13 tickers rebuilt (AMZN, NVDA, QCOM, SJM, LKQ, ANF, HRB, FMC, TFX, ITT, DRI, KR, AAP). Full-universe sweep (`tools/diag_cfo_universe.R`) surfaced two additional classes, both fixed: (a) Kroger 16/12/12/12 fiscal calendar -- Q1 3mo band extended from 60-110 to 60-120; (b) 4 tickers filing the `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations` variant -- alias added. Post-fix: 459/638 CLEAN (71.9%), 0 ANOMALOUS_TICKER, 1 IFRS-only blackout (SII).
- **C-2 (RESOLVED, 2026-04-20)** `.compute_piotroski` returns 0 (not NA) when prior-year inputs are missing. Newly added S&P 500 names with only one year of EDGAR data get a downward-biased `f_score`. Policy should be: NA-mask the component, and NA-mask `f_score` if any component is NA (or switch to a robust subscore with explicit denominator). **Fix:** `.bin()` helper returns `NA_integer_` when inputs missing; `f_score` is `NA_integer_` if any component is NA. Documented in `docs/INDICATORS.md`; unit-tested.
- **C-3 (RESOLVED, 2026-04-20)** `.derive_quantities` previously used `nafill(.., 0)` for several inputs (depreciation, capex, long-term debt, short-term debt, cash). When the base concept was missing, the derived quantity silently became partially fabricated (e.g., CapEx = 0 -> FCF = OpCF; LTD = 0 and STD = 0 -> total_debt = 0 for firms that simply did not report any debt tag at that fiscal_year). This was structurally different from "the firm has no debt/capex". **Fix:** per-quantity semantic in `.derive_quantities`: `ebitda` is NA when depreciation absent, `fcf` is NA when capex absent, `total_debt` is NA when both LTD and STD absent (one-present-one-NA retained as zero to match filing practice), `net_debt` is NA when `total_debt` or cash is NA. EV fallback in `.compute_valuation` (which substituted total_debt when net_debt was NA -- same antipattern) also dropped. Invested capital in `.compute_profitability` stops silently treating missing cash as zero. Unit-tested (12 new tests in `tests/test_indicator_compute.R`).
- **C-4 (PASS-WITH-NOTE)** ROA, ROIC, asset_turnover, inventory_turnover, receivables_turnover, GP/A all use end-of-period denominators rather than the academic convention of average ((t + t-1)/2). Internally consistent with the docs; but cross-paper comparability of alpha will be slightly biased for fast-growing firms.
- **C-5 (RESOLVED, 2026-04-20)** `interest_coverage` accepts the alias `InterestIncomeExpenseNet`, which can be negative (net interest income) for firms with large cash piles. Dividing OpInc by a negative net-interest figure produces a negative coverage ratio that is not comparable to the standard EBIT/InterestExpense signal. Either restrict to gross `InterestExpense` / `InterestExpenseDebt`, or take `abs()`. **Fix:** `InterestIncomeExpenseNet` removed from the alias chain in `R/fundamental_fetcher.R`; `.filter_deprecated_tags()` drops pre-existing cached rows on read; unit-tested.

---

## 1. Valuation (8 indicators)

### 1.1 `pe_trailing` -- Price / EPS (diluted)

| Check | Result |
|---|---|
| Academic formula | P / EPS (Graham, Damodaran). PASS |
| Doc formula | Price / EPS diluted, guard `|EPS| >= 0.01`. PASS |
| Code (L228) | `.safe_divide(price, eps, min_abs_denom = 0.01)`. Matches doc |
| Verdict | **PASS-WITH-NOTE** |

Note: guard allows negative EPS, producing negative P/E, which the paired guard on `pb` (equity > 0) and `pfcf` (FCF > 0) does *not* allow for their analogues. Inconsistent treatment; negative P/E values propagate into z-scoring as extreme low values. Consider aligning with `pb`/`pfcf` by requiring `eps > 0`, or keep but document the asymmetry.

Also note the name "trailing" is a misnomer: the code uses the latest annual EPS, not trailing-twelve-months (TTM = sum of last four quarters). For mid-fiscal-year snapshots, true TTM EPS is materially different.

### 1.2 `peg` -- P/E / (EPS growth x 100)

| Check | Result |
|---|---|
| Academic formula | Lynch PEG = PE / EPS_growth_pct. PASS |
| Doc formula | `P/E / (EPS Growth YoY x 100)`, guard growth > 0. PASS |
| Code (L745-750) | `.safe_divide(pe, eps_g * 100)` with `eps_g > 0` guard. Matches |
| Verdict | **PASS** |

### 1.3 `pb` -- Market Cap / Equity

| Check | Result |
|---|---|
| Academic formula | MC / Equity. PASS |
| Doc formula | Matches. PASS |
| Code (L229) | `if (equity > 0) safe_divide(MC, equity)`. Matches |
| Verdict | **PASS** |

### 1.4 `ps` -- Market Cap / Revenue

| Check | Result |
|---|---|
| Academic formula | MC / Revenue. PASS |
| Code (L230) | `safe_divide(MC, rev)`. Matches |
| Verdict | **PASS** |

### 1.5 `pfcf` -- Market Cap / FCF

| Check | Result |
|---|---|
| Academic formula | MC / FCF, positive-FCF only. PASS |
| Code (L231) | `if (fcf > 0) safe_divide(MC, fcf)`. Matches |
| FCF definition | `OpCF - CapEx` via `.derive_quantities`. Correct |
| Verdict | **PASS** |

### 1.6 `ev_ebitda` -- EV / EBITDA

| Check | Result |
|---|---|
| Academic formula | (MC + Net Debt) / EBITDA. PASS |
| EBITDA def | `OperatingIncome + D&A` via `.derive_quantities`. Correct: GAAP OpIncome already subtracts all D&A, adding it back reconstructs EBITDA |
| Code (L232) | `if (ebitda > 0) safe_divide(ev, ebitda)`. Matches |
| Verdict | **PASS-WITH-NOTE** |

Note on EV: code uses `EV = MC + Net Debt`. Textbook EV also adds minority interest + preferred stock. Code omits both. For S&P 500 large caps this is usually immaterial but not guaranteed (e.g., firms with non-trivial non-controlling interests).

### 1.7 `ev_revenue` -- EV / Revenue

| Check | Result |
|---|---|
| Code (L233) | `safe_divide(ev, rev)`. Matches |
| Verdict | **PASS-WITH-NOTE** (same EV caveat) |

### 1.8 `earnings_yield` -- EPS / Price

| Check | Result |
|---|---|
| Academic formula | 1 / P/E. PASS |
| Code (L234) | `safe_divide(eps, price, min_abs_denom = 0.01)`. Matches |
| Verdict | **PASS** |

---

## 2. Profitability (6 indicators)

### 2.1 `gross_margin` = GP / Revenue

| Check | Result |
|---|---|
| Code (L259) | `safe_divide(gp, rev)`. Matches |
| Verdict | **PASS** |

### 2.2 `operating_margin` = OpInc / Revenue

| Code (L260) | Matches | **PASS** |

### 2.3 `net_margin` = NI / Revenue

| Code (L261) | Matches | **PASS** |

### 2.4 `roe` = NI / Equity (guard equity > 0)

| Code (L262) | Matches | **PASS** |

### 2.5 `roa` = NI / Total Assets

| Check | Result |
|---|---|
| Academic convention | Often NI / AVG Assets, sometimes NI / A_t |
| Code (L263) | `safe_divide(ni, assets)` -- uses end-of-period assets |
| Doc | "Net Income / Total Assets" (end-of-period), consistent with code |
| Verdict | **PASS-WITH-NOTE** (see C-4) |

### 2.6 `roic` = NI / Invested Capital (guard IC > 0)

| Check | Result |
|---|---|
| Textbook formula | NOPAT / IC, where NOPAT = OpInc x (1 - tax rate), IC = Equity + Debt - Cash |
| Doc / code | NI / IC (not NOPAT). IC = `equity + total_debt - cash` |
| Verdict | **WARN** |

Using NI rather than NOPAT confounds tax policy and non-operating items into the numerator. For cross-sectional ranking this biases against high-tax and highly-leveraged firms. Recommend switching to NOPAT (approximate via `opinc * (1 - effective_tax_rate)` or use `opinc * (1 - 0.21)` assuming statutory rate).

---

## 3. Growth (5 indicators)

### 3.1 `revenue_growth_yoy` = (R_t - R_{t-1}) / |R_{t-1}|

| Code (L273) via `.safe_growth` | Matches | **PASS** |

The `abs()` in the denominator handles sign-changing prior periods correctly (otherwise a firm going from -$10M to +$10M would show growth of -2 instead of +2).

### 3.2 `revenue_growth_qoq` = (R_q - R_{q-1}) / |R_{q-1}|

| Check | Result |
|---|---|
| Code (L280-283) | Uses immediately-preceding quarter (Q3 vs. Q2, Q1 vs. prior Q4) |
| Seasonality | NOT adjusted |
| Verdict | **PASS-WITH-NOTE** |

Most production factor libraries use YoY-same-quarter (Q3_t vs. Q3_{t-1}) to remove seasonality. The code's sequential QoQ is useful as a *momentum* signal but should not be interpreted as pure growth. Doc acknowledges this.

### 3.3 `eps_growth_yoy` | Code (L274) | Matches | **PASS** |
### 3.4 `opinc_growth_yoy` | Code (L275) | Matches | **PASS** |
### 3.5 `ebitda_growth` | Code (L277) | Matches | **PASS** |

---

## 4. Leverage (5 indicators)

### 4.1 `debt_equity` = Total Debt / Equity (guard equity > 0)

| Code (L303) | Matches | **PASS** |

Total Debt = LTD + STD (both NA->0). See C-3 for caveat on missing tags.

### 4.2 `net_debt_ebitda` = Net Debt / EBITDA (guard EBITDA > 0)

| Code (L304) | Matches | **PASS** |

### 4.3 `interest_coverage` = OpInc / Interest Expense

| Check | Result |
|---|---|
| Academic formula | EBIT / Interest Expense |
| Code (L305) | `safe_divide(opinc, intexp, min_abs_denom = 1)` |
| Alias list (INDICATORS.md) | `InterestExpense`, `InterestExpenseDebt`, `InterestIncomeExpenseNet` |
| Verdict | **WARN** (C-5) |

The `InterestIncomeExpenseNet` alias can be negative for cash-rich firms (Apple, Google) -- net interest income. Dividing OpInc by a negative number flips the sign of the coverage ratio. Recommend: drop `InterestIncomeExpenseNet` from the alias chain, or wrap `intexp` in `abs()`.

### 4.4 `current_ratio` = CA / CL | **PASS** |
### 4.5 `quick_ratio` = (CA - Inv) / CL | **PASS-WITH-NOTE** |

Quick ratio variant used: (CA - Inventory) / CL. Stricter classical form subtracts prepaid expenses as well. Deviation is minor and consistent with the doc.

---

## 5. Efficiency (3 indicators)

### 5.1 `asset_turnover` = Rev / Assets
### 5.2 `inventory_turnover` = COGS / Inventory (NA for financials)
### 5.3 `receivables_turnover` = Rev / AR

All three use end-of-period denominators instead of averages -- consistent with doc.
| Code (L323-325) | Matches | **PASS-WITH-NOTE** (C-4) |

---

## 6. Cash Flow Quality (3 indicators)

### 6.1 `fcf_ni` = FCF / NI (guard |NI| >= 1) | **PASS** |
### 6.2 `opcf_ni` = OpCF / NI | **PASS** |
### 6.3 `capex_revenue` = CapEx / Revenue | **PASS** |

Sign convention: `PaymentsToAcquirePropertyPlantAndEquipment` is positive (payments). `.derive_quantities` computes FCF = OpCF - CapEx using this positive CapEx. Consistent.

---

## 7. Shareholder Return (3 indicators)

### 7.1 `dividend_yield` = |Div| / MCap | **PASS** |
### 7.2 `payout_ratio` = |Div| / NI (guard NI > 0) | **PASS** |
### 7.3 `buyback_yield` = |Buyback| / MCap | **PASS** |

`abs()` on dividends/buybacks correctly handles XBRL sign convention (cash-flow outflows are negative).

---

## 8. Size (3 indicators)

### 8.1 `market_cap` = Price x Shares | **PASS** |
### 8.2 `enterprise_value` = MCap + Net Debt | **PASS-WITH-NOTE** (no minority interest/preferred) |
### 8.3 `revenue_raw` = Revenue | **PASS** |

---

## 9. Tier 1 Research (5 + 10 Piotroski)

### 9.1 `gpa` = Gross Profit / Total Assets (NA for financials)

| Ref | Novy-Marx (2013, JFE) |
| Code (L386) | `.safe_divide(gp, assets)` -- current assets, not average |
| Novy-Marx paper | Uses current-period assets (not averaged). Match |
| Verdict | **PASS** |

### 9.2 `asset_growth` = (A_t - A_{t-1}) / A_{t-1}

| Ref | Cooper, Gulen, Schill (2008, JF) |
| Code (L388) | `.safe_growth(assets, p_assets)` = `(assets - p_assets)/abs(p_assets)` |
| Sign | Assets are always non-negative; abs() is harmless |
| Verdict | **PASS** |

### 9.3 `sloan_accrual` = (NI - CFO) / Avg Total Assets

| Ref | Sloan (1996), CF version per Hribar & Collins (2002) |
| Code (L381-393) | `avg_assets = (assets + p_assets)/2; sloan = (ni - cfo)/avg_assets`. Matches |
| Verdict | **PASS** |

### 9.4 `pct_accruals` = (NI - CFO) / |NI|

| Ref | Hafzalla, Lundholm, Van Winkle (2011) |
| Code (L395-396) | `safe_divide(ni - cfo, abs(ni), min_abs_denom = 1)`. Matches |
| Verdict | **PASS** |

### 9.5 `net_operating_assets` = (OA - OL) / Lagged Total Assets

where OA = Assets - Cash, OL = TotalLiab - TotalDebt.

| Ref | Hirshleifer, Hou, Teoh, Zhang (2004) |
| Code (L398-402) | Matches step-by-step |
| Verdict | **PASS** |

### 9.6 Piotroski F-Score (9 binary components + composite)

Reference: Piotroski (2000, JAR).

| Component | Academic rule | Code | Verdict |
|---|---|---|---|
| `f_roa` | ROA > 0 | L446 `roa > 0` | PASS |
| `f_droa` | ROA_t > ROA_{t-1} | L447 `roa > p_roa` | PASS |
| `f_cfo` | CFO > 0 (or CFO/Assets > 0) | L448 `cfo/assets > 0` | PASS (equivalent when assets > 0) |
| `f_accrual` | CFO > NI | L449 `cfo > ni` | PASS |
| `f_dlever` | LTD/A decreased YoY | L452-454 | PASS |
| `f_dliquid` | CA/CL increased YoY | L456-458 | PASS |
| `f_eq_off` | shares_t <= shares_{t-1} | L460 | PASS |
| `f_dmargin` | GM_t > GM_{t-1} | L463-465 | PASS |
| `f_dturn` | AT_t > AT_{t-1} | L467-469 | PASS |
| `f_score` | sum of 9 | L472-474 | PASS |

Components individually PASS. Composite **WARN** because of NA handling:

**Issue (C-2)**: any missing prior-year input yields component = 0 (via the
`if (!is.na(x) && x > y) 1L else 0L` pattern). A firm with only one year of
data can have at most 4 passes (`f_roa`, `f_cfo`, `f_accrual`, and possibly
`f_eq_off` -- the latter also requires prior shares, so 3). This creates a
strong survivorship / data-availability bias at the low-score end.

Recommended fix options:
1. Return `NA_integer_` for components with missing inputs; return
   `NA_integer_` for `f_score` if any component is NA.
2. Alternatively, return `f_score` as a proportion: `sum(non-NA 1s) / count(non-NA components)`, so scores are comparable across data-coverage regimes.

---

## 10. Tier 2 Research (6 indicators)

### 10.1 `cash_based_op` (NA for financials)

Formula: `(Rev - COGS - SGA + R&D - dAR - dINV - dPrepaid + dDefRev + dAP + dAccruedLiab) / Assets`.

| Ref | Ball, Gerakos, Linnainmaa, Nikolaev (2016, JFE) |
| Signs in code (L505-512) | -AR, -INV, -Prepaid, +DefRev, +AP, +AccruedLiab. Matches |
| R&D handling | If NA, add 0 (no add-back). Defensible |
| Working capital items | Partial computation: skip an item if either current or prior is NA. Doc authorizes this |
| Denominator | End-of-period Assets |
| Verdict | **PASS-WITH-NOTE** |

Note: Ball et al. paper uses average assets for scaling. Code uses current assets. Minor deviation, consistent with doc.

### 10.2 `fcf_stability` -- SD(CFO/Assets) over trailing 8-16 quarters

| Ref | Huang (2009, JEF) |
| Code (L524-536) | Uses quarterly `operating_cashflow` and `total_assets`, computes `sd(cfo/assets)` over last up-to-16 quarters |
| Verdict | **WARN** (C-1) |

Three concerns:

1. **Name mismatch**: indicator is `fcf_stability` but uses CFO, not FCF. Huang's paper uses CFO volatility, so the formula matches the source; the variable name is the problem.
2. **YTD vs. stand-alone quarter**: XBRL quarterly CFO from 10-Q is cumulative year-to-date in US GAAP. If `fundamental_fetcher.R` stores Q1/Q2/Q3 as YTD, the resulting time series (Q1=YTD1Q, Q2=YTD2Q, Q3=YTD3Q, Q4 derived or reported as annual FY) has an artificial monotone pattern within each fiscal year. SD computed over this series measures YTD build-up, not true cash flow volatility. This is the highest-impact single finding in this review. **Action required**: verify the fetcher de-cumulates quarterlies (`Q2_standalone = YTD_Q2 - YTD_Q1`, etc.), or implement the de-cumulation in `.compute_tier2` before computing SD.
3. **Minimum-quarter threshold**: doc mentions 12 quarters preferred, code accepts 8. Acceptable, but recently added S&P 500 names with exactly 8 quarters produce noisier SDs.

### 10.3 `sga_efficiency` = %d(SGA) - %d(Revenue)

| Ref | Lev & Thiagarajan (1993, JAR) |
| Code (L539-544) | `sga_g - rev_g` via `.safe_growth`. Matches |
| Verdict | **PASS** |

### 10.4 `capex_depreciation` = CapEx / D&A (capped at 5x; NA for financials)

| Ref | Titman, Wei, Xie (2004, JFQA) |
| Code (L547-551) | `safe_divide(capex, depr); if result > 5 -> 5`. Matches doc |
| Verdict | **PASS-WITH-NOTE** |

Note: doc also specifies "Set to NA when both are below materiality threshold" for asset-light firms. Code does not implement a materiality floor; both-small-numbers produce noisy ratios. Consider adding a threshold (e.g., NA if both < 0.1% of revenue). The cap at 5x partially mitigates the high-side, but not the near-zero denominator amplification.

### 10.5 `dso_change` = (AR_t/Rev_t - AR_{t-1}/Rev_{t-1}) x 365

| Ref | Beneish (1999, FAJ) -- DSRI component |
| Code (L553-563) | Computes DSO_c - DSO_p then multiplies by 365. Matches |
| Verdict | **PASS** |

### 10.6 `inventory_sales_change` = (Inv_t/COGS_t) - (Inv_{t-1}/COGS_{t-1})

| Ref | Thomas & Zhang (2002, RAS) |
| Code (L565-575) | Matches formula |
| Naming | Variable name suggests denominator = Sales, formula uses COGS. Doc explicitly confirms COGS is correct |
| Verdict | **PASS-WITH-NOTE** (naming confusion) |

Rename to `inventory_cogs_change` would remove ambiguity. Low priority.

---

## 11. Derived Quantities (`.derive_quantities`, L170-204)

| Quantity | Formula | Concern |
|---|---|---|
| `gross_profit` | `revenue - cogs` | OK |
| `ebitda` | `operating_income + depreciation (NA->0)` | If OpInc present but D&A NA, result = OpInc (i.e., EBIT, not EBITDA). Flag |
| `fcf` | `operating_cashflow - capex (NA->0)` | If CapEx NA, result = OpCF (overstates FCF). Flag |
| `total_debt` | `long_term_debt (NA->0) + short_term_debt (NA->0)` | Both missing -> 0. Same as "no debt". Flag |
| `net_debt` | `total_debt - cash (NA->0)` | Same cascading issue |

**C-3 WARN**. Recommended: treat missing component as missing (propagate NA) rather than silently zeroing. A firm that does not report LTD because it genuinely has none is indistinguishable in this code path from a firm whose XBRL tag was not captured. The fundamental fetcher should explicitly record which tags were attempted and failed, and derived quantities should use that presence map to decide between "true zero" and "NA".

---

## 12. Z-Scoring (`zscore_cross_section`, `zscore_expanding_window`)

**Updated 2026-04-20** to a robust standardization pipeline. Two entry
points share one core helper (`.robust_zscore` in `R/indicator_compute.R`):

| Step | Implementation |
|---|---|
| 1. Cross-sectional winsorization | Raw values clipped at per-snapshot `p2.5 / p97.5` |
| 2. Standardization location | `median` of calibration sample |
| 2. Standardization scale | `mad(., constant = 1.4826)` -- equal to `sd` under a Gaussian null |
| Calibration sample | `zscore_cross_section`: current cross-section; `zscore_expanding_window`: pooled winsorized values across all `pit_*_raw.parquet` with date < current plus current snapshot |
| Financial-NA masking | `.FINANCIAL_NA_INDICATORS` excluded from both winsorization and calibration; output NA-masked on the financial rows |
| Min N | `>= 3` non-NA required (caller-tunable) |
| Post-standardization clip | `c(-5, 5)` by default (see sec. 12a decision); `NULL` disables, `c(-3, 3)` for the tighter legacy bound. Diagnostic in `tools/diag_zscore_tails.R` is the source of truth for the final decision |
| NA preservation | Original NAs preserved in output |
| Verdict | **PASS** |

Design rationale:
- Pre-winsorize before computing location / scale so fat-tailed outliers
  cannot contaminate the standardization statistics.
- Median / MAD (scaled by 1.4826 to estimate sigma under a Gaussian
  null) is robust to the remaining within-band skew. On well-behaved
  indicators it coincides with mean / SD to within sampling noise.
- Expanding-window calibration stabilizes location / scale over time as
  evidence accumulates, avoiding single-quarter sampling noise.
  Per-snapshot winsorization bounds keep each cross-section's regime
  intact (a firm in the 2nd percentile this quarter gets pulled to p2.5
  of this quarter's distribution, not of a 10-year pool).
- Post-standardization clip default is `c(-5, 5)` (chosen 2026-04-20
  after running `tools/diag_zscore_tails.R`). See the decision note
  immediately below.

### 12a. Clip-default decision (2026-04-20)

The tail diagnostic built 8 quarterly snapshots (2024-Q1 through 2025-Q4)
using the expanding-window path with `clip = NULL`, then tabulated |z|
tails across ~140k cross-sectional observations.

**Unclipped pooled distribution:**
- p50 = 0.67, p90 = 2.87, p99 = 11.6, p99.9 = 23.2, max = 47.5.
- 9.43 % of values have |z| >= 3; 4.49 % have |z| >= 5.
- Tail asymmetry: 7.6 % of values above z = +3 vs. 1.8 % below z = -3
  (ratio 4.16 : 1 positive-heavy). The positive extremes are dominated
  by size / valuation / growth indicators (peg p999 = 47.5, pb = 29.1,
  market_cap = 18.1, enterprise_value = 20.3, revenue_raw = 19.6) --
  log-normal-by-construction, not distress. The negative extremes are
  dominated by genuine distress (negative book value, negative FCF,
  negative operating income).

**Options evaluated:**

| Option | Pros | Cons |
|---|---|---|
| A: `clip = c(-3, 3)` | Strong scale comparability (critical for factor blends: peg z at 47 would swamp gross_margin at 2). Robust to single-firm noise. Matches institutional precedent (Barra / Axioma / Qontigo). Preserves full rank ordering inside the 90.5 % of values within the band. | 9.46 % of values collapse onto exactly \|z\| = 3. For `peg`, 20 % of firms get the same score; for `pb`, 19 %; for `market_cap`, 15 %. Rank inside those buckets is destroyed. Damage is asymmetric: censors 1.8 % of distress-heavy negative tail alongside 7.6 % of noise-heavy positive tail. Treats symptom (heavy tails), not cause (log-normal size indicators). |
| B: `clip = c(-5, 5)` | Still scale-bounded for factor combinations. Negative tail effectively preserved (~0.15 % clipped). Positive tail in [3, 5] differentiable -- recovers ~5 % of values flattened by A. Honors the "negatives carry distress signal" principle. | ~4.49 % of values still saturate at 5; `peg` still pins 13 % at the bound. Threshold is arbitrary (why 5, not 4 or 6) -- no principled reason. Less institutional precedent. |
| C: `clip = NULL` | Full information fidelity. No arbitrary bound. Most consistent with "don't destroy information via clipping". | Size indicators dominate factor combinations (peg z at 47 swamps quieter signals). One quirky near-zero denominator moves aggregate factor scores. Diagnostic shows p999 = 23 is systemic, not a few outliers. Unsafe downstream until size / valuation indicators are log-transformed. |

**Decision:** default is **B, `clip = c(-5, 5)`**, as a stopgap. Rationale:

1. The tail asymmetry in the unclipped diagnostic makes A wrong-direction:
   A sacrifices 1.8 % of high-value distress granularity to also censor
   7.6 % of low-value size / growth puffery. B preserves virtually all
   of the distress granularity at the cost of a modest scale increase.
2. C is unsafe today because size / valuation indicators dominate the
   factor space without a log-transform.
3. B is an interim. The long-term answer is the §14 item #11 follow-up
   (log-transform size / valuation before winsorization); once that
   lands, re-run the diagnostic and, if p99.9 drops near 4 with
   symmetric tails, drop the clip (flip default to `NULL`).

Callers who want the aggressive bound can still pass `clip = c(-3, 3)`;
callers who want no clipping can pass `clip = NULL`.
- No sector-neutral z-scoring. For BSTAR, this may be intentional; the
  sector bucket is passed separately. Flag for downstream consumers:
  the z-scores are cross-sectional, not sector-neutral.

---

## 13. Summary Matrix

| # | Indicator | Constitution | Replication | Verdict |
|---|---|---|---|---|
| 1 | pe_trailing | OK | OK | PASS-WITH-NOTE (TTM misnomer; sign asymmetry **RESOLVED** 2026-04-20 via robust z-score) |
| 2 | peg | OK | OK | PASS |
| 3 | pb | OK | OK | PASS |
| 4 | ps | OK | OK | PASS |
| 5 | pfcf | OK | OK | PASS |
| 6 | ev_ebitda | OK | OK | PASS-WITH-NOTE (EV simplification) |
| 7 | ev_revenue | OK | OK | PASS-WITH-NOTE |
| 8 | earnings_yield | OK | OK | PASS |
| 9 | gross_margin | OK | OK | PASS |
| 10 | operating_margin | OK | OK | PASS |
| 11 | net_margin | OK | OK | PASS |
| 12 | roe | OK | OK | PASS |
| 13 | roa | OK (end-of-period, not avg) | OK | PASS-WITH-NOTE |
| 14 | roic | Uses NI not NOPAT | matches doc | **WARN** |
| 15 | revenue_growth_yoy | OK | OK | PASS |
| 16 | revenue_growth_qoq | Sequential not YoY-same-quarter | OK | PASS-WITH-NOTE |
| 17 | eps_growth_yoy | OK | OK | PASS |
| 18 | opinc_growth_yoy | OK | OK | PASS |
| 19 | ebitda_growth | OK | OK | PASS |
| 20 | debt_equity | OK | OK | PASS |
| 21 | net_debt_ebitda | OK | OK | PASS |
| 22 | interest_coverage | OK | Alias chain includes net-interest | **WARN** (C-5) |
| 23 | current_ratio | OK | OK | PASS |
| 24 | quick_ratio | Simpler variant | OK | PASS-WITH-NOTE |
| 25 | asset_turnover | End-of-period | OK | PASS-WITH-NOTE |
| 26 | inventory_turnover | End-of-period | OK | PASS-WITH-NOTE |
| 27 | receivables_turnover | End-of-period | OK | PASS-WITH-NOTE |
| 28 | fcf_ni | OK | OK | PASS |
| 29 | opcf_ni | OK | OK | PASS |
| 30 | capex_revenue | OK | OK | PASS |
| 31 | dividend_yield | OK | OK | PASS |
| 32 | payout_ratio | OK | OK | PASS |
| 33 | buyback_yield | OK | OK | PASS |
| 34 | market_cap | OK | OK | PASS |
| 35 | enterprise_value | simplified EV | OK | PASS-WITH-NOTE |
| 36 | revenue_raw | OK | OK | PASS |
| 37 | gpa | OK | OK | PASS |
| 38 | asset_growth | OK | OK | PASS |
| 39 | sloan_accrual | OK | OK | PASS |
| 40 | pct_accruals | OK | OK | PASS |
| 41 | net_operating_assets | OK | OK | PASS |
| 42-50 | f_roa..f_dturn | each OK | each OK | PASS (individually) |
| 51 | f_score | Piotroski composite | NA handled as 0 | **WARN** (C-2) |
| 52 | cash_based_op | OK | End-of-period denom | PASS-WITH-NOTE |
| 53 | fcf_stability | Name vs CFO source; YTD vs stand-alone | OK if fetcher de-cumulates | **WARN** (C-1) |
| 54 | sga_efficiency | OK | OK | PASS |
| 55 | capex_depreciation | OK; no materiality floor | OK | PASS-WITH-NOTE |
| 56 | dso_change | OK | OK | PASS |
| 57 | inventory_sales_change | OK | OK | PASS-WITH-NOTE (naming) |

---

## 14. Prioritized Recommendations

### Must-fix (correctness-impacting)

1. **[C-1] Verify quarterly CFO de-cumulation for `fcf_stability`.** Inspect `fundamental_fetcher.R` output for a representative ticker (e.g., AAPL) and confirm Q1/Q2/Q3 rows contain stand-alone-quarter values, not YTD. If YTD, de-cumulate in the tier-2 computation before running `sd()`.
2. **[C-2] Fix Piotroski NA handling.** When a prior-year input is missing, return `NA_integer_` for the component and `NA_integer_` for `f_score` (or implement proportional scoring). Otherwise low f_scores are conflated with "missing data".
3. **[C-5] Interest coverage alias chain.** Drop `InterestIncomeExpenseNet` from the interest_expense alias list, or wrap `intexp` in `abs()`. Current behavior flips sign for cash-rich firms.

### Should-fix (methodology)

4. **[ROIC] Switch numerator to NOPAT** (`opinc * (1 - tax_rate)`). Use statutory or effective rate. Current NI-based ROIC biases cross-section by tax and non-operating income.
5. **[C-3 RESOLVED 2026-04-20]** `.derive_quantities` now propagates NA from missing `depreciation`, `capex`, both-absent `long_term_debt + short_term_debt`, and `cash`. One-present / one-absent for LTD/STD retained as zero to match filing practice. EV fallback in `.compute_valuation` (which substituted `total_debt` for NA `net_debt`) dropped. `ic` in `.compute_profitability` stops silently treating missing cash as zero.
6. **[§14 #6 RESOLVED 2026-04-20, re-framed]** Rather than NA-ing out negative P/E, the asymmetry is resolved by removing positive-denominator guards across the indicator set (pb, pfcf, ev_ebitda, roe, roic, debt_equity, net_debt_ebitda, payout_ratio). Negative multiples now propagate as distress signals. Fat-tail protection handled by the new cross-sectional p2.5/p97.5 winsorization + expanding-window median/MAD z-score (Section 12).

### Nice-to-have

7. **[QoQ] Switch to YoY-same-quarter** for revenue_growth_qoq unless the sequential variant is explicitly wanted as a momentum signal.
8. **[capex_depreciation]** Add materiality threshold (e.g., NA if both CapEx and D&A are < 0.1% of revenue or of assets).
9. **[z-score RESOLVED 2026-04-20]** Implemented per-snapshot cross-sectional winsorization at [p2.5, p97.5] of raw values, then median / MAD (constant = 1.4826) standardization. Expanding-window variant in `pit_assembler.R` pools winsorized values across all prior pit_*_raw.parquet + current for location / scale. Clip default set to c(-5, 5) after running `tools/diag_zscore_tails.R` -- see Section 12a for the A/B/C trade-off analysis and decision.

### Should-fix (added 2026-04-20)

11. **Log-transform size / valuation indicators before winsorization.**
    The tail diagnostic (§12a) shows that `market_cap`, `revenue_raw`,
    `enterprise_value`, `peg`, `pb`, `pe_trailing`, `pfcf`, `ev_ebitda`,
    and similar heavy-right-skew indicators have p99.9 of |z| in the
    range 18-47 even after p2.5 / p97.5 cross-sectional winsorization.
    Root cause: these are log-normal by construction (mega-caps dwarf
    small-caps; growth-stock multiples span orders of magnitude).
    Percentile-based winsorization on the raw scale cannot tame an
    order-of-magnitude spread. Fix: apply `log(abs(x)) * sign(x)`
    (sign-preserving log, so negatives still propagate as distress
    signals) before passing into `.robust_zscore` for the affected
    indicators. Re-run `tools/diag_zscore_tails.R`; once p99.9 lands
    near 4 with symmetric tails, drop the clip (flip default from
    c(-5, 5) to NULL). The list of candidate indicators to log-transform
    is the Section 12a top-15 by p999; for margin / growth / quality
    indicators that are already bounded-ratio, no transform is needed.
10. **[naming]** Rename `fcf_stability` -> `cfo_stability`, `inventory_sales_change` -> `inventory_cogs_change`.

---

## 15. Replication Methodology Used in This Review

For each indicator:
1. Located the academic reference in `RESEARCH_INDICATORS.md` (where present).
2. Cross-checked the plain-English formula in `INDICATORS.md`.
3. Traced the formula into `indicator_compute.R` line-by-line.
4. Spot-checked sign conventions, guards, and alias handling against XBRL reporting practice.
5. Verified pass-through to the canonical output vector (`.INDICATOR_NAMES`) in `compute_ticker_indicators`.
6. Checked z-scoring behavior and financial-sector masking.

No indicator required abandoning the review due to undocumented formulas. All 57 fields were traceable end-to-end from XBRL tag -> derived quantity -> per-ticker vector -> cross-sectional z-score.

---

## 16. Session Handoff -- State at 2026-04-20 (part 2 of the day)

### 16.0 What was done in this (second) session (2026-04-20 PM)

Remaining "should-fix" methodology items from §14 addressed (ROIC still
pending, now the only open WARN):

- **C-3 derive_quantities NA-vs-zero split.** `.derive_quantities`
  propagates NA from missing `depreciation`, `capex`, `long_term_debt`
  + `short_term_debt` (both absent), and `cash`. One-present /
  one-absent for LTD/STD kept as zero to match filing practice. EV
  fallback in `.compute_valuation` (silently substituting total_debt
  for NA net_debt) dropped. `ic` in `.compute_profitability` stops
  silently treating missing cash as zero.
- **§14 #6 pe_trailing sign guard -- reframed.** The user's principle
  is *"negative denominators carry distress; don't discard via NA,
  handle fat tails with smart winsorization"*. Consequently the
  positive-denominator guards were removed from `pb`, `pfcf`,
  `ev_ebitda`, `roe`, `roic`, `debt_equity`, `net_debt_ebitda`, and
  `payout_ratio`. Negatives now propagate.
- **Section 12 z-score pipeline -- rewritten.** New
  `.robust_zscore(x, calib_sample, probs = c(0.025, 0.975), clip)`
  helper in `R/indicator_compute.R`: cross-sectional winsorization at
  p2.5 / p97.5 followed by median / MAD (constant = 1.4826)
  standardization. `zscore_cross_section` now calls it for the
  per-snapshot path.
- **Expanding-window standardization -- new.** `zscore_expanding_window`
  in `R/pit_assembler.R` pools winsorized per-snapshot values from all
  `pit_*_raw.parquet` with date < current + current snapshot,
  standardizes current via median / MAD of the pool. `assemble_snapshot`
  defaults `zscore_mode = "expanding_window"`; pass `"cross_section"`
  to revert.
- **Clip evaluation diagnostic.** `tools/diag_zscore_tails.R` builds
  8 quarterly snapshots (2024-Q1..2025-Q4), computes unclipped z-scores,
  tabulates per-indicator `|z|` tails and pooled `|z|` at p99 / p99.9
  with tail-symmetry check. Recommendation output at the bottom decides
  among "drop clip", "loosen to [-5, 5]", or "keep [-3, 3]" based on
  p99.9 and tail mass above 3 / 5.

Test status at handoff: **182 indicator_compute tests passing** (35 new
since the morning session) + **55 pit_assembler tests passing** (9 new
for zscore_expanding_window + .load_history_snapshots).

Files modified in this session:
- `R/indicator_compute.R`   -- `.robust_zscore`, guard removal,
  `.derive_quantities` NA-vs-zero split, `zscore_cross_section`
  rewrite
- `R/pit_assembler.R`       -- `zscore_expanding_window`,
  `.build_pooled_calib`, `.load_history_snapshots`,
  `assemble_snapshot` integration
- `tests/test_indicator_compute.R` -- 35 new tests (C-3, sign
  preservation, robust zscore, winsorization)
- `tests/test_pit_assembler.R`     -- 9 new tests
- `docs/research/06_indicator_verification_report.md` -- status updates,
  §12 rewrite, §14 updates, this §16 entry
- `tools/diag_zscore_tails.R`      -- new diagnostic script

### 16.1 What was done in the morning session (2026-04-20 AM)

Three cross-cutting findings from the 2026-04-19 report were implemented,
reviewed, unit-tested, and gate-tested in the session ending 2026-04-20:

- **C-2 Piotroski NA handling.** `.bin()` helper returns `NA_integer_` for
  missing inputs; `f_score` is `NA_integer_` when any component is NA.
  `R/indicator_compute.R`, `tests/test_indicator_compute.R`,
  `docs/INDICATORS.md`.
- **C-5 Interest-coverage alias.** `InterestIncomeExpenseNet` removed from
  the alias map in `R/fundamental_fetcher.R`. `.filter_deprecated_tags()`
  drops pre-existing cached rows on read without forcing a cache rebuild.
- **C-1 fcf_stability YTD cumulation.** Implemented both
  (a) compute-time de-cumulator `.decumulate_cfo()` in
  `R/indicator_compute.R`, and
  (b) fetcher dedup duration-match tie-break in
  `R/fundamental_fetcher.R` (`.FP_EXPECTED_DAYS`,
  `.duration_match_rank`, updated `dedup_fundamentals` sort order).
  Full diagnostic trail, three-class classification of affected tickers,
  gate-test results, and rationale for JPM/HRB's higher post-fix SD are
  all recorded in `docs/research/07_cfo_cumulation_issue.md`.

Test status at handoff: **280 unit tests passing** (145 indicator + 135
fetcher). Gate test in `tools/gate_test_fcf_stability.R` passes three
assertions (AMZN NA, majority SD drop on clean industrials, no non-finite
output).

### 16.2 Working-tree state (not yet committed)

Modified:
- `R/fundamental_fetcher.R`    -- dedup tie-break + interest_expense alias
- `R/indicator_compute.R`      -- Piotroski NA + de-cumulator + fcf_stability
- `R/pipeline_runner.R`        -- feature builder orchestration (unrelated)
- `docs/INDICATORS.md`         -- Piotroski NA policy note
- `tests/test_indicator_compute.R` -- 15 new unit tests

New (untracked):
- `R/feature_standardizer.R`   -- (unrelated, pre-existing work)
- `run_features.R`, `tests/test_feature_standardizer.R` -- (unrelated)
- `docs/research/00_README.md` ... `07_cfo_cumulation_issue.md` -- research log
- `tools/diag_cfo_*.R`         -- diagnostic scripts (keep as evidence)
- `tools/gate_test_fcf_stability.R` -- gate test script

### 16.3 What a new session needs to do to continue

Numbered in execution order. Each item is self-contained so a new session
can pick up without reconstructing context.

**1. [DONE] Cache rebuild for the 7 non-conformer tickers.**
    AMZN, NVDA, QCOM, SJM, LKQ, ANF, HRB refreshed via
    `tools/rebuild_cfo_nonconformers.R`. Gate test updated and passes;
    AMZN `sd_new` = 0.01697 (was NA).

**2. [DONE] Full-universe diagnostic sweep.**
    `tools/diag_cfo_universe.R` walks all 639 cached files and persists
    a per-ticker verdict to `cache/lookups/cfo_universe_sweep_<date>.parquet`.
    Sweep surfaced two new non-conformer classes (Kroger 16-week Q1,
    discontinued-ops CFO alias), both fixed. Current universe:
    71.9% CLEAN, 28.1% HAS_NONCONFORMING (all benign -- defensive filter
    in `.decumulate_cfo` handles them), 0 ANOMALOUS_TICKER, 1 NO_CFO_ROWS
    (SII, IFRS-only filer). Full detail in
    `07_cfo_cumulation_issue.md` §6.

**3. [DONE] Investigate ITT-class (zero CFO rows cached).**
    Sweep expanded the class from 1 (ITT) to 5 tickers: FMC, TFX, ITT,
    DRI all file `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`
    (added to alias chain, 4 tickers rebuilt). SII files IFRS-only
    (`ifrs-full` namespace), out of scope for the current fetcher; noted
    as a known blackout.

**4. Remaining "should-fix" verification-report items (any order):**

   - **ROIC**: switch numerator from NI to NOPAT
     (`opinc * (1 - tax_rate)`). See section 2.6 of this report.
   - **C-3**: `R/indicator_compute.R` `.derive_quantities` currently
     uses `nafill(.., 0)` for capex, depreciation, LTD, STD, cash. Decide
     per-component whether missing should propagate NA or be treated as
     true zero, and implement the split. See section 11 of this report.
   - **pe_trailing sign guard**: align with `pb`/`pfcf` by returning NA
     when `eps <= 0`, or explicitly document the asymmetry. See
     section 1.1.

**5. "Nice-to-have" items (backlog):**

   - Rename `fcf_stability` -> `cfo_stability` and
     `inventory_sales_change` -> `inventory_cogs_change` (section 14, item 10).
   - Revenue QoQ: switch sequential-quarter to YoY-same-quarter
     (section 3.2).
   - `capex_depreciation` materiality floor (section 10.4).
   - Z-score: pre-winsorize at `[1 %, 99 %]` or offer a median/MAD
     robust variant (section 12).

### 16.4 Pointers for the next session

- Start by reading `docs/research/07_cfo_cumulation_issue.md` -- it is
  the live log and has the diagnostic evidence + ticker-class table.
- `MEMORY.md` (if present) may carry feedback / user preferences from
  prior sessions.
- Task list at handoff: tasks #1-#6 completed; task #7
  ("Sweep remaining ~580 tickers for unseen patterns") is the immediate
  next action.

---

End of report.
