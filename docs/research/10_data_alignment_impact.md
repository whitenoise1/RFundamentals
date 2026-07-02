# Impact Assessment: PIT Data-Alignment Corrections (2026-07-02)

Quantifies the impact of the two defects fixed in commits e6c3918 (vintage
leak) and 5551df4 (pivot label-group pollution). Measurements compare the
preserved pre-fix artifacts (snapshot generated with pre-fix code from the
pre-fix cache; cache/timeseries_pre_vintage/) against the corrected
outputs, plus a staleness audit against as-reported filings.

## 1. The two defects, one line each

1. **Pivot label-group pollution** (5551df4, the severe one): a 10-K
   reports prior-year comparatives under the same fiscal_year label and
   filed date as the current period; the pivot's tie-break fell to parse
   order, which in companyfacts JSON is chronological -- so the OLDEST
   comparative won. Snapshots served systematically stale fundamentals.
2. **Vintage leak** (e6c3918): dedup kept only the latest accession per
   period, so re-reports replaced original rows and pushed filed dates
   forward; as-of queries lost observations, and every cache refresh
   compounded it.

## 2. Value impact (pivot bug), 2026-03-31 snapshot

Staleness audit (matching each ticker's old snapshot revenue to the
annual period it actually came from, n = 489):

| staleness | tickers | share |
|---|---|---|
| current (0y) | 29 | 5.9% |
| 1 year | 9 | 1.8% |
| exactly 2 years | 450 | **92.0%** |
| 3+ years | 1 | 0.2% |

92% at exactly two years is the fingerprint of the mechanism: the oldest
column of a three-year comparative income statement is fy-2.

Cell-level: 81% of the 23k snapshot cells changed. Cross-sectional rank
stability (Spearman correlation old vs new, per indicator; this is what
the z-scored factor input inherits):

| group | representative indicators (spearman) |
|---|---|
| Destroyed (rank ~ half-random) | fcf_ni 0.37, opcf_ni 0.37, pct_accruals 0.43, sloan_accrual 0.46, f_score 0.49, eps_growth_yoy 0.52, earnings_yield 0.53, pe_trailing 0.55, sga_efficiency 0.57 |
| Heavily distorted | peg 0.60, ebitda_growth 0.62, roe 0.64, revenue_growth_yoy 0.67, roa 0.73, ev_ebitda 0.75, debt_equity 0.77 |
| Moderately distorted | pb 0.84, operating_margin 0.85, cash_based_op 0.87, NOA 0.89, current_ratio 0.91, gpa 0.92 |
| Largely rank-preserved | ps 0.96, asset_turnover 0.96, market_cap 0.97, gross_margin 0.98, revenue_raw 0.98, enterprise_value 0.98 |

Median Spearman across 59 indicators: **0.70**; 44 of 59 below 0.9.

Why the pattern: indicators that compare two adjacent periods (growth,
accruals, F-score deltas, EPS-based valuation) were computed across wrong
or mixed year-pairs, which effectively randomizes them. Level ratios were
internally consistent -- numerator and denominator both came from the same
stale year -- and firm rankings on slow-moving ratios persist over two
years, so their cross-sectional ordering survived even though the values
were stale.

## 3. Timing impact (vintage leak), timeseries fund layer

Old vs new fund layers (9,013 matched ticker-fiscal-years):

- **401 ticker-fiscal-years recovered** (~4.3% of the layer): fiscal
  years that had been wiped from the collapsed cache entirely (ORCL
  FY2023 class) now exist.
- 97.2% of stamps unchanged; 2.1% had been stamped >30 days late.
  Among shifted stamps: median 68 days late, p75 = 214, p95 = 368 days.
- These numbers are a LOWER bound on what the leak would have become:
  the old cache was young (fetched June 2026). Each subsequent refresh
  would have converted more original rows into late-stamped re-reports.

## 4. What was and was not contaminated

- **Contaminated**: the timeseries layer built June 2026 (rebuilt), the
  2026-03-31 snapshot (regenerated), any ad-hoc analysis, demo output, or
  export made from them -- including the empirical results of the dropped
  cross-sectional signal-extraction branch (docs/research/08).
- **Not contaminated**: the historical quarterly snapshots -- they were
  first generated post-fix (2010-03-31..2026-06-30 backfill), so no
  stale-history artifact ever existed.
- **Worth re-running**: distributional diagnostics calibrated on pre-fix
  data, notably the z-score tail study behind the clip = c(-5, 5) choice
  (docs/research/06 sec. 12a, tools/diag_zscore_tails.R). Level-ratio
  shapes are likely similar, but growth/accrual tails may differ now that
  those indicators are no longer computed across wrong year-pairs.

## 5. Verification anchors (as-reported filings)

- AAPL as-of 2020-06-30: revenue 260.174B = FY2019 (filed 2019-10-31).
- AAPL as-of 2015-06-30: revenue 182.795B = FY2014.
- MSFT as-of 2020-06-30: revenue 125.843B = FY2019; FY2020 (filed
  2020-07-30) correctly invisible.
- ORCL as-of 2026-03-31: implied book equity from P/B = 20.451B, exactly
  the FY2025 10-K value; before the fix the row mixed FY2023 equity,
  revenue, and income under the FY2025 label.
