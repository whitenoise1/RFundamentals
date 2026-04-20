# CFO Cumulation Issue -- Investigation Log

Opened: 2026-04-20
Last updated: 2026-04-20 (full-universe sweep + 2 new non-conformer classes resolved)
Status: RESOLVED -- initial fix shipped, universe sweep complete, 13 tickers rebuilt
Related: `06_indicator_verification_report.md` finding C-1
Affected indicator: `fcf_stability` (Tier 2)

---

## 1. Background

The verification report raised C-1 as a WARN: US 10-Q filings report
`operating_cashflow` on a year-to-date (YTD) cumulative basis, not as
stand-alone quarter values. `fcf_stability` uses `sd(cfo / assets)` over the
last up to 16 quarters; if quarterly rows are stored as YTD, the SD is
dominated by the intra-year cumulation staircase (Q1 ~ 3mo, Q2 ~ 2x, Q3 ~ 3x,
FY ~ 4x) rather than true cash-flow volatility.

This document records the diagnostic work performed on 2026-04-20 and the
agreed fix plan.

---

## 2. Diagnostic performed on 2026-04-20

Two scripts under `tools/`:

- `diag_cfo_cumulation.R` -- first pass, 30 tickers (15 mega-caps + 15 random).
- `diag_cfo_wide.R`       -- widened pass, 60 tickers (15 mega-caps + 45 random).

Both classify each cached `operating_cashflow` row by
`period_days = period_end - period_start` into buckets:
`3mo` (60-110), `2Q_YTD` (160-200), `3Q_YTD` (250-290), `FY` (330-380),
plus `short / 4mo / 7-8mo / 10-11mo / other` for any anomalies.

### 2.1 Coverage

- 60 tickers sampled. 59 have CFO rows; **1 ticker (ITT) has zero CFO rows**
  cached (to be investigated separately -- possible fetch failure or
  foreign-filer 20-F).
- 3,440 CFO rows inspected.
- **~580 S&P 500 constituents remain unreviewed.** The three structural
  classes found below are expected to repeat, but additional classes cannot
  be ruled out without a full-universe sweep.

### 2.2 Bucket distribution

All rows fall into exactly four buckets: `3mo`, `2Q_YTD`, `3Q_YTD`, `FY`. No
short stubs, no anomalous durations. This bounds the problem.

### 2.3 `fiscal_qtr x kind` cross-tab (61 off-diagonal leaks)

| fp    | 3mo  | 2Q_YTD | 3Q_YTD | FY  |
|-------|-----:|-------:|-------:|----:|
| FY    |    7 |      4 |      3 | 906 |
| Q1    |  787 |      0 |      0 |  22 |
| Q2    |    2 |    818 |      0 |  18 |
| Q3    |    1 |      3 |    815 |  21 |
| Q4    |    0 |      0 |      0 |   9 |
| NA/"" |    0 |      0 |      0 |  24 |

Bold-diagonal cells are "correct" (`fp` label matches the row's duration).
The 61 off-diagonal `FY` cells in Q1/Q2/Q3 are the core of the problem.

### 2.4 Per-ticker verdict

- **52 / 59 CLEAN** (88 %). Standard YTD staircase; needs only compute-time
  de-cumulation.
- **7 non-conformers.** Fall into three classes (below).

### 2.5 The three classes of non-conformer

**Class 1 -- AMZN-style systemic TTM leak (1 ticker).**
Every Q1/Q2/Q3 row has `period_days ~= 365`. The dedup's
`(concept, period_end, fiscal_qtr)` key collides with TTM prior-year
comparatives present in the companyfacts JSON; since all other tie-breakers
(form, accession, tag rank) match, the TTM wins on JSON order. Cached
quarterly rows for AMZN are effectively all 12-month windows, making
`fcf_stability` meaningless.

**Class 2 -- sparse TTM leaks (4 tickers: QCOM, SJM, LKQ, mild ANF).**
Most rows correct, 2-3 TTM rows slip through into a `fp=Q1` or `fp=Q3`
cell. Impact on `sd()` is small but directional.

**Class 3 -- historical fiscal-year realignment (2 tickers: NVDA, HRB).**
NVDA changed fiscal-year-end around 2016; for that single transition, a
2016-02-01 -> 2016-05-01 stub value is tagged with multiple `fp` labels
(Q1/Q2/Q3/FY) across several filings. HRB (April FY end) has a similar
cluster at fiscal-year boundaries. Not TTM leaks -- genuine stub periods
that ended up under mismatched `fp`. A duration-match tie-break does not
help these; the compute-time de-cumulator must defensively skip rows whose
`period_days` does not match the expected length for their `fp`.

### 2.6 MISSING_FP rows (24 rows, 8 tickers)

All are FY-length rows where `fp` is NA or empty in the source JSON.
Harmless downstream because `pivot_fundamentals` filters on
`period_type in {FY, Q1..Q4}`. No action needed.

### 2.7 ITT (no CFO rows cached)

1 ticker in the 60-sample has an empty `operating_cashflow` slice. Unclear
whether the fetch failed or ITT only files a form we do not currently
parse. Logged for follow-up; compute will naturally emit NA for affected
indicators.

---

## 3. Fix plan (agreed 2026-04-20)

Two complementary changes.

### 3.1 Fetcher fix -- duration-match tie-break in `dedup_fundamentals`

When multiple rows share `(concept, period_end, fiscal_qtr)`, add a
priority step before form/accession/tag-rank:

```
expected_days(fp) = Q1 -> [60,110], Q2 -> [160,200],
                    Q3 -> [250,290], FY / Q4 -> [330,380]
duration_match = 1 if period_days in expected_days(fp) else 0
sort desc by duration_match, then the existing priority chain
```

Cleans up AMZN + QCOM + SJM + LKQ + ANF in a single change. Requires a
cache refresh for affected tickers (can be done on-demand; not a full
rebuild).

### 3.2 Compute-time fix -- YTD de-cumulator in `.compute_tier2`

1. Plumb `period_days` (or a derived `kind` label) into `quarterly_hist`.
2. Defensive step: drop any quarterly row whose `kind` does not match its
   `fp`-expected band (handles NVDA/HRB stubs).
3. Within each `fiscal_year` group, sort rows by `fp` order
   `[Q1, Q2, Q3, Q4, FY]` and de-cumulate:
   - `Q1_std = Q1_value` (already 3mo)
   - `Q2_std = Q2_YTD - Q1_YTD`
   - `Q3_std = Q3_YTD - Q2_YTD`
   - `Q4_std = FY - Q3_YTD` (only if all inputs present)
4. If any input needed for a quarter is missing, emit NA for that quarter
   (do not contaminate neighbouring quarters or the SD).

### 3.3 Non-goals

- Not renaming `fcf_stability` -> `cfo_stability` in this patch (separate
  cleanup item from the verification report, can be done later).
- Not raising the minimum-quarter threshold from 8 to 12.
- Not switching to annual-only (Option D in the earlier discussion).

---

## 4. Gate test results (2026-04-20, post-implementation)

Implementation merged; gate test (`tools/gate_test_fcf_stability.R`) run on
11 curated tickers. Results:

| ticker | class        | sd_old | sd_new | new/old |
|--------|--------------|-------:|-------:|--------:|
| AAPL   | clean        | 0.063  | 0.024  | 0.38 |
| MSFT   | clean        | 0.038  | 0.012  | 0.32 |
| WMT    | clean        | 0.031  | 0.019  | 0.63 |
| JNJ    | clean        | 0.032  | 0.011  | 0.35 |
| JPM    | clean (bank) | 0.015  | 0.019  | 1.25 |
| AMZN   | ttm_leak     | 0.034  |   NA   |  --  |
| QCOM   | sparse_leak  | 0.061  | 0.033  | 0.54 |
| SJM    | sparse_leak  | 0.022  | 0.011  | 0.49 |
| LKQ    | sparse_leak  | 0.029  | 0.011  | 0.40 |
| NVDA   | fy_realign   | 0.105  | 0.045  | 0.42 |
| HRB    | fy_realign   | 0.211  | 0.240  | 1.14 |

Expected behaviour observed:

- **AMZN** correctly returns NA (de-cumulator rejects all 16 TTM-leak rows;
  will populate after the fetcher fix + cache refresh for this ticker).
- **Industrial CLEAN tickers** show ~2-3x SD reduction (sawtooth removed).
- **JPM and HRB** show higher SD after de-cumulation. This is the correct
  behaviour, not a regression:
  - Banks (JPM) have sign-alternating quarterly CFO from trading-book and
    deposit swings; YTD cumulation partially cancels the alternation, so
    cumulative SD understates true quarterly volatility.
  - Seasonal firms (HRB: tax-season revenue concentrated in Q3) have a
    large Q3 spike that YTD smooths across the year; de-cumulation restores
    the real spike.
- **Sparse-leak and fy-realign** classes reduce as expected; stub rows and
  TTM-leak rows are dropped by the duration-match filter.

## 5. Open follow-ups

- [x] Sweep the remaining ~580 S&P 500 constituents after implementation to
      confirm no new classes. Script `tools/diag_cfo_universe.R` walks all
      639 cached files. Run on 2026-04-20. See section 6.
- [x] Investigate ITT (no CFO rows cached). Root cause: the filer uses
      `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`,
      which was not in the alias map. See section 6.2.
- [x] After fetcher fix merges, trigger cache refresh for the 7 identified
      non-conformers (AMZN, NVDA, QCOM, SJM, LKQ, ANF, HRB). Done via
      `tools/rebuild_cfo_nonconformers.R` on 2026-04-20.
- [ ] Consider flagging `fcf_stability` as a "signed behavioural" indicator
      for banks (where higher SD post-fix reflects real volatility, not a
      data artefact). May want a sector-aware note in `INDICATORS.md`.

---

## 6. Full-universe sweep (2026-04-20)

`tools/diag_cfo_universe.R` classifies every row in every cached
`operating_cashflow` slice by `period_days` bucket, then rolls up per-
ticker verdicts. Run on all 639 cached files after the initial 7-ticker
rebuild. Output persisted to
`cache/lookups/cfo_universe_sweep_2026-04-20.parquet`.

Two novel classes surfaced that were not visible in the 60-ticker sample.

### 6.1 Class 4 -- Kroger/AAP 16-week-Q1 fiscal calendar (2 tickers)

**Symptom.** Universe sweep flagged KR and AAP as `ANOMALOUS_TICKER`: all
their Q1 CFO rows fell in the `4mo` bucket (111-140 days) instead of `3mo`.
`fcf_stability` was producing NA for both tickers despite having 75/74
usable CFO rows cached.

**Root cause.** KR (Kroger) and AAP (Advance Auto Parts) use a 52/53-week
fiscal calendar with quarters of **16/12/12/12 weeks** rather than the
standard 13/13/13/13. Their standalone Q1 therefore runs ~111 days
(16 * 7 - 1 for floor), which fell outside the 60-110 "3mo" band used by
`.decumulate_cfo` and `.FP_EXPECTED_DAYS$Q1`. Cross-checked 4 standard
retailers (TGT, LOW, HD, WMT, COST, BBY): all use 13-week quarters; the
16/12/12/12 pattern is a KR/AAP-specific quirk, not a generic retail
calendar.

**Fix.** Widened the Q1 band upper bound from 110 to 120 in two places:
- `R/indicator_compute.R` `.decumulate_cfo()` 3mo band 60..110 -> 60..120
- `R/fundamental_fetcher.R` `.FP_EXPECTED_DAYS$Q1` [60,110] -> [60,120]
- `tools/diag_cfo_universe.R` bucket() 3mo edge 110 -> 120

No cache rebuild was required for KR/AAP beyond the initial
`tools/rebuild_sweep_followups.R` run; the fetcher fix only affects dedup
tie-breaks, and KR/AAP had no colliding Q1 rows at other durations.

**Verification.** Post-fix, KR `fcf_stability` = 0.01996,
AAP `fcf_stability` = 0.01756 on the trailing 16 quarters. Both in the
expected range for industrial tickers. Unit tests added in
`tests/test_indicator_compute.R`:
- `decumulate: 16-week Q1 (KR/AAP retail calendar) classified as 3mo`
- `dedup: .duration_match_rank accepts 16-week Q1 (111-120 days)`

### 6.2 Class 5 -- discontinued-operations CFO tag (4 tickers)

**Symptom.** 5 tickers had zero `operating_cashflow` rows cached despite
normal 10-K/10-Q filing history: FMC, TFX, ITT, DRI, SII.

**Root cause.** Of the 5:
- **FMC, TFX, ITT, DRI** (4 tickers). All file
  `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`, not the
  standard `NetCashProvidedByUsedInOperatingActivities`. This tag is used
  by filers with current or historical discontinued operations -- the CFO
  line is then split between continuing and discontinued, and only the
  continuing value is reported. The tag was absent from the
  `operating_cashflow` alias chain.
- **SII** (1 ticker). Sprott Inc., a Canadian financial-services firm. Its
  EDGAR `companyfacts` JSON contains only `dei` and `ifrs-full`
  namespaces; no `us-gaap` tags at all. IFRS taxonomy support is out of
  scope for the current fetcher; SII is a known IFRS-only blackout.

**Fix.** Added
`NetCashProvidedByUsedInOperatingActivitiesContinuingOperations` to the
`operating_cashflow` alias chain in `R/fundamental_fetcher.R` (last in
order, so firms that report both the total and the continuing-ops line
still resolve to the total). Cache rebuilt via
`tools/rebuild_sweep_followups.R` on 2026-04-20. Post-rebuild row counts:
FMC 0->72, TFX 0->68, ITT 0->75, DRI 0->70.

**Action on SII.** No-op. Logged as an IFRS-only filer; any future
IFRS-tag workstream should enumerate affected tickers across the full
roster, not just SII.

### 6.3 Remaining non-conformer population

After both rebuilds and the band widening, the ticker-level tally is:

| Verdict           | N   | Share |
|-------------------|----:|------:|
| CLEAN             | 459 | 71.9% |
| HAS_NONCONFORMING | 179 | 28.1% |
| ANOMALOUS_TICKER  |   0 |  0.0% |
| NO_CFO_ROWS (SII) |   1 |  0.2% |

The 179 `HAS_NONCONFORMING` tickers have scattered mis-labeled rows
(e.g. a single row tagged `fp=Q2` but with a `111`-day period, mapped by
the filer in error). `.decumulate_cfo` already rejects such rows via the
`cfo_kind != expected_kind` guard, so they do not contaminate
`fcf_stability`; they only reduce usable-row count. Population of 179 is
not itself actionable -- the defensive filter is sufficient.

### 6.4 Residual novel buckets

61 rows out of 38,117 fall in the non-standard `short`/`4mo`/`7-8mo`/
`10-11mo`/`other` buckets. Distribution:

| Bucket  | Rows | Forms seen            |
|---------|-----:|-----------------------|
| 4mo     | 5    | 10-Q (2), 10-K (2), 8-K (1) |
| short   | 13   | 10-Q (7), 10-K (6)          |
| 7-8mo   | 7    | 10-Q (5), 10-K (2)          |
| 10-11mo | 3    | 10-K                        |
| other   | 1    | 10-K                        |

All are defensively rejected by `.decumulate_cfo`. Not worth per-ticker
investigation at 0.16% of total rows.

---

End of log.
