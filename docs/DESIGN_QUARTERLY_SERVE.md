# DESIGN: Quarterly Serve Extension (mixed-cadence fund layer)

Status: SHIPPED 2026-08-14 (commit 8ed1201; base 6a85671).
Requested by the RPortfolios campaign (its CODING_PLAN_T2 v1.3, item A1a,
user-ruled 2026-08-13); designed from a definition-level census of this
repo (RPortfolios documentation/A1_CENSUS.md). One file changed:
R/timeseries_builder.R (+373/-53). indicator_compute.R untouched.

## Problem

The daily serve's fund layer was annual-only by design: one row per
fiscal year, values as-of the original 10-K filing. Every fundamental
value therefore changed once a year (~364d median gap, measured across
the 612-name pool), although the raw vintage cache carries Q1-Q3 10-Q
rows at a ~92d filed cadence and indicator_compute.R already computes
several natively quarterly signals internally (Wave 3 machinery) that
the annual layer discarded.

## Design

1. `build_ticker_fundamentals(..., quarterly = FALSE)` -- opt-in flag.
   FALSE (the default) reproduces prior behavior byte-identically; the
   live store's cadence can never flip silently.
2. With quarterly = TRUE, after the untouched FY loop, one row per
   (fiscal_year, Q1-Q3) with quarterly filings:
   - filed_date = the ORIGINAL 10-Q filing date (min filed over the
     quarter's plain 10-Q rows; amendments never move the stamp), and
     the row is computed as-of that date via pit_dedup -- the same
     original-filing law the FY loop uses.
   - period_end = the quarter's true end from DURATION rows (the DEI
     cover-shares instant, dated ~3 weeks later, must not contaminate
     the anchor -- found and fixed during validation).
   - Kept indicator columns: only the natively quarterly set
     (.Q_KEPT_INDICATORS: roaq, revenue/earnings surprise, QoQ revenue
     growth, tax change, earnings streaks, fcf_stability, MS volatility
     ingredients). Every other indicator column is NA on Q rows --
     annual-bound formulas are wrong-basis at Q; absence, never a
     plausible value.
   - Stubs: instant (balance-sheet) stubs at current values; flow stubs
     at TRAILING-TWELVE-MONTH basis = sum of the 4 standalone quarters
     from the Wave 3 panels (.flow_quarter_series) -- NEVER from the
     wide pivot (10-Q cash-flow items are YTD-cumulative and the wide
     pivot is duration-ambiguous; see docs/research/07). Unrecoverable
     stubs stay NA. FY density rule (>= 5 concepts) applies to Q rows.
3. `update_ticker_daily` becomes a two-pass as-of mapper:
   - Pass A (annual rows only): all non-kept indicator columns,
     fiscal_year, and filed_date -- filed_date keeps meaning "latest
     ANNUAL filing", so annual semantics are unchanged inside the
     quarterly serve.
   - Pass B (all rows, greedy on period_end desc): kept indicators,
     all stubs feeding the daily price-sensitive recomputation (pe/pb/
     ps/ev/... refresh quarterly at TTM basis), and the NEW column
     `filed_date_q` = the filed date of the pass-B row in effect.
   - Legacy fund layers (no `quarter` column): pass B degrades to pass
     A's rows; output identical to prior behavior plus
     filed_date_q == filed_date.

## Validation (as run, 2026-08-14)

- Repo suite: tests/test_timeseries_builder.R 244 passed / 0 failed.
- External acceptance gates (RPortfolios prototype/a1_gates.R, 6-name
  probe set BRO/MSI/LUV/AAPL/TDC/ABNB, old-code reference built from
  6a85671 BEFORE the edit): 45 PASS / 0 FAIL, every gate paired with a
  planted control that demonstrably fires --
  * builder isolation: quarterly=FALSE fund/pershare parquets
    byte-identical to the reference; daily identical on all shared
    columns with filed_date_q == filed_date;
  * annual semantics preserved INSIDE the quarterly serve (filed_date,
    fiscal_year, annual-only columns identical to reference);
  * cadence: filed_date_q median step 91d in [85, 98] (band frozen from
    the pool census); the old serve reads 364d and fails as planted;
  * release-time law: zero negative ages on both date columns;
  * original-filing law: 148/148 quarterly rows stamp the original 10-Q
    date (amendment-heavy names), shifted-fixture plant trips.
- Spot arithmetic: AAPL day 2025-03-03 pe_trailing 37.78 = 238.03/6.30
  (price / TTM eps stub) exactly; TTM revenue stub 395.8B -> 451.4B
  across FY25 quarters.

## Known limitations

See docs/KNOWN_LIMITATIONS.md ("Quarterly serve scope") -- notably:
fundamental-only ratio indicators (roe, roa, margins, ...) remain
annual-cadence on the serve (their TTM refresh needs either an
indicator_compute TTM mode or downstream computation from the TTM
stubs); on Q-row days cf_me / rd_me / z_score / kz_index are NA and
net_payout_yield degrades to gross payout (equity_issuance has no
quarterly construction here).

## Invocation

Legacy (byte-identical): build_ticker_fundamentals(tk, cik, sector,
ts_dir = ..., force = TRUE). Mixed cadence: add quarterly = TRUE.
update_ticker_daily is signature-unchanged and auto-detects the
`quarter` column. The RPortfolios consumer plans its full 612-name
rebuild (their item A1b) against this contract.
