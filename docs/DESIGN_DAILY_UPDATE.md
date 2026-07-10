# DESIGN_DAILY_UPDATE.md -- Incremental Data Update

## Purpose

The historical build (Aspect 1: PIT cross-sections + daily time series feeding
the factor model) is complete. This document specifies Aspect 2: keeping the
database current as each trading day passes, via **cheap append operations**
rather than reprocessing history. The consuming output is the **daily
cross-section** (`load_daily_cross_section`), confirmed by the user.

Guiding constraint: the update must be **point-in-time (PIT) safe** -- appending
future dates must never use future information to alter, or to compute, any
value for a date on or before that information was public.

---

## 1. PIT Audit (starting state)

Verified PIT-clean (no look-ahead):

- **Fundamentals stamping.** Both paths resolve via `pit_dedup(as_of = date)`
  and `get_latest_annual_filing(filed <= date)` (pit_assembler.R:717,479).
  Rows filed after the as-of date are dropped; vintages collapse to the row
  authoritative on that date.
- **As-traded price reconstruction.** `Close / .split_factor(post-date splits)`
  recovers the actual historical print -- using later splits to un-adjust is
  reconstruction, not look-ahead (pit_assembler.R:455).
- **Quarterly snapshot contemporaneity.** `assemble_snapshot` pairs the
  filing-date price with shares from that same filing, so a split between the
  filing and the snapshot cannot mis-scale market cap.
- **Daily append.** The `filed_date <= day` lookup (timeseries_builder.R:592)
  never lets a future filing leak into a past row.

Two defects found (carried into this design as fixes):

- **D1 -- Daily-layer market cap (value error, not look-ahead).** The daily
  path computes `mc = price(day) x stub_shares` where `stub_shares` comes from
  the latest **annual** 10-K (fund layer is annual-only,
  timeseries_builder.R:392-407,626). A split between the annual filing and the
  day puts price on the post-split basis while shares stay pre-split -> market
  cap and every MC-derived ratio (pb, ps, pfcf, ev*, yields, all Wave 4/5
  ME-scaled levels) are wrong until the next 10-K. Quarterly snapshots are
  exempt (contemporaneous). **Fix: #2 below.**
- **D2 -- Sector/industry look-ahead.** `sector_industry.parquet` has no date
  dimension; the current finviz value is joined by ticker onto every historical
  date (pit_assembler.R:673, timeseries_builder.R:956). Since z-scoring,
  Mohanram, Herfindahl, ChInvIA all group by sector/industry, a name reclassified
  since 2010 is grouped by its present sector in past cross-sections. **Fix:
  #3 below (A+B); strict historical correctness deferred -- see Residuals.**

- **D3 -- "Unknown" sector bucket silently disables the NA-mask (LIVE BUG,
  measured 2026-07-09, present in the shipped post-Wave-P rebuild).**
  Delisted names finviz cannot classify get `sector = "Unknown"` at merge.
  `"Unknown"` is not a key in `.SECTOR_NA_INDICATORS`, so the sector NA-mask
  **does not fire for them**. Measured on `pit_2020-12-31`: delisted banks
  CMA, DFS, PBCT, SIVB sit in the Unknown bucket carrying non-NA
  `net_debt_price` (all four) and `ch_asset_turnover` (CMA, SIVB), while
  genuine `sector=="Financial"` controls (AFL, AIG) correctly show zero non-NA
  masked indicators. Nonsensical bank values therefore enter the raw
  cross-section and its z-scores.

  Scale: after Wave P, daily-layer tickers with no sector row went 1 -> 25;
  **62 of 66 snapshots contain an Unknown bucket** (mean 13.8 tickers, max 24,
  ~5% of a cross-section). Current-dated snapshots show 0 because these names
  are no longer index members today -- the damage is concentrated in history.

  D3 is a *consequence* of the D2/A coverage gap but is a distinct failure mode
  (a value error, not a grouping shift) and is the reason D2/A is sequenced
  first. **Fix: D2/A coverage fallback + a hard gate (below).**

- **Gate gap (why none of this was caught).** `validate_snapshot` passed 66/66
  on data containing D3. Its sector check is only `multiple_sectors >= 5`, and
  `no_99pct_na_indicators` cannot see an unfired mask. D1 lives in the daily
  layer, which `validate_snapshot` does not inspect at all. The validator must
  be strengthened *before* the next rebuild, or the rebuild re-ships the same
  class of defect.

---

## 2. Fix D1 -- 10-Q refresh of *per-share* stubs + split-consistent basis

Root cause: fund layer is annual-only, so per-share quantities are up to ~1yr
stale, and they are not split-adjusted against the daily price.

Scope note -- only the **per-share** stubs are split-sensitive when paired with
a daily price: `stub_shares` (drives market cap and every mc-derived ratio) and
`stub_eps` (drives `pe_trailing` -> `peg` *directly*, not via mc). Aggregate-
dollar stubs (equity, revenue, fcf, ebitda, debt, cash, dividends, buybacks,
assets, ...) are split-invariant and become correct automatically once shares
is correct (a split cancels in `mc = price x shares`). So D1 = fix shares AND
eps; the original draft's "shares only" was incomplete -- `pe_trailing`/`peg`
have the identical split defect.

Data availability -- **already in cache, no refetch.** `shares_outstanding`
(and `eps_diluted`) carry quarterly vintages: verified on AAPL, Q1/Q2/Q3 rows
filed on 10-Q dates, including the `dei:EntityCommonStockSharesOutstanding`
cover count (period_end ~2wk pre-filing). The fund/stub extraction currently
keeps FY rows only (timeseries_builder.R:392); extend it to quarterly.
Extraction must pick the authoritative "as-of-filing" count from the several
share rows per filing (dei cover row = latest period_end), same ranking the FY
path already uses.

- Add quarterly per-share stubs to the fund layer, stamped by filed date.
- The daily lookup (`update_ticker_daily`) picks the most recent stub with
  `filed_date <= day` (the greedy PIT lookup already used).
- Apply the split factor to shares and eps in the daily recompute so per-share
  quantities and price share one basis.

Net: market cap, `pe_trailing`, `peg` in the daily cross-section become strictly
PIT-correct; residual staleness window shrinks from ~1yr to ~1qtr.

---

## 3. Fix D2 -- Sector dimension (A + B; C deferred)

Decision (no dated commercial source available): **A + B**, taxonomy stays
finviz's 11 sectors (canonical for `.SECTOR_NA_INDICATORS`).

**A -- Coverage fallback (closes the delisted-name gap; near-free).**
Of 342 delisted ever-members, only 143 have a finviz sector; finviz cannot
classify delisted names. Wave P is about to pull ~100 of them into the daily
universe, where they would land in an "Unknown" bucket and *degrade* the
sector-relative indicators -- defeating the survivorship-bias reduction the
Tiingo expansion is meant to deliver. Fill order:

1. finviz (live names, primary)
2. Sharadar TICKERS sector/industry (if a Nasdaq Data Link token is configured)
3. SEC SIC -> finviz-sector crosswalk (the `submissions/CIK.json` call in
   `fetch_filing_index`, fundamental_fetcher.R:958, already returns
   `subs$sic`/`subs$sicDescription` -- currently discarded)

`source` column records provenance (finviz / sharadar / sic).

Cost caveat -- the *submissions call* is free, but the crosswalk is a real,
validated artifact, so A is "cheap," not "near-free":

- **Exact-string requirement (silent-mask trap).** Both fallbacks must emit
  *exactly* the 11 finviz sector strings; `.SECTOR_NA_INDICATORS` keys on the
  literals `"Financial"` (NOT "Financial Services"), `"Utilities"`,
  `"Real Estate"` (indicator_compute.R:73). A crosswalk that emits a near-miss
  string silently disables the bank/utility/REIT NA-mask. Ship a unit test that
  asserts every crosswalk output is in `names of the finviz sector set`.
- **SIC gives sector, not industry.** SIC -> finviz *sector* is a defensible
  ~coarse map; SIC -> finviz *industry* (~150 buckets) is not. Delisted names
  filled from SIC will have a sector but a weak/absent industry, so Herfindahl
  (per-industry) stays thin for them. Sharadar's industry is better where a
  token exists. Note this as a per-source coverage tier.

**B -- Freeze-forward dated dimension (PIT-correct going forward).**
Add a `valid_from` column (SCD Type-2). Migration stamps all existing rows
`valid_from = <floor>` (best current estimate applied back to start). The
update tiers append a new-dated row only when a ticker's value *changes*.
Readers join on `max(valid_from) <= as_of` -- the change lands in
`assemble_snapshot` (pit_assembler.R:673) and `load_daily_cross_section`
(timeseries_builder.R:956), both of which currently join by ticker only.

**C -- deferred.** No dated source (WRDS/Compustat/Bloomberg) available.
SIC/Sharadar are current-value, not dated history, so strict historical PIT
sector is not achievable now. See Residuals.

---

## 4. State manifest

`cache/lookups/update_manifest.parquet`, one row per ticker:

| field | purpose |
|---|---|
| cik, ticker | key |
| last_filing_accession, last_filed_date | EDGAR freshness gate |
| last_companyfacts_fetch | last blob pull |
| last_price_date | price append cursor |
| last_daily_date | daily-layer cursor |
| splits_hash | detects a new split |
| sic | cached SIC (coverage fallback) |
| status | ok / price-purged / delisted / needs-backfill |

Plus a global watermark row (last successful run date). Every tier reads it,
does minimal work, writes it back -- idempotent and resumable.

**Bootstrap.** The manifest does not exist yet; the first run derives it from
current cache state -- `last_price_date` from each price parquet, `last_daily_date`
from each `_daily` layer, `last_filing_accession`/`last_filed_date` from the fund
layers / companyfacts cache -- then proceeds normally.

---

## 5. Cadence tiers

**Tier D -- daily (trading days), fast:**
1. Split guard FIRST (order matters). Refresh the splits cache and compare
   `splits_hash`. **The append in step 2 is only valid when no split occurred
   since the last fetch:** Yahoo's Close (col 4) is split-adjusted to *fetch-time*
   basis, so a split retro-rebases the *entire* history. Appending post-split
   rows onto a pre-split-basis cache produces a **mixed-basis series**, and
   `Close / .split_factor(date)` -- which assumes one consistent basis -- then
   yields wrong as-traded prices for every row. (Dividends do not trigger this;
   Close is split-adjusted only, not dividend-adjusted.) Therefore, on a new
   true split: **full price re-fetch (whole cache re-based) + splits-cache
   update + full rebuild of that ticker's daily layer.** No append path is
   taken for that ticker this run. This corrects the original draft, which said
   only "rebuild the daily layer."
2. Price *append* (no-split path): fetch `from = last_price_date + 1`, rbind,
   write. (Requires a price-cache append path; today `fetch_ticker_prices` is
   cache-first with `from` in the key and `update_all_daily` deletes +
   full-refetches.)
3. Daily-layer append (`update_ticker_daily`), now reading 10-Q per-share
   stubs (#2).
4. `augment_daily_ttm` (already wired) -- **but today it is a full rescan +
   rewrite of all ~605 `_daily` parquets every run** (timeseries_builder.R:881-892,
   not per-ticker scoped). This is the dominant per-run cost and undercuts the
   "sleek append" goal. Make it incremental (append eps_ttm/pe_ttm for new dates
   only, per changed ticker) as part of this work.
5. Output is a query: `load_daily_cross_section(today)`. Note it currently reads
   *every* ticker's full history to filter one date -- fine for occasional use,
   heavy for a daily consumer; a per-date cross-section cache is a candidate
   optimization (out of core scope, flagged).

**Tier W -- weekly / event-driven (fundamentals + shares):**
1. Freshness probe: `submissions/CIK.json` -> compare newest 10-K/10-Q
   accession vs manifest. Capture `sic` in the same call (A).
2. Only for changed CIKs: refetch companyfacts, **merge** (vintage-preserving,
   so `pit_dedup(as_of)` still resolves), rebuild fund layer + quarterly-share
   stub.

**Tier M -- monthly / quarterly (structure):**
1. Re-read `data/sp500_constituents_.csv` (externally fed membership) -> new
   constituent = full backfill (fundamentals -> fund layer -> daily layer from
   2010 -> `status=ok`); removal = stop updating (PIT membership already
   handles historical presence via `get_universe_at_date`).
2. Append a dated sector/industry snapshot (B): finviz + Sharadar/SIC fallback.
3. Extend quarterly `pit_*` snapshots when a new quarter-end passes.

---

## 6. Orchestration

`run_incremental_update(as_of = Sys.Date())`:
- reads the manifest, runs Tier D always, Tiers W/M when due (or forced by arg),
- never `stop()`s on one ticker (log + continue + mark `status`),
- writes updated manifest + run log.

`run_daily_update()` (pipeline_runner.R:159) becomes the shell that orchestrates
these tiers instead of re-assembling a single snapshot. Optionally wrapped by
the `schedule`/cron path for unattended runs.

---

## 7. Residual PIT ledger (the honest account)

After #2 and #3, the database is strictly PIT **except**:

- **Historical sector/industry (pre-dating).** Dates before we began dating the
  sector dimension carry today's best-estimate sector applied backward
  (Fix D2/A+B, C deferred). Impact: small for the 11 coarse sectors (large N per
  bucket); larger for the finer finviz industry that drives Herfindahl. To
  bound it, run the SIC-vs-finviz disagreement probe (compare the two
  classifications on current names) as a proxy for reclassification instability;
  revisit C only if disagreement is material.

Everything else -- fundamentals timing, price basis, market cap, membership --
is strictly PIT.

---

## 8. Rollout sequence

Context (2026-07-09): the post-Wave-P terminal rebuild (86f2429) is **already
complete**, so the "one rebuild" budget is spent -- and it shipped D1 and D3.
One more full rebuild is therefore unavoidable. It must be the last, and it must
be gated. The overnight runner (0182862) is reusable, so the cost is machine
time, not re-engineering.

Per THE_DISASTER_REBUILD_LESSON: verify on the 3-date golden subset; rebuild full
history exactly once, at the end.

**Phase 1 -- Strengthen the gate first (no rebuild; write the failing test).**
0. Add to `validate_snapshot`: (a) zero tickers with `sector == "Unknown"`;
   (b) for every sector in `.SECTOR_NA_INDICATORS`, its masked indicators are
   NA for all its members; (c) a daily-layer check (it is currently uninspected).
   Run against current snapshots -- it MUST fail 62/66 on (a)/(b). A gate that
   passes on known-bad data is not a gate.

**Phase 2 -- Value-changing fixes (no rebuild between them).**
1. **D2/A first** (it is the live bug, and sector feeds `.MASKED_STUB_OF` at
   fund-layer build, so fund layers must be rebuilt *with correct sectors already
   in place*): coverage fallback + crosswalk unit test asserting the 11 exact
   finviz strings. Confirms the 25 no-sector names resolve and D3 dies.
   Run the SIC-vs-finviz disagreement probe here (it is L1's bounding measurement
   and the crosswalk's validation, in one pass).
2. **D1**: 10-Q per-share stubs (shares AND eps, split-consistent). Golden-verify
   market cap, pe_trailing, peg on a name with a split in a no-annual-filing window.
3. **D2/B**: dated dimension + reader date-join. Value-neutral by construction
   (floor-stamping reproduces today's values), so the golden snapshots must come
   out **identical** -- a free, strong regression gate. If they differ, B is buggy.

**Phase 3 -- The single (final) full rebuild.** Run the reusable runner; the
strengthened gate from Phase 1 must pass 66/66, now including zero-Unknown and
mask-fires checks, plus the D1 anchors.

**Phase 4 -- Update machinery** (no historical value impact; safe after the
rebuild, which also leaves price caches on the consistent basis the append path
requires): manifest (+ bootstrap) -> split guard (full-refetch on split) + price
append -> incremental `augment_daily_ttm` -> `run_incremental_update` + D/W/M tiers.

**Phase 5 -- Optional:** per-date cross-section cache; scheduling; annual L1
drift report.
