# Design: Second Price Source for Yahoo-Purged Names (Wave P)

Status: IMPLEMENTED 2026-07-08 (commit b1e266f). Token was provided (free
signup) + phase-0 coverage verification.
Date: 2026-07-08. Companion doc: DESIGN_MULTICLASS_SHARES.md.

## 1. Problem

Yahoo purges all history when a ticker delists (or is renamed away).
Census 2026-07-08 across all 66 snapshots: 1,249 ticker-dates with
fundamentals present but market_cap NA purely for lack of price -- 35
tickers, far beyond the "13 names" previously documented:

- No price cache at all (Yahoo purged): COG, K, VIAC, HES, IPG, JNPR,
  MRO, CTXS, SEE, GPS, CERN, WBA, XLNX, HOLX, NLSN, PXD, KSU, ANSS,
  ATVI, WRK, HBI, FBHS, DISH, CMA, DRE, CDAY, DISCA, ABMD, TWTR, CTLT,
  SIVB, MXIM, DFS. (SEE/HBI/CMA/HOLX etc. looked alive but were
  acquired/purged by the time of the last full re-download.)
- Cache exists but wrong entity/window (ticker reuse): BTU (pre-2016
  Peabody vs relisted 2017 entity), INFO. These need OLD-entity data --
  overlaps the deferred old-CIK wave; recover here only if the provider
  distinguishes the old listing, else document as residual.
- DISCA additionally needs the multi-class shares fix (Wave M).

## 2. Provider choice

- Stooq: WALLED as of 2026-07-08 -- the CSV endpoint
  (`stooq.com/q/d/l/?s=...`) now sits behind a JavaScript proof-of-work
  challenge; plain HTTP is dead. Only usable via manual browser
  downloads. Rejected as the scripted source.
- Tiingo (CHOSEN, pending phase-0 verification): free tier, REST JSON,
  explicitly retains delisted tickers, and its `close` field is the raw
  as-traded print with `splitFactor`/`divCash` event columns -- exactly
  the basis this repo requires.
- Fallbacks if Tiingo coverage disappoints per name: Alpha Vantage
  (25 req/day free -- slow but fine for a handful), manual Stooq browser
  CSV for stragglers (one-time, documented provenance).

### Phase 0 (before any integration code)

User signs up at tiingo.com, sets `TIINGO_TOKEN` env var. A standalone
probe script (`tools/probe_tiingo_coverage.R`) hits
`api.tiingo.com/tiingo/daily/{symbol}/prices?startDate=2009-01-01`
for all 35 names + BTU/INFO variants and reports per name: date range,
splitFactor events, and the final close vs the known merger/final print.
GO/NO-GO per name before writing pipeline code.

## 3. Price-basis contract (the load-bearing part)

Downstream is built around ONE semantics (see CLAUDE.md price basis):
cached Close = split-adjusted (as of download), reader divides by
`.split_factor(date, splits)` to recover the as-traded print; real split
events must live in `cache/splits/{ticker}.parquet` because
`.split_ratio_between` also split-adjusts share issuance and TTM EPS.

Tiingo gives the INVERSE shape (raw close + split events). The adapter
therefore converts at write time to Yahoo-identical semantics:

- splits: days with `splitFactor != 1` -> quantmod-convention ratio
  (4:1 -> 0.25) -> write `cache/splits/{ticker}.parquet` (same schema as
  the getSplits path).
- prices: `cached_close = tiingo_raw_close * .split_factor(date, splits)`
  so that every existing reader (`get_price_on_date`,
  `timeseries_builder` vectorized path) recovers the as-traded print
  with ZERO changes.
- NEVER use Tiingo `adjClose` (dividend-rebased -- same defect class as
  Yahoo Adjusted; see the NVDA 2015 P/E 0.49 postmortem).

## 4. Integration points

1. New `.fetch_prices_tiingo(ticker, from)` in pit_assembler.R --
   httr GET, retry x3 with backoff, returns xts shaped like the Yahoo
   path (OHLCV columns, Close = col 4 in converted semantics). Symbol
   mapping: dots -> dashes does NOT apply (Tiingo uses e.g. BRK-B too --
   confirm in phase 0); delisted names keyed by final ticker.
2. `fetch_ticker_prices()` fallback chain: Yahoo (3 retries) ->
   Tiingo (if TIINGO_TOKEN set) -> NULL. Cache file
   `{ticker}_tiingo_{from}.parquet` -- honest provenance in the name.
3. New `.price_cache_path(ticker, from, cache_dir)` helper that resolves
   `_yahoo_` first then `_tiingo_`; replaces the 4 hand-rolled
   `sprintf("%s_yahoo_%s.parquet", ...)` call sites
   (pit_assembler.R:125,334; timeseries_builder.R:549;
   feature_compute_rF.R:197). This is the only downstream code change.
4. `load_ticker_splits()`: when the Tiingo path fetched a name, splits
   come from the same response (getSplits would fail on purged names);
   keep the existing empty-table-cache convention.
5. Never let a Tiingo series silently shadow a live Yahoo one: fallback
   fires only after Yahoo failure, and the probe report records which
   source owns each name.

## 5. Validation

- Unit: basis-conversion round trip on a synthetic series with two
  splits (raw -> cached -> reader -> raw identity); splitFactor -> ratio
  convention test.
- Truth anchors per recovered name (gate): final print vs known merger
  consideration (e.g. ATVI $95.00 cash 2023-10-13, TWTR $54.20
  2022-10-27, CERN ~$95 2022, MRO->COP exchange 2024-11), plus one
  mid-history as-traded close per name against SEC 10-K market-price
  tables (pre-2021 10-Ks disclose quarterly high/low -- PIT-clean
  reference).
- Cross-check basis: for 2-3 names also still on Yahoo until recently
  (DFS, CMA), compare overlapping dates Yahoo-vs-Tiingo as-traded
  (tolerance ~0.5%).
- Census regression: PRICE_ONLY 1,249 -> ~small residual (BTU/INFO
  old-entity + any phase-0 NO-GO names, all documented); zero changes to
  tickers already priced by Yahoo.
- Golden-dates protocol: 3 dates (2012-06-30, 2018-06-30, 2024-06-30),
  THEN one shared full-history rebuild together with Wave M.

## 6. Risks

- Tiingo free-tier limits (unique-symbol and hourly caps): 35 one-time
  names fit comfortably; cache makes it a non-issue after first fetch.
- Renamed-away tickers (GPS pre-rename, VIAC pre-PARA): provider may key
  history under the new symbol only; probe script decides per name,
  roster keeps the constituent-era ticker, adapter takes an optional
  `provider_symbol` override in a small map.
- Ticker reuse (BTU, INFO, possibly K): provider history under the
  reused symbol may be the NEW entity -- probe compares first/last dates
  against the constituent window; mismatches stay NA and move to the
  old-CIK wave.
- Survivorship of this fix: document that any future "refresh all
  prices" run must preserve `_tiingo_` caches (they will never
  re-download from Yahoo).
