# PENDING.md -- Queued Work Items

Register of accepted-but-not-started work. Unlike KNOWN_LIMITATIONS.md
(deliberate approximations with exit criteria), these are planned tasks
waiting for a session. Remove an entry when its work merges.

Recorded 2026-07-12, after the daily-update wave (PR #6/#7) closed: the
database is verified current and self-maintaining via
`run_incremental_update()`; both items below build on that.

---

## P1 -- Database backend + scheduled daily update

**Goal:** move consumption off ad-hoc parquet reads onto a proper DB
surface, and make the incremental update run unattended on a schedule.

### 1a. DB configuration

Today the store is ~3,000 parquet files on a Google-Drive-mounted
filesystem -- fine as a build cache, hostile as a serving layer (slow
sequential reads dominated every rebuild; see
docs/THE_DISASTER_REBUILD_LESSON.md, "Environment"). Wire the OUTPUT
layers (daily cross-sections, pit_* snapshots, per-ticker series) to a
database while keeping parquet as the build/PIT-vintage cache.

Considerations when picked up:
- DuckDB is the natural first candidate (zero-server, reads the existing
  parquet in place via `read_parquet`/views, R bindings mature); the
  ref/ corporate_action_db.R PostgreSQL patterns exist if a server DB is
  preferred.
- The expensive consumer today is `load_daily_cross_section`: it reads
  every ticker's full daily history to serve one date
  (timeseries_builder.R). A per-date cross-section table (or DuckDB view
  over the _daily parquets) is the flagged optimization from
  DESIGN_DAILY_UPDATE.md section 5, step 5.
- Cross-section-stage columns (ms_*, herf, ww_index, ch_inv_ia) are
  finalized at READ time by industry_adjust_cross_section -- a DB layer
  must either materialize finalized cross-sections per date or replicate
  that finalize step; never serve the raw ingredient columns.
- The dated sector SCD and the update manifest are small lookup tables;
  they can move to the DB as-is.

### 1b. Cron job for the daily update

The canonical entry point already exists and is cron-able:

```
Rscript tools/run_incremental_update.R
```

Remaining work:
- Schedule it (launchd on this Mac, or cron) for trading-day evenings,
  after close and after Yahoo settles (e.g. 22:00 ET). A run takes
  ~90 min for ~500 tickers (measured 2026-07-12: 104.1 min with all
  three tiers due; Tier D alone ~90 min), dominated by rate-limited
  per-ticker getSplits + getSymbols calls.
- Route the log somewhere durable (the run already appends a summary row
  to cache/update_run_log.csv; keep the full stdout log per run).
- Alert on failure: nonzero exit, or `split_rebuilds`/`price failures`
  spiking in the run log. A weekend/holiday run is a harmless no-op.
- Environment for the unattended shell: EDGAR_UA and tiingo_token must
  be present (~/.Renviron is read by R itself, so launchd/cron get them
  for free as long as HOME is set).
- Google Drive caveat: the cache must be fully hydrated/available when
  the job fires; File Provider placeholders caused one diagnostic detour
  historically.

---

## P2 -- Adaptation to a standalone R library (package)

**Goal:** turn the R/ module collection into an installable package
(`library(RFundamentals)`) instead of `source()` chains.

The current design assumes "all modules already sourced" (each file's
header says so) with cross-module dot-helpers and some deliberate
duplication for standalone sourcing. Packaging work when picked up:

- **Namespace:** DESCRIPTION/NAMESPACE; export the user-facing API
  (the README "Main functions" table + run_incremental_update,
  validate_snapshot/validate_daily_layer, build_* entry points), keep
  dot-prefixed helpers internal.
- **Collapse the keep-in-sync duplicates** that exist only because
  modules had to source standalone: .SPLITS_SYMBOL_MAP vs
  .PROVIDER_SYMBOL_MAP, .SPLITS_YAHOO_BLOCKLIST vs
  .YAHOO_REUSED_BLOCKLIST, .FUND_CACHE_ALIASES mirroring
  get_fundamentals' CIK-prefix fallback, the eleven .assert_output
  copies, the spinoff band constant (log 1.25) in three places. In a
  package these become one definition each; the sync tests in
  tests/test_price_fallback.R then retire.
- **Paths:** cache/, data/ CSVs and the two override CSVs are hardcoded
  relative paths. A package needs a configurable data root (option or
  env var, e.g. RFUNDAMENTALS_CACHE) with the current layout as default;
  ship the static CSVs (sector overrides, roster fallback, sector
  fallback) in inst/extdata.
- **Tests:** the tests/ scripts are Rscript-style with bespoke harnesses;
  port to testthat (or keep as-is and wire into R CMD check via a thin
  wrapper). The gate scripts in tools/ stay outside the package.
- **Non-exported surface consumed by scripts:** run_timeseries.R,
  demo_*.R, tools/*.R call internals (e.g. .sector_asof,
  build_ticker_fundamentals). Either export those or rewrite the scripts
  against the public API.
- **DESCRIPTION deps:** jsonlite, httr, arrow, data.table, quantmod,
  xts, zoo, xml2 (+ tidyselect used via arrow's col_select).
- **CLAUDE.md is gitignored**: the project instructions that describe
  module architecture live only locally; a package vignette should
  absorb the durable parts (PIT rules, data-source notes, storage
  layout).

**Sequencing note:** P2 (package paths/namespace) before P1a (DB layer)
would avoid wiring the DB against paths that then move; if P1a comes
first, keep its path handling behind the same configurable data root P2
will introduce.
