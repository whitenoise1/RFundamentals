# Library-Readiness Assessment

Assessment date: 2026-07-01. Reviews the repository through three lenses --
**documentation, metadata, gatekeeping** -- to scope the eventual extraction of
RFundamentals into a standalone, installable R library.

> **Status update (2026-07-05).** The count landscape below is superseded:
> the OpenAP expansion (docs/research/09) grew the indicator set
> 59 -> 130 across Waves 1-5, and the 47/57/59 reconciliation described
> in Phase 1 was overtaken by wave-by-wave doc syncs -- README, CLAUDE.md,
> docs/INDICATORS.md, and the module docstrings are now consistent at 130
> (18 families, 25 price-sensitive, 45 fetched concepts). Snapshots and
> timeseries layers are regenerated per wave (Wave 5 regeneration in
> progress as of this note). Still OPEN and unchanged: everything
> structural -- no package skeleton / DESCRIPTION / NAMESPACE / LICENSE
> (Phase 2), duplicated internals (`.assert_output` clones, config
> constants; Phase 3), no CI / testthat / hermetic-vs-cache-gated split
> (Phase 4), and the data-product hardening items (schema versioning,
> snapshot provenance columns, mislabel fixes; Phase 5). Root
> WORKPLAN.md / RESEARCH_INDICATORS.md duplicates now carry superseded
> banners pointing to docs/. The rfundamentals_guide.R count drift
> remains (still pre-expansion in places).

Scope note: this repo's purpose is the **recollection of fundamental
indicators** (fetch SEC EDGAR XBRL -> compute 59 indicators -> assemble
point-in-time cross-sections + daily timeseries). It is a data/indicator
database, not a forecasting project. All recommendations below serve that
purpose only.

## Bottom line

The code is correct and the data model is genuinely strong -- the blocker to a
standalone library is **structure, not logic**. The project is currently a
collection of `source()`-d scripts, not a package, and its documentation and
metadata fractured across three indicator counts (**47 / 57 / 59**) as the code
settled on 59. None of the gaps are deep; the work is mechanical packaging plus
consistency reconciliation.

## Cross-cutting root causes (appear in all three lenses)

1. **No package skeleton** -- no `DESCRIPTION`, `NAMESPACE`, `LICENSE`, `man/`,
   `vignettes/`, `.Rbuildignore`. A consumer cannot `library(rfundamentals)`;
   they must `source()` seven files in order.
2. **The 57 -> 59 rollout is only half-propagated** -- code + `README.md` are at
   59; `docs/INDICATORS.md`, `rfundamentals_guide.R`,
   `RF_MODULE_TECHNICAL_REFERENCE.md`, `PLAN_rF_MODULE.md`, `WORKPLAN.md` are at
   57; `CLAUDE.md` + `CLAUDE_CODE_SETUP.md` are still at **47**; and the
   production `cache/snapshots/` is stale at **57** columns.
3. **Duplication everywhere** -- docs (root vs `docs/`), the validator
   (`.assert_output` copy-pasted ~10x), config (`EDGAR_UA` / rate in three
   files), helpers (`.pad_cik`, `.edgar_fetch`).
4. **No verification gate** -- no CI, no `R CMD check`, no lockfile, no single
   test entry point.

## Findings by dimension (ranked, deduplicated)

### Documentation

- **[Critical]** `docs/INDICATORS.md` -- the canonical indicator reference --
  says 57 and entirely omits `shares_outstanding` and `public_float` (no
  formula/tag/reference). Retitle section 8 to "Size (5 indicators)" and add
  entries 8.4/8.5 (`public_float` uses XBRL tag `EntityPublicFloat`).
- **[Critical]** `CLAUDE.md` still says "47 indicators (35 baseline + ...)" in
  ~4 places (`:31`, `:66`, `:71`, `:101`) -- the master project spec.
- **[High]** Root-vs-`docs/` duplication: `RESEARCH_INDICATORS.md` is a
  byte-identical duplicate; root `WORKPLAN.md` is a stale subset missing the
  entire section 11 (r_F module). Also `CLAUDE.md:13` references
  `docs/CLAUDE_CODE_SETUP.md`, which does not exist (the file is at repo root)
  -- broken path.
- **[High]** `ttm_eps.R` has zero roxygen (3 public functions:
  `build_ttm_eps_series`, `load_ticker_splits`, `augment_daily_ttm`);
  `rfundamentals_guide.R` (the de-facto vignette) is stale at 57 in ~7 places.
- **[Medium]** Count drift also in `docs/RF_MODULE_TECHNICAL_REFERENCE.md`
  (57 -> 59; timeseries table `[T x 61]` should be `[T x 63]`),
  `docs/PLAN_rF_MODULE.md`, `docs/WORKPLAN.md`, `CLAUDE_CODE_SETUP.md`.
- **[Low]** Two public functions do the same thing: `indicator_names()` and
  `get_indicator_names()`. Pick one as the exported API, deprecate the other.

### Metadata

- **[Critical]** No `DESCRIPTION` / `NAMESPACE`. Real dependency list (derived
  by grepping `library()`/`require()`/`::`): `arrow, data.table, httr,
  jsonlite, quantmod, xts, zoo` plus base `stats, utils`. The public API
  (`build_*` / `compute_*` / `load_*`) vs internal (`.`-prefixed) export
  surface is undeclared. Per-file `library()` calls must become `Imports:` +
  roxygen `@importFrom`.
- **[Critical]** No real LICENSE. `README.md` says only "for research and
  educational use", which is not an OSI license and `R CMD check` rejects it.
  Pick MIT / Apache-2.0 / GPL-3 and add both the `License:` field and a
  `LICENSE` file. Separately, the default `EDGAR_UA` fallback
  (`"BSTAR/1.0 contact@email.com"`) is a placeholder that violates SEC's
  real-User-Agent policy -- document it as required config, do not ship it.
- **[High]** `CLAUDE.md` "Output Format" is factually wrong: it claims an M x N
  matrix with four metadata *header rows*; the actual parquet is **tidy** (one
  row per ticker; 4 meta columns `date, ticker, industry, sector` + indicator
  columns). Correct the doc to the real tidy schema.
- **[High]** Parquet files carry no embedded schema-version / build-timestamp /
  provenance -- the schema is positional/implicit (only R's auto data.table
  attributes are attached). Write explicit key/value metadata at save time
  (`rfundamentals_schema_version`, `built_at`, `source`, `n_indicators`) for
  snapshots, `_daily`, and `_fund`.
- **[Medium]** Provenance is uneven across the three product schemas. The
  fundamentals cache lineage is excellent (`cik, tag, concept, accession, form,
  filed, period_end/start, fiscal_year, fiscal_qtr, unit, period_type`). But
  `_daily` carries only `date, price, fiscal_year, filed_date`, and PIT
  snapshots drop `filed_date` / `accession` / `fiscal_year` entirely -- a
  snapshot value cannot be traced to a filing without re-joining the
  fundamentals cache. Carry at least `filed_date` + `accession` into snapshots.
- **[Medium]** Config constants duplicated across files: `.EDGAR_UA` /
  `.EDGAR_RATE_SEC` (0.11s) are redefined in `constituent_master.R`,
  `fundamental_fetcher.R`, and `company_info.R`. Centralize into one config
  module / `options()` / `zzz.R` `.onLoad`.
- **[Low]** Mislabels persist: `fcf_stability` is really a CFO stability
  (`cfo_std/total_assets`); `inventory_sales_change` is really
  inventory-vs-COGS. Units (fraction vs ratio vs raw dollars) are not
  documented machine-readably. Ship a units column in the indicator manifest.

### Gatekeeping

- **[Critical]** No CI / `R CMD check` / lockfile, and no single test runner --
  each test is a custom-harness script (`test()`/PASS/FAIL + `quit(status)`),
  not testthat, so CI has nothing to call.
- **[Critical]** The suite is not hermetic: 5 integration tests
  `stopifnot(file.exists("cache/..."))` and hard-abort on a fresh clone (the
  `cache/` is not in the repo); unit sections silently `SKIP` when the cache is
  absent, so real coverage collapses to near-zero on checkout. Split hermetic
  units (fixtures already exist) from cache-gated tests (make the latter `skip`,
  not `stopifnot`) and provide a fixture-builder.
- **[High]** `.assert_output` is copy-pasted ~10x (6 byte-identical defs + 3
  renamed clones + 1 function-local) and coverage is uneven. `ttm_eps.R` is
  **0/5** and overwrites every `_daily.parquet` with no output contract; most
  exported functions in `fundamental_fetcher.R`, `timeseries_builder.R`, and
  `pipeline_runner.R` are unvalidated -- violating the CLAUDE.md rule "every
  public function validates output via `.assert_output()`". Consolidate into one
  internal validator and apply it to every exported data-returning function.
- **[High]** Exported entry points do not validate inputs: bare
  `as.Date(snapshot_date)` returns `NA` on a malformed date; no ticker-format
  checks; enum params validated only by late `stop()`. Add guard clauses
  (`stopifnot`, validated date parse, `match.arg`) at each entry point.
- **[Medium]** `tools/gate_test_*.R` are hand-run diagnostics reading the live
  cache, not repeatable CI gates. Promote the stable ones into the testthat
  suite with fixtures, or mark `tools/` dev-only via `.Rbuildignore`.
- **[Medium]** No dedicated hermetic test for `company_info.R`;
  `test_pipeline_runner.R` is cache-dependent.
- **[Strength]** The `stop()`-on-single-ticker discipline is fully honored:
  every batch loop is `tryCatch -> warning -> next`; no `stop()` sits inside a
  per-ticker loop. Logging is `message()`-only (no `cat`/`print`). This runtime
  robustness contract is production-grade.

## What is already strong (the foundation is solid)

- Fundamentals cache lineage is exactly what a point-in-time library needs
  (full filing provenance).
- Filing-date stamping is preserved end-to-end -- point-in-time correctness is
  structurally intact.
- Disciplined naming (snake_case; coherent `_ttm` / `_yoy` / `_qoq` / `_raw` /
  `f_*` suffixes; `.`-prefix for internals).
- The output-contract pattern already exists (`.assert_output` with named
  predicate checks) -- it needs consolidation and universal application, not
  invention.
- `README.md` is fully current (59 throughout); `docs/INDICATORS.md` structure
  (per-indicator formula + XBRL tag + reference + financial-NA flags) is a
  strong basis for `man/`.
- Batch robustness (warn-and-continue) is production-grade.

## Roadmap to a standalone library (phased)

### Phase 1 -- Reconcile (cheap; closes 57 -> 59 loose ends)

- Update every doc to 59: `docs/INDICATORS.md` (including the two missing size
  entries `shares_outstanding`, `public_float`), `CLAUDE.md`,
  `rfundamentals_guide.R`, `RF_MODULE_TECHNICAL_REFERENCE.md`,
  `PLAN_rF_MODULE.md`, `WORKPLAN.md`, `CLAUDE_CODE_SETUP.md`.
- Delete the duplicated root copies (`WORKPLAN.md`, `RESEARCH_INDICATORS.md`);
  keep `docs/` as the single source. Fix the broken
  `docs/CLAUDE_CODE_SETUP.md` reference in `CLAUDE.md`.
- Correct the `CLAUDE.md` / `README.md` "Output Format" description to the real
  tidy (one-row-per-ticker) schema.
- Regenerate the stale production `cache/snapshots/` at 59 columns (as was
  already done for `cache/snapshots_diag/`).

### Phase 2 -- Packaging

- Add `DESCRIPTION` (Imports: arrow, data.table, httr, jsonlite, quantmod, xts,
  zoo, stats, utils; Depends: R (>= 4.1)) and a real `LICENSE`.
- Generate `NAMESPACE` via roxygen2; replace per-file `library()` calls with
  `@importFrom`; declare the export surface.
- Complete roxygen on `ttm_eps.R` and `company_info.R`; generate `man/`.
- Promote `rfundamentals_guide.R` to a real `vignettes/*.Rmd`.

### Phase 3 -- De-duplicate internals

- One shared `.assert_output` / `.pad_cik` / `.edgar_fetch` in a `utils.R`.
- Centralize `EDGAR_UA` / rate-limit config into `zzz.R` `.onLoad` / `options()`.

### Phase 4 -- Verification gate

- Migrate tests to testthat; split hermetic units from cache-gated tests
  (`skip`, not `stopifnot`); add a fixture-builder and a single runner.
- Add a CI workflow running `R CMD check` + the test suite; add a lockfile
  (`renv.lock`) or pinned `Imports` versions.
- Apply `.assert_output` to every exported data-returning function, starting
  with `ttm_eps.R`.
- Add input-validation guard clauses at each exported entry point.

### Phase 5 -- Data-product hardening

- Embed `schema_version` / `built_at` / `source` in every parquet.
- Propagate `filed_date` / `accession` into snapshots.
- Fix the `fcf_stability` / `inventory_sales_change` mislabels (with deprecation
  aliases).
- Publish a canonical indicator manifest (name -> formula -> units -> which
  output files contain it).

## Suggested sequencing

Phases 1 and 3 are low-risk and can be done immediately. Phase 2 is the
gate to installability. Phase 4 is what makes the library trustworthy for a
third party. Phase 5 turns "installs and runs" into "versioned, traceable data
product". Do 1 -> 2 -> 3 -> 4 -> 5; Phase 1 is a good standalone first pass.
