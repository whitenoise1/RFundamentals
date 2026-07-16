# Resolution — Prediction cross-repo audit RFundamentals items (2026-07-16)

Closes the three items in `docs/PENDING_prediction_audit_2026-07-15.md`
(RF-1, RF-2, RF-3). Every change was code-reviewed, unit-tested with positive
controls, and passed a full-stack integration load-gate (16/16) in the same
session. Not yet committed — commit in the same session or the next.

## Verification summary

- `tests/rf_tests` harness: **15/15** — RF-3 guard unit tests, RF-2 restatement
  positive control + fail-closed error + `vintages` bypass, RF-1 constant == 130
  registry (rF module loaded end-to-end).
- Integration load-gate: **16/16** — full module stack sources cleanly, guard
  defined exactly once, `get_fundamentals` carries `allow_restated` (default
  FALSE), `get_indicator_names()` == 130, rF candidate `setequal` registry.
- Every `get_fundamentals()` call site re-swept (after RF-3 restored the
  previously-deleted `tools/` files): all pass `as_of`, `vintages = TRUE`, or
  `allow_restated = TRUE`. No caller trips the new guard unintentionally.

## RF-1 — expand rF candidate set to the 130 registry

`.CANDIDATE_RF_INDICATORS` now `= get_indicator_names()` (derived, not
hardcoded). A load-time guard errors if the registry is unavailable rather than
falling back to a stale literal.

**Adjudicated design conflict.** The audit called the 57-list a silent stale bug
and asked for `setequal(., 130)`. But the rF module's *private, gitignored*
design docs (`PLAN_rF_MODULE.md`, `RF_MODULE_TECHNICAL_REFERENCE.md`) stated the
57-name subset was **deliberate** (pre-Wave, frozen feature space) — invisible to
the audit, which reviewed only the tracked repo. Verified the 57 was a clean
subset of the 130 (0 orphans, no dupes; 73 registry names deliberately excluded).
Presented the conflict to the user, who chose **expand to 130** (audit letter),
overriding the "deliberate 57." The two design docs were updated to record the
supersession. This is a modeling change: the rF PCA/regime classifier now sees
all 130 slow-moving indicators (subject to the per-ticker NA / zero-variance
filter already in `prepare_indicator_matrix()`).

Files: `R/feature_compute_rF.R` (constant + header), `R/pit_assembler.R:18`,
`rfundamentals_guide.R` (3 strings), `README.md:149` (already 130),
`docs/PLAN_rF_MODULE.md`, `docs/RF_MODULE_TECHNICAL_REFERENCE.md`,
`tests/test_feature_compute_rF.R` (assertion 57 → setequal/130).

Historical/dated docs that cite 47/57 as a point-in-time snapshot were left as-is
(e.g. `docs/research/06`, `00_README`, `LIBRARY_READINESS`, `WORKPLAN`); they are
explicitly labelled historical and several are gitignored.

## RF-2 — fail-closed `as_of` guard on `get_fundamentals()`

Added `allow_restated = FALSE`. When `as_of = NULL` and not `vintages` and not
`allow_restated`, the reader now `stop()`s: the latest-restated collapse is
look-ahead-contaminated. Chose hard error over warning per the global
Presumption-of-Leakage rule (fail closed). `vintages = TRUE` bypasses the guard
(raw table, no dedup) — the production PIT consumers use that path.

Positive control (`tests/test_fundamental_fetcher.R`): a planted 10-K/A
restatement of the same FY makes the `as_of = 2024-03-31` view read the original
1,000,000 while the latest view reads the restated 1,200,000; the no-arg default
errors. Callers updated to opt in: `demo_single_ticker.R`, `rfundamentals_guide.R`
(+ README, + docstring examples), `tests/test_fundamental_fetcher.R`,
`tests/test_integration_abcdef.R` (2), `tests/test_timeseries_builder.R`.

## RF-3 — restore (not delete) the Drive conflict copies

**Premise inversion.** The canonical files were not intact-with-extra-copies:
Drive had renamed each canonical to `foo (1).ext` and deleted the original. All
31 `(1)` copies were the only on-disk content — 17 tracked ones byte-identical to
the git index (shown `AD`: staged-added / worktree-deleted), 14 cache ones with no
canonical at all. So the correct fix was **restore** (rename `(1)` → canonical),
verify-before-rename, never overwriting an existing canonical. 0 `(N)` files
remain; the `AD` files are clean staged `A` again; the cache parquets were
preserved (no expensive regeneration).

Loader guard `.drop_drive_conflict_paths()` added (self-contained, `if(!exists)`
-guarded) to the 7 vulnerable read sites — broad `\.parquet$` and CIK/`ck`-prefix
globs — across `fundamental_fetcher.R` (2), `update_runner.R` (2), `ttm_eps.R`,
`timeseries_builder.R`, `pipeline_runner.R`. Suffix-anchored globs
(`_daily`/`_fund`/`pit_*_raw`) were already immune (the ` (N)` breaks the `$`
anchor); the two price `file.remove` globs need no guard (deleting a stale copy
during a forced refresh is harmless). `.gitignore` now ignores `* ([0-9]).*` /
`([0-9][0-9])` / `([0-9][0-9][0-9])` — verified to catch `(1)/(2)/(12)` but not a
legitimate 4-digit `(2023)`.

## Cross-repo follow-ups (handled in Prediction, not here)

- Confirm Prediction L0 assembly reads the 130-snapshot path, not the rF
  57-list (RF-1 "Prediction impact"). With RF-1 done both now yield 130, but the
  snapshot path remains the intended input.
- Prediction L0 loaders must also reject `(N)` paths and always pass `as_of`
  (RF-2/RF-3 Prediction counterparts) — per the audit, those live in Prediction
  `assembly/` / `contracts/`.
