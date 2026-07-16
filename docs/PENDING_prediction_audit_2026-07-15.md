# PENDING — tasks surfaced by the Prediction cross-repo audit (2026-07-15)

**Source:** `Prediction/documentation/analysis_001.md` refresh (2026-07-15). Every finding was
verified against live code; `file:line` citations below. These are **RFundamentals-side**
defects, to be fixed in a **dedicated RFundamentals work session** — they are not part of the
Prediction modeling work.

Where an item also needs a guard on the *consumer* side, the Prediction-side counterpart is
noted and is handled in Prediction (not here).

**Status legend:** `PENDING` = not started; `DONE` = fixed (date noted). Add a
fix commit + date on completion.

**All three resolved in the 2026-07-16 RFundamentals session** (RF-1 expand-to-130,
RF-2 fail-closed, RF-3 restore-not-delete). See the per-item DONE notes below and
`docs/PENDING_prediction_audit_resolution_2026-07-16.md` for the full write-up.

---

## RF-1 — Reconcile `.CANDIDATE_RF_INDICATORS` (57) with `.INDICATOR_NAMES` (130) · DONE (2026-07-16)

> **Resolution.** `.CANDIDATE_RF_INDICATORS` now derives from
> `get_indicator_names()` (the 130-registry) in `R/feature_compute_rF.R` — the
> registry is the single source of truth; the two can never diverge. The rF
> regime factor's feature space is now all 130 indicators (was a curated 57).
>
> **Design conflict surfaced and adjudicated.** The rF module's *private*
> (gitignored) design docs stated the 57-name subset was **deliberate** (pre-Wave,
> frozen). The audit — reviewing only the tracked repo — could not see those docs.
> The 57 was verified to be a clean subset of the 130 (0 orphans). The user was
> shown the conflict and chose **expand to 130** (audit letter), overriding the
> "deliberate 57" notes; `PLAN_rF_MODULE.md` / `RF_MODULE_TECHNICAL_REFERENCE.md`
> were updated to record the supersession. Stale "57" strings fixed in the guide,
> `pit_assembler.R` header, README, and the rF docs. Test: `setequal` + `== 130`
> in `tests/test_feature_compute_rF.R` (private) and the session gate.

**Problem.** `.CANDIDATE_RF_INDICATORS` is hardcoded to the legacy **57-indicator** era, while
the live registry is **130**. A consumer using the `feature_compute_rF` path silently gets 57.
- `R/feature_compute_rF.R:115` — the 57-entry vector; comment `R/feature_compute_rF.R:113`
  ("The 57 indicator columns emitted by timeseries_builder").
- Truth: `.INDICATOR_NAMES` `R/indicator_compute.R:160` (130); `.INDICATOR_FAMILIES`
  `R/indicator_compute.R:224` sums to 130.
- Stale "57"/"47" also in: `rfundamentals_guide.R:212,218,243`, `R/pit_assembler.R:18`,
  `docs/research/06_indicator_verification_report.md:12` (acknowledged-known).

**Action.**
1. Make the registry the single source of truth — either regenerate `.CANDIDATE_RF_INDICATORS`
   from `.INDICATOR_NAMES`, or delete it and have `feature_compute_rF` reference the registry.
2. Update every stale "57"/"47" string in the guide, `pit_assembler.R` header, and docs.

**Acceptance.** A test asserting `setequal(.CANDIDATE_RF_INDICATORS, .INDICATOR_NAMES)` and
`length == 130`; no source/doc file cites 57 or 47.

**Prediction impact — MEDIUM (verify, don't assume).** Confirm Prediction's L0 assembly reads
the **130-registry / PIT-snapshot** path (snapshots are built from the 130), not
`feature_compute_rF`'s 57-list. If it only reads the snapshots, we are unaffected — but the
trap should still be removed here.

---

## RF-2 — Add an `as_of`-required guard to `get_fundamentals()` · DONE (2026-07-16)

> **Resolution.** Chose **hard error** (fail-closed, per the global Presumption-of-
> Leakage rule). `get_fundamentals(as_of = NULL)` now `stop()`s unless
> `allow_restated = TRUE` is passed to opt in deliberately. `vintages = TRUE` still
> bypasses (returns the raw table before dedup) — the production PIT consumers
> (`pit_assembler.R`, `timeseries_builder.R`) use that path and are unaffected. All
> latest-view callers (demo, guide, README, 3 tests) updated to opt in. Positive
> control in `tests/test_fundamental_fetcher.R`: a planted 10-K/A restatement makes
> the `as_of` view (1,000,000) differ from the latest view (1,200,000), and the
> `as_of = NULL` default errors.

**Problem.** `get_fundamentals(..., as_of = NULL)` silently returns the **latest restated**
view (look-ahead-contaminated), with no error or warning.
- Signature `R/fundamental_fetcher.R:2142`; body ends `pit_dedup(dt, as_of = as_of)`
  `R/fundamental_fetcher.R:2187`; NULL default documented as intended `:2129-2132`.

**Action.** When `as_of` is NULL, either **error** ("`as_of` required for PIT-correct reads")
or emit a **loud warning** and tag the result non-PIT. Keep a deliberate opt-in for the
latest-restated use (e.g. `allow_restated = TRUE`).

**Acceptance.** Calling without `as_of` and without the opt-in errors/warns; a positive-control
test shows a known restatement differs between `as_of` and latest views.

**Prediction impact — HIGH as discipline, but self-protected.** Prediction's L0 assembly will
**always pass `as_of`** (a required argument at our wrapper). That call-site discipline is
**not deferred** — it lives in Prediction `assembly/`/`contracts/`. The repo-side guard here is
defense-in-depth (deferrable).

---

## RF-3 — Purge Google-Drive duplicate / conflict files · DONE (2026-07-16)

> **Resolution — the audit's premise was inverted.** The canonical files were NOT
> intact-with-extra-copies: Drive had **renamed each canonical to `foo (1).ext` and
> deleted the original**. All 31 `(1)` copies were the *only* on-disk content (the 17
> tracked ones were byte-identical to the git index; the 14 cache ones had no
> canonical at all). So the fix was to **restore** (rename `(1)` → canonical), not
> delete — verify-before-rename, never overwriting an existing canonical. Result: 0
> `(N)` files remain; the formerly `AD` (staged-added / worktree-deleted) files are
> clean staged `A` again. Loader guard `.drop_drive_conflict_paths()` added to 7
> vulnerable broad-`\.parquet$` / CIK-prefix read sites across 5 modules (the
> suffix-anchored `_daily`/`_fund`/`pit_*_raw` globs were already immune; the two
> price-`file.remove` globs need no guard). `.gitignore` now ignores `* ([0-9]).*`
> etc. (verified it catches `(1)/(2)/(12)` but not a legit 4-digit `(2023)`).

**Problem.** Drive sync has produced `(N)`-suffixed conflict copies that glob loaders can
silently read.
- 14 `(N)` copies in `cache/timeseries/` (e.g. `AAPL_daily (1).parquet`, `A_fund (1).parquet`).
- `(1)` source/doc copies in root, `tests/`, `tools/` (e.g. `tools/oldcik_fetch_prices (1).R`,
  `tests/test_update_runner (1).R`, `docs/DESIGN_DAILY_UPDATE (1).md`,
  `docs/KNOWN_LIMITATIONS (1).md`).

**Action.**
1. Delete all `* (N).*` copies after confirming the canonical file is intact.
2. Add a guard in glob-based loaders to reject/skip `(N)`-suffixed paths.
3. Add a `.gitignore` rule (`* (1).*` etc.) and review the Drive-sync workflow that creates them.

**Acceptance.** No `* (N).*` files under `cache/` or source dirs; loaders reject them.

**Prediction impact — MEDIUM.** Prediction's L0 loaders must also **reject `(N)` paths**
(a runtime guard in `assembly/`, plus the `.gitignore` pattern already added). Our-side guard
is not deferred; the repo cleanup is.

---

*Created by the Prediction audit. Not committed here — commit in the RFundamentals session.
Cross-ref: `Prediction/documentation/analysis_001.md` §2 (gate-zero status).*
