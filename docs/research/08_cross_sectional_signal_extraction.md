# Cross-Sectional Signal Extraction (Deferred Research Branch)

Status: **deferred** -- parked 2026-04-21 in favor of the time-series fundamental
factor build for beta3 of the multifactor regime model. To be revisited once
beta3 is wired into the downstream regime pipeline.

## Scope

Separate from the time-series factor build. This branch asks a different
question:

> Given the [~500 tickers x 57 indicators] cross-section produced daily by
> `pit_assembler.R` / `timeseries_builder.R`, which indicators carry
> *independent* information about forward stock-level returns, and how should
> they be combined into a stock-level expected return?

Output of this branch is a **per-ticker expected-return signal**, not a scalar
time-series factor. Complementary to the beta3 work, not a substitute.

## Why it is worth a dedicated pass

The 57-indicator menu is large enough to fall in the regime of the modern
"characteristics zoo" literature. Hard selection (pick k features, drop the
rest) is almost certainly inferior to shrinkage-based combination in this
regime -- but we do not yet know by how much, nor which method generalizes to
our universe (S&P 500 only, not CRSP-wide).

## Starting reading list

Order is roughly by relevance, not chronology.

| Reference | Why |
|-----------|-----|
| Fama & MacBeth (1973), JPE | Two-pass cross-sectional regression; grammar for per-date factor testing. Baseline we must beat. |
| Green, Hand, Zhang (2017), RFS -- "The characteristics that provide independent information about average US monthly stock returns" | ~100 signals, exactly our problem shape; establishes how few independently priced characteristics survive. |
| Kozak, Nagel, Santosh (2020), JF -- "Shrinking the cross-section" | Bayesian / ridge shrinkage over a large characteristic menu; the modern replacement for hard feature selection. |
| Kelly, Pruitt, Su (2019), JFE -- "Characteristics are covariances: A unified model of risk and return" (IPCA) | Latent factors whose loadings are linear in observable characteristics. Natural fit for our z-scored matrix. |
| Gu, Kelly, Xiu (2020), RFS -- "Empirical asset pricing via machine learning" | Benchmark panel-ML paper; comparison point for any non-linear method. |
| Lettau & Pelger (2020), RFS -- "Estimating latent asset pricing factors" | Risk-premium PCA: weights variance *and* pricing error. Directly addresses the "PCA finds structure but not return" critique embedded in the orthogonalization framework. |

## Open design questions for when we restart

1. **Selection vs. shrinkage.** The `hybrid_feature_selection()` framework
   does hard selection. With 57 correlated signals, Kozak-Nagel-Santosh-style
   ridge may dominate. Need a head-to-head on our data.
2. **Panel standard errors.** Per-date cross-sectional `lm` t-stats are
   biased when residuals are cross-sectionally correlated (common return
   factor). Newey-West / Driscoll-Kraay or Fama-MacBeth SEs required.
3. **Financial-sector handling.** GP/A, inventory turnover, CAPEX/DA are NA
   for financials by design (CLAUDE.md). Any panel estimator must tolerate
   structured missingness -- either per-sector submodels or per-indicator
   imputation with a sector dummy.
4. **Target variable.** Forward 1M / 3M / 12M returns have very different
   signal-to-noise profiles and very different implied rebalance frequencies.
   Need to commit to a horizon before selecting features.
5. **Stability across regimes.** Feature selection frequency across rolling
   windows is itself a signal -- a feature that wins everywhere is different
   from one that wins only in specific regimes. Connects back to the beta3
   regime machinery.

## Relation to the beta3 build

The beta3 factor (Goal 1) uses the cross-section only as an intermediate:
indicators -> decile-spread time series -> collapsed to a single fundamental
return series. This branch (Goal 2) keeps the cross-section as the *output*:
it produces a per-stock ranking, not a market-wide series. The two do not
conflict and can share the decile-spread / factor-mimicking infrastructure
built for Goal 1.

## Next concrete step when resumed

Write a short reading brief (1 page per paper: method, data, what it
contributes, implementation cost) before touching code. Pick two methods to
pilot head-to-head: one shrinkage-based (Kozak et al.), one latent-factor
(IPCA). Baseline against Fama-MacBeth with the 57 indicators.

## Architecture notes carried over from the r_F design discussion (2026-04-21)

These decisions were made during the r_F (fundamental regime) module design
but affect this branch when it resumes:

1. **`factor_orthogonalization.R` is not in the r_F runtime path.** The
   per-ticker pipeline uses z-score -> PCA (contribution ranking) -> ensemble
   classifier only. Gram-Schmidt validation is reserved for offline research
   -- pruning the candidate indicator pool or studying the cross-section
   itself. When this branch picks up, GS can be re-introduced as a research
   instrument, not a pipeline component.

2. **No combinatorial-mode pipeline for fundamentals.** `feature_compute_rR.R`
   offers a `method = "combinatorial"` path that enumerates C(n,1)..C(n,k)
   feature subsets per ticker. r_F skips this: single PCA classification per
   ticker. If subset enumeration becomes interesting later (e.g. for
   fundamental regime stability studies), it is a wrapper around the core
   pipeline, not a core change.

3. **When orthogonalization is later applied, the recipe is fixed.**
   - Target y: N-day forward return, horizon in the 5-22 trading-day range
     (weekly to monthly). This is the horizon at which fundamental signal is
     practically investable and where the signal-to-noise is tolerable.
     Exact N to be pinned by validation.
   - Predictors X: z-scored fundamental indicators (full 57 or a
     pre-filtered subset).
   - Procedure: standard PCA + Gram-Schmidt as implemented in
     `factor_orthogonalization.R` -- unsupervised distance selection first,
     supervised t-value validation second.
   - Current execution assumes all candidate indicators are informative (no
     GS pruning at runtime). Optimisation is deferred until after the
     baseline r_F pipeline is running end-to-end.
