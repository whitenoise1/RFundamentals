# Research: Multi-Dimensional Factor Standardization

Research compiled 2026-04-16. Informs the architectural decision on how to
standardize 47 fundamental indicators before feeding into BSTAR's
regime-conditional return forecasting pipeline.

## Core Question

How to handle three standardization dimensions:
1. Cross-sectional (stock vs. all peers today)
2. Time-series (stock vs. its own history)
3. Within-sector (stock vs. sector peers today)

## Files

| File | Topic |
|------|-------|
| 01_CS_vs_TS_factor_signals.md | Foundational CS vs TS literature |
| 02_combining_CS_TS_standardization.md | Evidence for combining dimensions |
| 03_sector_neutralization.md | Within-sector construction, double sort |
| 04_regime_conditional_factors.md | Interaction with regime models |
| 05_practitioner_methodologies.md | Barra, AQR, Robeco approaches + pitfalls |

## Key Conclusions

1. CS and TS are weakly correlated (r=0.43) -- combining yields diversification (Baz et al. 2015).
2. Within-industry component is the stronger predictor for L/S (Ehsani, Harvey, Li 2023).
3. Keep dimensions as separate features; let the regime model weight them (GSAM "Mixed" approach).
4. Factor effectiveness varies by regime; profitability and value benefit most from timing.
5. 5-year rolling window is the practitioner standard for TS z-scores.
6. Winsorize before standardizing (3 SD robust mean or MAD-based).
