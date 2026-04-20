# Regime-Conditional Factor Models

## Regime-Switching Factor Models

### Dynamic Factor Allocation Leveraging Regime-Switching Signals (2024)
arXiv 2410.14841.

- Uses sparse jump models to identify bull/bear regimes for individual factors.
- Standardized features based on risk and return measures from historical factor active returns plus broader market environment variables.
- Factors' conditional Sharpe ratios differ substantially across regimes.

### Regime-Switching Factor Investing with Hidden Markov Models (MDPI, 2020)

- After regime classification via HMMs, factor model quality evaluated in distinct regimes.
- Standardized factor exposures enable cross-regime comparison of factor effectiveness.

## Momentum Crashes and Regime Dependence

### Daniel and Moskowitz (2016) -- "Momentum Crashes"
Journal of Financial Economics, 122(2), 221-247.

- Momentum crashes occur in "panic" states following market declines when volatility is high.
- Implementable dynamic strategy based on forecasts of momentum's mean and variance approximately doubles the alpha and Sharpe ratio of a static strategy.
- Demonstrates that standardization dimension interacts strongly with market regimes.
- TS standardization must account for regime-dependent volatility.

## Factor Timing

### Neuhierl, Randl, Reschenhofer (2024) -- "Timing the Factor Zoo"

- Analysis of 300+ factors and 39 timing signals.
- Timing improves returns and Sharpe ratios while reducing drawdowns.
- Improvements are highest for profitability and value factors.
- Past factor returns and volatility are the most successful individual predictors.
- Aggregating multiple predictors using partial least squares dominates any individual predictor.

### Singha (2025) -- "Discovery of a 13-Sharpe OOS Factor: Drift Regimes Unlock Hidden Cross-Sectional Predictability"
arXiv 2511.12490.

- Combines value and short-term reversal signals only during stock-specific drift regimes (>60% positive days in trailing 63-day windows).
- Uses standardized z-scores ensuring zero mean and unit variance.
- Claims OOS Sharpe >13 on S&P 500 2004-2024 (extraordinary -- warrants skepticism).
- Methodology of regime-conditional signal activation is relevant to BSTAR architecture regardless.

## Implications for BSTAR Architecture

1. Factor effectiveness varies dramatically by regime -- TS signals may dominate in trending regimes, CS within-sector signals in mean-reverting regimes.
2. Let the regime model discover which standardization dimension matters in which regime rather than hard-coding combinations.
3. TS standardization must account for regime-dependent volatility (expanding/contracting windows or volatility-adjusted z-scores).
4. Profitability and value factors benefit most from timing -- prioritize regime conditioning for these factor groups.

## Sources

- https://arxiv.org/html/2410.14841v1
- https://www.mdpi.com/1911-8074/13/12/311
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2371227
- https://www.aeaweb.org/conference/2024/program/paper/8GNzb57Z
- https://arxiv.org/abs/2511.12490
