# Combining Cross-Sectional and Time-Series Standardization

## MSCI (2021) -- "The Theory of (Value) Relativity"

The single most relevant practitioner paper for the CS+TS combination question.

Compared over 2001-2020 using five valuation descriptors (B/P, E/P, CE/P, EBIT/EV, Fwd E/P):
- Time-series approach: z-score each stock's valuation ratio within its own 5-year history.
- Cross-sectional approach: z-score within the appropriate stock universe at each point in time.

Key findings:
1. Low correlation between TS and CS value factors -- they capture fundamentally different aspects.
2. TS performance was more consistent across decades (including value's "lost decade" 2011-2020).
3. TS approach showed lower volatility, enhancing risk-adjusted returns.
4. Combined approach (CS + TS) produced higher absolute returns over the full 20-year period.
5. TS is not susceptible to sector variation but can be affected by broad market valuation shifts.
6. CS quickly captures changes but is less stable.

Recommendation: Investors get "a clearer valuation picture by combining the two."

## GSAM (2018) -- "How to Combine Investment Signals in Long/Short Strategies"

Two approaches tested:
- Integrated: combine factor signals into a composite score, then determine weights.
- Mixed: calculate weights for individual signals, then combine weights.

Key findings:
- Mixed approach delivers higher Sharpe ratios than Integrated approach.
- At high levels of factor exposure, combining signals delivers higher risk-adjusted returns due to diversification.

Implication for BSTAR: Keep CS and TS as separate features; let the regime model discover optimal weighting rather than pre-combining.

## Asness, Friedman, Krail, Liew (2000) -- "Style Timing: Value versus Growth"

- Used composite of industry-adjusted valuation indicators.
- Out-of-sample z-scores calculated from the current B/P spread in context of its own history achieved "far more robust, higher Sharpe ratio strategy."
- Caveat: Asness et al. (2018) later found insufficient evidence for value as a timing signal when tested more rigorously.

## Research Affiliates -- "Factor Timing: Keep It Simple"

- Calculate factor portfolio's discount as current valuation relative to average historical valuation using P/B, P/E, P/S, P/D.
- Factors trading at significant discounts vs. long-term average produce larger future excess returns.
- Recommended: combine valuation-based signals with momentum for factor timing.

## Sources

- https://www.msci.com/research-and-insights/blog-post/the-theory-of-value-relativity
- https://www.gsam.com/content/dam/gsam/pdfs/institutions/en/articles/2018/Combining_Investment_Signals_in_LongShort_Strategies.pdf
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2595747
- https://www.researchaffiliates.com/publications/articles/828-factor-timing-keep-it-simple
