# Practitioner Methodologies

## MSCI/Barra (USE4 Model) -- Industry Standard

1. Compute raw descriptors.
2. Calculate robust mean and SD iteratively.
3. Winsorize to within 3 SD of robust mean.
4. Standardize to cap-weighted mean of 0, equal-weighted SD of 1.
5. For global models: style factors can explain more cross-sectional variation than country or industry factors in some periods.

## AQR

### Asness, Frazzini, Israel, Moskowitz (2015) -- "Fact, Fiction, and Value Investing"
Journal of Portfolio Management, 42(1), 34.

- Multiple measures of value produce more stable portfolios with higher Sharpe and information ratios.
- Intra-industry composites of multiple reasonable measures deliver higher Sharpes than any single measure.
- Value factor Sharpes increase further when combined with momentum.
- Industry neutrality is applied in their factor construction.

## Robeco

- Emphasizes enhanced vs. generic factor definitions.
- Measuring factor exposure with generic definitions (as in standard indices) can lead to misconceptions.
- Incorporates proprietary signals, short-term dynamics, and alternative data.
- Factor investments that appear unbalanced through a generic factor lens can be well-balanced through enhanced definitions.

## Practical Pitfalls

### Look-Ahead Bias in TS Z-Scores

- Expanding window: uses all data up to time t. Reacts strongly to initial movements; means/SDs stay elevated long after shocks.
- Rolling window: adapts quickly to recent data, ignores old patterns. Preferred when z-score should not "get used to" persistently high/low levels.
- Critical rule: all rolling/expanding operations must use only data available at time t (shift by 1 period when used as predictors).
- Recommendation: 5-year rolling windows are the practitioner standard (MSCI's TS value research uses this).

### Winsorization

- MSCI/Barra: winsorize to 3 SD of robust mean.
- MAD (Median Absolute Deviation): more robust; values with modified z-scores beyond +/-3.5 are labeled outliers (Iglewicz and Hoaglin).
- Always winsorize before standardization, not after.

### Survivorship Bias

- Dramatically inflates backtest returns and underestimates risk.
- Our S&P 500 dataset with 845 rows / 829 tickers (including historical constituents) partially addresses this.
- Ensure fundamentals for delisted stocks are retained in the database.

### Structural Breaks in Ratios

- Accounting standard changes (IFRS adoption, ASC 842 lease capitalization, goodwill impairment changes) cause structural breaks.
- Historical z-scores become misleading after structural breaks.
- Robust mean/SD approach (iterative, as in Barra) provides some protection.

## Sources

- https://www.top1000funds.com/wp-content/uploads/2011/09/USE4_Methodology_Notes_August_2011.pdf
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2595747
- https://www.robeco.com/files/docm/docu-robeco-guide-to-factor-investing-global.pdf
- https://www.msci.com/downloads/web/msci-com/research-and-insights/blog-post/factors-and-esg-the-truth-behind-three-myths/factors-and-esg-the-truth-behind-three-myths-MSCI-FaCS-Methodology.pdf
