# Sector Neutralization and Within-Industry Factor Construction

## Ehsani, Harvey, Li (2023) -- "Is Sector Neutrality in Factor Investing a Mistake?"
Financial Analysts Journal, 79(3), 95-117. Definitive academic treatment.

Key findings:
- Factor characteristics have two sources of predictive power: across-industry (sector bets) and within-industry (stock-specific).
- The firm-specific (within-industry) component is generally the stronger predictor.
- For long-short strategies: sector neutralization helps 78% of the time.
- For long-only strategies: Sharpe ratios similar with or without; adjusting actually reduces Sharpe in most cases.
- Mathematical condition: a mean-variance investor should form the sector-neutral factor only if the ratio of the Sharpe ratios (across vs. within) is less than their correlation. Frequently met in L/S portfolios.

## Cohen, Polk, Vuolteenaho (2003) -- "The Value Spread"
Journal of Finance.

- Decompose firm-level B/M into inter- and intra-industry components.
- The B/M effect in returns is mostly an intra-industry effect.
- Intra-industry ROE coefficient is 9x larger than industry ROE after 1 year, 19x larger after 15 years.
- Created "industry-relative" factor-mimicking portfolios using both components.

## S&P Dow Jones Indices -- "Exploring Techniques in Multi-Factor Index Construction"

- Sector-neutral portfolios may be more efficient than sector-agnostic ones.
- Top-down approaches may dilute exposures but are still efficient.
- Factor score-based weighting schemes improve efficiency.
- "Factor efficiency ratio" (FER) can measure factor purity without a risk model.

## Sector vs. Industry Granularity

Using industries instead of sectors provides more closely related comparison groups, but many industries are too small for meaningful comparisons. Sectors usually provide a better balance of granularity and adequate sample size.

## "Double Sort" / Sequential Standardization

Practical methodology:
1. First z-score within sector/industry (remove sector effects).
2. Then standardize across the full universe (make comparable).

### Barra/MSCI Approach (USE4 Model) -- Industry Standard

1. Compute raw descriptors.
2. Calculate robust mean and standard deviation iteratively.
3. Winsorize to within 3 SD of robust mean.
4. Standardize to cap-weighted mean of 0, equal-weighted SD of 1.

### Seeking Alpha Quant Ratings (Practical Example)

Each stock's five factors (Value, Growth, Profitability, Momentum, EPS Revisions) compared to same metrics for other stocks in its sector. Different sectors have different aggregate profitability and growth rates.

### QuantRocket Implementation

Uses zscore(groupby=sector) -- removes both the sector mean and normalizes by sector SD. Removes the effect of one sector having wider variation in a metric than another.

## Sources

- https://people.duke.edu/~charvey/Research/Published_Papers/P165_Is_sector_neutrality.pdf
- https://personal.lse.ac.uk/polk/research/jofi_5802005.pdf
- https://www.spglobal.com/spdji/en/documents/research/research-exploring-techniques-in-multi-factor-index-construction.pdf
- https://www.top1000funds.com/wp-content/uploads/2011/09/USE4_Methodology_Notes_August_2011.pdf
- https://www.quantrocket.com/blog/sector-neutralization/
