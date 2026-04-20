# Cross-Sectional vs. Time-Series Factor Signals

## Foundational Papers

### Asness, Moskowitz, Pedersen (2013) -- "Value and Momentum Everywhere"
Journal of Finance.

- Documents consistent value and momentum premia across 8 asset classes.
- Value and momentum are negatively correlated within and across asset classes.
- Factors studied are primarily cross-sectional (ranking assets vs. peers at each point in time).
- Global funding liquidity risk identified as partial source.

### Moskowitz, Ooi, Pedersen (2012) -- "Time Series Momentum"
Journal of Financial Economics, 104(2), 228-250.

- Introduces formal concept of time-series momentum (TSMOM).
- A security's own past 12-month return predicts its future return.
- Key distinction from CS momentum: CS ranks assets relative to each other (long winners, short losers); TS takes absolute positions based on each asset's own past return.
- Significant TSMOM across 58 liquid futures/forwards.
- Strategy performs best during extreme markets.

### Baz, Granger, Harvey, Le Roux, Rattray (2015) -- "Dissecting Investment Strategies in the Cross Section and Time Series"
Most directly relevant paper for architectural decisions.

Key findings:
- Value works well in cross-section but poorly in time-series.
- Momentum works well in time-series but poorly in cross-section.
- Carry works about equally well in both dimensions.
- Mathematical relationship: CS portfolio weights = TS portfolio weights minus the cross-sectional average (a "global factor"). A CS portfolio is a TS portfolio hedged for the global factor.
- Combined CS + TS across all strategies achieved Sharpe ratio of 1.61 over 25 years.
- Overall correlation between combined CS and combined TS: only 0.43 -- substantial diversification benefit.

## Time-Series Value as a Concept

TS value = comparing a stock's (or sector's) current valuation to its own historical valuation.

- TS value alone underperforms CS value (Baz et al.).
- TS value captures mean reversion in valuations (is Tech expensive relative to its own historical P/E?).
- CS value captures relative cheapness across the universe (is Tech cheap relative to Utilities?).
- The two are weakly correlated and thus complementary.

### Ehsani and Linnainmaa (2022) -- "Time-Series Efficient Factors"
- Factors in standard asset pricing models are unconditionally minimum-variance inefficient.
- By exploiting autocorrelation in factor returns, they construct "time-series efficient" versions that vary factor weight over time.
- Sharpe ratio improvements of ~0.32 units (z=3.13).
- Momentum is essentially timing other factors, not a distinct risk factor.

### Research Affiliates -- "The Value Premium Is Mean-Reverting"
- Valuation spreads between value and growth stocks mean-revert.
- Returns to value strategies are predictable by their respective value spreads.
- Supports the thesis that TS value signals contain useful information.

## Sources

- https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12021
- https://www.sciencedirect.com/science/article/pii/S0304405X11002613
- https://www.cmegroup.com/education/files/dissecting-investment-strategies-in-the-cross-section-and-time-series.pdf
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3555473
- https://www.researchaffiliates.com/publications/articles/7-the-value-premium-is-mean-reverting
