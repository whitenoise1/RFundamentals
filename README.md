# RFundamentals

Point-in-time fundamental database for S&P 500 constituents. Builds a daily-frequency matrix of 57 indicators for ~500 stocks from free public data (SEC EDGAR + Yahoo Finance). Designed as input to quantitative factor models.

## What it does

- Fetches XBRL financial statements from SEC EDGAR for all current and historical S&P 500 members
- Computes 57 fundamental indicators per ticker per trading day (valuation, profitability, growth, leverage, efficiency, cash flow quality, shareholder return, size, plus academic anomaly factors)
- Maintains a two-layer storage system: sparse fundamentals (one row per fiscal year) and dense daily series (one row per trading day, price-sensitive ratios updated with market close)
- Produces cross-sectional snapshots with raw values and z-scores, ready for factor model consumption
- All data is stamped by SEC filing date (not period end) to prevent look-ahead bias

## Quick start

### Dependencies

```r
install.packages(c("jsonlite", "httr", "arrow", "data.table", "quantmod", "xts", "zoo"))
```

### Environment

Set a valid SEC EDGAR user agent (required by their rate limit policy):

```r
Sys.setenv(EDGAR_UA = "YourName your@email.com")
```

### Initial build

Run the pipeline modules in order (one-time setup, ~30-60 min depending on network):

```r
source("R/constituent_master.R")   # resolve CIKs for all S&P 500 tickers
source("R/sector_classifier.R")    # fetch sector/industry from Finviz
source("R/fundamental_fetcher.R")  # download XBRL data from EDGAR
source("R/indicator_compute.R")
source("R/pit_assembler.R")
source("R/pipeline_runner.R")
source("R/timeseries_builder.R")

build_constituent_master()         # writes cache/lookups/constituent_master.parquet
build_sector_industry()            # writes cache/lookups/sector_industry.parquet
build_fundamentals()               # writes cache/fundamentals/{CIK}_{ticker}.parquet
build_timeseries()                 # writes cache/timeseries/{ticker}_fund.parquet
                                   #        cache/timeseries/{ticker}_daily.parquet
```

Or from the command line:

```
Rscript run_timeseries.R build
```

### Daily update

Append the latest trading day to all tickers (~1-2 min):

```r
update_all_daily()
```

Or:

```
Rscript run_timeseries.R update
```

## Usage

After the initial build, the library provides data as in-memory data.tables. Full API reference and copy-paste examples are in [`rfundamentals_guide.R`](rfundamentals_guide.R).

### Load a single stock's time series

```r
# data.table: date, price, pe_trailing, roe, ... (57 indicator columns)
aapl <- load_ticker_timeseries("AAPL")

# filter by date range
aapl_2024 <- load_ticker_timeseries("AAPL", from = "2024-01-01")
```

### Load a cross-section for one date

```r
# list with $raw and $zscored data.tables (~500 tickers x 57 indicators)
cs <- load_daily_cross_section("2024-06-28")

# raw factor matrix
cs$raw

# z-scored version (cross-sectional, winsorized at [-3, 3])
cs$zscored
```

### Screen and filter

```r
# cheapest 10 stocks by earnings yield
cs$raw[order(-earnings_yield)][1:10, .(ticker, sector, earnings_yield, pe_trailing)]

# highest quality: top ROE with moderate leverage
cs$raw[debt_equity < 2][order(-roe)][1:10, .(ticker, sector, roe, roa, debt_equity)]

# Piotroski value picks: F-score >= 8 and bottom-quartile P/E
cs$raw[f_score >= 8 & pe_trailing < quantile(pe_trailing, 0.25, na.rm = TRUE),
       .(ticker, sector, f_score, pe_trailing, roe)]
```

### Feed a factor model

```r
# z-scored matrix ready for model input
z_matrix <- as.matrix(cs$zscored[, -"ticker"])
rownames(z_matrix) <- cs$zscored$ticker

# sector-level factor profile: median z-score per indicator
model_input <- merge(cs$raw[, .(ticker, sector)], cs$zscored, by = "ticker")
sector_profile <- model_input[, lapply(.SD, median, na.rm = TRUE),
                              by = sector, .SDcols = get_indicator_names()][order(sector)]
```

## Main functions

| Function | Returns | Purpose |
|----------|---------|---------|
| `load_ticker_timeseries(ticker, from, to)` | data.table | One ticker, all dates |
| `load_daily_cross_section(date, zscore)` | list ($raw, $zscored) | All tickers, one date |
| `get_fundamentals(ticker)` | data.table | Raw SEC filings (long format) |
| `list_timeseries_tickers()` | character vector | Available tickers |
| `get_indicator_names()` | character vector | 57 indicator names |
| `build_timeseries()` | (side effect) | Historical build, all tickers |
| `update_all_daily()` | (side effect) | Daily incremental update |

## Indicators

57 indicators across 10 categories. Full formulas, XBRL tags, academic references, and interpretation in [`docs/INDICATORS.md`](docs/INDICATORS.md).

| Category | Count | Examples |
|----------|-------|---------|
| Valuation | 8 | P/E, P/B, P/S, P/FCF, EV/EBITDA, EV/Revenue, Earnings Yield, PEG |
| Profitability | 6 | Gross/Operating/Net Margin, ROE, ROA, ROIC |
| Growth | 5 | Revenue Growth YoY/QoQ, EPS Growth, OpInc Growth, EBITDA Growth |
| Leverage | 5 | D/E, Net Debt/EBITDA, Interest Coverage, Current/Quick Ratio |
| Efficiency | 3 | Asset/Inventory/Receivables Turnover |
| Cash Flow Quality | 3 | FCF/NI, OpCF/NI, CapEx/Revenue |
| Shareholder Return | 3 | Dividend Yield, Payout Ratio, Buyback Yield |
| Size | 3 | Market Cap, Enterprise Value, Revenue |
| Tier 1 Research | 15 | GP/A, Asset Growth, Sloan Accrual, Piotroski F-Score (9 components + composite) |
| Tier 2 Research | 6 | Cash-Based OP, FCF Stability, SGA Efficiency, CapEx/DA, DSO Change, Inventory/Sales Change |

12 indicators are price-sensitive (update daily with market close). 45 are fundamental-only (update when a new SEC filing appears).

## Architecture

```
R/
  constituent_master.R     CIK resolution, roster cleanup
  fundamental_fetcher.R    EDGAR XBRL fetch, dedup, cache
  sector_classifier.R      Finviz sector/industry lookup
  indicator_compute.R      Pure computation, 57 indicators
  pit_assembler.R          Point-in-time cross-sectional snapshots
  pipeline_runner.R        Orchestration, validation
  timeseries_builder.R     Daily time series builder

cache/                     (generated, not in repo)
  fundamentals/            EDGAR XBRL per ticker
  prices/                  Yahoo OHLCV per ticker
  lookups/                 constituent master, sector map
  timeseries/              two-layer daily time series
```

## Data sources

| Source | What | Access |
|--------|------|--------|
| SEC EDGAR | XBRL financial statements (28 tags, 10-K/10-Q) | `data.sec.gov/api/xbrl/companyfacts/` (free, rate-limited) |
| Yahoo Finance | Daily OHLCV prices | `quantmod::getSymbols()` (free) |
| Finviz | Sector/industry classification | Web scrape with CSV fallback |

## License

For research and educational use.
