# RFundamentals

Point-in-time fundamental database for S&P 500 constituents. Builds a daily-frequency matrix of 130 indicators for ~500 stocks from free public data (SEC EDGAR + Yahoo Finance). Designed as input to quantitative factor models.

## What it does

- Fetches XBRL financial statements from SEC EDGAR for all current and historical S&P 500 members; the cache preserves reporting vintages so any past date can be reconstructed as it was known then
- Computes 130 fundamental indicators per ticker per trading day (valuation, profitability, growth, leverage, efficiency, cash flow quality, shareholder return, size, plus 71 academic anomaly factors from the Chen-Zimmermann open asset pricing catalog: accrual decompositions, external financing, seasonal surprises, level ratios, investment, and composite scores)
- Maintains a two-layer storage system: sparse fundamentals (one row per fiscal year) and dense daily series (one row per trading day, price-sensitive ratios updated with market close)
- Produces cross-sectional snapshots with raw values and z-scores, ready for factor model consumption
- All data is stamped by SEC filing date (not period end) to prevent look-ahead bias; daily market cap and P/E pair the as-traded price with split-consistent, 10-Q-fresh share counts, and sector/industry is a dated dimension resolved as of each date
- Stays current via a manifest-driven incremental updater (daily price/layer appends, weekly EDGAR freshness probe, monthly roster/sector/snapshot maintenance) -- no history reprocessing

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
source("R/ttm_eps.R")              # split events + TTM EPS augment
source("R/pipeline_runner.R")
source("R/timeseries_builder.R")
source("R/update_runner.R")        # incremental update tiers

build_constituent_master()         # writes cache/lookups/constituent_master.parquet
build_sector_industry()            # writes cache/lookups/sector_industry.parquet (dated SCD)
build_fundamentals()               # writes cache/fundamentals/{CIK}_{ticker}.parquet
build_timeseries()                 # writes cache/timeseries/{ticker}_fund.parquet
                                   #        cache/timeseries/{ticker}_pershare.parquet
                                   #        cache/timeseries/{ticker}_daily.parquet
```

Or from the command line:

```
Rscript run_timeseries.R build
```

### Staying current

The manifest-driven incremental updater is the canonical way to keep the
database current (cron-able; ~90 min for ~500 tickers, dominated by
rate-limited network calls):

```
Rscript tools/run_incremental_update.R
```

Per run it always executes **Tier D** (per ticker: split guard first --
a new split forces a full price re-fetch + daily-layer rebuild for that
ticker, since Yahoo re-bases history; otherwise price append + daily-layer
append + TTM re-augment), plus **Tier W** weekly (EDGAR submissions
freshness probe; refetch + fund/per-share layer rebuild only for CIKs
with a new 10-K/10-Q) and **Tier M** monthly (roster diff with full
backfill of new constituents, dated sector refresh, quarterly snapshot
grid extension). State lives in `cache/lookups/update_manifest.parquet`;
the first run bootstraps it from cache state. Equivalent in R:

```r
run_incremental_update()                      # tiers on their own cadence
run_incremental_update(force_monthly = TRUE)  # force a tier
```

`update_all_daily()` / `Rscript run_timeseries.R update` remain as the
low-level layer-append path (no split guard, no manifest) for ad-hoc use.

## Usage

After the initial build, the library provides data as in-memory data.tables. Full API reference and copy-paste examples are in [`rfundamentals_guide.R`](rfundamentals_guide.R).

### Load a single stock's time series

```r
# data.table: date, price, pe_trailing, roe, ... (130 indicator columns)
aapl <- load_ticker_timeseries("AAPL")

# filter by date range
aapl_2024 <- load_ticker_timeseries("AAPL", from = "2024-01-01")
```

### Load a cross-section for one date

```r
# list with $raw and $zscored data.tables (~500 tickers x 130 indicators)
cs <- load_daily_cross_section("2024-06-28")

# raw factor matrix
cs$raw

# z-scored version (cross-sectional, robust median/MAD, clipped at [-5, 5])
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
| `get_indicator_names()` | character vector | 130 indicator names |
| `build_timeseries()` | (side effect) | Historical build, all tickers |
| `run_incremental_update()` | (side effect) | Keep everything current (tiers D/W/M) |
| `update_all_daily()` | (side effect) | Low-level daily layer append |

## Indicators

130 indicators across 18 families. Full formulas, XBRL tags, academic references, and interpretation in [`docs/INDICATORS.md`](docs/INDICATORS.md); construction provenance and OpenAP replication notes in `docs/RESEARCH_INDICATORS.md`.

| Category | Count | Examples |
|----------|-------|---------|
| Valuation | 8 | P/E, P/B, P/S, P/FCF, EV/EBITDA, EV/Revenue, Earnings Yield, PEG |
| Profitability | 6 | Gross/Operating/Net Margin, ROE, ROA, ROIC |
| Growth | 5 | Revenue Growth YoY/QoQ, EPS Growth, OpInc Growth, EBITDA Growth |
| Leverage | 5 | D/E, Net Debt/EBITDA, Interest Coverage, Current/Quick Ratio |
| Efficiency | 3 | Asset/Inventory/Receivables Turnover |
| Cash Flow Quality | 3 | FCF/NI, OpCF/NI, CapEx/Revenue |
| Shareholder Return | 3 | Dividend Yield, Payout Ratio, Buyback Yield |
| Size | 5 | Market Cap, Enterprise Value, Revenue, Shares Outstanding, Public Float |
| Tier 1 Research | 15 | GP/A, Asset Growth, Sloan Accrual, Piotroski F-Score (9 components + composite) |
| Tier 2 Research | 6 | Cash-Based OP, FCF Stability, SGA Efficiency, CapEx/DA, DSO Change, Inventory/Sales Change |
| Balance-Sheet Change (Wave 1) | 15 | dNOA, DelFINL, DelNetFin, Total Accruals, Inventory/PPE Investment, Equity Growth, LTNOA Growth |
| External Financing (Wave 2) | 7 | Net Debt/Equity Financing, XFIN, Composite Debt Issuance, Share Issuance 1y/5y, Net Payout Yield |
| Seasonal Surprise (Wave 3) | 6 | ChTax, Earnings-Increase Streak, Revenue Surprise, SUE, Earnings Consistency, ROA-quarterly |
| Levels (Wave 4) | 19 | R&D/ME, Enterprise B/M, CF/ME, Tangibility, Tax-to-Book, AT/ME, Book/Market Leverage, Operating Leverage |
| Investment (Wave 4) | 10 | Capex Growth 2y/3y, Percent Total Accruals, ChAssetTurnover, Industry-Adjusted Capex Growth |
| Mohanram (Wave 5) | 9 | G-score: 8 binary components vs sector medians + composite |
| Distress (Wave 5) | 2 | Ohlson O-Score, Altman Z-Score |
| Structure (Wave 5) | 3 | Industry Herfindahl, KZ Index, WW Index |

25 indicators are price-sensitive (update daily with market close). 105 are fundamental-only (update when a new SEC filing appears). A small set is finalized at the cross-section stage (Mohanram binaries, Herfindahl, WW industry term, industry-adjusted capex growth): per-ticker series carry ingredients, and any cross-section reader (`load_daily_cross_section`, snapshots) serves the finalized values. Sector exclusions: financials are NA for COGS/inventory-based indicators; financials, utilities and real estate are NA for the distress scores; utilities for Herfindahl.

## Architecture

```
R/
  constituent_master.R     CIK resolution, roster cleanup
  fundamental_fetcher.R    EDGAR XBRL fetch, vintage-preserving cache
  sector_classifier.R      Finviz sector/industry lookup
  indicator_compute.R      Pure computation, 130 indicators
  pit_assembler.R          Point-in-time cross-sectional snapshots
  pipeline_runner.R        Orchestration, validation (incl. daily-layer gate)
  timeseries_builder.R     Daily time series builder (fund/pershare/daily layers)
  update_runner.R          Incremental updates: manifest + Tier D/W/M cadence
  ttm_eps.R                Split events + split-adjusted trailing-TTM EPS augment
  feature_standardizer.R   Causal rolling / cross-sectional features
  feature_compute_rF.R     r_F factor-model feature pipeline
  company_info.R           Company metadata helpers

cache/                     (generated, not in repo)
  fundamentals/            EDGAR XBRL per ticker (filing vintages)
  prices/                  Yahoo/Tiingo OHLCV per ticker
  splits/                  split events per ticker
  lookups/                 constituent master, dated sector map, update manifest
  timeseries/              three-layer daily time series (fund/pershare/daily)
  snapshots/               PIT cross-sections (raw + z-score per date)
```

## Data sources

| Source | What | Access |
|--------|------|--------|
| SEC EDGAR | XBRL financial statements (45 concepts / 101 tag aliases, 10-K/10-Q) | `data.sec.gov/api/xbrl/companyfacts/` (free, rate-limited) |
| Yahoo Finance | Daily OHLCV prices, split events | `quantmod::getSymbols()` (free) |
| Tiingo | Price/split fallback for delisted names Yahoo purged | REST API (free token, optional -- `tiingo_token` in `~/.Renviron`) |
| Finviz | Sector/industry classification (dated SCD + static overrides for delisted names) | Web scrape with CSV fallback |

## License

For research and educational use.
