# RFundamentals: Indicator Reference

116 fundamental indicators computed from SEC EDGAR XBRL filings and Yahoo Finance
market data. Each indicator is listed with its formula, data sources, and
interpretation.

**Data sources:**
- **EDGAR**: SEC EDGAR XBRL companyfacts (10-K and 10-Q filings). Stamped by
  filing date, not period end date, to avoid look-ahead bias.
- **Yahoo**: Daily adjusted closing price via `quantmod::getSymbols()`.
- **Derived**: Intermediate quantities computed from EDGAR fields (gross profit,
  EBITDA, FCF, total debt, net debt).

**Notation:**
- `t` = current fiscal year, `t-1` = prior fiscal year
- `q` = current quarter, `q-1` = prior quarter
- All XBRL tags are in the `us-gaap` namespace
- Financial sector (banks) lack COGS and inventory; indicators marked
  **[NA for financials]** are set to NA for these firms

---

## Derived Quantities

These intermediate values are computed once and reused across multiple indicators.
They do not appear in the output; they are building blocks.

| Name | Formula | XBRL tags |
|------|---------|-----------|
| Gross Profit | Revenue - COGS | Revenues, CostOfGoodsAndServicesSold |
| EBITDA | Operating Income + D&A | OperatingIncomeLoss, DepreciationDepletionAndAmortization |
| Free Cash Flow (FCF) | Operating Cash Flow - CapEx | NetCashProvidedByUsedInOperatingActivities, PaymentsToAcquirePropertyPlantAndEquipment |
| Total Debt | Long-Term Debt + Short-Term Debt | LongTermDebt, ShortTermBorrowings |
| Net Debt | Total Debt - Cash | (derived), CashAndCashEquivalentsAtCarryingValue |
| Market Cap | Price x Shares Outstanding | Yahoo price, CommonStockSharesOutstanding |
| Enterprise Value | Market Cap + Net Debt | (derived) |
| Invested Capital | Equity + Total Debt - Cash | StockholdersEquity, (derived) |

---

## 1. Valuation (8 indicators)

These combine market price with accounting data. They update daily as price
moves (price-sensitive indicators).

### 1.1 `pe_trailing` -- Price-to-Earnings (Trailing)

| | |
|---|---|
| **Formula** | Price / EPS (diluted) |
| **Guard** | \|EPS\| >= 0.01; otherwise NA |
| **XBRL** | EarningsPerShareDiluted |
| **Price** | Yahoo adjusted close on filing date |
| **Interpretation** | How much investors pay per dollar of earnings. Lower = cheaper. |

### 1.2 `peg` -- Price/Earnings to Growth

| | |
|---|---|
| **Formula** | P/E / (EPS Growth YoY x 100) |
| **Guard** | EPS Growth must be > 0 |
| **XBRL** | EarningsPerShareDiluted (current and prior year) |
| **Interpretation** | P/E adjusted for growth. < 1 suggests undervalued relative to growth rate. |

### 1.3 `pb` -- Price-to-Book

| | |
|---|---|
| **Formula** | Market Cap / Stockholders' Equity |
| **Guard** | Equity must be > 0 |
| **XBRL** | StockholdersEquity, CommonStockSharesOutstanding |
| **Price** | Yahoo adjusted close |
| **Interpretation** | Market value vs. book value. < 1 means market values the firm below its accounting net worth. |

### 1.4 `ps` -- Price-to-Sales

| | |
|---|---|
| **Formula** | Market Cap / Revenue |
| **XBRL** | Revenues, CommonStockSharesOutstanding |
| **Price** | Yahoo adjusted close |
| **Interpretation** | What investors pay per dollar of revenue. Useful for unprofitable companies where P/E is undefined. |

### 1.5 `pfcf` -- Price-to-Free Cash Flow

| | |
|---|---|
| **Formula** | Market Cap / FCF |
| **Guard** | FCF must be > 0 |
| **XBRL** | NetCashProvidedByUsedInOperatingActivities, PaymentsToAcquirePropertyPlantAndEquipment |
| **Price** | Yahoo adjusted close |
| **Interpretation** | What investors pay per dollar of cash the business actually generates after reinvestment. |

### 1.6 `ev_ebitda` -- Enterprise Value to EBITDA

| | |
|---|---|
| **Formula** | Enterprise Value / EBITDA |
| **Guard** | EBITDA must be > 0 |
| **XBRL** | OperatingIncomeLoss, DepreciationDepletionAndAmortization, LongTermDebt, ShortTermBorrowings, CashAndCashEquivalentsAtCarryingValue |
| **Price** | Yahoo adjusted close |
| **Interpretation** | Valuation independent of capital structure. Lower = cheaper. Preferred over P/E for cross-company comparison because it normalizes for debt levels. |

### 1.7 `ev_revenue` -- Enterprise Value to Revenue

| | |
|---|---|
| **Formula** | Enterprise Value / Revenue |
| **XBRL** | Revenues, plus EV components |
| **Interpretation** | Like P/S but accounts for debt and cash. Useful for capital-intensive or highly leveraged firms. |

### 1.8 `earnings_yield` -- Earnings Yield

| | |
|---|---|
| **Formula** | EPS (diluted) / Price |
| **Guard** | \|Price\| >= 0.01 |
| **XBRL** | EarningsPerShareDiluted |
| **Price** | Yahoo adjusted close |
| **Interpretation** | Inverse of P/E. Higher = cheaper. Directly comparable to bond yields and risk-free rate. |

---

## 2. Profitability (6 indicators)

Fundamental-only: change only when a new filing appears.

### 2.1 `gross_margin` -- Gross Margin

| | |
|---|---|
| **Formula** | Gross Profit / Revenue |
| **XBRL** | Revenues, CostOfGoodsAndServicesSold |
| **Interpretation** | Revenue retained after direct production costs. Measures pricing power and production efficiency. Higher is better. |

### 2.2 `operating_margin` -- Operating Margin

| | |
|---|---|
| **Formula** | Operating Income / Revenue |
| **XBRL** | OperatingIncomeLoss, Revenues |
| **Interpretation** | Revenue retained after all operating expenses (COGS + SGA + D&A). Reflects operational efficiency before financing and taxes. |

### 2.3 `net_margin` -- Net Margin

| | |
|---|---|
| **Formula** | Net Income / Revenue |
| **XBRL** | NetIncomeLoss, Revenues |
| **Interpretation** | Bottom-line profitability. Includes all costs: operations, interest, taxes. |

### 2.4 `roe` -- Return on Equity

| | |
|---|---|
| **Formula** | Net Income / Stockholders' Equity |
| **Guard** | Equity must be > 0 |
| **XBRL** | NetIncomeLoss, StockholdersEquity |
| **Interpretation** | Return generated on shareholders' capital. Higher is better, but very high ROE with high leverage may indicate financial risk rather than operational excellence. |

### 2.5 `roa` -- Return on Assets

| | |
|---|---|
| **Formula** | Net Income / Total Assets |
| **XBRL** | NetIncomeLoss, Assets |
| **Interpretation** | Return generated on total capital employed (equity + debt). Comparable across capital structures. |

### 2.6 `roic` -- Return on Invested Capital

| | |
|---|---|
| **Formula** | Net Income / Invested Capital |
| **Guard** | Invested Capital must be > 0 |
| **XBRL** | NetIncomeLoss, StockholdersEquity, LongTermDebt, ShortTermBorrowings, CashAndCashEquivalentsAtCarryingValue |
| **Interpretation** | Return on capital actually deployed in the business (equity + debt - excess cash). Most precise measure of capital allocation skill. |

---

## 3. Growth (5 indicators)

Year-over-year and quarter-over-quarter growth rates. Fundamental-only.

### 3.1 `revenue_growth_yoy` -- Revenue Growth (Year-over-Year)

| | |
|---|---|
| **Formula** | (Revenue_t - Revenue_{t-1}) / \|Revenue_{t-1}\| |
| **XBRL** | Revenues (current and prior FY) |
| **Interpretation** | Annual top-line growth rate. Uses absolute value of prior year to handle sign changes correctly. |

### 3.2 `revenue_growth_qoq` -- Revenue Growth (Quarter-over-Quarter)

| | |
|---|---|
| **Formula** | (Revenue_q - Revenue_{q-1}) / \|Revenue_{q-1}\| |
| **XBRL** | Revenues (current and prior quarter) |
| **Interpretation** | Sequential quarterly revenue momentum. Not seasonally adjusted; compare with caution across seasonal businesses. |

### 3.3 `eps_growth_yoy` -- EPS Growth (Year-over-Year)

| | |
|---|---|
| **Formula** | (EPS_t - EPS_{t-1}) / \|EPS_{t-1}\| |
| **XBRL** | EarningsPerShareDiluted (current and prior FY) |
| **Interpretation** | Bottom-line growth per share. Accounts for dilution. |

### 3.4 `opinc_growth_yoy` -- Operating Income Growth (Year-over-Year)

| | |
|---|---|
| **Formula** | (OpInc_t - OpInc_{t-1}) / \|OpInc_{t-1}\| |
| **XBRL** | OperatingIncomeLoss (current and prior FY) |
| **Interpretation** | Growth in core operating earnings before interest and taxes. More stable than net income growth (strips out one-time items below the operating line). |

### 3.5 `ebitda_growth` -- EBITDA Growth (Year-over-Year)

| | |
|---|---|
| **Formula** | (EBITDA_t - EBITDA_{t-1}) / \|EBITDA_{t-1}\| |
| **XBRL** | OperatingIncomeLoss, DepreciationDepletionAndAmortization (current and prior FY) |
| **Interpretation** | Growth in cash-proxy operating earnings. Strips out depreciation policy differences. |

---

## 4. Leverage (5 indicators)

Balance sheet structure and solvency. Fundamental-only.

### 4.1 `debt_equity` -- Debt-to-Equity

| | |
|---|---|
| **Formula** | Total Debt / Stockholders' Equity |
| **Guard** | Equity must be > 0 |
| **XBRL** | LongTermDebt, ShortTermBorrowings, StockholdersEquity |
| **Interpretation** | Financial leverage. Higher = more debt-financed. > 2 indicates heavy leverage. Negative equity makes this undefined. |

### 4.2 `net_debt_ebitda` -- Net Debt to EBITDA

| | |
|---|---|
| **Formula** | Net Debt / EBITDA |
| **Guard** | EBITDA must be > 0 |
| **XBRL** | LongTermDebt, ShortTermBorrowings, CashAndCashEquivalentsAtCarryingValue, OperatingIncomeLoss, DepreciationDepletionAndAmortization |
| **Interpretation** | Years of current earnings needed to pay off net debt. < 1 = conservatively financed. > 4 = heavily leveraged. Used by credit analysts and covenant agreements. |

### 4.3 `interest_coverage` -- Interest Coverage Ratio

| | |
|---|---|
| **Formula** | Operating Income / Interest Expense |
| **Guard** | \|Interest Expense\| >= 1 |
| **XBRL** | OperatingIncomeLoss, InterestExpense |
| **Interpretation** | Times operating income covers interest payments. < 2 signals distress risk. > 10 indicates minimal debt burden. |

### 4.4 `current_ratio` -- Current Ratio

| | |
|---|---|
| **Formula** | Current Assets / Current Liabilities |
| **XBRL** | AssetsCurrent, LiabilitiesCurrent |
| **Interpretation** | Short-term liquidity. > 1 means the firm can cover near-term obligations. < 1 indicates potential liquidity stress. |

### 4.5 `quick_ratio` -- Quick Ratio

| | |
|---|---|
| **Formula** | (Current Assets - Inventory) / Current Liabilities |
| **XBRL** | AssetsCurrent, InventoryNet, LiabilitiesCurrent |
| **Interpretation** | Liquidity excluding inventory (which may be hard to liquidate quickly). More conservative than the current ratio. |

---

## 5. Efficiency (3 indicators)

How effectively the firm uses its assets. Fundamental-only.

### 5.1 `asset_turnover` -- Asset Turnover

| | |
|---|---|
| **Formula** | Revenue / Total Assets |
| **XBRL** | Revenues, Assets |
| **Interpretation** | Revenue generated per dollar of assets. Higher = more capital-efficient. Varies widely by industry (retailers high, utilities low). |

### 5.2 `inventory_turnover` -- Inventory Turnover **[NA for financials]**

| | |
|---|---|
| **Formula** | COGS / Inventory |
| **XBRL** | CostOfGoodsAndServicesSold, InventoryNet |
| **Interpretation** | Times inventory is sold and replaced per year. Higher = faster-moving inventory, less obsolescence risk. NA for service companies and banks. |

### 5.3 `receivables_turnover` -- Receivables Turnover

| | |
|---|---|
| **Formula** | Revenue / Accounts Receivable |
| **XBRL** | Revenues, AccountsReceivableNetCurrent |
| **Interpretation** | Times receivables are collected per year. Higher = faster collection, better credit quality of customers. |

---

## 6. Cash Flow Quality (3 indicators)

Relationship between reported earnings and actual cash generation. Fundamental-only.

### 6.1 `fcf_ni` -- Free Cash Flow to Net Income

| | |
|---|---|
| **Formula** | FCF / Net Income |
| **Guard** | \|Net Income\| >= 1 |
| **XBRL** | NetCashProvidedByUsedInOperatingActivities, PaymentsToAcquirePropertyPlantAndEquipment, NetIncomeLoss |
| **Interpretation** | Cash conversion of earnings. > 1 means the firm generates more cash than reported profit (high quality). < 0.5 suggests accrual-heavy earnings. |

### 6.2 `opcf_ni` -- Operating Cash Flow to Net Income

| | |
|---|---|
| **Formula** | Operating Cash Flow / Net Income |
| **Guard** | \|Net Income\| >= 1 |
| **XBRL** | NetCashProvidedByUsedInOperatingActivities, NetIncomeLoss |
| **Interpretation** | Similar to FCF/NI but before capital expenditures. Measures how much of reported earnings is backed by cash from operations. |

### 6.3 `capex_revenue` -- CapEx to Revenue

| | |
|---|---|
| **Formula** | CapEx / Revenue |
| **XBRL** | PaymentsToAcquirePropertyPlantAndEquipment, Revenues |
| **Interpretation** | Capital intensity. Higher = more reinvestment required to sustain operations. Asset-light businesses (software) have low ratios; capital-heavy businesses (utilities, manufacturing) have high ratios. |

---

## 7. Shareholder Return (3 indicators)

Capital returned to shareholders. Price-sensitive (dividend yield, buyback yield
use market cap).

### 7.1 `dividend_yield` -- Dividend Yield

| | |
|---|---|
| **Formula** | \|Dividends Paid\| / Market Cap |
| **XBRL** | PaymentsOfDividendsCommonStock |
| **Price** | Yahoo adjusted close (via Market Cap) |
| **Interpretation** | Annual dividend as a percentage of market value. Uses absolute value because dividends are reported as negative cash outflows. |

### 7.2 `payout_ratio` -- Payout Ratio

| | |
|---|---|
| **Formula** | \|Dividends Paid\| / Net Income |
| **Guard** | Net Income must be > 0 |
| **XBRL** | PaymentsOfDividendsCommonStock, NetIncomeLoss |
| **Interpretation** | Fraction of earnings distributed as dividends. > 1 means dividends exceed earnings (unsustainable). 0 = no dividend. |

### 7.3 `buyback_yield` -- Buyback Yield

| | |
|---|---|
| **Formula** | \|Buybacks\| / Market Cap |
| **XBRL** | PaymentsForRepurchaseOfCommonStock |
| **Price** | Yahoo adjusted close (via Market Cap) |
| **Interpretation** | Share repurchases as a percentage of market value. Combined with dividend yield gives total shareholder yield. |

---

## 8. Size (3 indicators)

Absolute scale measures. Price-sensitive (market cap, EV).

### 8.1 `market_cap` -- Market Capitalization

| | |
|---|---|
| **Formula** | Price x Shares Outstanding |
| **XBRL** | CommonStockSharesOutstanding |
| **Price** | Yahoo adjusted close |
| **Interpretation** | Total market value of equity. In dollars. |

### 8.2 `enterprise_value` -- Enterprise Value

| | |
|---|---|
| **Formula** | Market Cap + Net Debt |
| **XBRL** | CommonStockSharesOutstanding, LongTermDebt, ShortTermBorrowings, CashAndCashEquivalentsAtCarryingValue |
| **Price** | Yahoo adjusted close |
| **Interpretation** | Total value of the business (equity + debt - cash). The price an acquirer would pay. |

### 8.3 `revenue_raw` -- Revenue (Raw)

| | |
|---|---|
| **Formula** | Revenue (unscaled) |
| **XBRL** | Revenues |
| **Interpretation** | Absolute top-line revenue in dollars. A size proxy independent of market price. |

---

## 9. Tier 1 Research (15 indicators: 5 ratios + 10 Piotroski)

Academic anomaly factors with documented cross-sectional return predictability.

### 9.1 `gpa` -- Gross Profitability **[NA for financials]**

| | |
|---|---|
| **Formula** | Gross Profit / Total Assets |
| **Reference** | Novy-Marx (2013), *Journal of Financial Economics* |
| **XBRL** | Revenues, CostOfGoodsAndServicesSold, Assets |
| **Interpretation** | Cleanest measure of economic productivity. Sits above SGA, depreciation, and leverage. Roughly equal predictive power as book-to-market but negatively correlated with it. Combined long cheap + profitable is one of the strongest known factor combinations. No significant post-publication decay (Novy-Marx and Medhat, 2025 NBER). |

### 9.2 `asset_growth` -- Asset Growth

| | |
|---|---|
| **Formula** | (Assets_t - Assets_{t-1}) / Assets_{t-1} |
| **Reference** | Cooper, Gulen, and Schill (2008), *Journal of Finance* |
| **XBRL** | Assets (current and prior FY) |
| **Interpretation** | Low asset growth firms outperform. Combines behavioral over-extrapolation with q-theory (firms invest when discount rates are low). Sort ascending for alpha signal. Weakened post-GFC but remains significant, especially when decomposed into organic vs. acquisition-driven growth. |

### 9.3 `sloan_accrual` -- Sloan Accrual Ratio (CF Statement Version)

| | |
|---|---|
| **Formula** | (Net Income - Operating Cash Flow) / Average Total Assets |
| **Reference** | Sloan (1996), *The Accounting Review*; CF version per Hribar and Collins (2002) |
| **XBRL** | NetIncomeLoss, NetCashProvidedByUsedInOperatingActivities, Assets (current and prior FY) |
| **Interpretation** | High accruals = earnings propped up by accounting rather than cash. The CF-statement version avoids measurement error from M&A and foreign currency. Standalone anomaly is effectively dead (Green et al. 2011), but retains power as a component in quality composites. Sort ascending (low accruals = higher quality). |

### 9.4 `pct_accruals` -- Percent Accruals

| | |
|---|---|
| **Formula** | (Net Income - Operating Cash Flow) / \|Net Income\| |
| **Guard** | \|Net Income\| >= 1 |
| **Reference** | Hafzalla, Lundholm, and Van Winkle (2011), *The Accounting Review* |
| **XBRL** | NetIncomeLoss, NetCashProvidedByUsedInOperatingActivities |
| **Interpretation** | Proportion of earnings from accruals vs. cash. Produces a "radically different sort" from the standard Sloan ratio. Larger hedge returns, improvement concentrated in the long leg. Complements rather than duplicates the Sloan ratio. |

### 9.5 `net_operating_assets` -- Net Operating Assets (NOA)

| | |
|---|---|
| **Formula** | (Total Assets - Cash - (Total Liabilities - Total Debt)) / Lagged Total Assets |
| **Reference** | Hirshleifer, Hou, Teoh, and Zhang (2004), *Journal of Accounting and Economics* |
| **XBRL** | Assets, CashAndCashEquivalentsAtCarryingValue, Liabilities, LongTermDebt, ShortTermBorrowings (current FY), Assets (prior FY) |
| **Interpretation** | Cumulative balance sheet bloat. High NOA = accumulated accounting earnings far exceed accumulated free cash flows. Stock variable (cumulative), more stable than flow-based accrual measures. Strong negative predictor of 1-3 year returns. Sort ascending (low NOA outperforms). |

### 9.6-9.14 Piotroski F-Score Components (9 binary signals)

**Reference:** Piotroski (2000), *Journal of Accounting Research*

Each component is binary (0 or 1). The composite `f_score` is the sum (0-9).

| Field | Signal | Formula | Pass condition |
|-------|--------|---------|----------------|
| `f_roa` | Profitability | NI / Assets | ROA > 0 |
| `f_droa` | Improving ROA | ROA_t vs. ROA_{t-1} | ROA increased YoY |
| `f_cfo` | Cash flow | CFO / Assets | CFO/Assets > 0 |
| `f_accrual` | Accrual quality | CFO vs. NI | CFO > Net Income |
| `f_dlever` | Deleveraging | LTD/Assets_t vs. LTD/Assets_{t-1} | LT Debt ratio decreased |
| `f_dliquid` | Liquidity | CA/CL_t vs. CA/CL_{t-1} | Current ratio increased |
| `f_eq_off` | No dilution | Shares_t vs. Shares_{t-1} | Shares unchanged or decreased |
| `f_dmargin` | Margin improvement | GP/Rev_t vs. GP/Rev_{t-1} | Gross margin increased |
| `f_dturn` | Turnover improvement | Rev/Assets_t vs. Rev/Assets_{t-1} | Asset turnover increased |

**XBRL tags used:** NetIncomeLoss, Assets, NetCashProvidedByUsedInOperatingActivities, LongTermDebt, AssetsCurrent, LiabilitiesCurrent, CommonStockSharesOutstanding, Revenues, CostOfGoodsAndServicesSold (all current and prior FY).

### 9.15 `f_score` -- Piotroski F-Score (Composite)

| | |
|---|---|
| **Formula** | Sum of 9 binary components above |
| **Range** | 0 (all fail) to 9 (all pass) |
| **NA policy** | Each component is NA when its required inputs are missing; `f_score` is NA if any component is NA. This avoids the downward bias that would arise if missing inputs were scored as failures (e.g., firms with no prior-year data). |
| **Interpretation** | Comprehensive fundamental quality signal. Score >= 8 indicates strong, improving fundamentals. Interacts powerfully with valuation: long cheap + high F-Score, short expensive + low F-Score earned ~285% incremental return over 12 years (Piotroski and So, 2012). |

---

## 10. Tier 2 Research (6 indicators)

Practitioner signals requiring additional XBRL tags or multi-period computation.

### 10.1 `cash_based_op` -- Cash-Based Operating Profitability **[NA for financials]**

| | |
|---|---|
| **Formula** | (Revenue - COGS - SGA + R&D - dAR - dInventory - dPrepaid + dDeferredRev + dAP + dAccruedLiab) / Total Assets |
| **Reference** | Ball, Gerakos, Linnainmaa, and Nikolaev (2016), *Journal of Financial Economics* |
| **XBRL** | Revenues, CostOfGoodsAndServicesSold, SellingGeneralAndAdministrativeExpense, ResearchAndDevelopmentExpense, AccountsReceivableNetCurrent, InventoryNet, PrepaidExpenseAndOtherAssetsCurrent, DeferredRevenueCurrent, AccountsPayableCurrent, AccruedLiabilitiesCurrent, Assets (all current and prior FY) |
| **Interpretation** | Strips working capital accruals from operating profitability. Subsumes both the profitability premium and the accrual anomaly in a single measure. R&D added back (investment, not expense). Missing working capital items are skipped (partial computation) rather than zeroed. Fama and French (2018) acknowledged its dominance. |

### 10.2 `fcf_stability` -- Free Cash Flow Stability

| | |
|---|---|
| **Formula** | SD(Standalone-quarter Operating Cash Flow / Total Assets) over trailing 8+ quarters |
| **Guard** | Requires >= 8 quarters of standalone-quarter data; otherwise NA. Rows whose `period_days` do not match their `fp` label are defensively dropped (see Note). |
| **Reference** | Huang (2009), *Journal of Empirical Finance* |
| **XBRL** | NetCashProvidedByUsedInOperatingActivities, Assets (quarterly 10-Q filings) |
| **Interpretation** | Lower volatility = more stable cash generation = higher quality. Market underprices cash flow stability relative to earnings stability. Sort ascending (lower SD = better). Uses quarterly data, not annualized. |

**Note on YTD de-cumulation.** US 10-Q filings report CFO cumulatively within the fiscal year (Q1 = 3-month, Q2 = 6-month YTD, Q3 = 9-month YTD). `.decumulate_cfo` in `R/indicator_compute.R` converts to standalone-quarter values: Q1_std = Q1_YTD, Q2_std = Q2_YTD - Q1_YTD, Q3_std = Q3_YTD - Q2_YTD, Q4_std = FY - Q3_YTD. Rows whose `period_days` do not fall in the expected band for their `fp` label (Q1: 60-120, Q2: 160-200, Q3: 250-290, FY: 330-380) are dropped. The Q1 band upper of 120 accommodates the Kroger/AAP 16/12/12/12 fiscal calendar; all other 628 S&P 500 filers use 13-week standalone quarters. Full diagnostic: `docs/research/07_cfo_cumulation_issue.md`.

### 10.3 `sga_efficiency` -- SGA Efficiency Signal

| | |
|---|---|
| **Formula** | %Growth(SGA) - %Growth(Revenue) |
| **Reference** | Lev and Thiagarajan (1993), *Journal of Accounting Research*; validated by Abarbanell and Bushee (1997, 1998) |
| **XBRL** | SellingGeneralAndAdministrativeExpense, Revenues (current and prior FY) |
| **Interpretation** | When SGA grows faster than revenue, it signals deteriorating cost control. Positive values = bad (costs outpacing revenue). One of the original Lev-Thiagarajan twelve fundamental signals. Sort ascending. |

### 10.4 `capex_depreciation` -- CapEx-to-Depreciation Ratio **[NA for financials]**

| | |
|---|---|
| **Formula** | CapEx / Depreciation & Amortization |
| **Guard** | Capped at 5x to avoid extreme ratios |
| **Reference** | Titman, Wei, and Xie (2004), *JFQA* |
| **XBRL** | PaymentsToAcquirePropertyPlantAndEquipment, DepreciationDepletionAndAmortization |
| **Interpretation** | Ratio near 1.0 = maintenance-only spending. > 1.5 = significant growth investment. Combined with low ROIC, high capex/depreciation is a negative signal (empire building). NA for asset-light firms where both CapEx and D&A are immaterial. |

### 10.5 `dso_change` -- Days Sales Outstanding Change

| | |
|---|---|
| **Formula** | (AR_t/Revenue_t - AR_{t-1}/Revenue_{t-1}) x 365 |
| **Reference** | Beneish (1999), *Financial Analysts Journal* (DSRI component of M-Score) |
| **XBRL** | AccountsReceivableNetCurrent, Revenues (current and prior FY) |
| **Interpretation** | Rising DSO signals aggressive revenue recognition (channel stuffing), deteriorating collection quality, or shifting customer mix. Largest positive values = worst signal. DSRI is the strongest standalone component of the Beneish M-Score (earnings manipulation detection). |

### 10.6 `inventory_sales_change` -- Inventory-to-Sales Change **[NA for financials]**

| | |
|---|---|
| **Formula** | (Inventory_t/COGS_t) - (Inventory_{t-1}/COGS_{t-1}) |
| **Reference** | Thomas and Zhang (2002), *Review of Accounting Studies* |
| **XBRL** | InventoryNet, CostOfGoodsAndServicesSold (current and prior FY) |
| **Interpretation** | Inventory buildup relative to cost of sales. Signals declining demand, obsolescence risk, or production mismanagement. Drives the majority of the Sloan accrual anomaly's return predictability. Positive change (inventory building) = bad signal. NA for service firms and financials with no inventory. Uses COGS as denominator (not revenue) to avoid margin effects. |

---

## 11. Balance-Sheet Change Family (15 indicators, Wave 1)

Annual year-over-year changes in balance-sheet aggregates, from the OpenAP
expansion (docs/research/09; full references and reproduced t-stats in
docs/RESEARCH_INDICATORS.md, section WAVE 1). Building blocks per
Richardson et al. (2005): COA = CurrentAssets - Cash; COL =
CurrentLiabilities - ShortTermDebt; WC = COA - COL; NCOA = Assets -
CurrentAssets - LTInvestments; NCOL = Liabilities - CurrentLiabilities -
LongTermDebt; NCO = NCOA - NCOL; FNA = STInvestments + LTInvestments;
FINL = LongTermDebt + ShortTermDebt + PreferredStock; FIN = FNA - FINL;
NOA = (Assets - Cash) - (Liabilities - TotalDebt). avgAT = average of
current and prior total assets; lagAT = prior total assets.

Missing-input policy: st_investments, lt_investments, preferred_stock,
short_term_debt enter as 0 when unreported; FINL needs at least one
debt/preferred component; when the Liabilities tag is absent, LT is
derived as Assets - Equity - MinorityInterest (balance-sheet identity).
The prior year is selected by period_end, never by fiscal_year label
(re-reported comparatives carry the newer filing's label).

| Indicator | Formula | Scaling | Source study (CZ t-stat) |
|---|---|---|---|
| `del_coa` | ΔCOA | avgAT | Richardson 2005 (6.01) |
| `del_col` | ΔCOL | avgAT | Richardson 2005 (4.35) |
| `del_finl` | ΔFINL | avgAT | Richardson 2005 (12.23) |
| `del_lti` | ΔLTInvestments | avgAT | Richardson 2005 (2.55) |
| `del_equ` | ΔEquity | avgAT | Richardson 2005 (3.18) |
| `del_netfin` | ΔFIN | avgAT | Richardson 2005 (9.00) |
| `total_accruals` | ΔWC + ΔNCO + ΔFIN | avgAT | Richardson 2005 (2.63) |
| `dnoa` | ΔNOA | lagAT | Hirshleifer 2004 (9.25) |
| `ch_nncoa` | ΔNCO | lagAT | Soliman 2008 (4.43) |
| `ch_nwc` | ΔWC | lagAT | Soliman 2008 (2.83) |
| `inventory_change` **[NA for financials]** | ΔInventory | avgAT | Thomas-Zhang 2002 (6.24) |
| `inventory_growth` **[NA for financials]** | Inventory_t / Inventory_{t-1} - 1 | -- | Belo-Lin 2012 (7.19) |
| `ppe_inv_change` **[NA for financials]** | Δ(PPEgross + Inventory) | lagAT | Lyandres 2008 (7.86) |
| `equity_growth` | Equity_t / Equity_{t-1} - 1 (both > 0) | -- | Lockwood-Prombutr 2010 (4.26) |
| `gr_ltnoa` | ΔLTNOA, LTNOA = NOA - WC | avgAT | Fairfield 2003 (3.73) |

---

## 12. External Financing Family (7 indicators, Wave 2)

Annual financing-flow indicators from the OpenAP expansion (full
references and construction notes in docs/RESEARCH_INDICATORS.md, WAVE 2).
Proceeds*/Repayments* cash-flow tags are positive; buybacks/dividends
enter as absolute values. avgAT = average total assets.

| Indicator | Formula | Source study (CZ t-stat) |
|---|---|---|
| `net_debt_finance` | (debt issuance - repayment + ΔSTD) / avgAT | Bradshaw 2006 (7.70) |
| `net_equity_finance` | (equity issuance - \|buybacks\| - \|dividends\|) / avgAT | Bradshaw 2006 (5.40) |
| `xfin` | net_debt_finance + net_equity_finance | Bradshaw 2006 (4.84) |
| `composite_debt_issuance` | log(total debt_t / total debt_{t-5}) | Lyandres 2008 (5.19) |
| `share_iss_1y` | split-adjusted shares_t / shares_{t-1} - 1 | Pontiff-Woodgate 2008 (4.97) |
| `share_iss_5y` | split-adjusted shares_t / shares_{t-5} - 1 | Daniel-Titman 2006 (4.32) |
| `net_payout_yield` | (\|dividends\| + \|buybacks\| - equity issuance) / market cap | Boudoukh 2007 (2.57) |

Split adjustment: a share count's basis is the split state at its filed
date; the adjustment multiplies the current count by the product of split
ratios between the two rows' filed dates (empty window when both survive
from one filing). Tickers without cached split events (cache/splits) are
computed unadjusted. `net_payout_yield` is price-sensitive and updates
daily in the timeseries layer.

---

## 13. Quarterly Seasonal-Surprise Family (6 indicators, Wave 3)

Quarterly indicators from the OpenAP expansion (full references and
construction notes in docs/RESEARCH_INDICATORS.md, WAVE 3). Built on a
label-free standalone-quarter panel: duration rows are grouped by
period_start and classified by reported duration (3mo / 2Q / 3Q / FY with
NA gaps); a directly reported 3-month row wins, otherwise the standalone
value is recovered by differencing consecutive YTD rows (Q4 = FY - Q3ytd).
Seasonal lags are matched by period_end distance (4-quarter gap must fall
in [330, 400] days), never by fiscal labels. `q-4` = same quarter one
year earlier.

| Indicator | Formula | Source study (CZ t-stat) |
|---|---|---|
| `ch_tax` | (tax_q - tax_{q-4}) / total assets at q-4 | Thomas-Zhang 2011 (9.50) |
| `num_earn_increase` | consecutive quarters with NI_q > NI_{q-4}, OpenAP-exact | Loh-Warachka 2012 (6.75) |
| `revenue_surprise` | standardized seasonal surprise in revenue per share | Jegadeesh-Livnat 2006 (5.99) |
| `earnings_surprise` | standardized seasonal surprise in diluted EPS (SUE) | Foster-Olsen-Shevlin 1984 (4.94) |
| `earnings_consistency` | mean annual EPS growth over 5y, sign-consistent only | Alwathainani 2009 (2.51) |
| `roaq` | NI_q / total assets at q-1 | Balakrishnan 2010 (5.90) |

Surprise standardization (OpenAP-exact): surprise = seasonal diff minus
the mean of the prior 8 seasonal diffs (available-case), divided by the
sd of the prior 8 drift-adjusted surprises (at least 2 required; sd at or
below the floor -> NA). Per-share series are normalized to the current
split basis before differencing. All quarterly values reflect the latest
standalone quarter filed as of the snapshot date. Computed for
financials (taxes, NI, EPS are well-defined for banks); revenue-based
values degrade to NA where bank revenue tags are sparse.
`earnings_consistency` is NA by construction for firms whose growth sign
flipped year-over-year or whose EPS ratio exceeds 6 in magnitude
(Alwathainani's consistency filter, ~40-60% of the cross-section at any
snapshot).

`revenue_growth_qoq` (section 4) is also computed from this panel since
Wave 3: latest standalone quarter vs the one before it, matched by
period_end.

---

## 14. Levels Family (19 indicators, Wave 4)

Current-year level ratios from the OpenAP expansion (full references,
guards and OpenAP deviations in docs/RESEARCH_INDICATORS.md, W4.1-W4.13).
ME = market cap on the filing date. che = cash + short-term investments
(STI zero-if-na, cash required per the C-3 coverage rule). ND = FINL -
che with FINL = LTD + STD + preferred (>= 1 component reported). LT =
total liabilities (identity fallback AT - CEQ - NCI). Component
resolution is shared with the timeseries stub layer via
.level_components() so the two paths cannot drift.

| Indicator | Formula | Source study (CZ t-stat) |
|---|---|---|
| `rd_me` | R&D / ME (NA when R&D unreported) | Chan 2001 (5.82) |
| `ebm` | (BE + net cash) / (ME + net cash) | Penman 2007 (4.14) |
| `bpebm` | BE/ME - ebm | Penman 2007 (2.83) |
| `cf_me` | (NI + depreciation) / ME | LSV 1994 (4.04) |
| `cfp` | CFO / ME | Desai 2004 (2.18) |
| `net_debt_price` | ND / ME | Penman 2007 (3.88) |
| `tang` | (che + .715 REC + .547 INV + .535 PPE gross) / AT | Hahn-Lee 2009 (3.67) |
| `tax_to_book` | (tax / statutory rate) / NI; = 1 if tax > 0, NI <= 0 | Lev-Nissim 2004 (3.52) |
| `effective_tax_rate` | tax / pretax income | (database level) |
| `am` | AT / ME | Fama-French 1992 (3.50) |
| `book_leverage` | AT / BE, BE > 0 | Fama-French 1992 (3.30) |
| `leverage_mkt` | total liabilities / ME | Bhandari 1988 (2.64) |
| `cash_prod` | (ME - AT) / che | Chandrashekar-Rao (3.40) |
| `oper_prof` | (rev - COGS - SGA - interest) / BE, all required, BE > 0 | Fama-French 2006 (3.00) |
| `cash_assets` | che / AT | Palazzo 2012 (2.97) |
| `op_leverage` | (COGS + SGA) / AT, SGA zero-if-na | Novy-Marx 2011 (2.50) |
| `payout_yield` | (\|dividends\| + \|buybacks\|) / ME | Boudoukh 2007 (2.27) |
| `salecash` | revenue / che | Ou-Penman (placebo) |
| `depr_rate` | depreciation / PPE net | (placebo) |

Statutory rate for `tax_to_book`: 35% for fiscal years ending on or
before 2017-12-31, TCJA section 15 day-weighted blend for years
straddling 2018-01-01 (true fiscal-year start from the prior row when
available), 21% after. Ten indicators (rd_me, ebm, bpebm, cf_me, cfp,
net_debt_price, am, leverage_mkt, cash_prod, payout_yield) are
price-sensitive and update daily in the timeseries layer from the
Wave 4 stubs.

---

## 15. Investment Family (10 indicators, Wave 4)

Lagged and industry-adjusted constructions (references and deviations in
docs/RESEARCH_INDICATORS.md, W4.14-W4.22). Lag rows are label-free and
gap-guarded (.lag_fy_row). g2(x) denotes the Abarbanell-Bushee growth:
base = mean of the two prior years, 1-year fallback when unavailable.

| Indicator | Formula | Source study (CZ t-stat) |
|---|---|---|
| `pchdepr` | %change in (depreciation / PPE net) | (placebo) |
| `grcapx` | (capex_t - capex_{t-2}) / capex_{t-2}, base > 0 | Anderson-GF 2006 (4.96) |
| `grcapx3y` | 3 capex_t / (capex_{t-1} + capex_{t-2} + capex_{t-3}) | Anderson-GF 2006 (4.77) |
| `pct_tot_acc` | (dWC + dNCO + dFIN) / \|NI\| | Hafzalla 2011 (4.70) |
| `ch_asset_turnover` | (rev/avgNOA)_t - (rev/avgNOA)_{t-1}, negative -> NA | Soliman 2008 (3.77) |
| `gr_sale_to_gr_inv` | g2(sales) - g2(inventory) | Abarbanell-Bushee 1998 (3.30) |
| `surprise_rd` | 1 if R&D growth > 5% and R&D/AT growth > 5% | Eberhart 2004 (3.00) |
| `investment` | (capex/rev)_t / mean over t, t-1, t-2 (>= 2 years, rev >= $10M) | Titman 2004 (2.28) |
| `rd_cap` | (R&D + .8 lag1 + .6 lag2 + .4 lag3 + .2 lag4) / AT, 5 years required | Li 2011 (2.32) |
| `ch_inv_ia` | g2(capex) - sector mean of g2(capex) | Abarbanell-Bushee 1998 (5.50) |

`ch_inv_ia` is finalized at the cross-section stage: the compute layer
emits the firm-level g2(capex) ingredient and every cross-section
assembly point (compute_cross_section, load_daily_cross_section, the
feature layer's extraction) calls industry_adjust_cross_section to
subtract the equal-weighted sector mean. When the sector lookup is
unavailable the daily reader degrades the column to NA rather than
serving the un-demeaned ingredient. Per-ticker daily parquets store the
ingredient by design. `rd_cap` zero-fills missing R&D within existing
years (non-R&D firms get 0) but requires all five fiscal-year rows.

---

## XBRL Tag Alias Map

Each canonical concept maps to multiple XBRL tags. First match wins during
resolution. This handles variation in how companies report the same item.

| Concept | XBRL tags (priority order) |
|---------|---------------------------|
| revenue | Revenues, RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet, SalesRevenueGoodsNet, SalesRevenueServicesNet, RevenueFromContractWithCustomerIncludingAssessedTax |
| cogs | CostOfGoodsAndServicesSold, CostOfRevenue, CostOfGoodsSold, CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization |
| operating_income | OperatingIncomeLoss |
| net_income | NetIncomeLoss |
| eps_basic | EarningsPerShareBasic |
| eps_diluted | EarningsPerShareDiluted |
| interest_expense | InterestExpense, InterestExpenseDebt |
| sga | SellingGeneralAndAdministrativeExpense |
| rnd | ResearchAndDevelopmentExpense, ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost |
| depreciation | DepreciationDepletionAndAmortization, DepreciationAndAmortization, Depreciation |
| total_assets | Assets |
| stockholders_equity | StockholdersEquity, StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest |
| long_term_debt | LongTermDebt, LongTermDebtNoncurrent, LongTermDebtAndCapitalLeaseObligations |
| short_term_debt | ShortTermBorrowings, ShortTermDebtCurrent, DebtCurrent, ShortTermBankLoansAndNotesPayable |
| current_assets | AssetsCurrent |
| current_liabilities | LiabilitiesCurrent |
| total_liabilities | Liabilities |
| accounts_receivable | AccountsReceivableNetCurrent, AccountsReceivableNet |
| inventory | InventoryNet, InventoryFinishedGoodsNetOfReserves |
| cash | CashAndCashEquivalentsAtCarryingValue, CashCashEquivalentsAndShortTermInvestments, Cash |
| shares_outstanding | CommonStockSharesOutstanding, EntityCommonStockSharesOutstanding, WeightedAverageNumberOfShareOutstandingBasicAndDiluted, WeightedAverageNumberOfDilutedSharesOutstanding |
| accounts_payable | AccountsPayableCurrent, AccountsPayableAndAccruedLiabilitiesCurrent |
| accrued_liabilities | AccruedLiabilitiesCurrent |
| deferred_revenue | DeferredRevenueCurrent, ContractWithCustomerLiabilityCurrent, DeferredRevenueCurrentAndNoncurrent |
| prepaid_expenses | PrepaidExpenseAndOtherAssetsCurrent, PrepaidExpenseAndOtherAssets, PrepaidExpenseCurrent |
| operating_cashflow | NetCashProvidedByUsedInOperatingActivities, NetCashProvidedByOperatingActivities, NetCashProvidedByUsedInOperatingActivitiesContinuingOperations |
| capex | PaymentsToAcquirePropertyPlantAndEquipment, PaymentsToAcquireProductiveAssets |
| buybacks | PaymentsForRepurchaseOfCommonStock, PaymentsForRepurchaseOfEquity |
| dividends_paid | PaymentsOfDividendsCommonStock, PaymentsOfDividends, Dividends |
| public_float | EntityPublicFloat (dei namespace) |

### Phase 0 expansion concepts (fetched, not yet consumed)

Added for the OpenAP indicator expansion
(docs/research/09_openap_indicator_expansion.md). These concepts are present
in `cache/fundamentals/` parquet files; the indicators that consume them land
in Waves 1-5.

| Concept | XBRL tags (priority order) | Notes |
|---------|---------------------------|-------|
| income_tax_expense | IncomeTaxExpenseBenefit | |
| pretax_income | IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest, IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments | Domestic-only tag deliberately excluded (understates multinationals). Fallback: tax + net income |
| advertising | AdvertisingExpense, MarketingAndAdvertisingExpense | Sparse (~55% of tickers); NA-heavy expected |
| ppe_net | PropertyPlantAndEquipmentNet, PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization | Second tag includes ROU assets post-ASC 842 |
| ppe_gross | PropertyPlantAndEquipmentGross, PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetBeforeAccumulatedDepreciationAndAmortization | |
| st_investments | ShortTermInvestments, MarketableSecuritiesCurrent, AvailableForSaleSecuritiesCurrent | Missing usually means none held; treat as 0 |
| lt_investments | LongTermInvestments, MarketableSecuritiesNoncurrent, AvailableForSaleSecuritiesNoncurrent, EquityMethodInvestments | Aggregate ranked above components |
| retained_earnings | RetainedEarningsAccumulatedDeficit | |
| preferred_stock | PreferredStockValue, PreferredStockValueOutstanding | Missing means none outstanding; treat as 0 |
| goodwill | Goodwill | Kept separate from intangibles to allow summing |
| intangibles | IntangibleAssetsNetExcludingGoodwill, FiniteLivedIntangibleAssetsNet | Ex-goodwill only; IncludingGoodwill aggregate excluded |
| minority_interest | MinorityInterest | Missing means no NCI; treat as 0 |
| debt_issuance | ProceedsFromIssuanceOfLongTermDebt, ProceedsFromIssuanceOfSeniorLongTermDebt, ProceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet, ProceedsFromIssuanceOfDebt, ProceedsFromDebtNetOfIssuanceCosts, ProceedsFromDebtMaturingInMoreThanThreeMonths | Long-term focus; short-term via short_term_debt delta. YTD-cumulated in quarterly filings (like operating_cashflow) |
| debt_repayment | RepaymentsOfLongTermDebt, RepaymentsOfSeniorDebt, RepaymentsOfLongTermDebtAndCapitalSecurities, RepaymentsOfDebtAndCapitalLeaseObligations, RepaymentsOfDebt, RepaymentsOfDebtMaturingInMoreThanThreeMonths | Same YTD caveat |
| equity_issuance | ProceedsFromIssuanceOfCommonStock, ProceedsFromIssuanceOrSaleOfEquity, ProceedsFromIssuanceOfSharesUnderIncentiveAndShareBasedCompensationPlansIncludingStockOptions, ProceedsFromStockOptionsExercised | One line wins per period; treat as lower bound on total issuance. Same YTD caveat |

---

## Financial Sector Exclusions

Banks and financial institutions lack standard COGS, inventory, and operating
income structure. The following indicators are set to NA for tickers classified
under the "Financial" sector:

- `gpa` (Gross Profitability)
- `inventory_turnover`
- `cash_based_op` (Cash-Based Operating Profitability)
- `capex_depreciation`
- `inventory_sales_change`
- `inventory_change`, `inventory_growth`, `ppe_inv_change` (Wave 1)
- `tang`, `op_leverage`, `ch_asset_turnover`, `gr_sale_to_gr_inv`,
  `net_debt_price` (Wave 4; net_debt_price follows OpenAP's SIC
  6000-6999 exclusion and is additionally stub-masked in the daily
  layer and re-masked with current sector in load_daily_cross_section)

During z-scoring, these indicators are computed excluding financial sector firms,
so the cross-sectional distribution is not distorted by forced NAs.

---

## Z-Scoring Method

Cross-sectional z-scores are computed per indicator using a robust pipeline
(`.robust_zscore` in `indicator_compute.R`); this is the default output of
`assemble_snapshot` (`zscore_mode = "cross_section"`):

1. Winsorize the raw values at the per-snapshot p2.5 / p97.5 percentiles
   (excluding financials for the five financial-NA indicators), so fat tails
   cannot contaminate the location/scale estimates.
2. Standardize with the robust center and scale:
   Z = (value - median) / (1.4826 * MAD). Under a Gaussian null this equals
   (value - mean) / SD, but it is not distorted by skew or outliers. Note the
   median -- not the mean -- of the resulting z-scores sits at ~0.
3. Clip the z-scores at [-5, 5] by default (`clip` argument; pass `NULL` to
   disable, or `c(-3, 3)` for the tighter legacy bound). See
   `docs/research/06_indicator_verification_report.md` sec. 12a for the
   clip-default rationale.
4. Original NAs remain NA. Negative denominators are preserved (not NA-masked)
   so distress signals propagate.

Minimum 3 non-NA observations required; otherwise the entire indicator column
is set to NA for that cross-section.

An opt-in `zscore_mode = "expanding_window"` variant pools winsorized values
across all prior snapshots plus the current one for the location/scale estimate;
it is a research variant, not the default output contract.
