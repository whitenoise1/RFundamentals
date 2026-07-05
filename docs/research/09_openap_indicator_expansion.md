# OpenAP Indicator Expansion Work Plan

Source: Chen & Zimmermann (2021), "Open Source Cross-Sectional Asset Pricing"
(sources/research/ssrn_id3850947_code1486115.pdf, SSRN 3604626 / posted id
3850947). Annex Tables 2-4 list 319 firm characteristics with the original
study, the reproduced long-short mean return and t-stat, and the original
predictability evidence. Companion documentation (exact construction per
signal): https://github.com/OpenSourceAP/CrossSection ("SignalDocumentation").

Scope rule for this repo: RFundamentals is a point-in-time fundamental
*database*, not a forecasting engine. The replicated t-stat is used only as a
priority ranking for which indicators to add first. Construction must come
from regulatory data (SEC EDGAR XBRL companyfacts) plus market prices already
in the pipeline (Yahoo). Predictors requiring IBES analyst data, CRSP-only
fields, 13F ownership, options, TAQ, patents, or hand-collected data are out
of scope.

---

## 1. Classification of Tables 2-4

Table 2 (clear predictors, 137 rows), Table 3 (likely predictors, 44 rows),
Table 4 (not-predictors / indirect signals, ~118 rows, mostly HXZ quarterly
variants).

Family buckets (non-fundamental = excluded):

| Family                              | Examples                                   | Verdict |
|-------------------------------------|--------------------------------------------|---------|
| Accounting / fundamental            | Accruals, NOA, AssetGrowth, XFIN, F-score  | IN      |
| Price momentum / reversal / seasonality | Mom12m, STreversal, MomSeason          | out     |
| Volatility / beta / skewness        | IdioRisk, BetaFP, Coskewness               | out     |
| Volume / liquidity                  | Illiquidity, DolVol, zerotrade, std_turn   | out     |
| Analyst (IBES)                      | FEPS, REV6, ForecastDispersion, sfe, AOP   | out     |
| Ownership / short interest (13F/SI) | DelBreadth, RIO_*, IO_ShortInterest        | out     |
| Options / TAQ                       | SmileSlope, OptionVolume, BidAskTAQ        | out     |
| Events / listings / special data    | ExchSwitch, IndIPO, Spinoff, CredRatDG, Governance, sinAlgo | out |
| Patents / citations / brand / labor | PatentsRD, CitationsRD, hire (employees)   | out (data not in XBRL companyfacts) |
| Fundamental but composite w/ price history | CompEquIss, IntanBM/CFP/EP/SP, EquityDuration | borderline (see 4.6) |

Result: 71 accounting-based candidates across the three tables. 22 are
already covered by our 59 indicators (or are trivial inverses of them), 40
are new and feasible from EDGAR XBRL, 9 are excluded for data reasons
(sparse or missing XBRL tags -- see 4.7).

## 2. Already covered (no action, or variant note only)

| OpenAP acronym | Ours | Note |
|---|---|---|
| EP (Basu 1977) | earnings_yield / pe_trailing | -- |
| BM, BMdec (Rosenberg 1985; FF92) | pb (inverse) | -- |
| SP (Barbee 1996) | ps (inverse) | -- |
| cfp-adjacent P/FCF | pfcf | OpenAP cfp is CFO/P, see 4.4 |
| EntMult (Loughran-Wellman 2011) | ev_ebitda | same construction family |
| RoE (Haugen-Baker 1996) | roe | -- |
| roaq (Balakrishnan 2010, t=5.90) | roa | ours is filing-frequency; quarterly variant free once seasonal machinery lands (Wave 3) |
| GP (Novy-Marx 2013) | gpa | Tier 1 |
| AssetGrowth (Cooper 2008, t=7.64) | asset_growth | Tier 1 |
| Accruals (Sloan 1996) | sloan_accrual | Tier 1 (CF version) |
| PctAcc (Hafzalla 2011) | pct_accruals | operating version |
| NOA (Hirshleifer 2004, t=7.51) | net_operating_assets | Tier 1 |
| PS (Piotroski 2000) | f_score + 9 components | Tier 1 |
| CBOperProf (Ball 2016) | cash_based_op | Tier 2 |
| sgr (LSV 1994) | revenue_growth_yoy | -- |
| currat / quick (Ou-Penman) | current_ratio / quick_ratio | -- |
| saleinv / salerec | inventory_turnover / receivables_turnover | -- |
| AssetTurnover, PM (Soliman 2008) | asset_turnover, net/operating_margin | -- |
| Leverage-book (FF92 BookLeverage relative) | debt_equity | different scaling, see 4.4 |
| DivYield family | dividend_yield, payout_ratio, buyback_yield | net payout composite is new |
| CapEx/Rev (Titman Investment relative) | capex_revenue | 3y-relative variant is new |
| GrSaleToGrInv-adjacent | inventory_sales_change | Tier 2; AB98 formulation differs, keep both |

## 3. Prerequisite: fetcher tag expansion (Phase 0)

New indicators require ~12 new concepts in `.TAG_TO_CONCEPT`
(R/fundamental_fetcher.R). Cached parquet holds only mapped tags, so a full
re-fetch of companyfacts is required (chunked + resumable already supported;
~830 tickers at 0.11 s/call is under 2 minutes of rate-limit time, plus
parse).

| Concept | Primary XBRL tags (aliases to confirm in prototype) | Needed by |
|---|---|---|
| ppe_net | PropertyPlantAndEquipmentNet | tang, GrLTNOA, depr, OPLeverage checks |
| ppe_gross | PropertyPlantAndEquipmentGross | InvestPPEInv |
| income_tax_expense | IncomeTaxExpenseBenefit | Tax, ChTax, ETR |
| pretax_income | IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest (+ ...MinorityInterestAndIncomeLossFromEquityMethodInvestments) | Tax, ZScore (EBIT check) |
| debt_issuance | ProceedsFromIssuanceOfLongTermDebt, ProceedsFromNotesPayable, ProceedsFromIssuanceOfDebt | NetDebtFinance, XFIN |
| debt_repayment | RepaymentsOfLongTermDebt, RepaymentsOfDebt | NetDebtFinance, XFIN |
| equity_issuance | ProceedsFromIssuanceOfCommonStock, ProceedsFromIssuanceOrSaleOfEquity | NetEquityFinance, XFIN, NetPayoutYield |
| st_investments | ShortTermInvestments, MarketableSecuritiesCurrent | DelNetFin, Penman net debt |
| lt_investments | LongTermInvestments, MarketableSecuritiesNoncurrent, EquityMethodInvestments | DelLTI, DelNetFin, NCOA |
| retained_earnings | RetainedEarningsAccumulatedDeficit | ZScore |
| preferred_stock | PreferredStockValue | FINL (Richardson), NOA precision |
| goodwill_intangibles | Goodwill, IntangibleAssetsNetExcludingGoodwill, FiniteLivedIntangibleAssetsNet | GrLTNOA, tang variants |
| minority_interest | MinorityInterest | NOA / dNoa precision |
| advertising (optional, sparse) | AdvertisingExpense | GrAdExp, MS component 8 |

Deliverable: alias map validated on the 20-ticker prototype set (Session C
pattern), coverage report per concept before full build.

### Phase 0 results (2026-07-01)

15 concepts added to `.TAG_ALIASES` (30 -> 45 concepts, 101 tags total).
Prototype coverage on the 20-ticker set after alias refinement:

| Concept | Coverage | Verdict |
|---|---|---|
| income_tax_expense, pretax_income, retained_earnings | 100% | clean |
| ppe_net, goodwill, debt_issuance | 95% | clean (BRK.B financial-style CF) |
| debt_repayment | 90% | clean |
| equity_issuance | 85% | lower bound; one CF line wins per period |
| ppe_gross, lt_investments, intangibles | 80% | acceptable |
| st_investments | 70% | missing = none held, treat as 0 |
| minority_interest | 65% | missing = no NCI, treat as 0 |
| advertising | 55% | sparse as predicted; GrAdExp / MS component 8 degrade |
| preferred_stock | 50% | missing = none outstanding, treat as 0 |

Alias decisions made during validation:
1. `pretax_income`: the `...BeforeIncomeTaxesDomestic` tag was initially
   included as a fallback and REMOVED after it won dedup for multinationals
   (PG 2010-2019, AMP 2010-2017, +9 others) -- domestic-only pretax income
   silently understates consolidated pretax. Firms lacking a consolidated
   tag get NA; indicator code may fall back to tax + net income.
2. `debt_issuance`/`debt_repayment`: generic styles added after prototype
   misses (KO `ProceedsFromIssuanceOfDebt`, GOOGL
   `ProceedsFromDebtNetOfIssuanceCosts`, MSFT/CAT
   `ProceedsFromDebtMaturingInMoreThanThreeMonths`), lifting coverage
   75% -> 95%.
3. Identity check `pretax ~= income_tax_expense + net_income` holds on the
   prototype within noncontrolling-interest / equity-method noise.
4. Cash-flow-statement concepts (debt_issuance, debt_repayment,
   equity_issuance) are YTD-cumulated in quarterly filings, same as
   operating_cashflow. Wave 2 uses FY values only; any future quarterly use
   requires the .decumulate_cfo pattern.

Old cache backed up to `cache/fundamentals_pre_phase0/` (641 files).

Full-universe rebuild (640/641 tickers, 1.85M rows): new-concept coverage
income_tax_expense 99.4%, pretax_income 98.8%, retained_earnings 97.7%,
ppe_net 94.4%, equity_issuance 93.4%, goodwill 91.7%, debt_issuance 91.6%,
debt_repayment 89.7%, intangibles 88.8%, ppe_gross 86.1% -- all Wave 1-5
inputs clear the 70% bar. Below-bar concepts are the documented
absence-means-zero items (minority_interest 66.9%, lt_investments 65.2%,
preferred_stock 64.9%, st_investments 52.9%) and sparse advertising (47.9%).
81 tickers with Domestic-tag pretax rows were re-fetched post-fix; zero
remain. Known gap: FRC (First Republic, CIK 0001132979) returns permanent
404 from companyfacts (deregistered after the 2023 JPM acquisition); it was
never cached and its fundamentals are unavailable from this API.

Gate status: fetcher tests 148/148, indicator compute 182/182, integration
A-F 54/54. Phase 0 complete; Wave 1 unblocked.

### PIT dedup vintage fix (2026-07-01, post-Phase 0)

E2E verification of Phase 0 exposed a pre-existing design flaw: dedup kept
one row per (concept, period_end, fiscal_qtr) with the latest accession
winning, so a later filing that re-reports a period (comparative balance
sheet, amendment) replaced the original row and pushed its filed date
forward. Historical PIT snapshots then lost the observation entirely
(observed: ORCL FY2026 10-K wiped FY2025 equity from the 2026-03-31
snapshot; LIN Q1-2025 CFO silently lost to a post-snapshot comparative).

Fix: the cache now preserves reporting vintages -- dedup_fundamentals keys
on (concept, period_end, fiscal_qtr, accession), resolving tag aliases and
duration mismatches only within each filing. A new resolver pit_dedup(dt,
as_of) collapses vintages to one row per period using only rows filed on or
before as_of (NULL = latest view, the old cache shape). Consumers:
- get_fundamentals() gained as_of / vintages arguments; default output
  shape is unchanged (latest view).
- assemble_snapshot resolves each ticker via pit_dedup(as_of = snapshot
  date) -- prior-year rows retain their original fiscal-year labels, which
  also repairs lagged lookups needed by Wave 1.
- build_ticker_fundamentals stamps each fiscal year at the earliest
  annual-form filing date and computes indicators from the as-of view.
- build_ttm_eps_series picks the earliest (original) filing per quarter.

Cache impact: vintage rows increase files ~1.5-1.9x (prototype). Old
collapsed caches degrade gracefully (pit_dedup is a no-op on them) but
cannot reconstruct history until re-fetched. Full-universe re-fetch:
build_fundamentals(force_refresh = TRUE, chunk_size = 50L).

E2E: regenerated 2026-03-31 snapshot matches the pre-leak baseline
23,268/23,269 cells; ORCL's four lost indicators restored exactly; the one
differing cell is LIN fcf_stability where the fix RECOVERED a quarter of
CFO history the collapsed cache had lost.

### Pivot label-group fix (2026-07-02, follow-up)

Verifying the rebuilt timeseries layer exposed a second, longstanding bug
in pivot_fundamentals: within a (fiscal_year, period_type, concept) group
it kept the latest-FILED row, but a 10-K reports prior-year comparatives
under the SAME fiscal_year label and the SAME filed date as the current
period, so the survivor fell to parse order -- which in companyfacts JSON
is chronological, i.e. the OLDEST comparative. Wide rows could also mix
periods across concepts. Consequence: snapshots systematically carried
2-3-year-stale fundamentals (old 2026-03-31 snapshot: AAPL revenue was
FY2023's 383.3B, MSFT revenue was FY2022's 211.9B, ORCL fy2025 row was a
chimera of FY2023 equity/revenue/income). This predates the vintage fix;
old and new snapshots agreed because both shared it.

Fix: order by -period_end then -filed inside the group -- the current
period is always the group's max period_end; filed still breaks
original-vs-amendment ties for the same period. Corrected 2026-03-31
snapshot: 81% of cells changed across all 500 tickers, verified against
as-reported 10-K figures (AAPL FY2025 416.2B revenue / 46.9% gross margin,
MSFT FY2025 281.7B, ORCL implied equity exactly 20.451B). The 46 cells
that became NA are all peg -- its EPS-growth guard firing on corrected
growth numbers. Timeseries layer rebuilt from the fixed pivot (ORCL fund
layer now year-coherent with original-filing stamps).

Implication: any analysis produced from snapshots or the timeseries layer
before this fix used stale fundamentals and should be regenerated.

## 4. New indicators, prioritized by reproduced t-stat

Formulas below are stated in Compustat-style terms mapped to our concepts.
AT = total_assets, ACT = current_assets, LCT = current_liabilities, CHE =
cash (+st_investments where stated), INV = inventory, REC =
accounts_receivable, DLTT/DLC = long/short-term debt, CEQ =
stockholders_equity, LT = total_liabilities, ME = market_cap, BE = book
equity, avgAT = (AT_t + AT_{t-1})/2. Each formula must be re-verified against
the original paper and the OpenAP SignalDocumentation at implementation time;
entries go into docs/RESEARCH_INDICATORS.md as they are built.

### Wave 1 -- balance-sheet change family (12 indicators, t-stats 2.8-12.2)

STATUS: COMPLETED 2026-07-03 (commit df38cac) as 15 indicators (the 12
below + DelEqu, DelLTI, TotalAccruals). Canonical names and formulas in
docs/INDICATORS.md section 11 and RESEARCH_INDICATORS.md WAVE 1. All 66
snapshots + timeseries regenerated at 74 indicators. Coverage on
2024-06-30: del_finl/del_netfin 95%, del_equ/del_lti 99.6%,
equity_growth 92%, dnoa 78%, Richardson current-account family 74-84%
(bounded by AssetsCurrent tag coverage), inventory pair 59% (financials
NA + tag coverage), gr_ltnoa 71%. Implementation also fixed label-based
prior-year selection (lagged indicators could difference across two
years); quarterly QoQ selection retains the label issue -- follow-up
before any quarterly-surprise work (Wave 3).

Shared plumbing: lagged annual snapshot machinery + avgAT scaling. One
helper, twelve outputs. All annual (10-K to 10-K).

| # | Acronym | t | Original study | Formula |
|---|---|---|---|---|
| 1 | DelFINL | 12.23 | Richardson, Sloan, Soliman, Tuna (2005 JAE) | ΔFINL / avgAT, FINL = DLTT + DLC + preferred_stock |
| 2 | dNoa | 9.25 | Hirshleifer, Hou, Teoh, Zhang (2004 JAE) | ΔNOA / AT_{t-1}, NOA as already computed (Tier 1) |
| 3 | DelNetFin | 9.00 | Richardson et al. (2005) | Δ(FNA − FINL) / avgAT, FNA = st_investments + lt_investments |
| 4 | InvestPPEInv | 7.86 | Lyandres, Sun, Zhang (2008 RFS) | (ΔPPEgross + ΔINV) / AT_{t-1} |
| 5 | InvGrowth | 7.19 | Belo & Lin (2012 RFS) | inventory growth rate ΔINV / INV_{t-1} |
| 6 | ChInv | 6.24 | Thomas & Zhang (2002 RAS) | ΔINV / avgAT |
| 7 | DelCOA | 6.01 | Richardson et al. (2005) | Δ(ACT − CHE) / avgAT |
| 8 | ChNNCOA | 4.43 | Soliman (2008 TAR) | Δ(NCOA − NCOL) / AT_{t-1}; NCOA = AT − ACT − lt_investments, NCOL = LT − LCT − DLTT |
| 9 | DelCOL | 4.35 | Richardson et al. (2005) | Δ(LCT − DLC) / avgAT |
| 10 | ChEQ | 4.26 | Lockwood & Prombutr (2010) | CEQ_t / CEQ_{t-1} − 1 (BE > 0 both years) |
| 11 | GrLTNOA | 3.73 | Fairfield, Whisenant, Yohn (2003 TAR) | growth in long-term net operating assets (PPE + intangibles + other LT op assets, net of LT op liabilities) − depreciation adj; verify exact form |
| 12 | ChNWC | 2.83 | Soliman (2008) | Δ(COA − COL) / AT_{t-1} |

Also free once this lands: DelEqu (3.18), DelLTI (2.55), TotalAccruals
(2.63, Richardson full decomposition = ΔWC + ΔNCO + ΔFIN). Include if cheap.

### Wave 2 -- external financing family (7 indicators, t-stats 2.3-7.7)

STATUS: COMPLETED 2026-07-03 (commits 7b8121a, b45a2f0) as the
`financing` family, 74 -> 81 indicators. Canonical names/formulas in
docs/INDICATORS.md section 12 and RESEARCH_INDICATORS.md WAVE 2.
Coverage (2024-06-30): net_equity_finance 99%, share_iss_1y 98.4%,
share_iss_5y 95.6%, net_payout_yield 93.2%, net_debt_finance 87.3%,
composite_debt_issuance 82.9%. Split adjustment implemented on the
filed-date basis rule (a count's split basis is the split state at its
filing date) -- validated on AAPL 2020 4:1 across three vintage
scenarios. Share counts prefer the DEI cover instant after finding that
constant-issued-count filers (BA) zeroed share_iss for 2.4% of the
universe; BA's Oct-2024 $24B offering now reads +22.9% (1y) and its
net_equity_finance +12.4%. Anchor checks: AAPL FY2020 net equity
financing -25.8% (its 72B buybacks + 14B dividends over avgAT).

Requires Phase 0 cash-flow-statement tags. Annual, scaled by avgAT or ME.

| # | Acronym | t | Original study | Formula |
|---|---|---|---|---|
| 1 | NetDebtFinance | 7.70 | Bradshaw, Richardson, Sloan (2006 JAE) | (debt_issuance − debt_repayment + Δshort-term debt) / avgAT |
| 2 | NetEquityFinance | 5.40 | Bradshaw et al. (2006) | (equity_issuance − buybacks − dividends_paid) / avgAT |
| 3 | CompositeDebtIssuance | 5.19 | Lyandres, Sun, Zhang (2008) | log(totaldebt_t / totaldebt_{t-5}) |
| 4 | ShareIss1Y | 4.97 | Pontiff & Woodgate (2008 JF) | split-adjusted shares_t / shares_{t-1} − 1 |
| 5 | XFIN | 4.84 | Bradshaw et al. (2006) | NetDebtFinance + NetEquityFinance |
| 6 | ShareIss5Y | 4.32 | Daniel & Titman (2006 JF) | split-adjusted shares_t / shares_{t-5} − 1 |
| 7 | NetPayoutYield | 2.57 | Boudoukh et al. (2007 JF) | (dividends_paid + buybacks − equity_issuance) / ME |

Risk: XBRL share counts are as-reported; splits appear as jumps. ShareIss
needs split adjustment from the corporate-action/Yahoo adjusted data before
differencing. Gate test: AAPL 2020 4:1 split must not show as 300% issuance.

### Wave 3 -- quarterly seasonal-surprise family (6 indicators, t-stats 2.5-9.5)

STATUS: CODE COMPLETE 2026-07-03 as the `surprise` family, 81 -> 87.
Canonical names/formulas in docs/INDICATORS.md section 13 and
RESEARCH_INDICATORS.md WAVE 3. Constructions verified against OpenAP
pyCode (Signals/pyCode/Predictors); deltas from the draft plan adopted
from there: drift/sd are available-case over the prior 8 quarters (no
6-of-8 minimum; sd needs >= 2 and is over drift-adjusted surprises),
NumEarnIncrease replicates OpenAP exactly (missing quarter continues a
streak; a 9+ streak scores 0 -- MSFT reads 0 by design), ChTax scales by
assets at q-4, roaq by assets at q-1, EarningsConsistency is annual-only
with the CLG-CHG exception filters. Deviations documented: diluted EPS
(we do not cache basic ex-extraordinaries) and explicit split adjustment
(OpenAP relies on Compustat; unadjusted, AAPL 2020 fabricates a -3x
seasonal diff). revenue_growth_qoq rebased onto the panel (Wave 1
follow-up closed); it now reflects the latest FILED quarter.

Gates: indicator tests 254/254 (34 new), fetcher 159/159, pit_assembler
55/55, timeseries 188/188, ttm_eps 23/23, pipeline 34/34, integration
A-F 54/54. Anchors: AAPL FY2024 Q4 revenue 94.93B via FY - Q3ytd
(exact); AAPL 2020 split-adjusted EPS chain reproduces the post-split
basis to the cent, SUE sane at -0.71 (unadjusted revenue surprise would
read -9.7); JPM ch_tax/roaq computed; KR 16-week Q1 classifies and
seasonally aligns. A .q_shares_at direction bug (counts adjust opposite
to per-share values) was caught by the unit tests and fixed. Coverage on
a fresh 2024-06-30 snapshot (498 tickers): ch_tax 98.4%,
num_earn_increase 98.6%, revenue_surprise 96.6%, earnings_surprise
98.2%, roaq 95.4%, earnings_consistency 45.4% (by construction --
the Alwathainani consistency filter excludes sign-flippers). Cell-level
diff vs the 81-indicator baseline: only revenue_growth_qoq differs
(489/498 tickers, the intended rebase); all other 80 indicators
identical. Pre-existing observation, not Wave 3: validate_snapshot's
zscore_mean_near_zero check fails on the baseline too (12 skewed level
indicators, max |z-mean| 0.748 = revenue_raw); left untouched.

Regeneration COMPLETED 2026-07-04: all 66 snapshots at 87 indicators
(verified per-file), timeseries fund + daily layers rebuilt (639 layers,
all carrying the surprise columns; the 47 daily-layer failures are the
usual price-less tickers). One rebuild subtlety: the Wave 2 timeseries
run overlapped this wave's coding session and sourced indicator_compute
BEFORE the .q_shares_at split-direction fix -- its fund layers carried
wrong revenue_surprise for split tickers (AAPL, 14 cells). Caught by
recompute-diff, fixed by a forced layer rebuild; snapshots were never
affected (all regenerated post-fix). Old layers parked in
cache/timeseries_pre_wave3/ (deletable). Historical coverage matches the
XBRL-ramp expectation: SUE 83% (2011) -> 96% (2013) -> 98% (2024).

Requires standalone-quarter income-statement values (Q4 = FY − Q1..Q3, same
pattern as .decumulate_cfo for CFO). This machinery is the main cost; the
indicators are then near-free, and roaq / ChangeRoA / ChangeRoE variants
come along at no cost.

| # | Acronym | t | Original study | Formula |
|---|---|---|---|---|
| 1 | ChTax | 9.50 | Thomas & Zhang (2011 JAR) | (tax_q − tax_{q-4}) / AT_{q-4} |
| 2 | NumEarnIncrease | 6.75 | Loh & Warachka (2012 MS) | streak length: consecutive quarters with NI_q > NI_{q-4} (capped at 8) |
| 3 | RevenueSurprise | 5.99 | Jegadeesh & Livnat (2006 JFE) | (rev_q − rev_{q-4} − drift) / σ(seasonal diffs, prior 8q), per share |
| 4 | EarningsSurprise (SUE) | 4.94 | Foster, Olsen, Shevlin (1984 TAR) | (EPS_q − EPS_{q-4} − drift) / σ(seasonal diffs, prior 8q) |
| 5 | EarningsConsistency | 2.51 | Alwathainani (2009 BAR) | consistency of annual EPS growth sign/rank over prior 5y; verify exact form |
| 6 | OPLeverage_q etc. | -- | (variants) | optional once quarterly machinery exists |

#### Wave 3 implementation plan (2026-07-03)

Final scope: 6 indicators, new family `surprise`, 81 -> 87. The five core
rows above plus roaq (Balakrishnan 2010, t = 5.90, listed in section 2 as
free once the machinery lands). OPLeverage_q and other quarterly variants
deferred. No new fetcher concepts: income_tax_expense (99.4%), net_income,
revenue, eps_diluted, total_assets, shares_outstanding are all cached.

Step 1 -- standalone-quarter panel (the machinery, main cost).
New builder `.build_quarter_panel(fund_dt, splits)` in indicator_compute.R,
constructed from the LONG as-of view -- NOT from pivot (fiscal_year,
period_type) labels. Grouping by (period_start, duration) is label-free,
which resolves the quarterly label issue flagged in the Wave 1 status note
as a Wave 3 blocker; `build_ttm_eps_series` (ttm_eps.R) is the proven
template. Per concept x quarter:
- classify duration rows into 3mo/2Q/3Q/FY using the .decumulate_cfo bands
  (Q1 upper bound 120d for Kroger/AAP 16-week Q1);
- standalone value: prefer a direct 3-month row where the filer reports
  one; else difference consecutive YTD rows sharing period_start;
  Q4 = FY - Q3ytd. Classify per ROW, not per filer -- styles are mixed;
- seasonal lag q-4: match by period_end 330-400 days earlier (52/53-week
  safe), gap guard -> NA, never difference across a fiscal-year-change stub;
- eps_diluted normalized to current split basis via .split_factor(filed)
  BEFORE differencing (reuse from ttm_eps.R); per-share revenue uses
  split-adjusted shares (DEI cover instant preferred, per Wave 2);
- instants (total_assets, shares) taken at each quarter's period_end.
PIT is inherited: both call sites (assemble_snapshot, timeseries builder)
already hand compute_indicators the pit_dedup as-of view and splits.

Step 2 -- indicators (near-free given the panel).
- ch_tax = (tax_q - tax_{q-4}) / AT_{q-4}
- num_earn_increase = consecutive quarters with NI_q > NI_{q-4}, cap 8
- revenue_surprise / earnings_surprise (SUE) = (x_q - x_{q-4} - drift) /
  sd(seasonal diffs, prior 8 quarters), x = revenue per share / EPS;
  require >= 6 of 8 prior diffs (confirm threshold vs OpenAP
  SignalDocumentation at implementation time); sd floor guard for
  degenerate near-zero dispersion
- earnings_consistency = annual EPS growth consistency over prior 5y;
  verify exact Alwathainani form from SignalDocumentation before coding
- roaq = NI_q / AT_{q-1 quarter}
Latest quarter = max standalone period_end in the as-of view. Financial
sector: no COGS/inventory dependency, so financials are computed, not
masked; bank revenue tags are sparse, revenue_surprise will be NA-heavy
for financials -- document, do not force.

Step 3 -- also in this wave: rebase revenue_growth_qoq curr_q/prior_q
selection onto the panel (period_end order, not label order), closing the
Wave 1 follow-up.

Step 4 -- wiring: get_indicator_names() + family map (`surprise`),
.assert_output, snapshot row-count assertions (pit_assembler + pipeline
gates), INDICATORS.md section 13, RESEARCH_INDICATORS.md WAVE 3.

Step 5 -- per repo rules: code -> detailed code review -> unit tests ->
gate test. Unit tests: synthetic YTD-only / 3mo-only / mixed filers,
split-spanning SUE, 16-week Q1, fiscal-year-change stub -> NA, missing
Q3 -> Q4 NA, streak edge cases (exactly 8, broken streak, NA quarter).
Gate anchors: AAPL FY2024 Q4 revenue 94.93B via FY - Q3ytd; AAPL 2020 4:1
split must not distort SUE or share-based revenue_surprise; a known
earnings streak (e.g. MSFT) for num_earn_increase; ch_tax present for a
bank. Then integration A-F, 20-ticker prototype coverage report, full
regeneration of all snapshots + timeseries at 87 indicators, coverage on
2024-06-30 against the 70% bar.

Risks specific to this wave:
1. Mixed 3mo/YTD tagging within one filer history (classify per row).
2. Q4 derivation mixes tag aliases between FY and Q3ytd rows within a
   concept -- accept (same-concept differencing only), spot-check anchors.
3. History depth: SUE needs 13 consecutive quarters; snapshots before
   ~2013 will be NA-heavy (XBRL mandate ramp) -- document, matches the
   existing 5y-lookback caveat.
4. sd ~= 0 for ultra-stable seasonal diffs -> guard, NA not Inf.

Plan review notes (code-verified 2026-07-03, pre-implementation):
1. Duration-band conflict: two classification conventions coexist.
   ttm_eps.R uses catch-all cumulative bands (<=135 Q1, <=225 Q2,
   <=315 Q3, else FY); .decumulate_cfo uses strict bands with NA gaps
   (60-120 / 160-200 / 250-290 / 330-380). The panel builder must use
   the strict-band style (gap -> NA beats silent misclassification for
   differencing). Extract a shared .classify_duration() helper; leave
   ttm_eps.R on its own bands (TTM identity is robust to a missing
   middle quarter, differencing is not).
2. Panel dedup rule: per (concept, period_start, duration_class) keep
   non-8-K then EARLIEST filed -- exactly the build_ttm_eps_series
   vintage rule (ttm_eps.R lines 71-79), PIT-correct on the vintage
   cache and a no-op on collapsed caches.
3. History depth per indicator (sets coverage expectations for the
   2024-06-30 report): roaq 2 consecutive quarters; ch_tax 5 (needs
   AT instant at q-4 period_end); num_earn_increase 12 (cap-8 streak =
   8 seasonal comparisons); SUE / revenue_surprise 13; and
   earnings_consistency is ANNUAL-only (6 FY EPS values, split-adjusted)
   -- it does not depend on the quarter panel and can be built
   independently of Steps 1-2.
4. Call-site confirmation: compute_ticker_indicators already receives
   the long as-of fund_dt and splits (financing wave precedent), so
   .compute_surprise(fund_dt, splits, ...) requires no signature change
   in pit_assembler or timeseries_builder. The revenue_growth_qoq label
   defect is confirmed at indicator_compute.R:1404-1419.
5. Sequencing: Wave 3 coding may start immediately (touches no cache),
   but the full 87-indicator regeneration waits until the in-flight
   Wave 2 regeneration run completes and its 81-indicator snapshots are
   validated as the comparison baseline.

### Wave 4 -- levels and simple ratios (17 indicators, t-stats 2.2-5.8)

STATUS: COMPLETED 2026-07-05 as 29 indicators (87 -> 116), families
"levels" (19) and "investment" (10). Canonical names/formulas in
docs/INDICATORS.md sections 14-15 and RESEARCH_INDICATORS.md
W4.1-W4.22. Ten ME-scaled levels are price-sensitive in the timeseries
layer (8 new stubs; shared .level_components resolver keeps compute
and stub paths identical). ch_inv_ia demeaned at all three
cross-section assembly points; daily reader degrades it to NA when the
sector lookup is missing. Financial NA additions live (12 -> 13 with
net_debt_price also stub-masked + current-sector re-masked in the
daily reader). Code review (8 finder angles, verified): 10 findings
fixed, including the investment prior-gate 2-of-3 bug, un-demeaned
ch_inv_ia in the feature layer's historical path, and the TCJA
straddle now using the true fiscal-year start when a prior row exists.
Tests: indicator_compute 312/312, timeseries 217/217, assembler 55/55,
pipeline 34/34, standardizer 46/46, integration A-F 54/54. Coverage
(2024-06-30, 498 tickers): most levels 85-98%, ebm/bpebm 73.5%,
net_debt_price 71.3%; by-design low: rd_me 41% (R&D reporters only),
surprise_rd 42.6%, oper_prof 34.1% (OpenAP all-four-inputs rule,
interest_expense binding). 2015-06-30 median Wave 4 coverage 85.1%.
Full regeneration done via tools/regen_wave4.R: 66/66 snapshots at
116, 639 fund layers force-rebuilt with Wave 4 stubs, 595 daily
layers rebuilt from scratch (append-only updater cannot backfill new
columns), TTM augment reapplied. Anchors: AAPL effective_tax_rate
14.7% (= reported FY2023 ETR), tang/oper_prof/payout_yield match
hand computation from the FY2023 10-K, JPM daily net_debt_price NA.

Mostly one-liners on existing + Phase 0 concepts.

| # | Acronym | t | Original study | Formula |
|---|---|---|---|---|
| 1 | RD | 5.82 | Chan, Lakonishok, Sougiannis (2001 JF) | rnd / ME |
| 2 | ChInvIA | 5.50 | Abarbanell & Bushee (1998 TAR) | firm capex growth − industry (sector) mean capex growth |
| 3 | grcapx | 4.96 | Anderson & Garcia-Feijoo (2006 JF) | capex_t / capex_{t-2} − 1 |
| 4 | grcapx3y | 4.77 | same | capex_t / capex_{t-3} − 1 |
| 5 | PctTotAcc | 4.70 | Hafzalla, Lundholm, Van Winkle (2011 TAR) | total accruals / |NI| (Richardson TotalAccruals numerator) |
| 6 | EBM | 4.14 | Penman, Richardson, Tuna (2007 JAR) | (BE + ND) / (ME + ND), ND = FINL − FNA |
| 7 | CF | 4.04 | Lakonishok, Shleifer, Vishny (1994 JF) | (NI + depreciation) / ME |
| 8 | NetDebtPrice | 3.88 | Penman et al. (2007) | ND / ME |
| 9 | ChAssetTurnover | 3.77 | Soliman (2008) | Δ(revenue / avg NOA) |
| 10 | tang | 3.67 | Hahn & Lee (2009 JF) | (CHE + 0.715 REC + 0.547 INV + 0.535 PPEnet) / AT |
| 11 | Tax | 3.52 | Lev & Nissim (2004 TAR) | (tax_expense / statutory_rate) / NI; statutory rate 35% pre-2018, 21% after |
| 12 | AM | 3.50 | Fama & French (1992 JF) | AT / ME |
| 13 | CashProd | 3.40 | Chandrashekar & Rao (2009 WP) | (ME + DLTT − AT) / CHE |
| 14 | BookLeverage | 3.30 | Fama & French (1992) | AT / BE |
| 15 | GrSaleToGrInv | 3.30 | Abarbanell & Bushee (1998) | %ΔSales (vs 2y avg base) − %ΔINV (vs 2y avg base); AB98 form |
| 16 | SurpriseRD | 3.00 | Eberhart, Maxwell, Siddique (2004 JF) | indicator: R&D growth > 5% and R&D/AT growth > 5%, positive R&D |
| 17 | OperProf | 3.00 | Fama & French (2006 JFE) | (revenue − cogs − sga − interest_expense) / BE |

Plus (same wave, sub-2.6 t but one-liners): Cash 2.97 (cash+STI / avgAT,
Palazzo 2012), BPEBM 2.83 (Penman leverage component), OrgCapNoAdj-style
alternatives skipped, Leverage 2.64 (total debt / ME, Bhandari 1988),
OPLeverage 2.50 ((cogs + sga)/AT, Novy-Marx 2010), RDcap 2.32 (capitalized
R&D w/ 20% amortization / AT, Li 2011), Investment 2.28 (capex/revenue vs
own 3y mean, Titman, Wei, Xie 2004), PayoutYield 2.27, cfp 2.18 (CFO/ME,
Desai et al. 2004), salecash (revenue/cash, Ou-Penman ingredient), ETR and
depr/pchdepr (once tax + PPE tags exist).

Financial-sector NA policy applies: GrSaleToGrInv, InvGrowth, ChInv,
InvestPPEInv, tang, OPLeverage, ChAssetTurnover join the existing NA list
for financials where COGS/inventory-dependent.

#### Wave 4 preflight verification (2026-07-03)

All 28 constructions verified against the OpenAP Stata code
(github.com/OpenSourceAP/CrossSection, now under Signals/
LegacyStataCode/ -- the old Signals/Code/ paths 404). Canonical
formulas, deviations and guards are in RESEARCH_INDICATORS.md WAVE 4
(W4.1-W4.22); entries there SUPERSEDE the table above where they
differ. Corrections found:

1. grcapx3y is 3*capx_t/(capx_{t-1}+capx_{t-2}+capx_{t-3}), current
   capex over the prior-3y average -- not capx_t/capx_{t-3} - 1.
2. Leverage (Bhandari) is total LIABILITIES / ME (lt), not total debt.
3. CashProd as replicated is (ME - AT)/CHE; OpenAP omits the paper's
   + DLTT term. We follow the replicated code.
4. tang uses GROSS PP&E (ppegt), not net, and OpenAP restricts it to
   manufacturing (SIC 2000-3999); we compute for all non-financials.
5. Cash (Palazzo) is quarterly cheq/atq in OpenAP; ours is
   filing-frequency (cash + STI)/AT, documented as cash_assets.
6. ETR is an OpenAP PLACEBO and price-interacted; dropped in favor of
   a plain effective_tax_rate level (tax/pretax).
7. PctTotAcc's OpenAP numerator needs total financing + investing CF
   (fincf, ivncf -- not fetched); we use the Wave 1 Richardson
   balance-sheet decomposition over |NI| instead.
8. ChAssetTurnover's OpenAP NOA needs aco/lco/lo detail tags; we reuse
   the Tier 1 Hirshleifer NOA. Negative-turnover guard adopted.
9. Tax (Lev-Nissim): OpenAP grosses up txfo+txfed (fallback txt-txdi),
   neither split fetched -- we use total tax expense; OpenAP's rate
   table was never updated for TCJA, we use 35% through 2017, blended
   straddle year, 21% after.
10. RDcap zero-fills missing R&D and OpenAP keeps only the bottom size
    tercile -- computed for all, small-cap caveat documented.
11. Investment (TWX) benchmark mean INCLUDES the current year, min 2
    of 3 years, revenue >= $10M floor, no minus-one.
12. SurpriseRD thresholds strictly > 1.05 on both conditions; OpenAP's
    Stata missing-semantics leak (missing revt/at pass) fixed by
    requiring both nonmissing.
13. ChInvIA ingredient is the AB98 2y-average-base capex growth with
    1y fallback, demeaned by equal-weighted mean within 2-digit SIC;
    ours demeans within the finviz sector map at the assembler stage.
14. salecash, depr, pchdepr are OpenAP Placebos (Table 4); included
    as database quantities, documented as non-predictors.
15. Backtest-only sample filters (size terciles in OperProf/RDcap,
    B/M quintiles in NetDebtPrice, positive-payout + 24-month
    seasoning in PayoutYield, financials exclusion where it is a
    sample choice) documented, not applied.

Zero new fetcher concepts required: every variant chosen stays inside
the 45 fetched concepts (no txditc, txfo/txfed/txdi, fincf/ivncf,
pstkrv, aco/lco/lo, dc/dvpa/tstkp).

Final Wave 4 set: 29 indicators, 87 -> 116 once Wave 3's 6 land
(before Wave 3: 81 -> 110). Families: "levels" (19) -- rd_me, ebm,
bpebm, cf_me, cfp, net_debt_price, tang, tax_to_book,
effective_tax_rate, am, book_leverage, leverage_mkt, cash_prod,
oper_prof, cash_assets, op_leverage, payout_yield, salecash,
depr_rate; "investment" (10) -- pchdepr, grcapx, grcapx3y,
pct_tot_acc, ch_asset_turnover, gr_sale_to_gr_inv, surprise_rd,
investment, rd_cap, ch_inv_ia. Financial NA additions: tang,
op_leverage, ch_asset_turnover, gr_sale_to_gr_inv, net_debt_price.

Implementation gated on the Wave 3 commit (shared files: indicator
names, family map, gate assertions). ch_inv_ia adds a sector-demean
step to pit_assembler; everything else is indicator_compute only.
Fetched OpenAP .do files retained in the session scratchpad during
implementation for line-level reference.

### Wave 5 -- composite scores (4-6 indicators)

| Acronym | t | Original study | Notes |
|---|---|---|---|
| MS (G-score) | 5.44 | Mohanram (2005 RAS) | 8 binary components vs sector medians (ROA, CFROA, CFO>NI, σROA, σrev growth, R&D/AT, capex/AT, adv/AT). Store components individually like F-score. Advertising component may be NA-heavy (sparse tag) -- document degradation to 7 components. |
| OScore | 3.39 | Ohlson (1980 JAR) via Dichev (1998 JF) | −1.32 − 0.407 log(AT) + 6.03 LT/AT − 1.43 WC/AT + 0.076 LCT/ACT − 1.72 1{LT>AT} − 2.37 NI/AT − 1.83 FFO/LT + 0.285 1{loss 2y} − 0.521 ΔNI/(|NI_t|+|NI_{t-1}|) |
| ZScore | (1.20) | Altman (1968 JF) via Dichev (1998) | 1.2 WC/AT + 1.4 RE/AT + 3.3 EBIT/AT + 0.6 ME/LT + 1.0 revenue/AT. Weak predictor but canonical database indicator. |
| Herf | 2.30 | Hou & Robinson (2006 JF) | sum of squared within-industry sales shares, 3y average. Caveat: our universe is S&P 500 only, concentration is relative to large-cap peers, not the full market. |
| KZ / WW | (0.53/1.34) | Lamont et al. (2001) / Whited & Wu (2006) | financial-constraint indices; completeness only, lowest priority. |

#### Wave 5 preflight verification + implementation plan (2026-07-03)

All six constructions verified against the OpenAP code. Note: the repo's
current implementation is Python (Signals/pyCode/Predictors|Placebos/),
a line-by-line port of Signals/LegacyStataCode/; the Stata remains the
canonical spec and is what was verified. Fetched .do/.py files retained
in the session scratchpad for line-level reference during
implementation. Zero new fetcher concepts required; all inputs are
inside the 45 fetched concepts.

Final scope: 14 indicators, 116 -> 130 (81 committed + Wave 3's 6 +
Wave 4's 29 + these). Families: "mohanram" (9) -- ms_roa, ms_cfroa,
ms_accrual, ms_roa_vol, ms_rev_vol, ms_rd, ms_capex, ms_adv, ms_score
(components stored individually, f_score pattern); "distress" (2) --
o_score, z_score; "structure" (3) -- herf, kz_index, ww_index.

Construction findings and decisions (canonical entries go into
RESEARCH_INDICATORS.md WAVE 5 at implementation; they SUPERSEDE the
table above where they differ):

1. MS is quarterly in OpenAP: TTM sums (12-month rolling, min 12
   months) of quarterly NI, CFO, capex, R&D over average / lagged
   quarterly assets; advertising stays annual. The volatility
   components are sd of quarterly ROA (niq/atq) and sd of quarterly
   YoY sales growth (saleq vs 4 quarters back) over a 48-month window,
   min 18 months (~16 quarters, min ~6). Ours: level components
   (m1-m3, m6-m8) from FY filing-frequency values per repo convention;
   volatility components (m4, m5) from the Wave 3 standalone-quarter
   panel, trailing 16 quarters, min 6 observations.
2. MS sample filter: OpenAP restricts to the lowest-BM quintile
   (log(ceq/ME), monthly) with ceq > 0 BEFORE computing industry
   medians, so the medians are within-growth-firm medians. Backtest
   sample choice per the Wave 4 item-15 rule: we compute across the
   full snapshot universe and document that our medians are
   full-universe large-cap medians -- the largest deviation in this
   wave.
3. MS industry = 2-digit sicCRSP with >= 3 firms per industry-month;
   ours = finviz sector (11 groups, always >= 3 in a 500-name
   snapshot), medians computed at the assembler stage via the same
   cross-sectional hook Wave 4 adds for ch_inv_ia.
4. MS missing handling: OpenAP zero-fills advertising and quarterly
   R&D; adopted (absent R&D/advertising tags = immaterial spend). The
   Stata missing=+infinity artifact (missing numerator grants
   m1/m2/m3/m6/m7/m8 a 1; the Python port reproduces it deliberately)
   is NOT replicated -- missing profitability inputs give NA
   components; ms_score NA if any component NA (f_score precedent).
   With 47.9% advertising coverage the zero-fill keeps m8 computable,
   but sector medians of adv/AT will sit at or near 0, so m8
   degenerates toward "discloses advertising at all" -- document,
   keep the component stored.
5. MS discretization: OpenAP clamps the 0-8 sum into {1..6} (>=6 -> 6,
   <=1 -> 1). Traded-signal transform -- we store the raw 0-8 sum.
6. OScore: FFO = Compustat fopt with oancf fallback; post-1987 (our
   whole sample) that is simply CFO -- we use operating_cashflow. ib
   maps to net_income. INTWO in the replicated code is
   (ib_t + ib_{t-1}) < 0 -- the SUM negative, not loss-in-both-years
   as in Ohlson's paper -- follow the replicated code. Size term is
   log(AT/GNP deflator); the deflator is omitted (it is constant
   within a snapshot, so cross-sectional ordering and z-scores are
   exact; raw-level drift across years documented) -- no new external
   data dependency. o_score is price-free, so it is also available in
   the per-ticker timeseries fund layer.
7. OScore discretization per Dichev Table 5 (binary: top decile vs
   deciles 1-7, deciles 8-9 dropped) is a traded-signal transform --
   we store the continuous score. Sign: higher = more distressed.
8. ZScore (OpenAP Placebo): EBIT proxy is ni + xint + txt =
   net_income + interest_expense + income_tax_expense (not operating
   income); leverage term is 0.6 * ME / total LIABILITIES; revt/at
   coefficient 1.0. OpenAP refreshes ME monthly; ours is market_cap at
   filing date, making z_score price-sensitive: snapshot-only, NA in
   the timeseries fund layer.
9. OScore/ZScore exclusions: OpenAP sets NA for SIC 4000-4999 and
   > 5999 (transport/utilities, financials and beyond). Mapped to
   sectors: NA for Financial, Utilities, Real Estate (transports
   inside Industrials cannot be isolated with the finviz map;
   documented). Implemented by generalizing .FINANCIAL_NA_INDICATORS
   into a sector -> indicators NA map.
10. Herf: the industry is de facto 4-digit sicCRSP (the code's "sic3D"
    slices 4 characters; the Python port reproduces this). Ours =
    finviz INDUSTRY (~150 groups), the closest granularity our map
    offers. HHI = sum of squared within-industry revenue shares;
    3-year average (OpenAP: 36-month rolling mean, min 12 months ->
    ours: mean of up to 3 annual HHIs, min 1). Singleton industries
    give HHI = 1 identically -> NA (universe artifact). The permanent
    SIC-49 exclusion maps to the Utilities mask; shrcd, pre-1983
    regulated-industry and pre-1951 filters do not bind post-2009.
    S&P-500-only caveat stands: concentration among large-cap peers.
11. KZ (Placebo): -1.002*(ib+dp)/ppent + 0.283*q + 3.139*debt/(debt +
    seq) - 39.368*(dvc+dvp)/ppent - 1.315*che/ppent, with q = (at +
    ME - ceq - txdb)/at, debt = dlc + dltt. txdb (deferred-tax
    balance) is not fetched -> omitted (treated as 0), documented.
    che = cash + st_investments; dividends_paid covers dvc + dvp;
    ppent = ppe_net. Price-sensitive via q. No filters in OpenAP.
12. WW (Placebo): -0.091*(ib+dp)/(4*at) - 0.062*DIVPOS +
    0.021*dltt/at - 0.044*log(at) + 0.102*(industry sales growth)/4 -
    0.035*(firm sales growth)/4. DIVPOS = dividends_paid > 0.
    Industry = 3-digit sicCRSP -> finviz industry, computed at the
    assembler stage from the same revenue ingredients as Herf.
    OpenAP's firm sales-growth term takes a 1-MONTH lag on the
    monthly-expanded annual panel (zero for 11 of 12 months) -- a
    panel artifact, not replicated; we use FY-over-FY revenue growth
    / 4 (Wave 4 item-12 precedent for fixing Stata artifacts).
13. Placebo status: z_score, kz_index, ww_index are OpenAP Placebos --
    included as database quantities, documented as non-predictors
    (Wave 4 item-14 precedent).

Architecture split:
- Per-ticker (indicator_compute.R): o_score, z_score, kz_index
  composites; MS ingredient values (FY levels + quarter-panel sds);
  WW firm-level terms; current + 2 lagged FY revenues for Herf. The
  cross-sectional inputs travel as hidden ingredient columns, not
  indicator rows.
- Cross-sectional (assembler-stage post-pass, shared with Wave 4
  ch_inv_ia): finviz-sector medians -> m1..m8 binaries + ms_score;
  finviz-industry HHI (3y avg) -> herf; industry sales growth
  completes ww_index. ms_*, herf, ww_index are therefore
  snapshot-only (NA in the timeseries fund layer), like
  price-sensitive indicators; of the wave only o_score appears in the
  timeseries fund layer.

Steps, per repo rules (code -> detailed code review -> unit tests ->
gate test):
1. Sector-NA mask generalization; per-ticker composites and
   ingredients (reuse Wave 3 quarter panel + prior-FY machinery).
2. Cross-sectional pass: sector medians, binaries, HHI, WW assembly.
3. Wiring: .INDICATOR_NAMES (+14), family map (mohanram, distress,
   structure), .assert_output, snapshot row-count assertions
   (pit_assembler + pipeline gates), INDICATORS.md new section,
   RESEARCH_INDICATORS.md WAVE 5.
4. Unit tests: hand-computed OScore/ZScore/KZ/WW on synthetic firms;
   OENEG (lt > at) and INTWO (sum-negative vs both-negative) dummy
   edges; NI_t = NI_{t-1} = 0 in the CHIN denominator -> NA; median
   binarization on a synthetic sector incl. ties and NA inputs;
   R&D/advertising zero-fill; raw 0-8 sum (no clamp); sd window with
   exactly 6 and fewer than 6 quarters; singleton-industry herf ->
   NA; sector-NA mask hits all three sectors.
5. Gate anchors: 2020-era airline (AAL/DAL) vs AAPL orders o_score
   correctly; AAPL z_score deep in the safe zone (>> 3); bank
   z_score/o_score NA via mask, bank ms_score computed; herf high for
   a known concentrated finviz industry, NA for singletons; ms_score
   distribution within {0..8}; 20-ticker prototype coverage, then
   full regeneration (all snapshots + timeseries) at 130 indicators,
   coverage report on 2024-06-30 vs the 70% bar.

Sequencing: implementation gated on the Wave 3 and Wave 4 commits
(shared indicator-name/family/gate files; reuses the ch_inv_ia
cross-sectional hook). Regeneration queues behind the in-flight Wave
2/3 snapshot rebuilds.

Risks specific to this wave:
1. Sector-granularity mismatch: 11 finviz sectors vs ~70 2-digit SIC
   groups for MS medians; ~150 finviz industries vs 4-digit SIC for
   Herf. Documented per indicator.
2. Full-universe (not low-BM-quintile) MS medians shift component
   cut-points vs Mohanram; components remain internally consistent.
3. ms_roa_vol / ms_rev_vol need 6+ standalone quarters: NA-heavy
   before ~2013 (XBRL ramp), matching the Wave 3 caveat -- and they
   gate ms_score under the any-NA rule; report the ms_score coverage
   consequence explicitly.
4. Advertising median degeneracy (item 4).
5. O-Score/Z-Score coefficients presume industrials; the sector mask
   removes the worst offenders, remaining cross-sector application
   documented.

### Wave 6 -- deferred / stretch (document, do not schedule)

- AbnormalAccruals (Xie 2001, t=5.10): modified Jones model needs
  industry-year cross-sectional regressions; with a 500-name large-cap
  universe the residuals are not comparable to the original. Revisit if
  universe expands.
- EquityDuration (Dechow, Sloan, Soliman 2004, t=3.09): multi-step implied
  cash-flow duration model; medium-hard.
- OrgCap (Eisfeldt & Papanikolaou 2013, t=2.72): capitalized SG&A with 15%
  depreciation, requires CPI deflator (FRED) and long SG&A history.
- RDS real dirty surplus (Landsman et al. 2011, t=3.83): comprehensive-income
  detail tags, messy.
- ConvDebt (Valta 2016, t=4.34): convertible-debt tags are inconsistently
  reported; prototype coverage check first.
- GrAdExp (Lou 2014, t=3.89) and BrandInvest: AdvertisingExpense tag is
  sparse in XBRL (often text-only disclosure). Coverage check first.
- CompEquIss / IntanBM / IntanCFP / IntanEP / IntanSP (Daniel & Titman
  2006): need 5y cumulative return decomposition; fundamental x price
  composites, fit better in the rF/feature layer than the PIT database.

### 4.7 Excluded despite strong t-stats (data unavailable in EDGAR XBRL)

- hire, employment growth (Bazdresch et al., t=5.67): employee counts are
  10-K cover-page text, not companyfacts XBRL.
- OrderBacklog / OrderBacklogChg (t=3.43/2.49): Compustat OB; nearest XBRL
  concept (RevenueRemainingPerformanceObligation) only exists post-2018.
- ExclExp, EarningsStreak, all forecast-based signals: IBES.
- FR pension funding (t=1.74): tags exist but low signal; optional later.
- realestate (t=1.82): building/lease PPE detail tags too sparse.
- LaborforceEfficiency (t=-0.74): employees again, and a not-predictor.

## 5. Execution plan

Per-repo rules: each wave = code -> detailed code review -> unit tests ->
gate test, in that order. No stop() on single-ticker failure. All new
indicators go through .assert_output(), the financial-sector NA mask, and
the robust z-score pipeline unchanged.

| Phase | Content | New indicators | Est. size |
|---|---|---|---|
| 0 | Fetcher: +12-14 concepts, alias validation on 20-ticker prototype, full re-fetch, coverage report | 0 | S |
| 1 | Wave 1 balance-sheet changes (+ DelEqu, DelLTI, TotalAccruals) | 15 | M |
| 2 | Wave 2 financing flows + split-adjustment guard | 7 | M |
| 3 | Wave 3 quarterly de-cumulation of income statement + seasonal surprises | 6 | M (machinery), S (indicators) |
| 4 | Wave 4 levels/ratios | ~25 | M (many small) |
| 5 | Wave 5 composites (MS, OScore, ZScore, Herf) | 4 + MS components | M |
| -- | Wave 6 deferred | 0 | -- |

End state: 59 -> ~115 indicators. After each phase: update
docs/RESEARCH_INDICATORS.md (formula, XBRL tags, reference, replication
t-stat from CZ tables), get_indicator_names(), family map (likely new
families: "balance_sheet_change", "financing", "surprise", "distress"),
INDICATORS.md, and the snapshot row-count assertions in pit_assembler /
pipeline gates.

Cross-cutting risks:
1. Tag sparsity: every Phase 0 concept gets a coverage % on the prototype
   20 before full build; indicators whose inputs cover <70% of the universe
   get flagged in the doc rather than silently NA-heavy.
2. Split adjustment for share-count differences (Wave 2).
3. Industry adjustment (ChInvIA, MS, Herf) uses our sector map on a
   500-name universe -- document the large-cap-relative caveat.
4. Statutory tax rate regime change 2017/2018 (Tax indicator).
5. History depth: 5y-lookback indicators (CompositeDebtIssuance, ShareIss5Y,
   RDcap, EarningsConsistency) are NA for tickers with short XBRL history
   (XBRL mandate ~2009-2011 onward; fine for current snapshots).
