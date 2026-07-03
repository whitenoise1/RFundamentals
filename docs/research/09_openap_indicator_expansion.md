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

### Wave 4 -- levels and simple ratios (17 indicators, t-stats 2.2-5.8)

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

### Wave 5 -- composite scores (4-6 indicators)

| Acronym | t | Original study | Notes |
|---|---|---|---|
| MS (G-score) | 5.44 | Mohanram (2005 RAS) | 8 binary components vs sector medians (ROA, CFROA, CFO>NI, σROA, σrev growth, R&D/AT, capex/AT, adv/AT). Store components individually like F-score. Advertising component may be NA-heavy (sparse tag) -- document degradation to 7 components. |
| OScore | 3.39 | Ohlson (1980 JAR) via Dichev (1998 JF) | −1.32 − 0.407 log(AT) + 6.03 LT/AT − 1.43 WC/AT + 0.076 LCT/ACT − 1.72 1{LT>AT} − 2.37 NI/AT − 1.83 FFO/LT + 0.285 1{loss 2y} − 0.521 ΔNI/(|NI_t|+|NI_{t-1}|) |
| ZScore | (1.20) | Altman (1968 JF) via Dichev (1998) | 1.2 WC/AT + 1.4 RE/AT + 3.3 EBIT/AT + 0.6 ME/LT + 1.0 revenue/AT. Weak predictor but canonical database indicator. |
| Herf | 2.30 | Hou & Robinson (2006 JF) | sum of squared within-industry sales shares, 3y average. Caveat: our universe is S&P 500 only, concentration is relative to large-cap peers, not the full market. |
| KZ / WW | (0.53/1.34) | Lamont et al. (2001) / Whited & Wu (2006) | financial-constraint indices; completeness only, lowest priority. |

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
