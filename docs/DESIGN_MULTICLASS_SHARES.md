# Design: Multi-Class Shares / EPS Extraction (Wave M)

Status: IMPLEMENTED 2026-07-08 (branch feat/wave-m-multiclass-shares).
Companion doc: DESIGN_SECOND_PRICE_SOURCE.md.

Implementation deltas vs the original design (things the data taught us):

- ERIE class B uses a FIXED 2400:1 conversion, not eps_ratio: Erie's
  reported two-class EPS allocates participation ~150:1 while B's market
  value follows its conversion right into 2400 A shares.
- BRK's conversion fallback is DATED: 30 (A->B) for filings before the
  2010-01-21 B 50:1 split, 1500 after -- synthetic values are as-filed
  basis and downstream .split_factor rescales them, so a static 1500
  would double-adjust 2009 vintages. Fallbacks may be function(filed).
- MOS is not single-class in its gap window: post-Cargill-split filings
  tag the listed common as mos:NoClassCommonStockMember with restricted
  Class A/B converting 1:1 (priced = "NC" via member_map).
- Up-C (GDDY, BKR) synthesizes the LISTED-PARENT count (conv 0 for the
  paired class B). See the BKR note below: parent-only is consistent
  with parent-attributable NetIncomeLoss/StockholdersEquity even though
  vendors quote whole-company caps.
- Shares are synthesized from ONE period_end per filing (the one with
  the most classes): comparative instants carry partial class sets.
  Sources compete on class coverage: the DEI cover wins unless the
  balance sheet covers more classes; balance-sheet-sourced rows carry a
  demoted MulticlassSharesOutstandingBS tag.
- Accessions missing organic fy/fp metadata are skipped, not inferred:
  calendar inference mislabels non-calendar fiscal years and a wrong FY
  label corrupts the timeseries filed_date anchor.
- drop_organic_shares is ON for all 15 names, scoped by the registry
  window (filed date): inside the window organic rows are unreliable
  total/fragment mixtures that can win pit_dedup's accession-first
  ordering via later-filing comparatives.
- fetch_and_cache_ticker self-heals a registry cache that lost its
  synthetic rows (transient EDGAR failure) instead of returning the
  poisoned parquet forever; patch_multiclass_cache refuses to trade
  previously-good synthetic rows for nothing.
- Pre-XBRL-mandate filings are permanently marked (no instance in the
  filing index, n_rows = -2) and never retried; STZ/MKC/LEN early gap
  dates that predate each filer's XBRL phase-in stay NA -- documented
  residual, unrecoverable from EDGAR structured data.
- MKC was REMOVED from the registry after the golden gate: its 2011 gap
  is unrecoverable point-in-time (H2-2011 10-Qs tag no share counts; the
  first per-class counts arrive with the 10-K filed 2012-01-27, after
  every gap date), and a wide-window recipe actively hurt (replaced good
  organic weighted-average totals with an NV-only undercount). Final
  registry: 14 CIKs.
- KMI B/C classes carry fixed ~parity fallbacks (filings tag EPS only
  for P and A); DISCA's synthetic count (A+B+C = 258.5M in 2012) is
  correct and intentionally EXCLUDES the Advance/Newhouse convertible
  preferred that inflated the legacy 405M diluted-count row.

Golden-gate outcome (2012/2018/2024-06-30, vs pre-Wave-M snapshots):

- Registry tickers light up as designed (LEN 2012 market cap fixed from
  an absurd $4.3M fragment-based value to $4.2B; BRK.B $199B/2012,
  $520B/2018, $884B/2024; V, STZ, GDDY, UA, BKR, PRU, MOS, KMI all
  populate).
- Non-registry tickers: raw values byte-identical EXCEPT one ms_*
  Mohanram component flip each at 2018 (AMP) and 2024 (PM). Expected
  knock-on: ms components binarize against finviz-sector MEDIANS, and
  the registry names joining those pools (BRK.B/V/PRU in Financial, STZ
  in Consumer Defensive) move the medians. Same class of effect as the
  z-score shifts. The regression contract for future waves should read:
  raw values identical outside the target set except sector-relative
  composites (ms_*, chinvia, herf, ww industry term).
- The pivot's FY period_end anchor for registry tickers now lands on the
  DEI cover date, matching the universe-wide convention (every normal
  ticker's anchor already was its cover date) -- this shifts lookback
  windows for registry names only, visible as small ms_* changes.

## 1. Problem

The companyfacts API (`/api/xbrl/companyfacts/`) omits every fact that
carries a dimension. Multi-class filers tag cover-page share counts
(dei:EntityCommonStockSharesOutstanding) and per-class EPS with
StatementClassOfStockAxis members, so those facts never reach our cache.
Result: shares_outstanding and eps_diluted are NA, killing market_cap and
every ratio built on it.

Verified empirically (2026-07-08):

- BRK companyfacts holds only 7 cover-share entries (2009-2011, Class A
  fragments, one value 0) and EPS ending 2013. V holds 2 cover entries
  (2009-2010) and no EPS tag at all. STZ holds neither. GDDY has a hole
  exactly over its dual-class window (EPS 2017-2023, shares 2019-2024).
- The per-filing XBRL instances DO carry everything. BRK 10-Q Q1-2026
  instance yields: cover shares A = 505,697 / B = 1,398,308,677 (instant,
  per-class members) and EPS basic A = 7,027 / B = 4.68 (ratio = the
  1500:1 conversion). B-equivalent shares x BRK-B price ~= $1.06T, which
  matches Berkshire's actual market cap.

## 2. Affected universe (census 2026-07-08, all 66 snapshots)

Audit: `market_cap` NA while `revenue_raw` present, split by whether
`shares_outstanding` is NA and whether the price cache covers the date.

SHARES_ONLY (price fine, shares NA) -- 264 ticker-dates, 14 tickers:

| ticker | n dates | window | root cause (expected) |
|--------|---------|--------------|------------------------------------|
| V | 63 | 2010-2026 | class A/B/C, floating B conversion |
| STZ | 61 | 2011-2026 | class A/B 1:1 (B retired 2022) |
| BRK.B | 58 | 2012-2026 | class A/B, 1500:1 |
| BKR | 20 | 2018-2022 | BHGE Up-C: class B non-economic |
| PRU | 12 | 2011-2013 | triage -- single class, odd tagging |
| UA | 12+3 | 2016-2019 | class A/B/C; roster prices class C |
| ERIE | 8 | 2024-2026 | class A listed, class B unlisted |
| KMI | 7 | 2012-2013 | post-IPO class A/B/P structure |
| LEN | 6 | 2011-2026 | class A/B, B ~1:1 |
| MKC | 4 | 2011 | MKC / MKC.V dual class |
| MOS | 4 | 2015 | triage -- single class, odd tagging |
| NKE | 4 | 2010-2011 | class A unlisted / B listed |
| GDDY | 3 | 2024 | Up-C; dual-class window 2015-2022 |
| PG | 2 | 2010 | triage -- early XBRL sparsity |

Plus DISCA (29 BOTH-dates 2015-2022): shares fixed here, price fixed by
the second-price-source wave.

The original backlog said "4 large caps"; the same machinery covers all
of the above, so scope = all 14 + DISCA shares-side. PG/PRU/MOS may turn
out not to be class problems -- triage step decides (fix here if the
instance carries the fact non-dimensionally, else document as
unrecoverable-sparse).

## 3. Approach

Per-filing XBRL instance extraction for a registry of multi-class CIKs,
integrated into the existing fetcher pipeline.

### 3.1 New extraction layer (fundamental_fetcher.R additions)

1. `fetch_filing_index(cik)` -- `data.sec.gov/submissions/CIK{cik}.json`
   (+ older `-submissions-001.json` pages when present) -> data.table of
   (form, accession, filed, primary_doc) for 10-K/10-Q/amendments.
2. `locate_xbrl_instance(cik, accession)` -- fetch the filing
   `index.json`; instance = `*_htm.xml` (inline-XBRL era) else the
   `{tick}-{date}.xml` schema-named file (pre-2019 era), excluding
   `_cal/_def/_lab/_pre/R*.xml/FilingSummary`.
3. `parse_instance_classes(xml_path)` -- xml2-based, namespace-year
   agnostic (match local names; dei/us-gaap namespace years vary by
   filing). Extract, per context:
   - contexts: id, instant or start/end, explicitMember list
     (dimension, member)
   - facts: EntityCommonStockSharesOutstanding,
     EarningsPerShareBasic/Diluted,
     WeightedAverageNumberOf(Diluted)SharesOutstanding,
     CommonStockSharesOutstanding/Issued
   Keep both dimensional and non-dimensional facts. Long rows:
   (cik, accession, filed, form, tag, class_member, value, period_start,
   period_end, unit).
4. Raw extraction cached at `cache/multiclass_raw/{CIK}_{ticker}.parquet`,
   chunked + resumable, so full-history instance downloads (~600 filings,
   BRK instances up to ~30MB) happen once. Rate limit 0.11s, EDGAR_UA,
   same retry pattern as .edgar_fetch().

### 3.2 Per-company recipes (.MULTICLASS_REGISTRY)

A named list keyed by CIK. Fields:

- `ticker`, `priced_class` -- the class the roster ticker trades
  (BRK.B -> ClassB, V/GDDY/STZ/LEN/ERIE -> ClassA, UA -> ClassC
  post-2016...).
- `member_map` -- regexes mapping company-specific member names to
  canonical classes (BRK uses `brka:EquivalentClassAMember` for EPS but
  `us-gaap:CommonClassAMember` for cover shares).
- `conversion` -- per non-priced class, one of:
  - `eps_ratio` (default): conv = EPS_class / EPS_priced from the same
    filing; self-consistent, handles V's floating class-B rate and BRK's
    1500:1 automatically. Guard: |EPS_priced| must exceed a floor
    (0.05) else fall back.
  - fixed numeric (STZ B = 1, LEN B = 1, fallback for BRK A = 1500).
  - `0` for non-economic classes (GDDY class B, BKR class B) -- Up-C:
    market cap and parent-attributable NetIncomeLoss are both
    priced-class-only, so this stays internally consistent.
    NOTE (verified on BKR 2019): this yields the LISTED-PARENT market
    cap ($12.6B for BHGE mid-2019), not the vendor-quoted whole-company
    figure (~$25B incl. GE's class B stake). Parent-only is the coherent
    basis here because NetIncomeLoss and StockholdersEquity in the cache
    exclude the NCI share -- P/E, P/B, E/P stay internally consistent.

Synthesized canonical rows per filing:

- `shares_outstanding` = sum over classes of cover shares x conv
  (priced-class-equivalent count; multiplying by the priced ticker's
  as-traded close gives whole-company market cap).
- `eps_basic` / `eps_diluted` = the priced class's own reported EPS
  (P/E = price_priced / EPS_priced is exact by construction).

Rows carry the real accession/filed/form/period of the source filing --
PIT semantics identical to organic rows.

### 3.3 Merge + precedence

- Synthetic rows get first-ranked synthetic tags -- prepend
  `"MulticlassSharesOutstanding"`, `"MulticlassEPSBasic"`,
  `"MulticlassEPSDiluted"` to the respective .TAG_ALIASES vectors -- so
  existing tag_rank dedup automatically prefers them over the legacy
  Class-A-fragment rows already in the cache (which produced the BRK
  absurdities: 1.6M weighted-average Class A shares against a Class B
  price).
- `build_fundamentals_cache` (rebuild path): for CIKs in the registry,
  rbind synthetic rows before dedup+write, so any cache regeneration
  reproduces them. A one-off `patch_multiclass_cache()` applies the same
  union to the existing per-ticker parquets without a full EDGAR
  re-fetch.

### 3.4 New dependency

`xml2` -- add to CLAUDE.md dependency list. No Python (rule).

## 4. Validation

- Unit: fixture mini-instance XML (checked into tests/fixtures/)
  covering inline-era and pre-2019-era shapes + one Up-C case; recipe
  math tests (BRK 1500:1, eps_ratio fallback guard, conv=0).
- Truth anchors (gate): BRK.B market cap ~$355B mid-2015 / ~$1.0-1.1T
  mid-2025; V market cap ~$140B mid-2015; GDDY P/E finite in 2024
  snapshots; STZ market cap ~$45B in 2023.
- Census regression: SHARES_ONLY ticker-dates 264 -> ~0 (minus
  documented triage residuals); zero value changes for any ticker not in
  the registry (raw snapshots byte-compared on golden dates).
- Golden-dates protocol: verify on 3 dates (2012-06-30, 2018-06-30,
  2024-06-30 -- windows covering BRK/V/STZ, BKR/UA, GDDY/ERIE) BEFORE
  the single full-history rebuild, which is shared with the price-source
  wave (one rebuild total, per THE_DISASTER_REBUILD_LESSON.md).

## 5. Risks

- Member-name drift across years (custom extension members per filer):
  registry regexes per company, triage report lists unmatched members
  loudly instead of silently dropping.
- Older filings with no dimensional cover shares at all: fall back to
  WeightedAverage per-class from the EPS note; if truly absent, document
  as residual.
- V pre-2015 class C 4:1 handling comes free via eps_ratio; verify one
  2013 filing manually during implementation.
- Z-score files shift for ALL tickers on affected dates (cross-sectional
  stats gain ~15 names) -- expected, not a regression.
