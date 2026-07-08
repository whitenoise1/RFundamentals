# The Disaster-Rebuild Lesson

A postmortem of the 2026-07-05 to 2026-07-07 clean-room verification and
price-basis repair session: what it found, what it cost, and the one
process change that would have collapsed days into hours.

## The lesson (read this even if you read nothing else)

**The single biggest time sink was rebuild-after-fix cycles.** Every fix
was verified by regenerating the 66-date snapshot history (1-3 hours per
pass on the Drive-mounted cache), and every regeneration's verification
surfaced the *next* defect -- which invalidated the rebuild that had just
finished. Five full or partial history rebuilds were run; most were
thrown away.

**The improvement: keep a small golden-date subset (3 dates) as the fast
verification loop, and rebuild the full history exactly once, at the very
end.**

Concretely:

1. Pick 3 golden dates spanning the history: `2015-06-30`, `2020-06-30`,
   `2024-06-30` (early/middle/recent; one assembly each, ~8 min).
2. Iterate: fix -> assemble golden dates -> run the audit
   (`fundamentals-present but market_cap-NA`), the truth anchors
   (AAPL/NVDA/META/BRK.B), and `validate_snapshot` -> find the next
   defect -> fix again.
3. Only when the golden loop is clean AND stable across two consecutive
   passes, rebuild all 66 dates once and re-verify.

Had this session done that, the basis fix, dot-ticker fix, constituent
relabels, validator rework, and early-history exemption would all have
been batched into a single terminal rebuild instead of five.

## What the session set out to do

A clean-room verification of the 130-indicator database after the Wave 5
merge: move the cache aside, rebuild every layer from primary sources
(EDGAR, Yahoo, finviz), run every unit suite, integration gate, and
end-to-end build, and cross-check cell-by-cell against the verified
baseline.

## What it found (five silent, years-old defect classes)

The clean-room did its job precisely by failing informatively. All five
had been passing every existing test.

1. **finviz page redesign** broke the sector scraper entirely (0/829) --
   the industry link text moved inside a `<span>`. Fixed in
   `.finviz_link_text`.
2. **Adjusted-close basis defect** (the big one): price-sensitive
   indicators used Yahoo's `Adjusted` column -- retroactively divided by
   all *future* splits and dividends -- against as-reported fundamentals.
   NVDA's 2015 P/E read **0.49** (true ~20) because its 2021/2024 splits
   leaked 40x into 2015; every no-split stock was understated by its
   future dividend factor. Cross-sectional ranks of 25 indicators were
   corrupted on all historical dates, and values drifted on every
   re-download. Fixed: as-traded close = split-adjusted `Close` divided
   by the post-date split factor (`.split_factor`, `cache/splits`).
3. **Dot-ticker 404s**: `getSymbols("BRK.B")` fails (Yahoo wants
   `BRK-B`), so BRK.B and BF.B had **no price data in any snapshot since
   2010**. Fixed at every Yahoo call site.
4. **Untracked ticker renames**: eleven constituents (FB->META,
   ANTM->ELV, BLL->BALL, FLT->CPAY, NLOK->GEN, PEAK->DOC, PKI->RVTY,
   RE->EG, WLTW->WTW, ABC->COR, MMC->MRSH) kept their dead symbols in the
   roster. Yahoo purges or reuses old symbols -- Meta had an ETF's sector
   and zero prices since 2022. Fixed by relabeling to current tickers
   (identity verified per-CIK via EDGAR submissions; Yahoo carries full
   pre-rename history under the new symbol).
5. **Multi-class share companies** (BRK.B, V, GDDY, STZ): prices now
   arrive correctly, but `shares_outstanding`/`eps_diluted` do not
   populate from multi-class XBRL contexts, so their price ratios remain
   NA. Documented as a future fetcher wave, not fixed here.

Plus test-infrastructure decay fixed along the way: stale integration
gates asserting pre-vintage-cache invariants; a z-score validator
asserting mean/sd on median/MAD-standardized data (permanently red on
~25 skewed indicators -- replaced with the exact construction invariants
median(z)=0, mad(z)=1); an indicator-count pin (exactly 130) so a
shrunken set cannot pass; truth-anchor tests against external facts.

## What the verification now proves

- **Reproducibility**: 641/641 fundamentals files byte-identical from a
  fresh EDGAR fetch; zero CIK diffs; sector values identical on all
  shared tickers.
- **Correctness against external truth**: anchors on dates no internal
  consistency test could check (AAPL/NVDA 2015, META/BRK.B/MRSH/ELV
  2024).
- **Wave isolation**: every wave boundary 0->1->2->3->4->5 diffed on
  identical inputs; every diff attributes to a documented intended
  change (W3->W4: zero diffs; W2->W3: only revenue_growth_qoq, matching
  the recorded 489/498 cell count).
- **All green**: 9 unit suites, 5 integration gates, 66/66 dates pass
  `validate_snapshot`.
- **Bounded residuals**: the steady-state audit on 2024-06-30 is exactly
  17 fundamentals-but-no-price names = 13 Yahoo-purged delistings + 4
  multi-class-share companies. Anything beyond that set is a regression.

## The cost accounting

Roughly two days wall-clock. Breakdown of avoidable vs. unavoidable:

- **Unavoidable machine time**: one full EDGAR re-fetch (~1h), one full
  price/timeseries build (~1h), ONE terminal 66-date rebuild (~3h).
- **Avoidable machine time**: ~4 extra full/partial history rebuilds
  invalidated by subsequently-discovered defects (the core lesson).
- **Agent errors** (~2-3h of rework): a regression harness that omitted
  `ttm_eps.R` (spurious diffs; silver lining -- the silent degradation it
  exposed is now a loud warning); a sector-lookup prune on a wrong
  predicate (index-removal is not delisting) that deleted 129 valid rows
  and needed a re-scrape; resumable-rebuild semantics leaving 4 stale
  spread dates; a chain-regression script whose output path was
  clobbered by sourcing old module trees (five 9-minute assemblies
  computed and not written); assorted small churn (CSV line endings, a
  tautological CIK check).
- **Environment**: Google Drive File Provider is a hostile home for a
  1 GB, ~3,000-file parquet store -- slow sequential reads dominated
  every rebuild and its placeholder hydration caused one diagnostic
  detour.

## Operating rules distilled

1. **Golden dates first, full rebuild last.** (The headline lesson.)
2. **Anchor to external truth.** Internal-consistency tests cannot catch
   basis errors; a handful of "AAPL 2015 market cap ~ $725B" assertions
   would have caught the adjusted-close defect years earlier.
3. **Never gate correctness on silent existence checks.** The
   `exists("load_ticker_splits")` gating produced plausible-but-wrong
   outputs when a module was not sourced; warn loudly instead.
4. **After any Yahoo re-download, run the price-coverage audit**
   (`fundamentals-present but market_cap-NA` on a recent date) -- it
   surfaces every price-coverage regression class at once.
5. **A permanently-red check is worse than no check.** Rewrite stale
   assertions to the current design's invariants instead of tolerating
   or ignoring them.
6. **Index removal != delisting != ticker death.** Rosters, price
   vendors, and scrapers each have their own notion of "gone"; verify
   identity per-CIK via EDGAR before touching roster data.

## Artifacts

Nine commits on `fix/finviz-markup-and-stale-gates` (see git log
8b61e9f..24bdd69), this document, and the regenerated 66-date history in
`cache/snapshots`.
