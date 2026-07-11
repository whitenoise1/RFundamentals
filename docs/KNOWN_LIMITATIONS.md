# KNOWN_LIMITATIONS.md -- Accepted Approximations & Future Issues

Durable register of **deliberately accepted** deviations from the ideal, each
with the decision, its rationale, its blast radius, and the exit criteria that
would close it. An entry here is a conscious trade, not an unknown bug.

Rule: nothing lands here without (a) a stated impact, (b) an exit criterion,
and (c) a bounding measurement or a plan to obtain one.

Status legend: OPEN (accepted, live) | MEASURING | CLOSED

---

## L1 -- Historical sector/industry is a current-value approximation

**Status:** OPEN (accepted 2026-07-08)
**Severity:** LOW for coarse sector | MEDIUM for industry-level indicators
**Owner doc:** docs/DESIGN_DAILY_UPDATE.md (section 3, "Residual PIT ledger")

### The decision

We apply **today's best-estimate sector/industry to all historical dates**, and
accept the resulting mild look-ahead in cross-sectional grouping, rather than
block the database on acquiring a dated sector history.

Going forward this stops accumulating: from the moment the dated dimension
(Fix D2/B, `valid_from`) is live, every new observation is stamped with the
sector that was current *at that time*. The approximation is therefore
**frozen in extent** -- it covers only dates prior to the dating cutover and
never grows.

### Why (rationale)

Strict PIT sector requires a *dated* classification history that also covers
delisted names. The only real sources are licensed (WRDS/Compustat historical
GICS with effective dates, Bloomberg PIT GICS, Refinitiv/FactSet). None is
available to this project. The free/cheap sources we do have -- SEC SIC (from
the `submissions/CIK.json` call we already make) and Sharadar -- provide
**coverage, not history**: they carry a single current value per ticker.

Accepting the approximation was chosen over three alternatives:

- *Do nothing:* rejected. Wave P pulls ~100 delisted names into the daily
  universe with no finviz sector; they would pool into an "Unknown" bucket and
  actively degrade every other name's sector-relative indicators, defeating the
  survivorship-bias reduction Tiingo coverage is meant to deliver.
- *Buy a dated source (option "C"):* rejected for now -- unavailable; would also
  force a taxonomy remap of `.SECTOR_NA_INDICATORS`, Mohanram sector-medians,
  and Herfindahl-by-industry, plus a full re-verify.
- *Block the daily-update work until sector is exact:* rejected -- the sector
  residual is small relative to the two real defects (D1 market cap, D2 look-
  ahead) that the update work fixes outright.

### What is affected

Sector/industry enters the pipeline as a **grouping key**, so the exposure is to
cross-sectional statistics, not to any single firm's raw fundamentals:

- Cross-sectional z-scoring (grouped by sector)
- Mohanram G-score binarization (vs sector medians)
- Herfindahl sales concentration (per finviz **industry**) -- most exposed, as
  industry buckets are ~150 and small-N, so one misassignment moves the statistic
- ChInvIA (sector-demeaned at cross-section stage)
- The sector NA-mask (`.SECTOR_NA_INDICATORS`): a firm reclassified across the
  `Financial` / `Utilities` / `Real Estate` boundary would have historical rows
  masked (or not masked) according to its *present* sector. Rare, but note it is
  a value change, not merely a grouping shift.

### What is NOT affected

Everything else is strictly PIT after fixes D1 and D2: fundamentals timing
(filed-date stamping, vintage resolution), price basis (as-traded reconstruction),
market cap and all per-share ratios, and constituent membership.

### Direction and expected magnitude

A name reclassified since 2010 is grouped, on past dates, by a sector it had not
yet been assigned -- i.e. future information leaks into the *group statistics*.
With 11 coarse sectors and large N per bucket, the effect on sector medians and
z-scores is expected to be small. The finer industry dimension driving Herfindahl
is the more exposed piece and is simultaneously the hardest to source.

Additionally, delisted names filled from **SIC carry a sector but a weak or absent
industry**, so Herfindahl coverage for those names stays thin (Sharadar's industry
is better, where a token exists).

### Bounding measurement (required, not optional)

We do not currently know the reclassification rate. Two cheap probes bound it:

1. **SIC-vs-finviz disagreement probe.** Compare the SIC-derived sector against
   the finviz sector across the ~600 current names. The disagreement rate is a
   proxy for how ambiguous/unstable sector assignment is for this universe.
2. **Observed drift, once D2/B accrues.** After the dated dimension is live,
   count actual sector/industry changes per year. This directly measures what the
   historical approximation is silently assuming away.

Neither probe requires a rebuild. Probe 1 should run before the single full
rebuild; probe 2 reports annually.

**Probe 1 result (2026-07-11, `tools/probe_sic_vs_finviz.R`):** across 495
mapped current constituents, SIC-derived sector agrees with finviz for 374
(75.6%) and disagrees for 121 (24.4%). The disagreements concentrate on
structural taxonomy boundaries (consumer-vs-industrial suppliers, healthcare
distributors/IT, exchange-listed financial data firms), not on instability --
i.e. SIC is a coarse *fallback taxonomy*, roughly 1-in-4 names would be
grouped differently under it. Consequences: (a) delisted names whose sector
came from SIC-family reasoning carry that ambiguity into sector-relative
indicators; (b) the probe does NOT measure time-drift, so probe 2 (dated
dimension) remains the exit-criterion measurement. Raw table:
`cache/lookups/sic_finviz_probe.parquet`.

### Exit criteria (what would close L1)

Close L1 only if BOTH:

- A dated sector/industry history covering delisted names becomes available
  (WRDS/Compustat `co_hgic` GICS history, Bloomberg PIT, or equivalent), **and**
- The bounding measurement shows the approximation materially moves factor
  exposures (i.e. the reclassification rate is high enough to matter).

Closing L1 then requires: ingest a dated `{ticker|cik, valid_from, sector,
industry}` table, remap `.SECTOR_NA_INDICATORS` / Mohanram / Herfindahl to the new
taxonomy, backfill, and re-verify the full database once.

If a dated source appears but probe results show negligible drift, the correct
action is to record that finding here and **leave L1 open by choice** -- not to
spend a full re-verify on a non-effect.

---

## L2 -- FRC (First Republic Bank) has no recoverable fundamentals

**Status:** OPEN (accepted 2026-07-09, old-CIK wave Tier 1)
**Severity:** LOW (one ticker, 17 member dates, 2019-01-02 .. 2023-05-04)

### The decision

FRC stays an all-NA column on its 17 member dates. Its sector row
(Financial / Banks - Regional, `.SECTOR_OVERRIDES`) is present so the
metadata header is complete, but no indicator can be computed.

### Why (rationale)

First Republic Bank operated **without a holding company**: as a
state-chartered bank it filed its 10-K/10-Q with the **FDIC** under
Exchange Act 12(i), not with the SEC. Its EDGAR CIK (0001132979) carries
only third-party SC 13G filings and one 40-6B/A -- there is no XBRL
companyfacts payload to fetch (hard 404). The only EDGAR 10-K filers
named "First Republic" are its preferred-capital REIT subsidiaries,
whose financials are not the bank's. The scoping census marked FRC
RECOVERABLE from CIK presence alone; the probe above corrects that.

FDIC call reports (FFIEC CDR) do contain the bank's financials but are a
different format, taxonomy, and pipeline -- out of scope for the
EDGAR-XBRL machinery by design.

### Price footnote (for a future exit)

Tiingo carries the full price history 2010-12-09 .. present under
**FRCB** (the post-failure OTC symbol). Do NOT wire FRC -> FRCB into
`.PROVIDER_SYMBOL_MAP`: Yahoo also serves FRCB but only the post-2023
OTC pennies, and the Yahoo attempt precedes the Tiingo fallback, so the
mapping would poison the price cache with a useless series. Prices are
worthless without fundamentals anyway (every indicator needs shares or
statement data).

### Exit criteria

An FDIC/FFIEC call-report ingestion path, or a licensed fundamentals
source covering FDIC-only filers. If one lands: fetch fundamentals, then
fetch prices via a direct Tiingo pull under FRCB (bypassing the provider
map), then rebuild FRC's 17 dates.

---

## L3 -- SBNY (Signature Bank) has no recoverable fundamentals

**Status:** OPEN (accepted 2026-07-11, old-CIK wave Tier 2)
**Severity:** LOW (one ticker, 6 member dates, 2021-12-20 .. 2023-03-15)

Same class as L2/FRC: Signature Bank was a New York state-chartered bank
with **no holding company**, filing its 10-K/10-Q with the **FDIC** under
Exchange Act 12(i). Its EDGAR CIK (1288784, "Signature Bank Corp")
carries only third-party SC 13G/13D filings -- zero 10-K/10-Q forms, no
companyfacts payload. Verified 2026-07-11 during Tier 2 CIK resolution
(`tools/oldcik_resolve_ciks.R` marked it UNRECOVERABLE_FDIC). Exit
criteria identical to L2.

---

## L4 -- Old-CIK wave price-unrecoverable windows

**Status:** OPEN (accepted 2026-07-11, old-CIK wave Tier 2)
**Severity:** LOW-MEDIUM (fundamentals present, market-cap/price ratios NA)

### The decision

Tier 2 recovered FUNDAMENTALS for ~147 delisted constituents, but a
subset of their membership windows has no recoverable daily price series
from either provider: Yahoo purges delisted symbols (and REUSES some for
unrelated listings -- see the `.YAHOO_REUSED_BLOCKLIST` in
pit_assembler.R, added after Yahoo's "COL" served a $0.15 penny stock
across Rockwell Collins' window), and Tiingo's delisted-chain backfill
for many dead names only reaches back to 2016-01-04.

These names keep their fundamentals columns (balance-sheet ratios,
margins, accruals, growth) on their member dates; only price-dependent
indicators (market cap, valuation ratios, price-scaled composites) stay
NA. This enlarges the fundamentals-present-but-market-cap-NA census
relative to the pre-Tier-2 ledger BY DESIGN -- those rows previously had
NO data at all.

### The ledger (window with no or partial price coverage; final 2026-07-11)

- **No provider coverage** (window fully dark, 31 names): ALTR, APOL,
  BIG, BMC, CA, CBE, CEPH, DF, DNB, DO, DTV, EMC, ENDP, ESV, FTR, GR,
  IGT, JCP, LXK, MNK, MOLX, MON, NFX, NSM, NYX, PGN, SHLD, SPLS, TLAB,
  WFR, WIN (+ carried over from Wave P: BTU and INFO old-entity windows,
  WRK). Tiingo's metadata advertises chains for several of these (NYX
  1996-2013) but its /prices payload for names delisted before ~2016 is
  empty -- the metadata is not the data.
- **Tail-only coverage** (Tiingo's 2016-01-04 delisted backfill floor,
  or Yahoo's residual series): HAR, LLTC, CVC, PCP, BRCM (2016+ only),
  HOT (2015+ only) -- earlier member dates stay price-NA.
- Yahoo-reused symbols recovered via the Tiingo chain instead
  (`.YAHOO_REUSED_BLOCKLIST`): COL, CAM, PCL, PCLN, PCS.
- Recovered IN FULL for the grid despite earlier windows: YHOO, LO
  (caches 2009+, windows start pre-grid -- all in-grid member dates
  priced).

### Exit criteria

A price source with pre-2016 delisted coverage (CRSP, Sharadar SEP,
Norgate). If one lands: fetch the listed windows, warm splits, and
regenerate only the affected ticker-dates.

---
