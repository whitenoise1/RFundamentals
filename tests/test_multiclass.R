# Unit tests for Wave M multi-class share/EPS extraction (fundamental_fetcher.R
# section 4b). Standalone (synthetic fixtures, no network, no cache).
# Usage: Rscript tests/test_multiclass.R
suppressMessages(suppressWarnings(source("R/fundamental_fetcher.R")))

np <- 0L; nf <- 0L
ok <- function(cond, name) {
  if (isTRUE(cond)) { np <<- np + 1L; cat(sprintf("  PASS: %s\n", name)) }
  else { nf <<- nf + 1L; cat(sprintf("  FAIL: %s\n", name)) }
}
approx <- function(a, b, tol = 1e-6) !is.na(a) && !is.na(b) && abs(a - b) < tol

cat("=== .mc_canonical_class ===\n")
ok(.mc_canonical_class("us-gaap:CommonClassAMember") == "A", "CommonClassAMember -> A")
ok(.mc_canonical_class("brka:EquivalentClassBMember") == "B", "custom EquivalentClassBMember -> B")
ok(.mc_canonical_class("kmi:CommonClassPMember") == "P", "ClassP -> P")
ok(.mc_canonical_class("x:SeriesCMember") == "C", "SeriesC -> C")
ok(is.na(.mc_canonical_class("x:SeriesAPreferredMember")), "preferred member -> NA")
ok(.mc_canonical_class("x:ClassOfStockDomain") == "ClassOfStockDomain",
   "ClassOf... not falsely matched as class O")
mm <- list("NonVoting|Nonvoting" = "NV", "Voting" = "V")
ok(.mc_canonical_class("mkc:NonVotingCommonStockMember", mm) == "NV", "member_map NonVoting first")
ok(.mc_canonical_class("mkc:VotingCommonStockMember", mm) == "V", "member_map Voting second")
ok(.mc_canonical_class("x:CommonStockMember") == "CommonStockMember", "unknown member passes through raw")

cat("\n=== parse_instance_classes (fixture XML) ===\n")
fixture_xml <- paste0(
'<?xml version="1.0" encoding="utf-8"?>
<xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:dei="http://xbrl.sec.gov/dei/2024"
      xmlns:us-gaap="http://fasb.org/us-gaap/2024"
      xmlns:seg="http://x/seg">
  <xbrli:context id="cA">
    <xbrli:entity><xbrli:identifier scheme="s">1</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonClassAMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2024-04-15</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cB">
    <xbrli:entity><xbrli:identifier scheme="s">1</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonClassBMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2024-04-15</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cDur">
    <xbrli:entity><xbrli:identifier scheme="s">1</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonClassBMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cSeg">
    <xbrli:entity><xbrli:identifier scheme="s">1</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="seg:BusinessSegmentAxis">seg:UnitXMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2024-04-15</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cPlain">
    <xbrli:entity><xbrli:identifier scheme="s">1</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <dei:EntityCommonStockSharesOutstanding contextRef="cA" unitRef="sh">1000</dei:EntityCommonStockSharesOutstanding>
  <dei:EntityCommonStockSharesOutstanding contextRef="cB" unitRef="sh">50000</dei:EntityCommonStockSharesOutstanding>
  <dei:EntityCommonStockSharesOutstanding contextRef="cSeg" unitRef="sh">999999</dei:EntityCommonStockSharesOutstanding>
  <us-gaap:EarningsPerShareBasic contextRef="cDur" unitRef="ps">2.00</us-gaap:EarningsPerShareBasic>
  <us-gaap:EarningsPerShareBasic contextRef="cPlain" unitRef="ps">2.05</us-gaap:EarningsPerShareBasic>
  <us-gaap:EarningsPerShareDiluted contextRef="cDur" unitRef="ps"></us-gaap:EarningsPerShareDiluted>
</xbrl>')
p <- parse_instance_classes(fixture_xml)
ok(is.data.table(p) && nrow(p) == 4, "4 facts kept (nil + non-class-dim dropped)")
ok(!any(p$value == 999999), "segment-dimensioned fact excluded")
ok(p[tag == "EntityCommonStockSharesOutstanding" & grepl("ClassA", member), value] == 1000,
   "class A cover count")
ok(p[tag == "EarningsPerShareBasic" & member == "", value] == 2.05,
   "non-dimensional EPS kept with empty member")
ok(p[tag == "EarningsPerShareBasic" & member != "",
     identical(c(period_start, period_end), as.Date(c("2024-01-01", "2024-03-31")))],
   "duration context parsed")
ok(is.null(parse_instance_classes("not xml at all <<<")), "garbage input -> NULL")

cat("\n=== .mc_conversions ===\n")
mk_raw <- function(tag, class, value, ps, pe)
  data.table(tag = tag, class = class, value = value,
             period_start = as.Date(ps), period_end = as.Date(pe))
sub <- rbindlist(list(
  mk_raw("EarningsPerShareBasic", "B", 3.0,  "2024-01-01", "2024-03-31"),
  mk_raw("EarningsPerShareBasic", "A", 4500, "2024-01-01", "2024-03-31"),
  mk_raw("EarningsPerShareBasic", "B", 2.0,  "2023-01-01", "2023-03-31"),
  mk_raw("EarningsPerShareBasic", "A", 3000, "2023-01-01", "2023-03-31")
))
rec_brk <- list(ticker = "T", priced = "B",
                conv = list(A = list(mode = "eps_ratio", fallback = 1500)))
cv <- .mc_conversions(sub, rec_brk)
ok(approx(cv[["A"]], 1500), "eps_ratio: median(4500/3, 3000/2) = 1500")
ok(approx(cv[["B"]], 1), "priced class conv = 1")
tiny <- rbindlist(list(
  mk_raw("EarningsPerShareBasic", "B", 0.01, "2024-01-01", "2024-03-31"),
  mk_raw("EarningsPerShareBasic", "A", 15.0, "2024-01-01", "2024-03-31")
))
cv2 <- .mc_conversions(tiny, rec_brk)
ok(approx(cv2[["A"]], 1500), "tiny priced EPS guarded -> fallback used")
rec_fixed <- list(ticker = "T", priced = "A",
                  conv = list(B = list(mode = "fixed", fallback = 0)))
ok(approx(.mc_conversions(sub, rec_fixed)[["B"]], 0), "fixed mode uses fallback directly")

cat("\n=== synthesize_multiclass_rows ===\n")
mk_full <- function(tag, member, value, ps, pe, accn = "ACC-1",
                    form = "10-Q", filed = "2024-05-01")
  data.table(tag = tag, member = member, value = value,
             period_start = as.Date(ps), period_end = as.Date(pe),
             accession = accn, form = form, filed = as.Date(filed))

# BRK-shaped: per-class cover + per-class EPS, basic only (no diluted)
raw_brk <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          1000, NA, "2024-04-15"),
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassBMember",
          50000, NA, "2024-04-15"),
  mk_full("EarningsPerShareBasic", "x:EquivalentClassAMember",
          4500, "2024-01-01", "2024-03-31"),
  mk_full("EarningsPerShareBasic", "x:EquivalentClassBMember",
          3.0, "2024-01-01", "2024-03-31")
))
org <- data.table(accession = "ACC-1", fiscal_year = 2024L, fiscal_qtr = "Q1")

reg_bak <- .MULTICLASS_REGISTRY
.MULTICLASS_REGISTRY[["9999999901"]] <- list(
  ticker = "TSTB", priced = "B",
  conv = list(A = list(mode = "eps_ratio", fallback = 1500)),
  organic_eps_class = "A")
syn <- synthesize_multiclass_rows(raw_brk, "9999999901", "TSTB", organic_dt = org)
sh <- syn[concept == "shares_outstanding"]
ok(nrow(sh) == 1 && approx(sh$value, 1000 * 1500 + 50000),
   "shares = A x eps-ratio conv + B")
ok(sh$tag == "MulticlassSharesOutstanding", "synthetic shares tag")
ok(sh$fiscal_qtr == "Q1" && sh$fiscal_year == 2024L, "fy/fp joined from organic")
eb <- syn[concept == "eps_basic"]
ed <- syn[concept == "eps_diluted"]
ok(nrow(eb) == 1 && approx(eb$value, 3.0), "eps_basic = priced class B EPS")
ok(nrow(ed) == 1 && approx(ed$value, 3.0), "eps_diluted falls back to basic when absent")

# non-dimensional EPS converted via organic_eps_class (early-BRK shape)
raw_nodim <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          1000, NA, "2024-04-15"),
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassBMember",
          50000, NA, "2024-04-15"),
  mk_full("EarningsPerShareBasic", "", 4500, "2024-01-01", "2024-03-31")
))
syn2 <- synthesize_multiclass_rows(raw_nodim, "9999999901", "TSTB", organic_dt = org)
ok(approx(syn2[concept == "eps_basic", value], 3.0),
   "non-dim class-A EPS / fallback 1500 -> priced basis")
ok(approx(syn2[concept == "shares_outstanding", value], 1000 * 1500 + 50000),
   "shares use fallback conv when no dimensional EPS pair")

# Up-C: class B non-economic
.MULTICLASS_REGISTRY[["9999999902"]] <- list(
  ticker = "TSTU", priced = "A",
  conv = list(B = list(mode = "fixed", fallback = 0)))
raw_upc <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          700, NA, "2024-04-15"),
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassBMember",
          9000, NA, "2024-04-15"),
  mk_full("EarningsPerShareBasic", "us-gaap:CommonClassAMember",
          1.5, "2024-01-01", "2024-03-31")
))
syn3 <- synthesize_multiclass_rows(raw_upc, "9999999902", "TSTU", organic_dt = org)
ok(approx(syn3[concept == "shares_outstanding", value], 700),
   "Up-C: non-economic class contributes 0")

# triage: unknown single member counted via default_conv = 1
.MULTICLASS_REGISTRY[["9999999903"]] <- list(
  ticker = "TSTT", priced = "A", conv = list(), default_conv = 1)
raw_triage <- mk_full("EntityCommonStockSharesOutstanding",
                      "x:CommonStockMember", 1234, NA, "2024-04-15")
syn4 <- suppressWarnings(
  synthesize_multiclass_rows(raw_triage, "9999999903", "TSTT", organic_dt = org))
ok(approx(syn4[concept == "shares_outstanding", value], 1234),
   "triage default_conv counts unknown member")

# duplicated (class, period_end) cover rows must not double-sum
raw_dup <- rbindlist(list(raw_upc, raw_upc[1]))
syn5 <- synthesize_multiclass_rows(raw_dup, "9999999902", "TSTU", organic_dt = org)
ok(approx(syn5[concept == "shares_outstanding", value], 700),
   "restated duplicate cover row not double-counted")

# partial comparative instant: the period_end with the most classes wins
raw_cmp <- rbindlist(list(
  raw_brk,
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          990, NA, "2024-06-01")   # later one-class instant must not win
))
syn6 <- synthesize_multiclass_rows(raw_cmp, "9999999901", "TSTB", organic_dt = org)
ok(nrow(syn6[concept == "shares_outstanding"]) == 1 &&
     syn6[concept == "shares_outstanding", period_end] == as.Date("2024-04-15"),
   "max-classes period_end beats later partial instant")

# NA conversion at the chosen date -> no shares row (NA beats undercount)
reg_bak_na <- .MULTICLASS_REGISTRY
.MULTICLASS_REGISTRY[["9999999905"]] <- list(
  ticker = "TSTN", priced = "A",
  conv = list(B = list(mode = "eps_ratio", fallback = NA_real_)))
raw_na <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          700, NA, "2024-04-15"),
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassBMember",
          300, NA, "2024-04-15")
))
syn7 <- suppressWarnings(
  synthesize_multiclass_rows(raw_na, "9999999905", "TSTN", organic_dt = org))
ok(is.null(syn7) || nrow(syn7[concept == "shares_outstanding"]) == 0,
   "underivable class conversion -> no shares row emitted")
.MULTICLASS_REGISTRY <- reg_bak_na

# umbrella member alongside a known class must not double-count (triage)
.MULTICLASS_REGISTRY[["9999999903"]] <- list(
  ticker = "TSTT", priced = "A", conv = list(), default_conv = 1)
raw_umb <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          500, NA, "2024-04-15"),
  mk_full("EntityCommonStockSharesOutstanding", "x:CommonStockMember",
          500, NA, "2024-04-15")
))
syn8 <- suppressWarnings(
  synthesize_multiclass_rows(raw_umb, "9999999903", "TSTT", organic_dt = org))
ok(approx(syn8[concept == "shares_outstanding", value], 500),
   "known class beats umbrella member at same date (no double count)")

# balance-sheet-sourced counts carry the demoted BS tag
raw_bs <- rbindlist(list(
  mk_full("CommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          700, NA, "2024-03-31"),
  mk_full("EarningsPerShareBasic", "us-gaap:CommonClassAMember",
          1.5, "2024-01-01", "2024-03-31")
))
syn9 <- synthesize_multiclass_rows(raw_bs, "9999999902", "TSTU", organic_dt = org)
ok(syn9[concept == "shares_outstanding", tag] == "MulticlassSharesOutstandingBS",
   "balance-sheet source gets demoted BS tag")

# non-dim cover trusted only for triage (conv = list()); strict entries skip
raw_nodim_cover <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "", 1.6e6, NA, "2024-04-15"),
  mk_full("EarningsPerShareBasic", "", 4500, "2024-01-01", "2024-03-31")
))
syn10 <- synthesize_multiclass_rows(raw_nodim_cover, "9999999901", "TSTB",
                                    organic_dt = org)
ok(is.null(syn10) || nrow(syn10[concept == "shares_outstanding"]) == 0,
   "undimensioned cover count skipped for true multi-class recipe")
syn11 <- suppressWarnings(
  synthesize_multiclass_rows(raw_nodim_cover, "9999999903", "TSTT",
                             organic_dt = org))
ok(approx(syn11[concept == "shares_outstanding", value], 1.6e6),
   "undimensioned cover count trusted for triage recipe")

# dated fallback: conversion resolved by filing date (BRK 30 pre-2010 split)
.MULTICLASS_REGISTRY[["9999999906"]] <- list(
  ticker = "TSTF", priced = "B",
  conv = list(A = list(mode = "eps_ratio",
                       fallback = function(filed)
                         if (!is.na(filed) && filed < as.Date("2010-01-21")) 30 else 1500)))
raw_old <- rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          1000, NA, "2009-09-30", accn = "ACC-2", filed = "2009-11-06"),
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassBMember",
          50000, NA, "2009-09-30", accn = "ACC-2", filed = "2009-11-06")
))
org2 <- data.table(accession = "ACC-2", fiscal_year = 2009L, fiscal_qtr = "Q3")
syn12 <- synthesize_multiclass_rows(raw_old, "9999999906", "TSTF", organic_dt = org2)
ok(approx(syn12[concept == "shares_outstanding", value], 1000 * 30 + 50000),
   "dated fallback: pre-split filing uses as-filed conversion 30")

# accession without organic fy/fp metadata is skipped, not mislabeled
raw_two <- rbindlist(list(raw_upc,
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          800, NA, "2024-07-15", accn = "ACC-NOMETA")))
syn13 <- suppressWarnings(
  synthesize_multiclass_rows(raw_two, "9999999902", "TSTU", organic_dt = org))
ok(!any(syn13$accession == "ACC-NOMETA"),
   "accession lacking organic fy/fp skipped")
ok(!anyNA(syn13$fiscal_qtr), "no NA fiscal labels in output")

# basic must not masquerade as diluted when organic diluted exists
org_dil <- rbindlist(list(org,
  data.table(accession = "ACC-1", fiscal_year = 2024L, fiscal_qtr = "Q1")))
org_dil <- data.table(accession = "ACC-1", fiscal_year = 2024L,
                      fiscal_qtr = "Q1", concept = "eps_diluted")
syn14 <- synthesize_multiclass_rows(raw_upc, "9999999902", "TSTU",
                                    organic_dt = org_dil)
ok(nrow(syn14[concept == "eps_diluted"]) == 0,
   "basic-as-diluted suppressed when filing has organic diluted EPS")
ok(nrow(syn14[concept == "eps_basic"]) == 1,
   "eps_basic still synthesized alongside organic diluted")

.MULTICLASS_REGISTRY <- reg_bak

cat("\n=== dedup precedence of synthetic tags ===\n")
mk_cache <- function(concept, tag, value, pe, accn, filed, fq = "FY")
  data.table(ticker = "TST", cik = "9999999901", concept = concept, tag = tag,
             value = value, period_end = as.Date(pe),
             period_start = as.Date(NA), filed = as.Date(filed),
             form = "10-K", accession = accn, fiscal_year = 2023L,
             fiscal_qtr = fq, unit = "shares")
both <- rbindlist(list(
  mk_cache("shares_outstanding", "EntityCommonStockSharesOutstanding",
           111, "2024-02-15", "ACC-9", "2024-03-01"),
  mk_cache("shares_outstanding", "MulticlassSharesOutstanding",
           76e6, "2024-02-15", "ACC-9", "2024-03-01")
))
dd <- dedup_fundamentals(both)
ok(nrow(dd) == 1 && dd$tag == "MulticlassSharesOutstanding",
   "dedup_fundamentals: synthetic outranks organic within accession")
pd <- pit_dedup(both, as_of = "2024-06-30")
ok(nrow(pd) == 1 && pd$tag == "MulticlassSharesOutstanding",
   "pit_dedup: synthetic outranks organic within accession")
ranks <- .add_dedup_ranks(both)
ok(all(ranks[tag == "MulticlassSharesOutstanding", tag_rank] == 1L),
   "Multiclass tag is rank 1 in alias map")

cat("\n=== .merge_multiclass drop flags ===\n")
reg_bak2 <- .MULTICLASS_REGISTRY
.MULTICLASS_REGISTRY[["9999999904"]] <- list(
  ticker = "TSTD", priced = "B",
  conv = list(A = list(mode = "fixed", fallback = 1500)),
  drop_organic_shares = TRUE, drop_organic_eps = TRUE)
cache_dt <- rbindlist(list(
  mk_cache("shares_outstanding", "WeightedAverageNumberOfDilutedSharesOutstanding",
           1.6e6, "2013-12-31", "ACC-0", "2014-03-01"),
  mk_cache("total_assets", "Assets", 5e11, "2013-12-31", "ACC-0", "2014-03-01"),
  mk_cache("eps_diluted", "EarningsPerShareDiluted",
           2950, "2013-12-31", "ACC-0", "2014-03-01")
))

# synthesis failure (stubbed NULL): organic rows must be KEPT, not gutted
build_multiclass_raw <- function(...) NULL
merged0 <- suppressWarnings(.merge_multiclass(cache_dt, "TSTD", "9999999904"))
ok(any(merged0$concept == "shares_outstanding") &&
     any(merged0$concept == "eps_diluted"),
   "failed synthesis leaves organic rows untouched")

# successful synthesis: drops applied, synthetic rows appended
build_multiclass_raw <- function(...) rbindlist(list(
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassAMember",
          1000, NA, "2013-12-31", accn = "ACC-0", form = "10-K",
          filed = "2014-03-01"),
  mk_full("EntityCommonStockSharesOutstanding", "us-gaap:CommonClassBMember",
          50000, NA, "2013-12-31", accn = "ACC-0", form = "10-K",
          filed = "2014-03-01")
))
merged <- suppressWarnings(.merge_multiclass(cache_dt, "TSTD", "9999999904"))
ok(!any(merged$concept == "shares_outstanding" & !grepl("^Multiclass", merged$tag)),
   "drop_organic_shares removes legacy share rows")
ok(!any(merged$concept == "eps_diluted"),
   "drop_organic_eps removes legacy eps rows")
ok(any(merged$concept == "total_assets"),
   "unrelated concepts untouched")
ok(approx(merged[concept == "shares_outstanding" & grepl("^Multiclass", tag), value],
          1000 * 1500 + 50000),
   "synthetic shares row appended on success")
rm(build_multiclass_raw)
.MULTICLASS_REGISTRY <- reg_bak2

cat(sprintf("\n%d passed, %d failed\n", np, nf))
if (nf > 0) stop("test_multiclass: failures")
