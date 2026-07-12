# Unit tests for old-CIK wave Tier 1: shared-CIK cache aliasing
# (get_fundamentals CIK-prefix fallback, ttm_eps .FUND_CACHE_ALIASES,
# NWS/FOX multiclass registry entries, static sector rows).
# Standalone, no network. Usage: Rscript tests/test_oldcik_aliases.R
suppressWarnings(suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/sector_classifier.R")
  source("R/ttm_eps.R")
}))

np <- 0L; nf <- 0L
ok <- function(cond, name) {
  if (isTRUE(cond)) { np <<- np + 1L; cat(sprintf("  PASS: %s\n", name)) }
  else { nf <<- nf + 1L; cat(sprintf("  FAIL: %s\n", name)) }
}

cat("=== get_fundamentals: CIK-prefix alias fallback (synthetic) ===\n")
td <- file.path(tempdir(), "fund_alias_test")
dir.create(td, showWarnings = FALSE)
fake <- data.table(
  ticker = "AAA", cik = "0000000001", concept = "revenue",
  tag = "Revenues", value = 100, period_end = as.Date("2020-12-31"),
  period_start = as.Date("2020-01-01"), filed = as.Date("2021-02-01"),
  form = "10-K", accession = "acc-1", fiscal_year = 2020L,
  fiscal_qtr = 4L, unit = "USD", period_type = "FY")
arrow::write_parquet(fake, file.path(td, "0000000001_AAA.parquet"))

r_exact <- get_fundamentals("AAA", "0000000001", cache_dir = td, vintages = TRUE)
ok(!is.null(r_exact) && nrow(r_exact) == 1, "exact {cik}_{ticker} path still hits")
r_alias <- get_fundamentals("BBB", "0000000001", cache_dir = td, vintages = TRUE)
ok(!is.null(r_alias) && nrow(r_alias) == 1, "sibling ticker resolves via CIK prefix")
r_miss <- get_fundamentals("CCC", "0000000002", cache_dir = td, vintages = TRUE)
ok(is.null(r_miss), "unknown CIK still returns NULL")
r_nocik <- get_fundamentals("AAA", cache_dir = td, vintages = TRUE)
ok(!is.null(r_nocik) && nrow(r_nocik) == 1, "NULL-cik ticker glob unchanged")
unlink(td, recursive = TRUE)

cat("\n=== get_fundamentals: real shared-CIK pairs ===\n")
pairs <- list(
  DISCK = "0001437107", WBD = "0001437107", UAA = "0001336917",
  NWSA = "0001564708", FOXA = "0001754301")
for (tk in names(pairs)) {
  r <- get_fundamentals(tk, pairs[[tk]], vintages = TRUE)
  ok(!is.null(r) && nrow(r) > 100, sprintf("%s serves from CIK %s", tk, pairs[[tk]]))
}

cat("\n=== .FUND_CACHE_ALIASES consistency ===\n")
master <- as.data.table(arrow::read_parquet("cache/lookups/constituent_master.parquet"))
for (al in names(.FUND_CACHE_ALIASES)) {
  tgt <- .FUND_CACHE_ALIASES[[al]]
  cik_al  <- master[ticker == al,  cik][1]
  cik_tgt <- master[ticker == tgt, cik][1]
  ok(!is.na(cik_al) && identical(cik_al, cik_tgt),
     sprintf("%s and %s share a CIK in the master", al, tgt))
  ok(file.exists(file.path("cache/fundamentals",
                           sprintf("%s_%s.parquet", cik_tgt, tgt))),
     sprintf("target cache %s_%s exists", cik_tgt, tgt))
  ok(length(list.files("cache/fundamentals",
                       pattern = sprintf("^[0-9]+_%s\\.parquet$", al))) == 0,
     sprintf("alias %s has no cache file of its own", al))
}

cat("\n=== multiclass registry: NWS + FOX entries ===\n")
ok(length(.MULTICLASS_REGISTRY) == 16, "registry has 16 entries")
nws <- .MULTICLASS_REGISTRY[["0001564708"]]
ok(!is.null(nws) && nws$ticker == "NWS" && nws$priced == "B" &&
     identical(names(nws$conv), "A"), "News Corp entry: NWS priced B, conv A")
fox <- .MULTICLASS_REGISTRY[["0001754301"]]
ok(!is.null(fox) && fox$ticker == "FOX" && fox$priced == "B" &&
     identical(names(fox$conv), "A"), "Fox entry: FOX priced B, conv A")

cat("\n=== synthesized shares: PIT truth anchors ===\n")
# News Corp: ~580.6M total shares on the cover filed 2015-08-13
r <- get_fundamentals("NWSA", "0001564708", as_of = "2015-09-30")
sh <- r[tag == "MulticlassSharesOutstanding"][which.max(period_end), value]
ok(length(sh) == 1 && sh > 550e6 && sh < 620e6,
   sprintf("News Corp shares as of 2015-09-30 in range (%.1fM)", sh / 1e6))
# Fox: ~603.7M total shares on the cover filed 2020-05-07
r <- get_fundamentals("FOXA", "0001754301", as_of = "2020-06-30")
sh <- r[tag == "MulticlassSharesOutstanding"][which.max(period_end), value]
ok(length(sh) == 1 && sh > 580e6 && sh < 630e6,
   sprintf("Fox shares as of 2020-06-30 in range (%.1fM)", sh / 1e6))
# organic garbage dropped: FOX registration placeholder (value 1), NWS 2013
# fragments
r <- get_fundamentals("FOX", "0001754301", vintages = TRUE)
ok(nrow(r[concept == "shares_outstanding" & !grepl("^Multiclass", tag)]) == 0,
   "FOX organic share rows dropped (incl. value-1 placeholder)")
r <- get_fundamentals("NWS", "0001564708", vintages = TRUE)
ok(nrow(r[concept == "shares_outstanding" & !grepl("^Multiclass", tag)]) == 0,
   "NWS organic share rows dropped")

cat("\n=== static sector rows ===\n")
si <- as.data.table(arrow::read_parquet("cache/lookups/sector_industry.parquet"))
for (tk in c("DISCA", "DISCK", "FRC")) {
  ok(tk %in% si$ticker && si[ticker == tk, source] == "override",
     sprintf("%s has an override sector row", tk))
}
ok(si[ticker == "FRC", sector] == "Financial", "FRC sector Financial")
ok(si[ticker == "DISCK", industry] == "Entertainment", "DISCK industry Entertainment")

cat("\n=== split caches warm for Tier 1 tickers ===\n")
# assemble_snapshot loads splits with fetch = FALSE (no network in the
# snapshot loop), so a missing splits cache silently yields factor 1 and
# breaks the as-traded price reconstruction: UAA priced HALF its true
# market cap until its 2016-04-08 class-C distribution event was cached.
for (tk in c("UAA", "NWSA", "FOXA", "WBD", "DISCK", "NWS", "FOX")) {
  ok(file.exists(sprintf("cache/splits/%s.parquet", tk)),
     sprintf("splits cache exists for %s", tk))
}
ua_sp <- as.data.table(arrow::read_parquet("cache/splits/UAA.parquet"))
ok(nrow(ua_sp[as.Date(ex_date) == as.Date("2016-04-08") & abs(ratio - 0.5) < 1e-9]) == 1,
   "UAA splits include the 2016-04-08 class-C distribution (ratio 0.5)")

cat("\n=== augment_daily_ttm: alias serves DISCK daily (sandbox) ===\n")
fund_td <- file.path(tempdir(), "fund_ttm_test")
ts_td   <- file.path(tempdir(), "ts_ttm_test")
dir.create(fund_td, showWarnings = FALSE); dir.create(ts_td, showWarnings = FALSE)
invisible(file.copy("cache/fundamentals/0001437107_DISCA.parquet",
                    file.path(fund_td, "0001437107_DISCA.parquet"),
                    overwrite = TRUE))
daily <- data.table(date = as.Date("2018-06-27") + 0:2,
                    price = c(25.0, 25.5, 26.0))
arrow::write_parquet(daily, file.path(ts_td, "DISCK_daily.parquet"))
res <- suppressWarnings(augment_daily_ttm(
  fund_dir = fund_td, ts_dir = ts_td, fetch_splits = FALSE, verbose = FALSE))
d <- as.data.table(arrow::read_parquet(file.path(ts_td, "DISCK_daily.parquet")))
ok(all(c("eps_ttm", "pe_ttm") %in% names(d)), "DISCK daily gained eps_ttm/pe_ttm")
ok(res$with_ttm == 1, "DISCK TTM EPS computed from aliased DISCA cache")
unlink(c(fund_td, ts_td), recursive = TRUE)

cat(sprintf("\n%d passed, %d failed\n", np, nf))
if (nf > 0) quit(status = 1)
