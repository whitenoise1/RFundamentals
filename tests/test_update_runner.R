# ============================================================================
# test_update_runner.R  --  Unit Tests for Module 8 (update_runner.R)
# ============================================================================
# Run: Rscript tests/test_update_runner.R
# Offline by design: network paths (split guard, EDGAR probe, finviz
# scrape, Yahoo append) are exercised with refresh/scrape flags off or
# against pre-seeded caches; the live paths are covered by the gate.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")
source("R/pit_assembler.R")
source("R/ttm_eps.R")
source("R/timeseries_builder.R")
source("R/pipeline_runner.R")
source("R/update_runner.R")

.n_pass <- 0L
.n_fail <- 0L
test <- function(name, expr) {
  ok <- tryCatch(isTRUE(expr), error = function(e) FALSE)
  if (ok) { .n_pass <<- .n_pass + 1L; message(sprintf("  PASS  %s", name)) }
  else    { .n_fail <<- .n_fail + 1L; message(sprintf("  FAIL  %s", name)) }
}


# ============================================================================
# TEST 1: .splits_hash
# ============================================================================
message("\n=== .splits_hash ===")

s1 <- data.table(ex_date = as.Date(c("2020-08-31", "2014-06-09")),
                 ratio = c(0.25, 1 / 7))
s2 <- data.table(ex_date = as.Date(c("2014-06-09", "2020-08-31")),
                 ratio = c(1 / 7, 0.25))

test("hash: order-invariant", identical(.splits_hash(s1), .splits_hash(s2)))
test("hash: empty and NULL give the sentinel",
     .splits_hash(NULL) == "0" && .splits_hash(s1[0]) == "0")
test("hash: a new event changes the hash",
     !identical(.splits_hash(s1),
                .splits_hash(rbind(s1, data.table(
                  ex_date = as.Date("2024-06-10"), ratio = 0.1)))))


# ============================================================================
# TEST 2: manifest save/load roundtrip + assertions
# ============================================================================
message("\n=== manifest I/O ===")

mpath <- file.path(tempdir(), "test_manifest.parquet")
mk_manifest <- function() rbind(
  data.table(cik = "0000320193", ticker = "AAPL",
             last_filing_accession = "0000320193-24-000123",
             last_filed_date = as.Date("2024-11-01"),
             last_companyfacts_fetch = as.Date("2026-07-01"),
             last_price_date = as.Date("2026-07-10"),
             last_daily_date = as.Date("2026-07-10"),
             splits_hash = "x", sic = "3571", status = "ok"),
  data.table(cik = NA_character_, ticker = .WATERMARK_TK,
             last_filing_accession = NA_character_,
             last_filed_date = as.Date("2026-07-08"),
             last_companyfacts_fetch = as.Date("2026-07-01"),
             last_price_date = as.Date(NA),
             last_daily_date = as.Date("2026-07-11"),
             splits_hash = NA_character_, sic = NA_character_,
             status = NA_character_))

m <- mk_manifest()
save_update_manifest(m, mpath)
m2 <- load_update_manifest(mpath)

test("roundtrip: identical content", isTRUE(all.equal(m, m2)))
test("load: missing file returns NULL",
     is.null(load_update_manifest(file.path(tempdir(), "nope.parquet"))))
test("save: rejects invalid status", {
  bad <- copy(m)[ticker == "AAPL", status := "bogus"]
  tryCatch({ save_update_manifest(bad, mpath); FALSE },
           error = function(e) grepl("valid statuses", e$message))
})
test("save: rejects manifest without watermark", {
  tryCatch({ save_update_manifest(m[ticker != .WATERMARK_TK], mpath); FALSE },
           error = function(e) grepl("watermark", e$message))
})
unlink(mpath)


# ============================================================================
# TEST 3: .append_ticker_prices no-op / skip paths (offline)
# ============================================================================
message("\n=== .append_ticker_prices (offline paths) ===")

ap_dir <- file.path(tempdir(), "test_ap_prices")
dir.create(ap_dir, showWarnings = FALSE)

arrow::write_parquet(
  data.frame(date = as.character(seq(as.Date("2026-07-01"),
                                     as.Date("2026-07-10"), by = "day")),
             close = 100),
  file.path(ap_dir, "UPTODATE_yahoo_2009-01-01.parquet"))
test("append: cache already current -> 0 rows, no network",
     .append_ticker_prices("UPTODATE", through_date = as.Date("2026-07-10"),
                           price_dir = ap_dir) == 0L)

arrow::write_parquet(
  data.frame(date = "2020-01-02", close = 5),
  file.path(ap_dir, "DEADCO_tiingo_2009-01-01.parquet"))
test("append: tiingo-sourced cache skipped",
     .append_ticker_prices("DEADCO", through_date = Sys.Date(),
                           price_dir = ap_dir) == 0L)

test("append: missing cache -> NA",
     is.na(.append_ticker_prices("GHOST", through_date = Sys.Date(),
                                 price_dir = ap_dir)))
unlink(ap_dir, recursive = TRUE)


# ============================================================================
# TEST 4: run_tier_daily (offline: refresh_splits = FALSE)
# ============================================================================
message("\n=== run_tier_daily (offline fixture) ===")

td_ts    <- file.path(tempdir(), "td_ts");     dir.create(td_ts, showWarnings = FALSE)
td_price <- file.path(tempdir(), "td_prices"); dir.create(td_price, showWarnings = FALSE)

fx_dates <- seq(as.Date("2026-06-01"), as.Date("2026-06-30"), by = "day")
fx_dates <- fx_dates[!format(fx_dates, "%u") %in% c("6", "7")]

arrow::write_parquet(
  data.frame(date = as.character(fx_dates), close = 100),
  file.path(td_price, "FIXCO_yahoo_2009-01-01.parquet"))
arrow::write_parquet(data.table(
  fiscal_year = 2025L, filed_date = as.Date("2026-02-15"),
  period_end = as.Date("2025-12-31"),
  stub_shares = 1e9, stub_eps = 5,
  stub_equity = NA_real_, stub_revenue = NA_real_, stub_fcf = NA_real_,
  stub_ebitda = NA_real_, stub_total_debt = NA_real_,
  stub_net_debt = NA_real_, stub_cash = NA_real_,
  stub_dividends = NA_real_, stub_buybacks = NA_real_,
  stub_equity_issuance = NA_real_),
  file.path(td_ts, "FIXCO_fund.parquet"))

fx_manifest <- rbind(
  data.table(cik = "0000000001", ticker = "FIXCO",
             last_filing_accession = NA_character_,
             last_filed_date = as.Date("2026-02-15"),
             last_companyfacts_fetch = as.Date(NA),
             last_price_date = as.Date(NA),
             last_daily_date = as.Date(NA),
             splits_hash = "0", sic = NA_character_, status = "ok"),
  data.table(cik = NA_character_, ticker = .WATERMARK_TK,
             last_filing_accession = NA_character_,
             last_filed_date = as.Date(NA),
             last_companyfacts_fetch = as.Date(NA),
             last_price_date = as.Date(NA), last_daily_date = as.Date(NA),
             splits_hash = NA_character_, sic = NA_character_,
             status = NA_character_))

r1 <- run_tier_daily(as_of = as.Date("2026-06-30"), manifest = fx_manifest,
                     ts_dir = td_ts, price_dir = td_price,
                     refresh_splits = FALSE)

test("tier D: daily layer built and cursor advanced",
     r1$manifest[ticker == "FIXCO", last_daily_date] == max(fx_dates))
test("tier D: one layer advanced", r1$stats$appended == 1L)
test("tier D: watermark stamped",
     r1$manifest[ticker == .WATERMARK_TK,
                 last_daily_date] == as.Date("2026-06-30"))

r2 <- run_tier_daily(as_of = as.Date("2026-06-30"), manifest = r1$manifest,
                     ts_dir = td_ts, price_dir = td_price,
                     refresh_splits = FALSE)
test("tier D: idempotent re-run advances nothing", r2$stats$appended == 0L)

unlink(c(td_ts, td_price), recursive = TRUE)


# ============================================================================
# TEST 5: run_tier_monthly (offline: roster diff only)
# ============================================================================
message("\n=== run_tier_monthly (offline fixture) ===")

tm_snap <- file.path(tempdir(), "tm_snaps"); dir.create(tm_snap, showWarnings = FALSE)
# cover the only quarter-end <= as_of so no assembly is attempted
file.create(file.path(tm_snap, "pit_2010-03-31_raw.parquet"))

tm_roster <- file.path(tempdir(), "tm_roster.csv")
fwrite(data.table(
  ticker = c("FIXCO", "GONECO", "NEWCO"),
  cik = c("0000000001", "0000000002", ""),
  date_added = c("2010-01-01", "2010-01-01", "2010-03-15"),
  date_removed = c(NA, "2010-03-01", NA)), tm_roster)

tm_manifest <- rbind(
  data.table(cik = c("0000000001", "0000000002"),
             ticker = c("FIXCO", "GONECO"),
             last_filing_accession = NA_character_,
             last_filed_date = as.Date(NA),
             last_companyfacts_fetch = as.Date(NA),
             last_price_date = as.Date(NA), last_daily_date = as.Date(NA),
             splits_hash = "0", sic = NA_character_, status = "ok"),
  data.table(cik = NA_character_, ticker = .WATERMARK_TK,
             last_filing_accession = NA_character_,
             last_filed_date = as.Date(NA),
             last_companyfacts_fetch = as.Date(NA),
             last_price_date = as.Date(NA), last_daily_date = as.Date(NA),
             splits_hash = NA_character_, sic = NA_character_,
             status = NA_character_))

r3 <- run_tier_monthly(as_of = as.Date("2010-04-01"), manifest = tm_manifest,
                       roster_csv = tm_roster,
                       snapshot_dir = tm_snap,
                       scrape_sectors = FALSE)

test("tier M: new roster ticker added as needs-backfill (no CIK)",
     r3$manifest[ticker == "NEWCO", status] == "needs-backfill" &&
       is.na(r3$manifest[ticker == "NEWCO", cik]))
test("tier M: removal flipped to removed",
     r3$manifest[ticker == "GONECO", status] == "removed")
test("tier M: active ticker untouched",
     r3$manifest[ticker == "FIXCO", status] == "ok")
test("tier M: no snapshots attempted (grid covered)",
     r3$stats$new_snapshots == 0L)
test("tier M: watermark stamped",
     r3$manifest[ticker == .WATERMARK_TK,
                 last_companyfacts_fetch] == as.Date("2010-04-01"))

unlink(c(tm_snap, tm_roster), recursive = TRUE)


# ============================================================================
# TEST 6: run_daily_update shell delegates
# ============================================================================
message("\n=== run_daily_update shell ===")

test("run_daily_update exists and requires update_runner",
     is.function(run_daily_update))


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== test_update_runner.R: %d passed, %d failed ===",
                .n_pass, .n_fail))
if (.n_fail > 0) stop("Tests failed", call. = FALSE)
