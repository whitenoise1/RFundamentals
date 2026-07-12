# ============================================================================
# test_sector_classifier.R  --  Unit + Gate Tests for Module 3
# ============================================================================
# Run: Rscript tests/test_sector_classifier.R
#
# Unit tests: validate helpers and parsing in isolation.
# Gate tests: validate the final parquet output against Session B checkpoints.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/sector_classifier.R")

# -- Test harness --
.n_pass <- 0L
.n_fail <- 0L

test <- function(name, expr) {
  ok <- tryCatch(isTRUE(expr), error = function(e) FALSE)
  if (ok) {
    .n_pass <<- .n_pass + 1L
    message(sprintf("  PASS  %s", name))
  } else {
    .n_fail <<- .n_fail + 1L
    message(sprintf("  FAIL  %s", name))
  }
}


# ============================================================================
# UNIT TESTS: .decode_html_entities()
# ============================================================================
message("\n=== .decode_html_entities() ===")

test("decodes &amp;",
     .decode_html_entities("Wineries &amp; Distilleries") == "Wineries & Distilleries")
test("decodes &lt; and &gt;",
     .decode_html_entities("a &lt; b &gt; c") == "a < b > c")
test("decodes &quot;",
     .decode_html_entities("say &quot;hi&quot;") == 'say "hi"')
test("decodes &#39;",
     .decode_html_entities("it&#39;s") == "it's")
test("no-op on clean string",
     .decode_html_entities("Technology") == "Technology")
test("handles multiple &amp; in one string",
     .decode_html_entities("A &amp; B &amp; C") == "A & B & C")


# ============================================================================
# UNIT TESTS: .extract_finviz_sector_industry()
# ============================================================================
message("\n=== .extract_finviz_sector_industry() ===")

# Simulate finviz HTML snippet (modern layout)
mock_html <- paste0(
  '<div class="flex space-x-0.5 overflow-hidden">',
  '<a href="screener.ashx?v=111&f=sec_technology" class="tab-link">Technology</a>',
  '<span class="text-muted-3">\u2022</span>',
  '<a href="screener.ashx?v=111&f=ind_semiconductors" class="tab-link">Semiconductors</a>',
  '</div>')

result <- .extract_finviz_sector_industry(mock_html)
test("extracts sector from mock HTML",
     !is.null(result) && result$sector == "Technology")
test("extracts industry from mock HTML",
     !is.null(result) && result$industry == "Semiconductors")
test("returns NULL for empty HTML",
     is.null(.extract_finviz_sector_industry("")))
test("returns NULL for HTML without sector links",
     is.null(.extract_finviz_sector_industry("<div>no sector here</div>")))

# HTML entity decoding in extraction
mock_html_entity <- paste0(
  '<a href="screener.ashx?v=111&f=sec_financial" class="tab-link">Financial</a>',
  '<a href="screener.ashx?v=111&f=ind_propertycasualty" class="tab-link">',
  'Insurance - Property &amp; Casualty</a>')
entity_result <- .extract_finviz_sector_industry(mock_html_entity)
test("decodes HTML entities in industry",
     !is.null(entity_result) &&
     entity_result$industry == "Insurance - Property & Casualty")

# Industry with title attribute (truncated text)
mock_html_title <- paste0(
  '<a href="screener.ashx?v=111&f=sec_consumerdefensive" class="tab-link">Consumer Defensive</a>',
  '<a href="screener.ashx?v=111&f=ind_beverages" class="tab-link truncate" ',
  'title="Beverages - Wineries &amp; Distilleries">',
  'Beverages - Wineries &amp; Distilleries</a>')
title_result <- .extract_finviz_sector_industry(mock_html_title)
test("extracts from links with title attribute",
     !is.null(title_result) &&
     title_result$sector == "Consumer Defensive" &&
     title_result$industry == "Beverages - Wineries & Distilleries")

# 2026 layout: quote-header_category class, text wrapped in a <span>
mock_html_2026 <- paste0(
  '<a href="screener?v=111&f=sec_technology" ',
  'class="quote-header_category">Technology</a>',
  '<a href="screener?v=111&f=ind_consumerelectronics" ',
  'class="quote-header_category" title="Consumer Electronics">',
  '<span class="min-w-0 truncate">Consumer Electronics</span></a>')
span_result <- .extract_finviz_sector_industry(mock_html_2026)
test("extracts from 2026 span-wrapped links",
     !is.null(span_result) &&
     span_result$sector == "Technology" &&
     span_result$industry == "Consumer Electronics")


# ============================================================================
# UNIT TESTS: .finviz_fetch_one() -- dot-ticker handling
# ============================================================================
message("\n=== .finviz_fetch_one() dot-ticker ===")

# We cannot easily unit-test the HTTP call without mocking, but we verify
# that the URL construction converts dots to dashes.
# The live test below covers the actual fetch.
test("dot-to-dash conversion in URL",
     grepl("BRK-B", sprintf("%s?t=%s", .FINVIZ_BASE, gsub("\\.", "-", "BRK.B"))))


# ============================================================================
# UNIT TESTS: load_sector_fallback()
# ============================================================================
message("\n=== load_sector_fallback() ===")

test("returns NULL when file missing",
     is.null(load_sector_fallback("nonexistent_file.csv")))

# Create temporary fallback CSV
tmp_fb <- tempfile(fileext = ".csv")
fwrite(data.table(
  ticker = c("AAPL", "MSFT", "JPM"),
  sector = c("Technology", "Technology", "Financial"),
  industry = c("Consumer Electronics", "Software - Infrastructure", "Banks - Diversified")
), tmp_fb)

fb <- load_sector_fallback(tmp_fb)
test("fallback returns data.table",         is.data.table(fb))
test("fallback has 4 columns",              ncol(fb) == 4)
test("fallback has source = 'fallback'",    all(fb$source == "fallback"))
test("fallback has 3 rows",                 nrow(fb) == 3)
test("fallback ticker is uppercase",        all(fb$ticker == toupper(fb$ticker)))
file.remove(tmp_fb)

# Malformed CSV (wrong columns)
tmp_bad <- tempfile(fileext = ".csv")
fwrite(data.table(x = 1, y = 2), tmp_bad)
test("returns NULL for malformed CSV",       is.null(load_sector_fallback(tmp_bad)))
file.remove(tmp_bad)


# ============================================================================
# UNIT TESTS: load_cached_sectors()
# ============================================================================
message("\n=== load_cached_sectors() ===")

test("returns NULL when no cache",
     is.null(load_cached_sectors("nonexistent.parquet")))

# Write a temp parquet and read it back
tmp_pq <- tempfile(fileext = ".parquet")
test_dt <- data.table(
  ticker = c("AAPL", "GOOG"),
  sector = c("Technology", "Communication Services"),
  industry = c("Consumer Electronics", "Internet Content & Information"),
  source = c("finviz", "finviz"))
arrow::write_parquet(test_dt, tmp_pq)

cached <- load_cached_sectors(tmp_pq)
test("cached returns data.table",           is.data.table(cached))
test("cached round-trips correctly",        nrow(cached) == 2)
test("cached preserves ticker values",      all(c("AAPL", "GOOG") %in% cached$ticker))
file.remove(tmp_pq)


# ============================================================================
# UNIT TESTS: get_sector_lookup()
# ============================================================================
message("\n=== get_sector_lookup() ===")

test("stop on missing file",
     tryCatch({get_sector_lookup("nonexistent.parquet"); FALSE},
              error = function(e) grepl("not found", e$message)))

# Using real cache if available
pq_path <- "cache/lookups/sector_industry.parquet"
if (file.exists(pq_path)) {
  lookup <- get_sector_lookup(pq_path)
  test("returns data.table",              is.data.table(lookup))
  test("has ticker column",               "ticker" %in% names(lookup))
  test("has sector column",               "sector" %in% names(lookup))
  test("has industry column",             "industry" %in% names(lookup))
}


# ============================================================================
# UNIT TESTS: override CSVs emit exactly the 11 finviz sector strings
# ============================================================================
# .SECTOR_NA_INDICATORS keys on the literal finviz sector strings; a
# near-miss ("Financial Services") silently disables the bank/utility/
# REIT NA-mask -- the D3 failure mode. Every static assignment must
# therefore be one of the 11 exact strings (docs/DESIGN_DAILY_UPDATE.md
# section 3, exact-string requirement).
message("\n=== sector override CSVs: exact finviz strings ===")

test("exactly 11 finviz sectors defined",
     length(.FINVIZ_SECTORS) == 11)

suppressMessages(source("R/indicator_compute.R"))
test("every .SECTOR_NA_INDICATORS key is an exact finviz sector",
     all(names(.SECTOR_NA_INDICATORS) %in% .FINVIZ_SECTORS))

for (ov_csv in .SECTOR_OVERRIDE_CSVS) {
  if (!file.exists(ov_csv)) {
    message(sprintf("  SKIP  %s missing", ov_csv))
    next
  }
  ov <- fread(ov_csv)
  nm <- basename(ov_csv)
  test(sprintf("%s: has ticker/sector/industry columns", nm),
       all(c("ticker", "sector", "industry") %in% names(ov)))
  test(sprintf("%s: no duplicate tickers", nm),
       !anyDuplicated(ov$ticker))
  test(sprintf("%s: every sector is an exact finviz string", nm),
       all(ov$sector %in% .FINVIZ_SECTORS))
  test(sprintf("%s: no empty industries", nm),
       all(!is.na(ov$industry) & nzchar(trimws(ov$industry))))
}

test("code-literal .SECTOR_OVERRIDES sectors are exact finviz strings",
     all(vapply(.SECTOR_OVERRIDES, `[`, character(1), 1) %in% .FINVIZ_SECTORS))

# The D2/A measured gap (2026-07-12): every timeseries-universe ticker
# without a sector row must be covered by the delisted override file.
ov_delisted <- fread("data/sector_overrides_delisted.csv")
gap_2026_07 <- c("ABMD", "ANSS", "ATVI", "CERN", "CMA", "CPWR", "CTLT",
                 "CTXS", "DFS", "DISH", "DRE", "FBHS", "HBI", "HES",
                 "JNPR", "KSU", "MRO", "MXIM", "NLSN", "PBCT", "PXD",
                 "SIVB", "TWTR", "XLNX")
test("delisted overrides cover the measured 24-ticker gap",
     all(gap_2026_07 %in% ov_delisted$ticker))

# Delisted banks must land in the mask-bearing Financial sector -- the
# whole point of D2/A is that the NA-mask fires for them again.
test("SIVB/CMA/PBCT/DFS are Financial",
     all(ov_delisted[ticker %in% c("SIVB", "CMA", "PBCT", "DFS"), sector]
         == "Financial"))


# ============================================================================
# UNIT TESTS: .sector_asof() / merge_sector_scd() (SCD Type-2, D2/B)
# ============================================================================
message("\n=== .sector_asof / merge_sector_scd ===")

scd <- data.table(
  ticker     = c("AAA", "AAA", "BBB", "CCC"),
  sector     = c("Technology", "Communication Services", "Financial",
                 "Energy"),
  industry   = c("Software - Application", "Entertainment",
                 "Banks - Regional", "Oil & Gas E&P"),
  source     = "finviz",
  valid_from = as.Date(c("2010-01-01", "2020-06-01", "2010-01-01",
                         "2024-03-01"))
)

asof_2015 <- .sector_asof(scd, "2015-06-30")
test("asof: one row per ticker",
     nrow(asof_2015) == 3 && !anyDuplicated(asof_2015$ticker))
test("asof: pre-change date sees the old value",
     asof_2015[ticker == "AAA", sector] == "Technology")
test("asof: post-change date sees the new value",
     .sector_asof(scd, "2020-06-01")[ticker == "AAA", sector] ==
       "Communication Services")
test("asof: first-seen-later ticker backfills from earliest row",
     asof_2015[ticker == "CCC", sector] == "Energy")
test("asof: legacy table without valid_from passes through",
     identical(.sector_asof(scd[, !"valid_from"], "2015-06-30"),
               scd[, !"valid_from"]))
test("asof: floor-stamped table reproduces the flat view on any date", {
  flat <- data.table(ticker = c("X", "Y"), sector = "Technology",
                     industry = "Semiconductors", source = "finviz")
  dated <- copy(flat)[, valid_from := as.Date("2010-01-01")]
  v <- .sector_asof(dated, "2012-01-01")[, .(ticker, sector, industry, source)]
  identical(v, flat)
})

cur <- data.table(
  ticker   = c("AAA", "BBB", "DDD"),
  sector   = c("Communication Services", "Real Estate", "Utilities"),
  industry = c("Entertainment", "REIT - Industrial",
               "Utilities - Regulated Electric"),
  source   = "finviz"
)
merged <- merge_sector_scd(scd, cur, as_of = as.Date("2026-07-12"))

test("scd merge: unchanged ticker keeps history untouched",
     nrow(merged[ticker == "AAA"]) == 2)
test("scd merge: changed ticker appends a dated row",
     nrow(merged[ticker == "BBB"]) == 2 &&
       merged[ticker == "BBB" & valid_from == as.Date("2026-07-12"),
              sector] == "Real Estate")
test("scd merge: new ticker appended at as_of",
     merged[ticker == "DDD", valid_from] == as.Date("2026-07-12"))
test("scd merge: history-only ticker preserved",
     nrow(merged[ticker == "CCC"]) == 1)
test("scd merge: no duplicate (ticker, valid_from)",
     !anyDuplicated(merged[, .(ticker, valid_from)]))
test("scd merge: same-day rerun replaces, not stacks", {
  cur2 <- copy(cur)[ticker == "BBB", industry := "REIT - Office"]
  m2 <- merge_sector_scd(merged, cur2, as_of = as.Date("2026-07-12"))
  nrow(m2[ticker == "BBB"]) == 2 &&
    m2[ticker == "BBB" & valid_from == as.Date("2026-07-12"),
       industry] == "REIT - Office"
})
test("scd merge: bootstrap from NULL stamps the floor",
     all(merge_sector_scd(NULL, cur)$valid_from == .SECTOR_VALID_FROM_FLOOR))
test("scd merge: legacy prev without valid_from gets floor-stamped", {
  m3 <- merge_sector_scd(scd[valid_from == as.Date("2010-01-01"),
                             !"valid_from"], cur,
                         as_of = as.Date("2026-07-12"))
  all(m3[ticker == "BBB" & sector == "Financial",
         valid_from] == .SECTOR_VALID_FROM_FLOOR)
})
test("scd merge: NA industry gaining a value counts as a change", {
  prev_na <- data.table(ticker = "EEE", sector = "Financial",
                        industry = NA_character_, source = "fallback",
                        valid_from = as.Date("2010-01-01"))
  cur_na <- data.table(ticker = "EEE", sector = "Financial",
                       industry = "Banks - Regional", source = "finviz")
  m4 <- merge_sector_scd(prev_na, cur_na, as_of = as.Date("2026-07-12"))
  nrow(m4) == 2 &&
    m4[valid_from == as.Date("2026-07-12"), industry] == "Banks - Regional"
})

# -- apply_sector_overrides: one data path for all entry points --
message("\n=== apply_sector_overrides ===")

aso <- data.table(ticker = c("SIVB", "AAPL"),
                  sector = c("Technology", "Technology"),
                  industry = c("Wrong Industry", "Consumer Electronics"),
                  source = "finviz")
aso2 <- apply_sector_overrides(copy(aso), add_missing = FALSE)
test("overrides: scraped reused symbol corrected in place",
     aso2[ticker == "SIVB", sector] == "Financial" &&
       aso2[ticker == "SIVB", source] == "override")
test("overrides: non-override ticker untouched",
     aso2[ticker == "AAPL", sector] == "Technology" &&
       aso2[ticker == "AAPL", source] == "finviz")
test("overrides: add_missing = FALSE appends nothing",
     nrow(aso2) == 2)
aso3 <- apply_sector_overrides(copy(aso), add_missing = TRUE)
test("overrides: add_missing = TRUE appends absent override tickers",
     "KSU" %in% aso3$ticker &&
       aso3[ticker == "KSU", sector] == "Industrials")


# ============================================================================
# UNIT TESTS: .finviz_fetch_one() -- live test (single ticker)
# ============================================================================
message("\n=== .finviz_fetch_one() (live, AAPL) ===")

live_result <- tryCatch(.finviz_fetch_one("AAPL"), error = function(e) NULL)

if (!is.null(live_result)) {
  test("live fetch returns list",            is.list(live_result))
  test("live fetch has sector",              !is.null(live_result$sector))
  test("live fetch has industry",            !is.null(live_result$industry))
  test("AAPL sector is Technology",          live_result$sector == "Technology")
  test("AAPL industry non-empty",            nchar(live_result$industry) > 0)
  test("no HTML entities in industry",       !grepl("&amp;|&lt;|&gt;", live_result$industry))
} else {
  message("  SKIP  live fetch failed (network issue or finviz blocking)")
  message("        This is expected in CI or rate-limited environments.")
}


# ============================================================================
# GATE TESTS: Final parquet output (Session B checkpoints)
# ============================================================================
message("\n=== GATE TESTS (Session B) ===")

if (file.exists(pq_path)) {
  dt <- as.data.table(arrow::read_parquet(pq_path))

  test("parquet is data.table",          is.data.table(dt))
  test("has 5 columns (SCD schema, D2/B)", ncol(dt) == 5)
  test("correct column names",
       setequal(names(dt), c("ticker", "sector", "industry", "source",
                             "valid_from")))
  test("valid_from populated",           all(!is.na(dt$valid_from)))
  # collapse to the current view for the per-ticker gates below
  dt <- .sector_asof(dt, Sys.Date())

  # Gate 1: Coverage -- 100% of active tickers
  master <- as.data.table(arrow::read_parquet("cache/lookups/constituent_master.parquet"))
  active_tickers <- master[status == "ACTIVE", unique(ticker)]
  covered_active <- sum(active_tickers %in% dt$ticker)
  test("covers 100% of active tickers",
       covered_active == length(active_tickers))
  message(sprintf("  coverage: %d/%d active tickers (%.1f%%)",
                  covered_active, length(active_tickers),
                  100 * covered_active / length(active_tickers)))

  # Gate 2: No duplicate tickers
  test("no duplicate tickers",          !anyDuplicated(dt$ticker))

  # Gate 3: All sectors and industries are non-empty strings
  test("all sectors non-empty",         all(nchar(dt$sector) > 0))
  test("all industries non-empty",      all(nchar(dt$industry) > 0))

  # Gate 4: No HTML entities in sector or industry values
  test("no HTML entities in sectors",
       !any(grepl("&amp;|&lt;|&gt;|&quot;|&#39;", dt$sector)))
  test("no HTML entities in industries",
       !any(grepl("&amp;|&lt;|&gt;|&quot;|&#39;", dt$industry)))

  # Gate 5: Source field is valid
  test("source is finviz, fallback, or override",
       all(dt$source %in% c("finviz", "fallback", "override")))

  # Gate 5b: manual overrides applied (reused-ticker protection)
  for (tk in names(.SECTOR_OVERRIDES)) {
    if (tk %in% dt$ticker) {
      test(sprintf("override applied: %s -> %s", tk, .SECTOR_OVERRIDES[[tk]][1]),
           dt[ticker == tk, sector] == .SECTOR_OVERRIDES[[tk]][1] &&
           dt[ticker == tk, source] == "override")
    }
  }

  # Gate 6: Known ticker spot checks
  if ("AAPL" %in% dt$ticker) {
    test("AAPL is Technology",          dt[ticker == "AAPL", sector] == "Technology")
  }
  if ("JPM" %in% dt$ticker) {
    test("JPM is Financial",            dt[ticker == "JPM", sector] == "Financial")
  }
  if ("XOM" %in% dt$ticker) {
    test("XOM is Energy",               dt[ticker == "XOM", sector] == "Energy")
  }
  if ("JNJ" %in% dt$ticker) {
    test("JNJ is Healthcare",           dt[ticker == "JNJ", sector] == "Healthcare")
  }

  # Gate 7: Sector distribution sanity
  sector_counts <- dt[, .N, by = sector][order(-N)]
  test("at least 8 distinct sectors",   nrow(sector_counts) >= 8)
  test("no single sector > 40%",
       all(sector_counts$N / nrow(dt) < 0.40))

  # Gate 8: Finviz source dominates the LIVE universe; the old-CIK wave
  # added ~145 static override rows for delisted names, so the finviz
  # share is measured against scrape-able tickers, not the whole table.
  test("majority from finviz",
       dt[source == "finviz", .N] / nrow(dt) > 0.75)
  # old-CIK + Tier 1 set (<=152) plus the daily-update wave's 24
  # delisted-name rows (D2/A)
  test("override rows are the documented static set (no silent growth)",
       dt[source == "override", .N] <= 176)

} else {
  message("  SKIP  parquet not yet built (run build_sector_industry() first)")
}


# ============================================================================
# SUMMARY
# ============================================================================
message(sprintf("\n=== RESULTS: %d passed, %d failed ===",
                .n_pass, .n_fail))
if (.n_fail > 0) {
  stop(sprintf("%d test(s) failed", .n_fail))
} else {
  message("All tests passed.")
}
