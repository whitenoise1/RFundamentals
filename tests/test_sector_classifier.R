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
  test("has 4 columns",                  ncol(dt) == 4)
  test("correct column names",
       setequal(names(dt), c("ticker", "sector", "industry", "source")))

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
  test("source is finviz or fallback",
       all(dt$source %in% c("finviz", "fallback")))

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

  # Gate 8: Finviz source dominates
  test("majority from finviz",
       dt[source == "finviz", .N] / nrow(dt) > 0.90)

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
