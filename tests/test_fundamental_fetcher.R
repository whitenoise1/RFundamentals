# ============================================================================
# test_fundamental_fetcher.R  --  Unit + Gate Tests for Module 2
# ============================================================================
# Run: Rscript tests/test_fundamental_fetcher.R
#
# Unit tests: validate parsing, dedup, alias resolution, period classification.
# Integration tests: validate against EDGAR live data (20-ticker prototype).
# Gate tests: validate cached parquet outputs against Session C checkpoints.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/fundamental_fetcher.R")

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
# UNIT TESTS: Tag alias map integrity
# ============================================================================
message("\n=== Tag Alias Map ===")

test("TAG_ALIASES is a named list",
     is.list(.TAG_ALIASES) && !is.null(names(.TAG_ALIASES)))

test("28+ canonical concepts defined",
     length(.TAG_ALIASES) >= 28)

test("no duplicate tags across concepts", {
  all_tags <- unlist(.TAG_ALIASES, use.names = FALSE)
  !anyDuplicated(all_tags)
})

test("TAG_TO_CONCEPT covers all tags",
     length(.TAG_TO_CONCEPT) == length(unlist(.TAG_ALIASES)))

test("ALL_TARGET_TAGS matches flattened aliases",
     setequal(.ALL_TARGET_TAGS, unlist(.TAG_ALIASES, use.names = FALSE)))

# Spot-check key aliases
test("revenue has Revenues as first alias",
     .TAG_ALIASES[["revenue"]][1] == "Revenues")

test("cogs has CostOfGoodsAndServicesSold",
     "CostOfGoodsAndServicesSold" %in% .TAG_ALIASES[["cogs"]])

test("operating_cashflow has both variants",
     length(.TAG_ALIASES[["operating_cashflow"]]) >= 2)

test("reverse lookup: Revenues -> revenue",
     .TAG_TO_CONCEPT[["Revenues"]] == "revenue")

test("reverse lookup: Assets -> total_assets",
     .TAG_TO_CONCEPT[["Assets"]] == "total_assets")

# Form priority
test("10-K and 10-K/A share same priority",
     .FORM_PRIORITY[["10-K"]] == .FORM_PRIORITY[["10-K/A"]])

test("10-Q and 10-Q/A share same priority",
     .FORM_PRIORITY[["10-Q"]] == .FORM_PRIORITY[["10-Q/A"]])

test("annual forms beat quarterly forms",
     .FORM_PRIORITY[["10-K"]] < .FORM_PRIORITY[["10-Q"]])

test("20-F shares annual priority with 10-K",
     .FORM_PRIORITY[["20-F"]] == .FORM_PRIORITY[["10-K"]])


# ============================================================================
# UNIT TESTS: parse_companyfacts() with synthetic data
# ============================================================================
message("\n=== parse_companyfacts() ===")

# Build a minimal synthetic companyfacts JSON structure
.make_test_facts <- function() {
  list(
    facts = list(
      `us-gaap` = list(
        Revenues = list(
          units = list(
            USD = list(
              list(val = 1000000, end = "2023-12-31", start = "2023-01-01",
                   filed = "2024-02-15", form = "10-K", accn = "0001-24-000001",
                   fy = 2023L, fp = "FY"),
              list(val = 250000, end = "2023-03-31", start = "2023-01-01",
                   filed = "2023-05-01", form = "10-Q", accn = "0001-23-000010",
                   fy = 2023L, fp = "Q1")
            )
          )
        ),
        Assets = list(
          units = list(
            USD = list(
              list(val = 5000000, end = "2023-12-31",
                   filed = "2024-02-15", form = "10-K", accn = "0001-24-000001",
                   fy = 2023L, fp = "FY"),
              list(val = 4800000, end = "2022-12-31",
                   filed = "2023-02-20", form = "10-K", accn = "0001-23-000001",
                   fy = 2022L, fp = "FY")
            )
          )
        ),
        NetIncomeLoss = list(
          units = list(
            USD = list(
              list(val = 100000, end = "2023-12-31", start = "2023-01-01",
                   filed = "2024-02-15", form = "10-K", accn = "0001-24-000001",
                   fy = 2023L, fp = "FY")
            )
          )
        ),
        # Tag not in our target list -- should be skipped
        SomeObscureTag = list(
          units = list(
            USD = list(
              list(val = 999, end = "2023-12-31", filed = "2024-02-15",
                   form = "10-K", fy = 2023L, fp = "FY")
            )
          )
        )
      ),
      dei = list(
        EntityCommonStockSharesOutstanding = list(
          units = list(
            shares = list(
              list(val = 1000000, end = "2023-12-31",
                   filed = "2024-02-15", form = "10-K", accn = "0001-24-000001",
                   fy = 2023L, fp = "FY")
            )
          )
        )
      )
    )
  )
}

test_facts <- .make_test_facts()
parsed <- parse_companyfacts(test_facts, "TEST", "0000000001")

test("parse returns data.table",         is.data.table(parsed))
test("parse has expected columns",
     all(c("ticker", "cik", "concept", "tag", "value", "period_end",
            "filed", "form", "accession") %in% names(parsed)))

test("parse found revenue rows",         "revenue" %in% parsed$concept)
test("parse found total_assets rows",    "total_assets" %in% parsed$concept)
test("parse found net_income rows",      "net_income" %in% parsed$concept)
test("parse found shares_outstanding",   "shares_outstanding" %in% parsed$concept)
test("parse skipped SomeObscureTag",     !("SomeObscureTag" %in% parsed$tag))

test("revenue FY value correct",
     parsed[concept == "revenue" & fiscal_qtr == "FY", value] == 1000000)

test("total_assets 2023 value correct",
     parsed[concept == "total_assets" & fiscal_year == 2023L, value] == 5000000)

test("filed dates are Date class",       inherits(parsed$filed, "Date"))
test("period_end is Date class",         inherits(parsed$period_end, "Date"))

test("ticker column is TEST",            all(parsed$ticker == "TEST"))
test("cik column is correct",            all(parsed$cik == "0000000001"))

test("parse NULL returns NULL",          is.null(parse_companyfacts(NULL, "X", "0")))

# Edge case: facts with only dei namespace, no us-gaap
dei_only_facts <- list(facts = list(
  dei = list(
    EntityCommonStockSharesOutstanding = list(
      units = list(
        shares = list(
          list(val = 500, end = "2023-12-31", filed = "2024-01-15",
               form = "10-K", accn = "0001-24-000001", fy = 2023L, fp = "FY")
        )
      )
    )
  )
))
dei_only_parsed <- parse_companyfacts(dei_only_facts, "DEI", "0000000002")
test("parse dei-only facts returns data",
     !is.null(dei_only_parsed) && nrow(dei_only_parsed) == 1)

# Edge case: facts with no matching tags at all
empty_facts <- list(facts = list(
  `us-gaap` = list(
    SomeIrrelevantTag = list(
      units = list(USD = list(list(val = 1, end = "2023-12-31")))
    )
  )
))
test("parse returns NULL when no target tags match",
     is.null(parse_companyfacts(empty_facts, "EMPTY", "0000000003")))

# Edge case: observation with null val is skipped
null_val_facts <- list(facts = list(
  `us-gaap` = list(
    Revenues = list(
      units = list(USD = list(
        list(val = NULL, end = "2023-12-31", filed = "2024-01-01",
             form = "10-K", fp = "FY"),
        list(val = 100, end = "2023-12-31", filed = "2024-01-01",
             form = "10-K", fp = "FY", accn = "0001-24-000001", fy = 2023L)
      ))
    )
  )
))
null_val_parsed <- parse_companyfacts(null_val_facts, "NV", "0000000004")
test("parse skips null-val observations",
     !is.null(null_val_parsed) && nrow(null_val_parsed) == 1)


# ============================================================================
# UNIT TESTS: dedup_fundamentals()
# ============================================================================
message("\n=== dedup_fundamentals() ===")

# Build synthetic data with duplicates that need dedup
dedup_input <- data.table(
  ticker       = "TEST",
  cik          = "0000000001",
  concept      = c("revenue", "revenue", "revenue", "total_assets"),
  tag          = c("Revenues", "Revenues", "SalesRevenueNet", "Assets"),
  value        = c(1000000, 1010000, 1000000, 5000000),
  period_end   = as.Date(c("2023-12-31", "2023-12-31", "2023-12-31", "2023-12-31")),
  period_start = as.Date(c("2023-01-01", "2023-01-01", "2023-01-01", NA)),
  filed        = as.Date(c("2024-02-15", "2024-05-01", "2024-02-15", "2024-02-15")),
  form         = c("10-K", "10-K/A", "10-K", "10-K"),
  accession    = c("0001-24-000001", "0001-24-000099", "0001-24-000001", "0001-24-000001"),
  fiscal_year  = c(2023L, 2023L, 2023L, 2023L),
  fiscal_qtr   = c("FY", "FY", "FY", "FY"),
  unit         = "USD"
)

deduped <- dedup_fundamentals(dedup_input)

test("dedup returns data.table",          is.data.table(deduped))
test("dedup reduces row count",           nrow(deduped) < nrow(dedup_input))
test("dedup: one row per concept+period+qtr",
     !anyDuplicated(deduped[, .(concept, period_end, fiscal_qtr)]))

# 10-K/A should beat 10-K: same priority, but 10-K/A has later accession
test("dedup prefers 10-K/A over 10-K (amendment wins)",
     deduped[concept == "revenue", form] == "10-K/A")

test("dedup picks amendment value",
     deduped[concept == "revenue", value] == 1010000)

test("dedup preserves total_assets",
     nrow(deduped[concept == "total_assets"]) == 1)

test("dedup handles NULL input",
     is.null(dedup_fundamentals(NULL)))

test("dedup handles empty input",
     nrow(dedup_fundamentals(data.table())) == 0)

# Test FY vs Q4 same period_end: both must survive dedup
fy_q4_input <- data.table(
  ticker       = "TEST",
  cik          = "0000000001",
  concept      = c("revenue", "revenue"),
  tag          = c("Revenues", "Revenues"),
  value        = c(4000000, 1100000),
  period_end   = as.Date(c("2023-12-31", "2023-12-31")),
  period_start = as.Date(c("2023-01-01", "2023-10-01")),
  filed        = as.Date(c("2024-02-15", "2024-02-15")),
  form         = c("10-K", "10-Q"),
  accession    = c("0001-24-000001", "0001-24-000002"),
  fiscal_year  = c(2023L, 2023L),
  fiscal_qtr   = c("FY", "Q4"),
  unit         = "USD"
)

fy_q4_deduped <- dedup_fundamentals(fy_q4_input)
test("dedup preserves both FY and Q4 for same period_end",
     nrow(fy_q4_deduped) == 2)
test("FY row has full-year value",
     fy_q4_deduped[fiscal_qtr == "FY", value] == 4000000)
test("Q4 row has quarter value",
     fy_q4_deduped[fiscal_qtr == "Q4", value] == 1100000)

# Test 10-K vs 10-Q for same concept+period+qtr: annual wins
annual_vs_quarterly <- data.table(
  ticker       = "TEST",
  cik          = "0000000001",
  concept      = c("total_assets", "total_assets"),
  tag          = c("Assets", "Assets"),
  value        = c(5000000, 5000000),
  period_end   = as.Date(c("2023-12-31", "2023-12-31")),
  period_start = as.Date(c(NA, NA)),
  filed        = as.Date(c("2024-02-15", "2024-01-30")),
  form         = c("10-K", "10-Q"),
  accession    = c("0001-24-000010", "0001-24-000005"),
  fiscal_year  = c(2023L, 2023L),
  fiscal_qtr   = c("FY", "FY"),
  unit         = "USD"
)

ann_q_deduped <- dedup_fundamentals(annual_vs_quarterly)
test("dedup: annual form beats quarterly for same period",
     nrow(ann_q_deduped) == 1 && ann_q_deduped$form == "10-K")

# Test tag rank: within same form and accession, preferred alias wins
tag_rank_input <- data.table(
  ticker       = "TEST",
  cik          = "0000000001",
  concept      = c("revenue", "revenue"),
  tag          = c("SalesRevenueNet", "Revenues"),
  value        = c(1000000, 1000000),
  period_end   = as.Date(c("2023-12-31", "2023-12-31")),
  period_start = as.Date(c("2023-01-01", "2023-01-01")),
  filed        = as.Date(c("2024-02-15", "2024-02-15")),
  form         = c("10-K", "10-K"),
  accession    = c("0001-24-000001", "0001-24-000001"),
  fiscal_year  = c(2023L, 2023L),
  fiscal_qtr   = c("FY", "FY"),
  unit         = "USD"
)

tag_rank_deduped <- dedup_fundamentals(tag_rank_input)
test("dedup prefers primary alias (Revenues over SalesRevenueNet)",
     tag_rank_deduped[concept == "revenue", tag] == "Revenues")


# ============================================================================
# UNIT TESTS: classify_period()
# ============================================================================
message("\n=== classify_period() ===")

period_input <- data.table(
  concept      = c("revenue", "revenue", "total_assets", "revenue"),
  period_end   = as.Date(c("2023-12-31", "2023-03-31", "2023-12-31", "2023-06-30")),
  period_start = as.Date(c("2023-01-01", "2023-01-01", NA, "2023-04-01")),
  fiscal_year  = c(2023L, 2023L, 2023L, 2023L),
  fiscal_qtr   = c("FY", "Q1", "FY", "Q2")
)

classified <- classify_period(period_input)

test("classify adds period_type column",   "period_type" %in% names(classified))
test("FY classified correctly",
     classified[fiscal_qtr == "FY" & concept == "revenue", period_type] == "FY")
test("Q1 classified correctly",
     classified[fiscal_qtr == "Q1", period_type] == "Q1")
test("Q2 classified correctly",
     classified[fiscal_qtr == "Q2", period_type] == "Q2")
test("instant (balance sheet) classified",
     classified[concept == "total_assets", period_type] == "FY")

# Test fallback: period_days inference when fiscal_qtr is NA
fallback_input <- data.table(
  concept      = c("revenue", "revenue"),
  period_end   = as.Date(c("2023-12-31", "2023-06-30")),
  period_start = as.Date(c("2023-01-01", "2023-04-01")),
  fiscal_year  = c(2023L, 2023L),
  fiscal_qtr   = c(NA_character_, NA_character_)
)

fallback_classified <- classify_period(fallback_input)
test("fallback: 365-day period classified as FY",
     fallback_classified[period_end == as.Date("2023-12-31"), period_type] == "FY")
test("fallback: 91-day period classified as Q2",
     fallback_classified[period_end == as.Date("2023-06-30"), period_type] == "Q2")

# Test that invalid fiscal_qtr values are NOT assigned as period_type
invalid_fp_input <- data.table(
  concept      = "total_assets",
  period_end   = as.Date("2023-12-31"),
  period_start = as.Date(NA),
  fiscal_year  = 2023L,
  fiscal_qtr   = "CY"
)

invalid_classified <- classify_period(invalid_fp_input)
test("invalid fiscal_qtr (CY) not assigned as period_type",
     is.na(invalid_classified$period_type))

# Test classify on NULL and empty
test("classify_period handles NULL",
     is.null(classify_period(NULL)))
test("classify_period handles empty dt",
     nrow(classify_period(data.table())) == 0)


# ============================================================================
# INTEGRATION TESTS: Live EDGAR fetch (single ticker)
# ============================================================================
message("\n=== Integration: Live EDGAR fetch (AAPL) ===")

# Fetch AAPL (CIK 0000320193) as a known stable reference
aapl_facts <- fetch_companyfacts("0000320193")

test("AAPL companyfacts fetched",          !is.null(aapl_facts))

if (!is.null(aapl_facts)) {
  aapl_parsed <- parse_companyfacts(aapl_facts, "AAPL", "0000320193")

  test("AAPL parsed successfully",         !is.null(aapl_parsed))
  test("AAPL has > 100 rows",             nrow(aapl_parsed) > 100)
  test("AAPL has revenue data",           "revenue" %in% aapl_parsed$concept)
  test("AAPL has total_assets",           "total_assets" %in% aapl_parsed$concept)
  test("AAPL has net_income",             "net_income" %in% aapl_parsed$concept)
  test("AAPL has operating_cashflow",     "operating_cashflow" %in% aapl_parsed$concept)
  test("AAPL has capex",                  "capex" %in% aapl_parsed$concept)
  test("AAPL has stockholders_equity",    "stockholders_equity" %in% aapl_parsed$concept)

  # Dedup
  aapl_deduped <- dedup_fundamentals(aapl_parsed)

  test("AAPL dedup reduces rows",
       nrow(aapl_deduped) < nrow(aapl_parsed))
  test("AAPL dedup: no dups per concept+period+qtr",
       !anyDuplicated(aapl_deduped[, .(concept, period_end, fiscal_qtr)]))

  # Classify
  aapl_classified <- classify_period(aapl_deduped)
  test("AAPL has FY observations",        "FY" %in% aapl_classified$period_type)
  test("AAPL has quarterly observations",
       any(c("Q1", "Q2", "Q3", "Q4") %in% aapl_classified$period_type))

  # Spot-check: AAPL FY2023 revenue should be ~383B (fiscal year ending Sep 2023)
  aapl_rev_fy23 <- aapl_classified[concept == "revenue" & fiscal_year == 2023 &
                                     period_type == "FY", value]
  if (length(aapl_rev_fy23) > 0) {
    test("AAPL FY2023 revenue in plausible range (300B-500B)",
         aapl_rev_fy23 >= 300e9 && aapl_rev_fy23 <= 500e9)
  } else {
    message("  SKIP  AAPL FY2023 revenue (not found -- fiscal year mismatch)")
  }

  # AAPL total assets should be > 200B
  aapl_assets <- aapl_classified[concept == "total_assets" & fiscal_year >= 2022 &
                                   period_type == "FY", value]
  if (length(aapl_assets) > 0) {
    test("AAPL recent total assets > 200B",
         max(aapl_assets) > 200e9)
  }

  # period_type should only contain valid values
  test("AAPL period_type values are valid",
       all(aapl_classified[!is.na(period_type),
                           period_type %in% c("FY", "Q1", "Q2", "Q3", "Q4")]))
} else {
  message("  SKIP  AAPL integration tests (fetch failed)")
}


# ============================================================================
# INTEGRATION TESTS: Financial sector (JPM -- banks lack COGS)
# ============================================================================
message("\n=== Integration: Financial sector (JPM) ===")

jpm_facts <- fetch_companyfacts("0000019617")

test("JPM companyfacts fetched",           !is.null(jpm_facts))

if (!is.null(jpm_facts)) {
  jpm_parsed <- parse_companyfacts(jpm_facts, "JPM", "0000019617")

  test("JPM parsed successfully",          !is.null(jpm_parsed))
  test("JPM has revenue data",             "revenue" %in% jpm_parsed$concept)
  test("JPM has total_assets",             "total_assets" %in% jpm_parsed$concept)
  test("JPM has net_income",               "net_income" %in% jpm_parsed$concept)

  # Banks typically do not report COGS or inventory
  jpm_deduped <- dedup_fundamentals(jpm_parsed)
  has_cogs <- "cogs" %in% jpm_deduped$concept
  has_inventory <- "inventory" %in% jpm_deduped$concept
  message(sprintf("  INFO  JPM has COGS: %s, Inventory: %s (expected: mostly FALSE)",
                  has_cogs, has_inventory))
} else {
  message("  SKIP  JPM integration tests (fetch failed)")
}


# ============================================================================
# INTEGRATION TESTS: fetch_and_cache_ticker()
# ============================================================================
message("\n=== Integration: fetch_and_cache_ticker() ===")

test_cache_dir <- file.path(tempdir(), "test_fundamentals_cache")
if (dir.exists(test_cache_dir)) unlink(test_cache_dir, recursive = TRUE)

aapl_cached <- fetch_and_cache_ticker("AAPL", "0000320193",
                                       cache_dir = test_cache_dir)

test("fetch_and_cache returns data.table",
     is.data.table(aapl_cached))
test("cache file created",
     file.exists(file.path(test_cache_dir, "0000320193_AAPL.parquet")))
test("cached data has period_type column",
     "period_type" %in% names(aapl_cached))

# Second call should use cache (no network)
aapl_cached2 <- fetch_and_cache_ticker("AAPL", "0000320193",
                                        cache_dir = test_cache_dir)
test("cached load returns same data",
     identical(aapl_cached, aapl_cached2))

# force_refresh should re-fetch
aapl_refreshed <- fetch_and_cache_ticker("AAPL", "0000320193",
                                          cache_dir = test_cache_dir,
                                          force_refresh = TRUE)
test("force_refresh returns data.table",
     is.data.table(aapl_refreshed))
test("force_refresh has same row count (stable data)",
     nrow(aapl_refreshed) == nrow(aapl_cached))

# Clean up test cache
unlink(test_cache_dir, recursive = TRUE)


# ============================================================================
# INTEGRATION TESTS: get_fundamentals() reader
# ============================================================================
message("\n=== Integration: get_fundamentals() ===")

# Create test cache with known data
test_cache_dir2 <- file.path(tempdir(), "test_fundamentals_reader")
if (dir.exists(test_cache_dir2)) unlink(test_cache_dir2, recursive = TRUE)
dir.create(test_cache_dir2, recursive = TRUE)

test_dt <- data.table(
  ticker = "TEST", cik = "0000000001", concept = "revenue",
  tag = "Revenues", value = 1000000, period_end = as.Date("2023-12-31"),
  period_start = as.Date("2023-01-01"), filed = as.Date("2024-02-15"),
  form = "10-K", accession = "0001-24-000001", fiscal_year = 2023L,
  fiscal_qtr = "FY", unit = "USD"
)
arrow::write_parquet(test_dt, file.path(test_cache_dir2, "0000000001_TEST.parquet"))

loaded <- get_fundamentals("TEST", cik = "0000000001", cache_dir = test_cache_dir2)
test("get_fundamentals with CIK loads",    !is.null(loaded))
test("get_fundamentals data correct",       loaded[1, value] == 1000000)

loaded2 <- get_fundamentals("TEST", cache_dir = test_cache_dir2)
test("get_fundamentals without CIK loads",  !is.null(loaded2))

loaded3 <- get_fundamentals("NONEXISTENT", cache_dir = test_cache_dir2)
test("get_fundamentals returns NULL for missing", is.null(loaded3))

unlink(test_cache_dir2, recursive = TRUE)


# ============================================================================
# UNIT TESTS: coverage_report()
# ============================================================================
message("\n=== coverage_report() ===")

# Build minimal results for coverage testing
cov_results <- list(
  TICKER_A = data.table(
    concept = c("revenue", "total_assets", "net_income"),
    tag     = c("Revenues", "Assets", "NetIncomeLoss"),
    value   = c(100, 200, 50)
  ),
  TICKER_B = data.table(
    concept = c("revenue", "total_assets"),
    tag     = c("SalesRevenueNet", "Assets"),
    value   = c(150, 300)
  )
)

cov <- coverage_report(cov_results)

test("coverage_report returns list",
     is.list(cov))
test("coverage has concept_coverage",
     "concept_coverage" %in% names(cov))
test("coverage has ticker_coverage",
     "ticker_coverage" %in% names(cov))
test("coverage has detail",
     "detail" %in% names(cov))

test("revenue covered by 2 tickers",
     cov$concept_coverage[concept == "revenue", n_tickers_with_data] == 2)
test("net_income covered by 1 ticker",
     cov$concept_coverage[concept == "net_income", n_tickers_with_data] == 1)
test("cogs covered by 0 tickers",
     cov$concept_coverage[concept == "cogs", n_tickers_with_data] == 0)

test("TICKER_A has 3 concepts",
     cov$ticker_coverage[ticker == "TICKER_A", n_concepts] == 3)
test("TICKER_B has 2 concepts",
     cov$ticker_coverage[ticker == "TICKER_B", n_concepts] == 2)

# Tags-used should show both aliases for revenue
test("coverage tracks multiple aliases for revenue",
     grepl("Revenues", cov$concept_coverage[concept == "revenue", tags_used]) &&
     grepl("SalesRevenueNet", cov$concept_coverage[concept == "revenue", tags_used]))

test("coverage_report returns NULL for empty input",
     is.null(coverage_report(list())))


# ============================================================================
# GATE TESTS: 20-ticker prototype output (run after prototype_20_tickers)
# ============================================================================
message("\n=== GATE TESTS (Session C) ===")

cache_dir <- "cache/fundamentals"
if (dir.exists(cache_dir)) {
  parquet_files <- list.files(cache_dir, pattern = "\\.parquet$", full.names = TRUE)

  if (length(parquet_files) >= 15) {
    message(sprintf("  Found %d cached parquet files", length(parquet_files)))

    # Gate 1: At least 20 prototype tickers fetched (more after full build)
    test(">= 20 cached ticker files", length(parquet_files) >= 20)

    # Gate 2: Validate schema of each file
    schema_ok <- TRUE
    required_cols <- c("ticker", "cik", "concept", "tag", "value",
                       "period_end", "filed", "form", "accession",
                       "fiscal_year", "fiscal_qtr", "unit")
    for (pf in parquet_files) {
      dt <- tryCatch(as.data.table(arrow::read_parquet(pf)), error = function(e) NULL)
      if (is.null(dt) || !all(required_cols %in% names(dt))) {
        schema_ok <- FALSE
        message(sprintf("    Schema invalid: %s", basename(pf)))
      }
    }
    test("all cached files have correct schema", schema_ok)

    # Gate 3: No duplicate (concept, period_end, fiscal_qtr) in any file
    dedup_ok <- TRUE
    for (pf in parquet_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      if (anyDuplicated(dt[, .(concept, period_end, fiscal_qtr)])) {
        dedup_ok <- FALSE
        message(sprintf("    Dedup violation: %s", basename(pf)))
      }
    }
    test("no dedup violations in cached files", dedup_ok)

    # Gate 4: Key concepts present across most tickers
    concept_counts <- list()
    for (pf in parquet_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      for (cn in unique(dt$concept)) {
        concept_counts[[cn]] <- (concept_counts[[cn]] %||% 0L) + 1L
      }
    }

    n_files <- length(parquet_files)
    core_concepts <- c("revenue", "net_income", "total_assets",
                       "operating_cashflow", "stockholders_equity")
    for (cn in core_concepts) {
      count <- concept_counts[[cn]] %||% 0L
      pct <- round(count / n_files * 100)
      test(sprintf("%s coverage >= 80%% (%d/%d = %d%%)", cn, count, n_files, pct),
           count / n_files >= 0.80)
    }

    # Gate 5: Filed dates are plausible (1993+ for XBRL era, mostly 2009+)
    date_ok <- TRUE
    for (pf in parquet_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      filed_dates <- dt[!is.na(filed), filed]
      if (length(filed_dates) > 0) {
        if (min(filed_dates) < as.Date("1993-01-01") ||
            max(filed_dates) > Sys.Date() + 30) {
          date_ok <- FALSE
          message(sprintf("    Implausible dates: %s (range %s to %s)",
                          basename(pf), min(filed_dates), max(filed_dates)))
        }
      }
    }
    test("all filed dates in plausible range", date_ok)

    # Gate 6: Values are numeric and finite (no Inf, no NaN)
    values_ok <- TRUE
    for (pf in parquet_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      if (any(!is.finite(dt$value))) {
        values_ok <- FALSE
        message(sprintf("    Non-finite values: %s", basename(pf)))
      }
    }
    test("all values are finite", values_ok)

    # Gate 7: period_type only contains valid values or NA
    period_type_ok <- TRUE
    valid_period_types <- c("FY", "Q1", "Q2", "Q3", "Q4")
    for (pf in parquet_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      if ("period_type" %in% names(dt)) {
        non_na <- dt[!is.na(period_type), period_type]
        if (length(non_na) > 0 && !all(non_na %in% valid_period_types)) {
          period_type_ok <- FALSE
          bad <- setdiff(unique(non_na), valid_period_types)
          message(sprintf("    Invalid period_type in %s: %s",
                          basename(pf), paste(bad, collapse = ", ")))
        }
      }
    }
    test("all period_type values are valid", period_type_ok)

    # Gate 8: Vast majority of ticker files have both FY and quarterly data
    # Some removed constituents (foreign filers, short stints) may lack Q data
    fy_q_count <- 0L
    for (pf in parquet_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      if ("period_type" %in% names(dt)) {
        has_fy <- "FY" %in% dt$period_type
        has_q  <- any(c("Q1", "Q2", "Q3") %in% dt$period_type)
        if (has_fy && has_q) fy_q_count <- fy_q_count + 1L
      }
    }
    fy_q_pct <- round(fy_q_count / n_files * 100, 1)
    test(sprintf(">= 95%% tickers have both FY and Q data (%d/%d = %.1f%%)",
                 fy_q_count, n_files, fy_q_pct),
         fy_q_pct >= 95)

    # Gate 9: Spot-check known ticker file names
    expected_files <- c("0000320193_AAPL.parquet", "0000019617_JPM.parquet",
                        "0000789019_MSFT.parquet")
    for (ef in expected_files) {
      test(sprintf("file exists: %s", ef),
           file.exists(file.path(cache_dir, ef)))
    }

  } else {
    message(sprintf("  SKIP gate tests: only %d cached files (need >= 15). Run prototype_20_tickers() first.",
                    length(parquet_files)))
  }
} else {
  message("  SKIP gate tests: cache/fundamentals/ does not exist. Run prototype_20_tickers() first.")
}


# ============================================================================
# UNIT TESTS: fetch_fundamentals_batch() chunking (Session D)
# ============================================================================
message("\n=== Chunked batch execution ===")

# Use the existing prototype cache to test chunking behavior
# Create a small test set with known cached tickers
chunk_test_tickers <- c("AAPL", "MSFT", "NVDA", "JPM", "AMZN", "WMT")
chunk_test_ciks    <- c("0000320193", "0000789019", "0001045810",
                        "0000019617", "0001018724", "0000104169")

# chunk_size=2 should split 6 tickers into 3 chunks
chunk_result <- fetch_fundamentals_batch(
  tickers    = chunk_test_tickers,
  ciks       = chunk_test_ciks,
  cache_dir  = "cache/fundamentals",
  chunk_size = 2L
)

test("chunked batch returns all 6 tickers",
     length(chunk_result$success) == 6)
test("chunked batch all from cache",
     length(chunk_result$cached) == 6)
test("chunked batch no failures",
     length(chunk_result$failed) == 0)

# chunk_size=NULL (no chunking) should also work
no_chunk_result <- fetch_fundamentals_batch(
  tickers    = chunk_test_tickers[1:3],
  ciks       = chunk_test_ciks[1:3],
  cache_dir  = "cache/fundamentals",
  chunk_size = NULL
)
test("no-chunk batch returns 3 tickers",
     length(no_chunk_result$success) == 3)

# chunk_size larger than n should work (single chunk)
big_chunk_result <- fetch_fundamentals_batch(
  tickers    = chunk_test_tickers[1:2],
  ciks       = chunk_test_ciks[1:2],
  cache_dir  = "cache/fundamentals",
  chunk_size = 100L
)
test("oversized chunk_size works",
     length(big_chunk_result$success) == 2)


# ============================================================================
# UNIT TESTS: validate_fundamentals_build() (Session D)
# ============================================================================
message("\n=== validate_fundamentals_build() ===")

# Create a temp cache with known data to test validation
val_test_dir <- file.path(tempdir(), "test_validate_build")
if (dir.exists(val_test_dir)) unlink(val_test_dir, recursive = TRUE)
dir.create(val_test_dir, recursive = TRUE)

# Write two test parquet files
dt1 <- data.table(
  ticker = "AAA", cik = "0000000001",
  concept = c("revenue", "total_assets", "net_income"),
  tag = c("Revenues", "Assets", "NetIncomeLoss"),
  value = c(1e9, 5e9, 1e8),
  period_end = as.Date("2023-12-31"),
  period_start = as.Date(c("2023-01-01", NA, "2023-01-01")),
  filed = as.Date("2024-02-15"),
  form = "10-K", accession = "0001-24-000001",
  fiscal_year = 2023L, fiscal_qtr = c("FY", "FY", "FY"),
  unit = "USD", period_type = "FY"
)
dt2 <- data.table(
  ticker = "BBB", cik = "0000000002",
  concept = c("revenue", "total_assets"),
  tag = c("Revenues", "Assets"),
  value = c(2e9, 10e9),
  period_end = as.Date("2023-12-31"),
  period_start = as.Date(c("2023-01-01", NA)),
  filed = as.Date("2024-03-01"),
  form = "10-K", accession = "0002-24-000001",
  fiscal_year = 2023L, fiscal_qtr = c("FY", "FY"),
  unit = "USD", period_type = "FY"
)
arrow::write_parquet(dt1, file.path(val_test_dir, "0000000001_AAA.parquet"))
arrow::write_parquet(dt2, file.path(val_test_dir, "0000000002_BBB.parquet"))

val_result <- validate_fundamentals_build(
  cache_dir = val_test_dir,
  master_path = "nonexistent.parquet"  # skip master gap check
)

test("validate returns list",
     is.list(val_result))
test("validate summary has n_tickers",
     val_result$summary$n_tickers == 2)
test("validate summary has total_rows",
     val_result$summary$total_rows == 5)
test("validate concept_coverage is data.table",
     is.data.table(val_result$concept_coverage))
test("validate: revenue covered by 2 tickers",
     val_result$concept_coverage[concept == "revenue", n_tickers] == 2)
test("validate: net_income covered by 1 ticker",
     val_result$concept_coverage[concept == "net_income", n_tickers] == 1)
test("validate ticker_stats has 2 rows",
     nrow(val_result$ticker_stats) == 2)
test("validate: empty cache returns NULL",
     is.null(validate_fundamentals_build(cache_dir = file.path(tempdir(), "empty_dir_xxx"))))

unlink(val_test_dir, recursive = TRUE)


# ============================================================================
# GATE TESTS: Full build (Session D -- run after build_fundamentals())
# ============================================================================
message("\n=== GATE TESTS (Session D) ===")

cache_dir_d <- "cache/fundamentals"
if (dir.exists(cache_dir_d)) {
  parquet_files_d <- list.files(cache_dir_d, pattern = "[.]parquet$", full.names = TRUE)

  if (length(parquet_files_d) >= 500) {
    message(sprintf("  Found %d cached parquet files (full build)", length(parquet_files_d)))

    # Gate D1: At least 600 tickers fetched (647 unique CIKs, some may fail)
    test("full build: >= 600 cached files", length(parquet_files_d) >= 600)

    # Gate D2: Schema consistent across a random sample
    set.seed(42)
    sample_files <- sample(parquet_files_d, min(50, length(parquet_files_d)))
    schema_ok_d <- TRUE
    required_cols_d <- c("ticker", "cik", "concept", "tag", "value",
                         "period_end", "filed", "form", "accession",
                         "fiscal_year", "fiscal_qtr", "unit", "period_type")
    for (pf in sample_files) {
      dt <- tryCatch(as.data.table(arrow::read_parquet(pf)), error = function(e) NULL)
      if (is.null(dt) || !all(required_cols_d %in% names(dt))) {
        schema_ok_d <- FALSE
        message(sprintf("    Schema invalid: %s", basename(pf)))
      }
    }
    test("full build: schema correct (50-file sample)", schema_ok_d)

    # Gate D3: No dedup violations in sample
    dedup_ok_d <- TRUE
    for (pf in sample_files) {
      dt <- as.data.table(arrow::read_parquet(pf))
      if (anyDuplicated(dt[, .(concept, period_end, fiscal_qtr)])) {
        dedup_ok_d <- FALSE
        message(sprintf("    Dedup violation: %s", basename(pf)))
      }
    }
    test("full build: no dedup violations (50-file sample)", dedup_ok_d)

    # Gate D4: Core concepts >= 90% coverage across full corpus
    concept_counts_d <- list()
    for (pf in parquet_files_d) {
      dt <- as.data.table(arrow::read_parquet(pf))
      for (cn in unique(dt$concept)) {
        concept_counts_d[[cn]] <- (concept_counts_d[[cn]] %||% 0L) + 1L
      }
    }

    n_files_d <- length(parquet_files_d)
    core_concepts_d <- c("revenue", "net_income", "total_assets",
                         "stockholders_equity", "operating_cashflow")
    for (cn in core_concepts_d) {
      count <- concept_counts_d[[cn]] %||% 0L
      pct <- round(count / n_files_d * 100)
      test(sprintf("full build: %s >= 90%% (%d/%d = %d%%)", cn, count, n_files_d, pct),
           count / n_files_d >= 0.90)
    }

    # Gate D5: validate_fundamentals_build() runs without error
    val_full <- tryCatch(validate_fundamentals_build(), error = function(e) NULL)
    test("full build: validate_fundamentals_build() succeeds",
         !is.null(val_full))

    if (!is.null(val_full)) {
      test("full build: median concepts/ticker >= 20",
           median(val_full$ticker_stats$n_concepts) >= 20)
      test("full build: total size > 5 MB",
           val_full$summary$total_mb > 5)
    }

  } else {
    message(sprintf("  SKIP Session D gate tests: only %d files (need >= 500). Run build_fundamentals() first.",
                    length(parquet_files_d)))
  }
} else {
  message("  SKIP Session D gate tests: cache/fundamentals/ does not exist.")
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
