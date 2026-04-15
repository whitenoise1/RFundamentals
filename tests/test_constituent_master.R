# ============================================================================
# test_constituent_master.R  --  Unit + Gate Tests for Module 1
# ============================================================================
# Run: Rscript tests/test_constituent_master.R
#
# Unit tests: validate each function's contract in isolation.
# Gate tests: validate the final parquet output against Session A checkpoints.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")

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
# UNIT TESTS: load_raw_constituents()
# ============================================================================
message("\n=== load_raw_constituents() ===")

raw <- load_raw_constituents("data/sp500_constituents_.csv")

test("returns data.table",         is.data.table(raw))
test("845 rows",                   nrow(raw) == 845)
test("has ticker column",          "ticker" %in% names(raw))
test("has cik column",             "cik" %in% names(raw))
test("has occurrence column",      "occurrence" %in% names(raw))
test("has date_added column",      "date_added" %in% names(raw))
test("has date_removed column",    "date_removed" %in% names(raw))
test("ticker is character",        is.character(raw$ticker))
test("cik is character",           is.character(raw$cik))
test("date_added is Date",         inherits(raw$date_added, "Date"))
test("date_removed is Date",       inherits(raw$date_removed, "Date"))
test("occurrence is integer",      is.integer(raw$occurrence))
test("no empty tickers",           all(nchar(raw$ticker) > 0))
test("829 unique tickers",         uniqueN(raw$ticker) == 829)
test("occurrence range [1,2]",     all(raw$occurrence %in% c(1L, 2L)))
test("505 have CIK in CSV",        sum(!is.na(raw$cik)) == 505)


# ============================================================================
# UNIT TESTS: classify_duplicates()
# ============================================================================
message("\n=== classify_duplicates() ===")

dt_dup <- classify_duplicates(raw)

test("adds duplicate_class column",  "duplicate_class" %in% names(dt_dup))
test("3 distinct values (incl NA)",
     setequal(unique(dt_dup$duplicate_class),
              c(NA_character_, "SAME_COMPANY", "DIFFERENT_COMPANY")))

# Category A tickers
cat_a <- c("AMD", "CEG", "DOW", "DD", "EQT", "FSLR", "JBL",
           "MXIM", "PCG", "TER", "DELL")
cat_b <- c("GAS", "OI", "CBE", "Q", "AGN")

test("all Cat A classified SAME_COMPANY",
     all(dt_dup[ticker %in% cat_a & occurrence == 2, duplicate_class] == "SAME_COMPANY"))
test("all Cat B classified DIFFERENT_COMPANY",
     all(dt_dup[ticker %in% cat_b & occurrence == 2, duplicate_class] == "DIFFERENT_COMPANY"))
test("Cat A occurrence 1 also classified",
     all(dt_dup[ticker %in% cat_a & occurrence == 1, duplicate_class] == "SAME_COMPANY"))
test("Cat B occurrence 1 also classified",
     all(dt_dup[ticker %in% cat_b & occurrence == 1, duplicate_class] == "DIFFERENT_COMPANY"))

# Every ticker with occurrence > 1 must be classified
dup_tickers <- dt_dup[occurrence > 1, unique(ticker)]
test("16 duplicate tickers exist",  length(dup_tickers) == 16)
test("all dup tickers classified",
     all(dt_dup[ticker %in% dup_tickers, !is.na(duplicate_class)]))

# Non-duplicate tickers should have NA
test("singletons have NA duplicate_class",
     all(is.na(dt_dup[!(ticker %in% c(cat_a, cat_b)), duplicate_class])))


# ============================================================================
# UNIT TESTS: assign_status()
# ============================================================================
message("\n=== assign_status() ===")

dt_status <- assign_status(dt_dup)

test("adds status column",     "status" %in% names(dt_status))
test("adds successor column",  "successor" %in% names(dt_status))
test("no NA status",           all(!is.na(dt_status$status)))

valid_statuses <- c("ACTIVE", "REMOVED_ACQUIRED", "REMOVED_PRIVATE",
                     "REMOVED_BANKRUPT", "REMOVED_TICKER_CHANGE",
                     "REMOVED_DOWNGRADED")
test("all statuses are valid",
     all(dt_status$status %in% valid_statuses))

test("active = no date_removed",
     all(dt_status[status == "ACTIVE", is.na(date_removed)]))
test("removed = has date_removed",
     all(dt_status[status != "ACTIVE", !is.na(date_removed)]))

# Known specific cases
test("DLPH -> APTV ticker change",
     dt_status[ticker == "DLPH" & occurrence == 1, successor] == "APTV")
test("DISCA -> WBD ticker change",
     dt_status[ticker == "DISCA", successor] == "WBD")
test("SIVB is bankrupt",
     dt_status[ticker == "SIVB" & !is.na(date_removed), status] == "REMOVED_BANKRUPT")
test("DELL occ 1 went private",
     dt_status[ticker == "DELL" & occurrence == 1, status] == "REMOVED_PRIVATE")

# Successor only for ticker changes
test("successor NA for non-ticker-change",
     all(is.na(dt_status[status != "REMOVED_TICKER_CHANGE", successor])))
test("successor non-NA for ticker changes",
     all(!is.na(dt_status[status == "REMOVED_TICKER_CHANGE", successor])))


# ============================================================================
# GATE TESTS: Final parquet output (Session A checkpoints)
# ============================================================================
message("\n=== GATE TESTS (Session A) ===")

pq_path <- "cache/lookups/constituent_master.parquet"
test("parquet file exists", file.exists(pq_path))

dt <- as.data.table(arrow::read_parquet(pq_path))

test("845 rows in parquet",       nrow(dt) == 845)
test("9 columns in parquet",      ncol(dt) == 9)
test("correct column names",
     setequal(names(dt),
              c("ticker", "name", "cik", "date_added", "date_removed",
                "status", "successor", "occurrence", "duplicate_class")))

# Gate 1: All 503 current constituents have non-NA CIK
current <- dt[status == "ACTIVE"]
test("503 active constituents",         nrow(current) == 503)
test("ALL 503 current have CIK",        sum(!is.na(current$cik)) == 503)
test("no empty-string CIKs in current", all(nchar(current$cik) == 10))

# Gate 2: All 16 duplicate tickers classified
dup_tks <- dt[occurrence > 1, unique(ticker)]
test("16 duplicate tickers",            length(dup_tks) == 16)
test("all dup rows have class",
     all(!is.na(dt[ticker %in% dup_tks, duplicate_class])))

# Gate 3: No duplicate (ticker, occurrence) pairs
test("no duplicate (ticker, occurrence)",
     !anyDuplicated(dt[, .(ticker, occurrence)]))

# Gate 4: CIK format -- all resolved CIKs are 10-digit zero-padded
resolved <- dt[!is.na(cik)]
test("CIKs are 10-char zero-padded",
     all(nchar(resolved$cik) == 10) && all(grepl("^[0-9]{10}$", resolved$cik)))

# Gate 5: Status coverage
test("342 removed constituents",
     dt[status != "ACTIVE", .N] == 342)
test("all removed have a REMOVED_ status",
     all(grepl("^REMOVED_", dt[status != "ACTIVE", status])))

# Gate 6: Date integrity
test("no future date_added",
     all(dt[!is.na(date_added), date_added] <= Sys.Date()))
test("no future date_removed",
     all(dt[!is.na(date_removed), date_removed] <= Sys.Date()))
test("date_removed > date_added where both exist",
     all(dt[!is.na(date_added) & !is.na(date_removed),
            date_removed > date_added]))

# Gate 7: Cross-check known tickers
test("AAPL is active",
     dt[ticker == "AAPL", status] == "ACTIVE")
test("AAPL has CIK 0000320193",
     dt[ticker == "AAPL", cik] == "0000320193")
test("MSFT is active",
     dt[ticker == "MSFT", status] == "ACTIVE")
test("JPM is active",
     dt[ticker == "JPM", status] == "ACTIVE")
test("ABMD is removed",
     dt[ticker == "ABMD", status] != "ACTIVE")


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
