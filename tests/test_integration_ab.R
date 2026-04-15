# ============================================================================
# test_integration_ab.R  --  Integration Tests: Session A + Session B
# ============================================================================
# Run: Rscript tests/test_integration_ab.R
#
# Validates that constituent_master.parquet and sector_industry.parquet
# are consistent and join correctly, as downstream modules will use them.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")
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
# SETUP: Load both outputs
# ============================================================================
message("\n=== Loading outputs ===")

master_path <- "cache/lookups/constituent_master.parquet"
sector_path <- "cache/lookups/sector_industry.parquet"

stopifnot(file.exists(master_path))
stopifnot(file.exists(sector_path))

master <- as.data.table(arrow::read_parquet(master_path))
sectors <- get_sector_lookup(sector_path)

message(sprintf("  master: %d rows, %d unique tickers", nrow(master), uniqueN(master$ticker)))
message(sprintf("  sectors: %d rows", nrow(sectors)))


# ============================================================================
# 1. JOIN INTEGRITY
# ============================================================================
message("\n=== Join Integrity ===")

# Inner join on ticker
joined <- merge(master, sectors, by = "ticker", all = FALSE)
test("join produces rows",                  nrow(joined) > 0)
test("no duplicate rows after join",
     nrow(joined) == nrow(master[ticker %in% sectors$ticker]))

# Active tickers: every one must have a sector
active <- master[status == "ACTIVE"]
active_joined <- merge(active, sectors, by = "ticker", all.x = TRUE)

test("all active tickers have sector data",
     all(!is.na(active_joined$sector)))
test("all active tickers have industry data",
     all(!is.na(active_joined$industry)))
test("503 active rows after join",          nrow(active_joined) == 503)

# No active ticker should be left unmatched
active_missing <- active_joined[is.na(sector)]
test("zero active tickers without sector",  nrow(active_missing) == 0)
if (nrow(active_missing) > 0) {
  message("  Missing: ", paste(active_missing$ticker, collapse = ", "))
}


# ============================================================================
# 2. SECTOR DISTRIBUTION BY STATUS
# ============================================================================
message("\n=== Sector Distribution by Status ===")

# Active tickers should have all 11 sectors represented
active_sectors <- active_joined[, uniqueN(sector)]
test("active covers all 11 sectors",       active_sectors == 11)

# Print distribution
active_dist <- active_joined[, .N, by = sector][order(-N)]
message("  Active sector distribution:")
for (i in seq_len(nrow(active_dist))) {
  message(sprintf("    %s: %d", active_dist$sector[i], active_dist$N[i]))
}

# No sector should dominate (> 30% of active)
test("no sector > 30% of active",
     all(active_dist$N / nrow(active_joined) < 0.30))


# ============================================================================
# 3. FINANCIAL SECTOR IDENTIFICATION
# ============================================================================
message("\n=== Financial Sector Identification ===")

# This is critical: indicator_compute.R will set certain indicators to NA
# for financials, so the sector join must correctly identify them.
financials <- active_joined[sector == "Financial"]
test("financials identified",               nrow(financials) > 0)
test("JPM is financial",                    "JPM" %in% financials$ticker)
test("GS is financial",                     "GS" %in% financials$ticker)
test("BAC is financial",                    "BAC" %in% financials$ticker)
test("BRK.B is financial",                  "BRK.B" %in% financials$ticker)
test("AAPL is NOT financial",               !("AAPL" %in% financials$ticker))
test("XOM is NOT financial",                !("XOM" %in% financials$ticker))

n_fin <- nrow(financials)
message(sprintf("  %d active financials (%.1f%%)",
                n_fin, 100 * n_fin / nrow(active_joined)))

# Reasonable range: ~60-90 financials in S&P 500
test("financial count in range [50, 100]",  n_fin >= 50 && n_fin <= 100)


# ============================================================================
# 4. CROSS-CHECK KNOWN TICKERS
# ============================================================================
message("\n=== Cross-Check Known Tickers ===")

# Spot check: ticker -> (status, sector) must be consistent
check <- function(tk, expected_status, expected_sector) {
  m <- master[ticker == tk & occurrence == max(master[ticker == tk, occurrence])]
  s <- sectors[ticker == tk]
  label <- sprintf("%s: status=%s, sector=%s", tk, expected_status, expected_sector)

  if (nrow(m) == 0) {
    test(paste(label, "(in master)"), FALSE)
    return(invisible(NULL))
  }
  test(paste(label, "(status)"),  m$status == expected_status)

  if (nrow(s) > 0) {
    test(paste(label, "(sector)"),  s$sector == expected_sector)
  } else if (expected_status == "ACTIVE") {
    test(paste(label, "(has sector)"), FALSE)
  }
}

check("AAPL",  "ACTIVE", "Technology")
check("MSFT",  "ACTIVE", "Technology")
check("AMZN",  "ACTIVE", "Consumer Cyclical")
check("JPM",   "ACTIVE", "Financial")
check("JNJ",   "ACTIVE", "Healthcare")
check("XOM",   "ACTIVE", "Energy")
check("PG",    "ACTIVE", "Consumer Defensive")
check("NEE",   "ACTIVE", "Utilities")
check("PLD",   "ACTIVE", "Real Estate")
check("LIN",   "ACTIVE", "Basic Materials")
check("GOOG",  "ACTIVE", "Communication Services")
check("CAT",   "ACTIVE", "Industrials")


# ============================================================================
# 5. DUPLICATE TICKER HANDLING
# ============================================================================
message("\n=== Duplicate Ticker Handling ===")

# Duplicate tickers in master have multiple rows (occurrence 1, 2).
# Sector lookup has one row per ticker. The join should produce one
# sector per ticker, applied to all occurrences.
dup_tickers <- master[, .N, by = ticker][N > 1, ticker]
test("duplicate tickers exist in master",   length(dup_tickers) > 0)

for (tk in head(dup_tickers, 5)) {
  master_rows <- master[ticker == tk]
  sector_rows <- sectors[ticker == tk]
  label <- sprintf("dup %s: %d master rows, %d sector rows",
                   tk, nrow(master_rows), nrow(sector_rows))
  # Sector should have at most 1 row per ticker
  test(label, nrow(sector_rows) <= 1)
}


# ============================================================================
# 6. CIK + SECTOR COMPLETENESS FOR ACTIVE
# ============================================================================
message("\n=== Active Ticker Completeness ===")

# For downstream modules (fundamental_fetcher, indicator_compute),
# every active ticker needs both CIK (to fetch EDGAR data) and
# sector (for financial sector handling).
complete <- active_joined[!is.na(cik) & !is.na(sector)]
test("all 503 active have both CIK and sector",
     nrow(complete) == 503)

incomplete <- active_joined[is.na(cik) | is.na(sector)]
if (nrow(incomplete) > 0) {
  message(sprintf("  WARNING: %d active tickers incomplete:", nrow(incomplete)))
  print(incomplete[, .(ticker, cik_ok = !is.na(cik), sector_ok = !is.na(sector))])
}


# ============================================================================
# 7. DATA TYPE CONSISTENCY
# ============================================================================
message("\n=== Data Type Consistency ===")

test("master ticker is character",         is.character(master$ticker))
test("sectors ticker is character",        is.character(sectors$ticker))
test("master cik is character",            is.character(master$cik))
test("sectors sector is character",        is.character(sectors$sector))
test("sectors industry is character",      is.character(sectors$industry))
test("master date_added is Date",          inherits(master$date_added, "Date"))
test("master date_removed is Date",        inherits(master$date_removed, "Date"))


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
