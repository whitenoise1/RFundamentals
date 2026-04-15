# ============================================================================
# constituent_master.R  --  S&P 500 Constituent Roster (Module 1)
# ============================================================================
# Clean, deduplicate, enrich sp500_constituents_.csv.
# Resolve all CIKs. Classify duplicate tickers. Assign removal status.
# Output: cache/lookups/constituent_master.parquet
#
# Output schema:
#   ticker          chr   Canonical ticker symbol
#   name            chr   Company name
#   cik             chr   10-digit zero-padded CIK
#   date_added      Date  Date added to S&P 500 (NA = original member)
#   date_removed    Date  Date removed (NA = current member)
#   status          chr   ACTIVE | REMOVED_ACQUIRED | REMOVED_PRIVATE |
#                         REMOVED_BANKRUPT | REMOVED_DOWNGRADED |
#                         REMOVED_TICKER_CHANGE
#   successor       chr   Successor ticker if TICKER_CHANGE, else NA
#   occurrence      int   Which occurrence (for duplicate tickers)
#   duplicate_class chr   SAME_COMPANY | DIFFERENT_COMPANY | NA
#
# Dependencies: data.table, arrow, jsonlite, httr
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(jsonlite)
  library(httr)
})


# =============================================================================
# CONSTANTS
# =============================================================================

.EDGAR_BASE     <- "https://data.sec.gov"
.EDGAR_UA       <- Sys.getenv("EDGAR_UA", "BSTAR/1.0 contact@email.com")
.EDGAR_RATE_SEC <- as.numeric(Sys.getenv("EDGAR_RATE_SEC", "0.11"))


# =============================================================================
# PRIVATE HELPERS
# =============================================================================

.assert_output <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}

.pad_cik <- function(cik) {
  formatC(as.integer(cik), width = 10, flag = "0")
}

.edgar_fetch <- function(url, retries = 3L) {
  for (attempt in seq_len(retries)) {
    resp <- tryCatch({
      httr::GET(url, httr::add_headers(`User-Agent` = .EDGAR_UA),
                httr::timeout(30))
    }, error = function(e) NULL)

    if (!is.null(resp) && httr::status_code(resp) == 200) {
      Sys.sleep(.EDGAR_RATE_SEC)
      return(httr::content(resp, as = "text", encoding = "UTF-8"))
    }

    if (!is.null(resp) && httr::status_code(resp) == 429) {
      Sys.sleep(2^attempt)
      next
    }

    if (attempt < retries) Sys.sleep(2^attempt)
  }
  NULL
}


# =============================================================================
# 1. load_raw_constituents()
# =============================================================================
#' Read and parse sp500_constituents_.csv
#' @param path Character. Path to CSV file.
#' @return data.table with cleaned columns and proper types.
load_raw_constituents <- function(path = "data/sp500_constituents_.csv") {

  raw <- fread(path, stringsAsFactors = FALSE, na.strings = c("NA", ""))

  # Drop the row-number column if present
  if ("V1" %in% names(raw) || "X" %in% names(raw)) {
    drop_col <- intersect(c("V1", "X"), names(raw))
    raw[, (drop_col) := NULL]
  }

  # Standardize column names
  expected <- c("ticker", "name", "cik", "occurrence",
                "date_added", "date_removed",
                "name_when_added", "name_when_removed")
  setnames(raw, names(raw), expected[seq_len(ncol(raw))])

  # Type coercion
  raw[, cik          := as.character(cik)]
  raw[, date_added   := as.Date(date_added)]
  raw[, date_removed := as.Date(date_removed)]
  raw[, occurrence   := as.integer(occurrence)]
  raw[, ticker       := trimws(toupper(ticker))]

  .assert_output(raw, "load_raw_constituents", list(
    "is data.table"       = is.data.table,
    "has ticker col"      = function(x) "ticker" %in% names(x),
    "has cik col"         = function(x) "cik" %in% names(x),
    "has occurrence col"  = function(x) "occurrence" %in% names(x),
    "rows > 800"          = function(x) nrow(x) > 800,
    "no empty tickers"    = function(x) all(nchar(x$ticker) > 0)
  ))

  message(sprintf("load_raw_constituents: %d rows, %d unique tickers, %d with CIK",
                  nrow(raw), uniqueN(raw$ticker), sum(!is.na(raw$cik))))
  raw
}


# =============================================================================
# 2. resolve_all_ciks()
# =============================================================================
#' Resolve CIKs for all tickers via EDGAR company_tickers.json
#'
#' Pass 1: Use existing CIK column from CSV.
#' Pass 2: Fetch EDGAR company_tickers.json for unresolved.
#' Pass 3: Manual overrides for known edge cases.
#'
#' @param dt data.table from load_raw_constituents().
#' @return data.table with cik column filled (10-digit zero-padded).
resolve_all_ciks <- function(dt) {

  dt <- copy(dt)

  # -- Pass 1: pad existing CIKs --
  has_cik <- !is.na(dt$cik)
  dt[has_cik, cik := .pad_cik(cik)]
  n_pass1 <- sum(has_cik)
  message(sprintf("resolve_all_ciks: pass 1 (CSV seed) -- %d CIKs", n_pass1))

  # -- Pass 2: EDGAR company_tickers.json --
  missing_idx <- which(is.na(dt$cik))
  if (length(missing_idx) > 0) {
    message(sprintf("resolve_all_ciks: pass 2 -- fetching company_tickers.json for %d tickers...",
                    length(missing_idx)))

    json_text <- .edgar_fetch("https://www.sec.gov/files/company_tickers.json")

    if (!is.null(json_text)) {
      ct <- tryCatch(jsonlite::fromJSON(json_text, simplifyVector = FALSE),
                     error = function(e) NULL)

      if (!is.null(ct)) {
        # Build ticker -> CIK lookup from EDGAR
        edgar_map <- list()
        for (entry in ct) {
          tk <- toupper(trimws(entry$ticker))
          edgar_map[[tk]] <- list(
            cik  = .pad_cik(entry$cik_str),
            name = entry$title
          )
        }

        n_matched <- 0L
        for (i in missing_idx) {
          tk <- dt$ticker[i]
          if (tk %in% names(edgar_map)) {
            dt$cik[i] <- edgar_map[[tk]]$cik
            # Fill name if missing
            if (is.na(dt$name[i]) || nchar(dt$name[i]) == 0) {
              dt$name[i] <- edgar_map[[tk]]$name
            }
            n_matched <- n_matched + 1L
          }
        }
        message(sprintf("resolve_all_ciks: pass 2 -- matched %d from EDGAR", n_matched))
      }
    } else {
      warning("resolve_all_ciks: failed to fetch company_tickers.json")
    }
  }

  # -- Pass 3: Manual overrides for known edge cases --
  # These are tickers that EDGAR maps differently (historical names,
  # ticker reuse, or special situations).
  manual <- list(
    # CDAY = Ceridian HCM, now Dayforce Inc.
    # EDGAR lists under ticker DAY, not CDAY
    "CDAY" = "1725057",
    # ANTM -> ELV (Anthem renamed to Elevance Health)
    "ANTM" = "1156039",
    # FB -> META
    "FB"   = "1326801",
    # FISV
    "FISV" = "798354"
  )

  still_missing <- which(is.na(dt$cik))
  for (i in still_missing) {
    tk <- dt$ticker[i]
    if (tk %in% names(manual)) {
      dt$cik[i] <- .pad_cik(manual[[tk]])
    }
  }

  n_pass3 <- sum(!is.na(dt$cik)) - n_pass1 - ifelse(exists("n_matched"), n_matched, 0)
  message(sprintf("resolve_all_ciks: pass 3 (manual) -- %d overrides", max(0, n_pass3)))

  # Summary
  n_resolved <- sum(!is.na(dt$cik))
  n_missing  <- sum(is.na(dt$cik))
  current <- dt[is.na(date_removed)]
  n_current_missing <- sum(is.na(current$cik))

  message(sprintf("resolve_all_ciks: %d/%d resolved total, %d current constituents missing CIK",
                  n_resolved, nrow(dt), n_current_missing))

  if (n_current_missing > 0) {
    missing_current <- current[is.na(cik), .(ticker, name, occurrence)]
    message("  Missing CIK (current): ",
            paste(missing_current$ticker, collapse = ", "))
  }

  dt
}


# =============================================================================
# 3. classify_duplicates()
# =============================================================================
#' Classify the 16 duplicate tickers as SAME_COMPANY or DIFFERENT_COMPANY
#'
#' Category A (SAME_COMPANY): same company removed then re-added.
#' Category B (DIFFERENT_COMPANY): different company reused the same ticker.
#'
#' @param dt data.table with ticker and occurrence columns.
#' @return data.table with duplicate_class column added.
classify_duplicates <- function(dt) {

  dt <- copy(dt)
  dt[, duplicate_class := NA_character_]

  # Category A: same company, re-added (consolidate into one series)
  cat_a <- c("AMD", "CEG", "DOW", "DD", "EQT", "FSLR", "JBL",
             "MXIM", "PCG", "TER", "DELL")

  # Category B: different company, same ticker (split by date range)
  cat_b <- c("GAS", "OI", "CBE", "Q", "AGN")

  dt[ticker %in% cat_a & occurrence > 0, duplicate_class := "SAME_COMPANY"]
  dt[ticker %in% cat_b & occurrence > 0, duplicate_class := "DIFFERENT_COMPANY"]

  n_a <- dt[duplicate_class == "SAME_COMPANY", .N]
  n_b <- dt[duplicate_class == "DIFFERENT_COMPANY", .N]
  message(sprintf("classify_duplicates: %d SAME_COMPANY rows, %d DIFFERENT_COMPANY rows",
                  n_a, n_b))

  # Validation: all tickers with occurrence > 1 must be classified
  dup_tickers <- dt[occurrence > 1, unique(ticker)]
  classified  <- unique(c(cat_a, cat_b))
  unclassified <- setdiff(dup_tickers, classified)
  if (length(unclassified) > 0) {
    warning("classify_duplicates: unclassified duplicates: ",
            paste(unclassified, collapse = ", "))
  }

  dt
}


# =============================================================================
# 4. assign_status()
# =============================================================================
#' Assign status to all constituents
#'
#' ACTIVE: currently in S&P 500 (date_removed is NA).
#' REMOVED_ACQUIRED, REMOVED_PRIVATE, REMOVED_BANKRUPT, REMOVED_DOWNGRADED,
#' REMOVED_TICKER_CHANGE: for removed constituents.
#'
#' @param dt data.table with date_removed column.
#' @return data.table with status and successor columns added.
assign_status <- function(dt) {

  dt <- copy(dt)
  dt[, status    := NA_character_]
  dt[, successor := NA_character_]

  # Active: no removal date
  dt[is.na(date_removed), status := "ACTIVE"]

  # Known ticker changes (company still exists under new ticker)
  ticker_changes <- list(
    "FB"   = "META",
    "ANTM" = "ELV",
    "TWTR" = NA_character_,   # acquired, not a ticker change
    "XLNX" = NA_character_,   # acquired by AMD
    "CTXS" = NA_character_,   # acquired, taken private
    "DLPH" = "APTV",          # Delphi Automotive -> Aptiv
    "BRK.B" = "BRK.B",       # still the same
    "DISCA" = "WBD",          # Discovery -> Warner Bros Discovery
    "DISCK" = "WBD",
    "VIAC"  = "PARA",         # ViacomCBS -> Paramount
    "PBCT"  = NA_character_,  # acquired by M&T Bank
    "CERN"  = NA_character_,  # acquired by Oracle
    "NLSN"  = NA_character_,  # taken private
    "KSU"   = NA_character_,  # acquired by CP Rail
    "INFO"  = NA_character_,  # acquired by ICE
    "SIVB"  = NA_character_,  # bankrupt (SVB)
    "SBNY"  = NA_character_,  # bankrupt (Signature Bank)
    "FRC"   = NA_character_   # bankrupt (First Republic)
  )

  for (tk in names(ticker_changes)) {
    succ <- ticker_changes[[tk]]
    if (!is.na(succ)) {
      dt[ticker == tk & !is.na(date_removed),
         `:=`(status = "REMOVED_TICKER_CHANGE", successor = succ)]
    }
  }

  # Known bankruptcies
  bankruptcies <- c("SIVB", "SBNY", "FRC", "LEH", "LLTC", "WMI", "CIT",
                     "GM", "ABI", "BSC", "ENE")
  dt[ticker %in% bankruptcies & !is.na(date_removed) & is.na(status),
     status := "REMOVED_BANKRUPT"]

  # Known taken-private
  went_private <- c("NLSN", "CTXS", "ATVI", "TWTR", "KSU")
  # Occurrence 1 of DELL was taken private, occurrence 2 is current
  dt[ticker == "DELL" & occurrence == 1 & !is.na(date_removed),
     status := "REMOVED_PRIVATE"]
  dt[ticker %in% went_private & !is.na(date_removed) & is.na(status),
     status := "REMOVED_PRIVATE"]

  # Default remaining removed tickers to REMOVED_ACQUIRED
  # This is the most common outcome (M&A). Specific misclassifications
  # can be patched later if needed, but the indicator computation
  # treats all REMOVED_* the same (data up to removal date).
  dt[!is.na(date_removed) & is.na(status), status := "REMOVED_ACQUIRED"]

  # Summary
  status_counts <- dt[, .N, by = status][order(-N)]
  message("assign_status:")
  for (i in seq_len(nrow(status_counts))) {
    message(sprintf("  %s: %d", status_counts$status[i], status_counts$N[i]))
  }

  dt
}


# =============================================================================
# 5. build_constituent_master()  --  Orchestrator
# =============================================================================
#' Build the constituent master parquet file
#'
#' Orchestrates: load -> resolve CIKs -> classify duplicates -> assign status.
#' Writes cache/lookups/constituent_master.parquet.
#'
#' @param csv_path Character. Path to sp500_constituents_.csv.
#' @param output_path Character. Path for output parquet file.
#' @return data.table (the final enriched roster, invisibly).
build_constituent_master <- function(
    csv_path    = "data/sp500_constituents_.csv",
    output_path = "cache/lookups/constituent_master.parquet") {

  message("build_constituent_master: starting...")

  # Step 1: Load raw CSV
  dt <- load_raw_constituents(csv_path)

  # Step 2: Resolve CIKs
  dt <- resolve_all_ciks(dt)

  # Step 3: Classify duplicate tickers
  dt <- classify_duplicates(dt)

  # Step 4: Assign status
  dt <- assign_status(dt)

  # Step 5: Select and order output columns
  out_cols <- c("ticker", "name", "cik", "date_added", "date_removed",
                "status", "successor", "occurrence", "duplicate_class")
  dt <- dt[, ..out_cols]

  # Step 6: Validate
  .assert_output(dt, "build_constituent_master", list(
    "is data.table" = is.data.table,
    "has 9 columns" = function(x) ncol(x) == 9,
    "rows > 800"    = function(x) nrow(x) > 800,
    "no dup ticker+occurrence" = function(x)
      !anyDuplicated(x[, .(ticker, occurrence)]),
    "all current have status ACTIVE" = function(x)
      all(x[is.na(date_removed), status] == "ACTIVE"),
    "all removed have status" = function(x)
      all(!is.na(x[!is.na(date_removed), status]))
  ))

  # Step 7: Write parquet
  out_dir <- dirname(output_path)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  arrow::write_parquet(dt, output_path)

  # Final summary
  current <- dt[status == "ACTIVE"]
  n_current_cik <- sum(!is.na(current$cik))
  message(sprintf(
    "build_constituent_master: wrote %d rows to %s",
    nrow(dt), output_path))
  message(sprintf(
    "  %d ACTIVE (%d with CIK), %d removed",
    nrow(current), n_current_cik, dt[status != "ACTIVE", .N]))

  invisible(dt)
}
