# ============================================================================
# fundamental_fetcher.R  --  EDGAR XBRL Fundamental Fetcher (Module 2)
# ============================================================================
# Fetch companyfacts JSON from SEC EDGAR, parse XBRL data into long-format
# data.tables, resolve tag aliases, deduplicate, and cache as parquet.
#
# Output schema (per-ticker parquet, long format):
#   ticker       chr   Ticker symbol
#   cik          chr   10-digit zero-padded CIK
#   concept      chr   Canonical concept name (e.g., "revenue")
#   tag          chr   Original XBRL tag used
#   value        num   Reported value
#   period_end   Date  Period end date
#   period_start Date  Period start date (NA for instant tags)
#   filed        Date  SEC filing date (point-in-time stamp)
#   form         chr   Filing form (10-K, 10-Q, 10-K/A, etc.)
#   accession    chr   Accession number (unique filing ID)
#   fiscal_year  int   Fiscal year
#   fiscal_qtr   chr   Quarter label (Q1-Q4 or FY)
#   unit         chr   Unit (USD, shares, etc.)
#   period_type  chr   Classified period (FY, Q1-Q4, or NA if unclassifiable)
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

# Form priority for deduplication (lower = preferred).
# Annual and amended-annual share the same priority so that -accession
# sort picks the amendment (later accession) over the original.
# Same logic for quarterly forms.
.FORM_PRIORITY <- c(
  "10-K"   = 1L,
  "10-K/A" = 1L,   # amendment supersedes original via later accession
  "20-F"   = 1L,   # foreign private issuers (annual)
  "20-F/A" = 1L,
  "40-F"   = 1L,   # Canadian cross-listed (annual)
  "40-F/A" = 1L,
  "10-Q"   = 2L,
  "10-Q/A" = 2L
)

# Expected period_days bands per fp label. Used by dedup_fundamentals to
# tie-break on duration match when multiple rows share (concept, period_end,
# fp). Balance-sheet (instant) concepts have NA period_start -> NA period_days
# and are handled by treating NA as a neutral match (rank 0).
.FP_EXPECTED_DAYS <- list(
  Q1 = c(60L, 120L),   # 3-month standalone; upper 120 accommodates 16-week
                       # Q1 used by Kroger/AAP fiscal calendars (12-12-12-16)
  Q2 = c(160L, 200L),  # 6-month YTD
  Q3 = c(250L, 290L),  # 9-month YTD
  Q4 = c(330L, 380L),  # 12-month (when filers tag annual as Q4)
  FY = c(330L, 380L)
)

# Return 0L if period_days falls in the expected band for fp (preferred),
# 1L otherwise. NA period_days -> 0L (balance-sheet instants). NA / unknown
# fp -> 0L (no basis to discriminate).
.duration_match_rank <- function(period_days, fp) {
  out <- integer(length(period_days))
  for (i in seq_along(period_days)) {
    d <- period_days[i]; f <- fp[i]
    if (is.na(d) || is.na(f) || is.null(.FP_EXPECTED_DAYS[[f]])) {
      out[i] <- 0L
    } else {
      band <- .FP_EXPECTED_DAYS[[f]]
      out[i] <- if (d >= band[1] && d <= band[2]) 0L else 1L
    }
  }
  out
}


# =============================================================================
# XBRL TAG ALIAS MAP
# =============================================================================
# Maps canonical concept names to ordered vectors of XBRL tags (us-gaap).
# First match wins during resolution. Order reflects prevalence.

.TAG_ALIASES <- list(

  # --- Income Statement ---
  revenue = c(
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax"
  ),

  cogs = c(
    "CostOfGoodsAndServicesSold",
    "CostOfRevenue",
    "CostOfGoodsSold",
    "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization"
  ),

  operating_income = c(
    "OperatingIncomeLoss"
  ),

  net_income = c(
    "NetIncomeLoss"
  ),

  eps_basic = c(
    "EarningsPerShareBasic"
  ),

  eps_diluted = c(
    "EarningsPerShareDiluted"
  ),

  interest_expense = c(
    "InterestExpense",
    "InterestExpenseDebt"
  ),

  sga = c(
    "SellingGeneralAndAdministrativeExpense"
  ),

  rnd = c(
    "ResearchAndDevelopmentExpense",
    "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"
  ),

  depreciation = c(
    "DepreciationDepletionAndAmortization",
    "DepreciationAndAmortization",
    "Depreciation"
  ),

  # --- Balance Sheet ---
  total_assets = c(
    "Assets"
  ),

  stockholders_equity = c(
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
  ),

  long_term_debt = c(
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "LongTermDebtAndCapitalLeaseObligations"
  ),

  short_term_debt = c(
    "ShortTermBorrowings",
    "ShortTermDebtCurrent",
    "DebtCurrent",
    "ShortTermBankLoansAndNotesPayable"
  ),

  current_assets = c(
    "AssetsCurrent"
  ),

  current_liabilities = c(
    "LiabilitiesCurrent"
  ),

  total_liabilities = c(
    "Liabilities"
  ),

  accounts_receivable = c(
    "AccountsReceivableNetCurrent",
    "AccountsReceivableNet"
  ),

  inventory = c(
    "InventoryNet",
    "InventoryFinishedGoodsNetOfReserves"
  ),

  cash = c(
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsAndShortTermInvestments",
    "Cash"
  ),

  shares_outstanding = c(
    "CommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
    "WeightedAverageNumberOfDilutedSharesOutstanding"
  ),

  accounts_payable = c(
    "AccountsPayableCurrent",
    "AccountsPayableAndAccruedLiabilitiesCurrent"
  ),

  accrued_liabilities = c(
    "AccruedLiabilitiesCurrent"
  ),

  deferred_revenue = c(
    "DeferredRevenueCurrent",
    "ContractWithCustomerLiabilityCurrent",
    "DeferredRevenueCurrentAndNoncurrent"
  ),

  prepaid_expenses = c(
    "PrepaidExpenseAndOtherAssetsCurrent",
    "PrepaidExpenseAndOtherAssets",
    "PrepaidExpenseCurrent"
  ),

  # --- Cash Flow Statement ---
  operating_cashflow = c(
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByOperatingActivities",
    # Filers with (current or historical) discontinued operations tag the
    # continuing-operations line separately. Used by FMC, TFX, ITT, DRI and
    # similar. Listed last so firms that report both the total and the
    # continuing-ops line still resolve to the total.
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
  ),

  capex = c(
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets"
  ),

  buybacks = c(
    "PaymentsForRepurchaseOfCommonStock",
    "PaymentsForRepurchaseOfEquity"
  ),

  dividends_paid = c(
    "PaymentsOfDividendsCommonStock",
    "PaymentsOfDividends",
    "Dividends"
  )
)

# Reverse lookup: XBRL tag -> canonical concept
.TAG_TO_CONCEPT <- {
  out <- list()
  for (concept in names(.TAG_ALIASES)) {
    for (tag in .TAG_ALIASES[[concept]]) {
      out[[tag]] <- concept
    }
  }
  out
}

# All target tags as a flat vector (for filtering companyfacts JSON)
.ALL_TARGET_TAGS <- unlist(.TAG_ALIASES, use.names = FALSE)


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

# Null-coalesce operator (available in base R >= 4.4.0, define for older versions)
if (!exists("%||%", mode = "function", envir = baseenv())) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
}


# =============================================================================
# 1. fetch_companyfacts()
# =============================================================================
#' Fetch EDGAR companyfacts JSON for a single CIK
#'
#' @param cik Character. 10-digit zero-padded CIK.
#' @return Parsed list from JSON, or NULL on failure.
fetch_companyfacts <- function(cik) {
  url <- sprintf("%s/api/xbrl/companyfacts/CIK%s.json", .EDGAR_BASE, cik)
  json_text <- .edgar_fetch(url)
  if (is.null(json_text)) return(NULL)
  tryCatch(
    jsonlite::fromJSON(json_text, simplifyVector = FALSE),
    error = function(e) {
      warning(sprintf("fetch_companyfacts: JSON parse failed for CIK %s: %s",
                      cik, e$message), call. = FALSE)
      NULL
    }
  )
}


# =============================================================================
# 2. parse_companyfacts()
# =============================================================================
#' Parse companyfacts JSON into a long-format data.table
#'
#' Extracts all target XBRL tags (from .TAG_ALIASES) found in the JSON.
#' Each row is one (tag, period, filing) observation.
#'
#' @param facts List. Parsed companyfacts JSON from fetch_companyfacts().
#' @param ticker Character. Ticker symbol (for output).
#' @param cik Character. 10-digit CIK (for output).
#' @return data.table in long format, or NULL if no target tags found.
parse_companyfacts <- function(facts, ticker, cik) {

  if (is.null(facts)) return(NULL)

  # companyfacts JSON structure:
  # facts -> us-gaap -> {TagName} -> units -> {USD|shares|...} -> [array of obs]
  # Each observation has: val, end, start, filed, form, accn, fy, fp, frame
  us_gaap <- facts[["facts"]][["us-gaap"]]
  dei     <- facts[["facts"]][["dei"]]

  if (is.null(us_gaap) && is.null(dei)) return(NULL)

  rows <- list()
  row_idx <- 0L

  # Helper to process one namespace
  process_namespace <- function(ns_data) {
    if (is.null(ns_data)) return()
    for (tag_name in names(ns_data)) {
      if (!(tag_name %in% .ALL_TARGET_TAGS)) next

      concept <- .TAG_TO_CONCEPT[[tag_name]]
      tag_data <- ns_data[[tag_name]]
      units_data <- tag_data[["units"]]
      if (is.null(units_data)) next

      for (unit_name in names(units_data)) {
        obs_list <- units_data[[unit_name]]
        if (length(obs_list) == 0) next

        for (obs in obs_list) {
          val <- obs[["val"]]
          if (is.null(val)) next

          row_idx <<- row_idx + 1L
          rows[[row_idx]] <<- list(
            ticker       = ticker,
            cik          = cik,
            concept      = concept,
            tag          = tag_name,
            value        = as.numeric(val),
            period_end   = as.character(obs[["end"]]   %||% NA_character_),
            period_start = as.character(obs[["start"]] %||% NA_character_),
            filed        = as.character(obs[["filed"]] %||% NA_character_),
            form         = as.character(obs[["form"]]  %||% NA_character_),
            accession    = as.character(obs[["accn"]]  %||% NA_character_),
            fiscal_year  = as.integer(obs[["fy"]]      %||% NA_integer_),
            fiscal_qtr   = as.character(obs[["fp"]]    %||% NA_character_),
            unit         = unit_name
          )
        }
      }
    }
  }

  process_namespace(us_gaap)
  process_namespace(dei)

  if (row_idx == 0L) return(NULL)

  dt <- rbindlist(rows)

  # Type conversions
  dt[, period_end   := as.Date(period_end)]
  dt[, period_start := as.Date(period_start)]
  dt[, filed        := as.Date(filed)]

  dt
}


# =============================================================================
# 3. dedup_fundamentals()
# =============================================================================
#' Deduplicate fundamental observations
#'
#' For each (concept, period_end, fiscal_year, fiscal_qtr) combination:
#' 1. Apply form priority: annual (10-K/10-K/A/20-F/40-F) > quarterly (10-Q/10-Q/A).
#'    Within a group, amendments share priority with originals so that accession
#'    sort picks the amendment (later accession number) over the original.
#' 2. Within same form priority, keep most recent accession number (latest amendment).
#' 3. Within same form and accession, prefer tag with lower alias rank.
#' 4. When multiple rows still share the dedup key, prefer the row whose
#'    period length matches the expected duration for its `fp` label
#'    (Q1 ~ 3mo, Q2 ~ 6mo YTD, Q3 ~ 9mo YTD, FY/Q4 ~ 12mo). Companyfacts
#'    JSON often includes prior-period or TTM comparatives under the same
#'    (concept, period_end, fp) key; the duration-match tie-break keeps
#'    the current-period value and discards the comparative.
#'
#' @param dt data.table from parse_companyfacts().
#' @return Deduplicated data.table (one row per concept x period).
dedup_fundamentals <- function(dt) {

  if (is.null(dt) || nrow(dt) == 0) return(dt)

  dt <- copy(dt)

  # Assign form priority (unrecognized forms get lowest priority)
  dt[, form_priority := .FORM_PRIORITY[form]]
  dt[is.na(form_priority), form_priority := 99L]

  # Assign tag rank within concept (lower = preferred alias)
  dt[, tag_rank := mapply(function(concept, tag) {
    aliases <- .TAG_ALIASES[[concept]]
    if (is.null(aliases)) return(99L)
    idx <- match(tag, aliases)
    if (is.na(idx)) 99L else idx
  }, concept, tag)]

  # Duration-match rank: 0 if period length matches fp-expected band, else 1.
  # Instant tags (balance sheet) have NA period_start and are unaffected.
  dt[, period_days := as.integer(period_end - period_start)]
  dt[, duration_match := .duration_match_rank(period_days, fiscal_qtr)]

  # Sort: concept + period + fiscal_qtr, duration-match first (so good rows
  # win against TTM/prior-period comparatives), then form priority, accession
  # (desc so amendments beat originals), then tag rank.
  setorder(dt, concept, period_end, fiscal_qtr,
           duration_match, form_priority, -accession, tag_rank)

  # Keep first row per (concept, period_end, fiscal_qtr).
  # fiscal_qtr distinguishes FY from Q4 when period_end is the same.
  dt <- dt[!duplicated(dt[, .(concept, period_end, fiscal_qtr)])]

  # Clean up helper columns
  dt[, c("form_priority", "tag_rank",
         "period_days", "duration_match") := NULL]

  dt
}


# =============================================================================
# 4. classify_period()
# =============================================================================
#' Classify observations as annual (FY) or quarterly (Q1-Q4)
#'
#' Uses filing form and fiscal period to determine granularity.
#' Filters out observations that cannot be classified (e.g., fragments).
#'
#' @param dt data.table from dedup_fundamentals().
#' @return data.table with period_type column added (FY or Q1-Q4).
classify_period <- function(dt) {

  if (is.null(dt) || nrow(dt) == 0) return(dt)

  dt <- copy(dt)

  # Determine period length in days (for duration tags)
  dt[, period_days := as.integer(period_end - period_start)]

  # Classify based on fiscal_qtr field from EDGAR
  # fp values: FY, Q1, Q2, Q3, Q4
  dt[, period_type := fifelse(
    fiscal_qtr %in% c("FY"), "FY",
    fifelse(fiscal_qtr %in% c("Q1", "Q2", "Q3", "Q4"), fiscal_qtr,
            NA_character_)
  )]

  # For unclassified rows, infer from period length
  # Annual: > 300 days, Quarterly: 60-120 days
  dt[is.na(period_type) & !is.na(period_days) & period_days > 300,
     period_type := "FY"]
  dt[is.na(period_type) & !is.na(period_days) &
       period_days >= 60 & period_days <= 120,
     period_type := {
       # Infer quarter from period_end month
       m <- as.integer(format(period_end, "%m"))
       fifelse(m %in% 1:3, "Q1",
               fifelse(m %in% 4:6, "Q2",
                       fifelse(m %in% 7:9, "Q3", "Q4")))
     }]

  # Instant tags (balance sheet) have no period_start -> NA period_days
  # Keep these; they are classified by their fiscal_qtr from EDGAR.
  # Only assign valid period_type values.
  valid_fp <- c("FY", "Q1", "Q2", "Q3", "Q4")
  dt[is.na(period_type) & is.na(period_start) & fiscal_qtr %in% valid_fp,
     period_type := fiscal_qtr]

  # Drop helper column
  dt[, period_days := NULL]

  dt
}


# =============================================================================
# 5. fetch_and_cache_ticker()
# =============================================================================
#' Fetch, parse, dedup, and cache fundamentals for a single ticker
#'
#' Idempotent: if cache parquet exists for this ticker, returns cached data.
#' Use force_refresh = TRUE to re-fetch.
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit zero-padded CIK.
#' @param cache_dir Character. Directory for per-ticker parquet files.
#' @param force_refresh Logical. If TRUE, re-fetch even if cached.
#' @return data.table in long format, or NULL on failure.
fetch_and_cache_ticker <- function(ticker, cik,
                                   cache_dir = "cache/fundamentals",
                                   force_refresh = FALSE) {

  cache_file <- file.path(cache_dir, sprintf("%s_%s.parquet", cik, ticker))

  # Return cached if exists and not forcing refresh
  if (!force_refresh && file.exists(cache_file)) {
    dt <- tryCatch(
      as.data.table(arrow::read_parquet(cache_file)),
      error = function(e) NULL)
    if (!is.null(dt) && nrow(dt) > 0) return(dt)
  }

  # Fetch from EDGAR
  facts <- fetch_companyfacts(cik)
  if (is.null(facts)) {
    warning(sprintf("fetch_and_cache_ticker: EDGAR fetch failed for %s (CIK %s)",
                    ticker, cik), call. = FALSE)
    return(NULL)
  }

  # Parse
  dt <- parse_companyfacts(facts, ticker, cik)
  if (is.null(dt) || nrow(dt) == 0) {
    warning(sprintf("fetch_and_cache_ticker: no target tags found for %s (CIK %s)",
                    ticker, cik), call. = FALSE)
    return(NULL)
  }

  # Dedup
  dt <- dedup_fundamentals(dt)

  # Classify periods
  dt <- classify_period(dt)

  # Validate before caching
  .assert_output(dt, "fetch_and_cache_ticker", list(
    "is data.table"        = is.data.table,
    "has concept column"   = function(x) "concept" %in% names(x),
    "has filed column"     = function(x) "filed" %in% names(x),
    "has value column"     = function(x) "value" %in% names(x),
    "no duplicate concept+period+qtr" = function(x)
      !anyDuplicated(x[, .(concept, period_end, fiscal_qtr)])
  ))

  # Cache
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  arrow::write_parquet(dt, cache_file)

  dt
}


# =============================================================================
# 6. fetch_fundamentals_batch()
# =============================================================================
#' Fetch fundamentals for a batch of tickers
#'
#' Resumable: skips tickers already cached (unless force_refresh).
#' Chunked: processes tickers in groups of chunk_size with progress
#' summaries between chunks. Logs progress. Never stops on single-ticker failure.
#'
#' @param tickers Character vector. Ticker symbols.
#' @param ciks Character vector. Matching 10-digit CIKs (same order as tickers).
#' @param cache_dir Character. Directory for per-ticker parquet files.
#' @param force_refresh Logical. Re-fetch all, even if cached.
#' @param chunk_size Integer. Number of tickers per chunk. NULL = no chunking.
#' @return List with $results (named list of data.tables), $success (tickers),
#'         $failed (tickers), $cached (tickers loaded from cache).
fetch_fundamentals_batch <- function(tickers, ciks,
                                     cache_dir = "cache/fundamentals",
                                     force_refresh = FALSE,
                                     chunk_size = NULL) {

  stopifnot(length(tickers) == length(ciks))

  results <- list()
  success <- character()
  failed  <- character()
  cached  <- character()
  n <- length(tickers)

  # Determine chunk boundaries
  if (is.null(chunk_size) || chunk_size >= n) {
    chunk_starts <- 1L
    chunk_ends   <- n
  } else {
    chunk_starts <- seq(1L, n, by = chunk_size)
    chunk_ends   <- pmin(chunk_starts + chunk_size - 1L, n)
  }
  n_chunks <- length(chunk_starts)

  for (ch in seq_len(n_chunks)) {
    idx_from <- chunk_starts[ch]
    idx_to   <- chunk_ends[ch]

    if (n_chunks > 1) {
      message(sprintf("\n--- Chunk %d/%d (tickers %d-%d of %d) ---",
                      ch, n_chunks, idx_from, idx_to, n))
    }

    for (i in idx_from:idx_to) {
      tk  <- tickers[i]
      cik <- ciks[i]

      if (i %% 25 == 1 || i == idx_from || i == n) {
        message(sprintf("  [%d/%d] %s (CIK %s)", i, n, tk, cik))
      }

      cache_file <- file.path(cache_dir, sprintf("%s_%s.parquet", cik, tk))
      was_cached <- !force_refresh && file.exists(cache_file)

      dt <- tryCatch(
        fetch_and_cache_ticker(tk, cik, cache_dir, force_refresh),
        error = function(e) {
          warning(sprintf("fetch_fundamentals_batch: error for %s: %s",
                          tk, e$message), call. = FALSE)
          NULL
        }
      )

      if (!is.null(dt) && nrow(dt) > 0) {
        results[[tk]] <- dt
        success <- c(success, tk)
        if (was_cached) cached <- c(cached, tk)
      } else {
        failed <- c(failed, tk)
      }
    }

    # Chunk summary
    if (n_chunks > 1) {
      message(sprintf("  Chunk %d done: %d total success, %d total failed so far",
                      ch, length(success), length(failed)))
    }
  }

  message(sprintf("fetch_fundamentals_batch: %d success (%d from cache), %d failed",
                  length(success), length(cached), length(failed)))
  if (length(failed) > 0) {
    message("  Failed: ", paste(failed, collapse = ", "))
  }

  list(results = results, success = success, failed = failed, cached = cached)
}


# =============================================================================
# 7. coverage_report()
# =============================================================================
#' Generate XBRL tag coverage report across fetched tickers
#'
#' Inspects which canonical concepts are covered, which XBRL tags were
#' actually used (alias resolution), and which tickers have gaps.
#'
#' @param results Named list of data.tables (output from fetch_fundamentals_batch).
#' @return data.table with coverage statistics.
coverage_report <- function(results) {

  if (length(results) == 0) {
    message("coverage_report: no results to analyze")
    return(NULL)
  }

  all_concepts <- names(.TAG_ALIASES)
  tickers <- names(results)

  # Build coverage matrix: concept x ticker
  coverage <- data.table(
    concept = rep(all_concepts, each = length(tickers)),
    ticker  = rep(tickers, times = length(all_concepts))
  )

  # Check which concepts have data per ticker
  coverage[, has_data := FALSE]
  coverage[, tag_used := NA_character_]
  coverage[, n_obs    := 0L]

  for (tk in tickers) {
    dt_tk <- results[[tk]]
    if (is.null(dt_tk) || nrow(dt_tk) == 0) next

    # Compute per concept
    concept_summary <- dt_tk[, .(
      n_obs    = .N,
      tag_used = tag[1]
    ), by = concept]

    for (j in seq_len(nrow(concept_summary))) {
      cn <- concept_summary$concept[j]
      coverage[concept == cn & ticker == tk,
               `:=`(has_data = TRUE,
                    tag_used = concept_summary$tag_used[j],
                    n_obs    = concept_summary$n_obs[j])]
    }
  }

  # Aggregate per concept
  concept_agg <- coverage[, .(
    n_tickers_with_data = sum(has_data),
    pct_coverage = round(sum(has_data) / .N * 100, 1),
    tags_used = paste(unique(na.omit(tag_used)), collapse = " | ")
  ), by = concept]

  setorder(concept_agg, -pct_coverage)

  # Print report
  message("\n=== XBRL Tag Coverage Report ===")
  message(sprintf("Tickers analyzed: %d", length(tickers)))
  message(sprintf("Concepts defined: %d", length(all_concepts)))
  message("")

  for (i in seq_len(nrow(concept_agg))) {
    row <- concept_agg[i]
    message(sprintf("  %-25s %3d/%d (%5.1f%%)  tags: %s",
                    row$concept, row$n_tickers_with_data,
                    length(tickers), row$pct_coverage, row$tags_used))
  }

  # Ticker-level summary
  ticker_agg <- coverage[, .(
    n_concepts = sum(has_data),
    pct_concepts = round(sum(has_data) / .N * 100, 1)
  ), by = ticker]

  setorder(ticker_agg, pct_concepts)

  message("\n=== Per-Ticker Coverage ===")
  for (i in seq_len(nrow(ticker_agg))) {
    row <- ticker_agg[i]
    message(sprintf("  %-6s %2d/%d concepts (%5.1f%%)",
                    row$ticker, row$n_concepts,
                    length(all_concepts), row$pct_concepts))
  }

  invisible(list(
    concept_coverage = concept_agg,
    ticker_coverage  = ticker_agg,
    detail           = coverage
  ))
}


# =============================================================================
# 8. build_fundamentals()  --  Orchestrator
# =============================================================================
#' Build fundamental data for S&P 500 constituents
#'
#' Reads the constituent master, selects tickers with CIKs,
#' and fetches fundamentals in batch. Handles duplicate tickers:
#' - SAME_COMPANY: one CIK, highest occurrence wins (single fetch).
#' - DIFFERENT_COMPANY: if both occurrences share a CIK, one fetch covers
#'   both. If they have distinct CIKs, both are fetched (rare in practice).
#'
#' @param master_path Character. Path to constituent_master.parquet.
#' @param cache_dir Character. Directory for per-ticker parquet files.
#' @param tickers Character vector. If NULL, fetches all tickers with CIK.
#'   If provided, fetches only these tickers.
#' @param force_refresh Logical. Re-fetch all, even if cached.
#' @param chunk_size Integer. Tickers per chunk for progress logging. NULL = no chunking.
#' @return List from fetch_fundamentals_batch().
build_fundamentals <- function(
    master_path = "cache/lookups/constituent_master.parquet",
    cache_dir   = "cache/fundamentals",
    tickers     = NULL,
    force_refresh = FALSE,
    chunk_size  = 50L) {

  message("build_fundamentals: starting...")

  # Load constituent master
  if (!file.exists(master_path)) {
    stop("build_fundamentals: constituent_master.parquet not found. Run Session A first.")
  }
  master <- as.data.table(arrow::read_parquet(master_path))

  # Filter to tickers with CIK
  master <- master[!is.na(cik)]

  if (!is.null(tickers)) {
    master <- master[ticker %in% tickers]
    if (nrow(master) == 0) {
      stop("build_fundamentals: none of the requested tickers found in master with CIK")
    }
  }

  # Deduplicate to unique (ticker, cik) pairs.
  # SAME_COMPANY duplicates share a CIK -> one fetch per ticker.
  # DIFFERENT_COMPANY duplicates may have distinct CIKs -> fetch both.
  # Within same (ticker, cik), keep highest occurrence (most recent stint).
  master <- master[order(ticker, cik, -occurrence)]
  master <- master[!duplicated(master[, .(ticker, cik)])]

  # Multiple rows with the same CIK but different tickers can't happen
  # (CIK is unique per filer). Multiple rows with the same ticker but
  # different CIKs are DIFFERENT_COMPANY cases -- each CIK needs its own
  # fetch. The cache filename {CIK}_{ticker}.parquet keeps them distinct.
  # However, the same CIK appearing under the same ticker (SAME_COMPANY
  # re-additions) is already collapsed above. Check for same-CIK collisions
  # across different tickers (shouldn't happen, but guard against it).
  fetch_master <- copy(master)
  n_dup_cik <- sum(duplicated(fetch_master$cik))
  if (n_dup_cik > 0) {
    message(sprintf("  %d duplicate CIK(s) across different tickers -- fetching once each",
                    n_dup_cik))
    fetch_master <- fetch_master[!duplicated(cik)]
  }

  message(sprintf("  %d unique CIKs to fetch (%d ticker-CIK pairs)",
                  nrow(fetch_master), nrow(master)))

  # Fetch
  result <- fetch_fundamentals_batch(
    tickers       = fetch_master$ticker,
    ciks          = fetch_master$cik,
    cache_dir     = cache_dir,
    force_refresh = force_refresh,
    chunk_size    = chunk_size
  )

  result
}


# =============================================================================
# 9. validate_fundamentals_build()
# =============================================================================
#' Post-build validation and summary for the full fundamental corpus
#'
#' Scans all cached parquet files and reports: total tickers, concept coverage,
#' date ranges, file sizes, and any anomalies.
#'
#' @param cache_dir Character. Cache directory.
#' @param master_path Character. Path to constituent_master.parquet (for gap check).
#' @return Invisible list with $summary, $concept_coverage, $ticker_stats.
validate_fundamentals_build <- function(
    cache_dir   = "cache/fundamentals",
    master_path = "cache/lookups/constituent_master.parquet") {

  if (!dir.exists(cache_dir)) {
    message("validate_fundamentals_build: no parquet files found")
    return(invisible(NULL))
  }
  files <- list.files(cache_dir, pattern = "[.]parquet$", full.names = TRUE)
  if (length(files) == 0) {
    message("validate_fundamentals_build: no parquet files found")
    return(invisible(NULL))
  }

  message(sprintf("\n=== Fundamentals Build Validation ==="))
  message(sprintf("Cache dir: %s", cache_dir))
  message(sprintf("Files found: %d", length(files)))

  # Aggregate stats across all files
  all_concepts <- names(.TAG_ALIASES)
  ticker_stats <- list()
  concept_ticker_count <- setNames(integer(length(all_concepts)), all_concepts)
  total_rows <- 0L
  total_bytes <- 0L
  min_filed <- as.Date("2099-01-01")
  max_filed <- as.Date("1900-01-01")

  for (f in files) {
    dt <- tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
    if (is.null(dt) || nrow(dt) == 0) next

    tk <- dt$ticker[1]
    fsize <- file.size(f)
    total_rows <- total_rows + nrow(dt)
    total_bytes <- total_bytes + fsize

    filed_range <- range(dt$filed, na.rm = TRUE)
    if (filed_range[1] < min_filed) min_filed <- filed_range[1]
    if (filed_range[2] > max_filed) max_filed <- filed_range[2]

    concepts_present <- unique(dt$concept)
    n_fy <- sum(dt$period_type == "FY", na.rm = TRUE)
    n_q  <- sum(dt$period_type %in% c("Q1", "Q2", "Q3", "Q4"), na.rm = TRUE)

    ticker_stats[[tk]] <- list(
      ticker     = tk,
      n_rows     = nrow(dt),
      n_concepts = length(concepts_present),
      n_fy       = n_fy,
      n_q        = n_q,
      filed_min  = filed_range[1],
      filed_max  = filed_range[2],
      file_kb    = round(fsize / 1024, 1)
    )

    for (cn in concepts_present) {
      concept_ticker_count[cn] <- concept_ticker_count[cn] + 1L
    }
  }

  n_tickers <- length(ticker_stats)
  ticker_dt <- rbindlist(ticker_stats)

  # Concept coverage
  concept_dt <- data.table(
    concept    = names(concept_ticker_count),
    n_tickers  = unname(concept_ticker_count),
    pct        = round(unname(concept_ticker_count) / n_tickers * 100, 1)
  )
  setorder(concept_dt, -pct)

  # Summary output
  message(sprintf("\nTickers cached: %d", n_tickers))
  message(sprintf("Total rows: %s", format(total_rows, big.mark = ",")))
  message(sprintf("Total size: %.1f MB", total_bytes / 1024^2))
  message(sprintf("Filed date range: %s to %s", min_filed, max_filed))
  message(sprintf("Median rows/ticker: %.0f", median(ticker_dt$n_rows)))
  message(sprintf("Median concepts/ticker: %.0f", median(ticker_dt$n_concepts)))

  # Concept coverage table
  message("\n--- Concept Coverage ---")
  for (i in seq_len(nrow(concept_dt))) {
    row <- concept_dt[i]
    message(sprintf("  %-25s %3d/%d (%5.1f%%)",
                    row$concept, row$n_tickers, n_tickers, row$pct))
  }

  # Tickers with low coverage (< 10 concepts)
  low_cov <- ticker_dt[n_concepts < 10]
  if (nrow(low_cov) > 0) {
    setorder(low_cov, n_concepts)
    message(sprintf("\n--- Low-Coverage Tickers (%d with < 10 concepts) ---", nrow(low_cov)))
    for (i in seq_len(min(nrow(low_cov), 20))) {
      row <- low_cov[i]
      message(sprintf("  %-6s %2d concepts, %4d rows", row$ticker, row$n_concepts, row$n_rows))
    }
    if (nrow(low_cov) > 20) message(sprintf("  ... and %d more", nrow(low_cov) - 20))
  }

  # Check against master for gaps
  if (file.exists(master_path)) {
    master <- as.data.table(arrow::read_parquet(master_path))
    expected <- master[!is.na(cik)]
    expected <- expected[order(ticker, cik, -occurrence)]
    expected <- expected[!duplicated(expected[, .(ticker, cik)])]
    missing <- setdiff(expected$ticker, ticker_dt$ticker)
    if (length(missing) > 0) {
      message(sprintf("\n--- Missing Tickers (%d expected but not cached) ---", length(missing)))
      message("  ", paste(head(missing, 30), collapse = ", "))
      if (length(missing) > 30) message(sprintf("  ... and %d more", length(missing) - 30))
    } else {
      message("\nAll expected tickers are cached.")
    }
  }

  message("\n=== Validation Complete ===")

  invisible(list(
    summary = list(
      n_tickers   = n_tickers,
      total_rows  = total_rows,
      total_mb    = round(total_bytes / 1024^2, 1),
      filed_min   = min_filed,
      filed_max   = max_filed
    ),
    concept_coverage = concept_dt,
    ticker_stats     = ticker_dt
  ))
}


# =============================================================================
# 10. get_fundamentals()  --  Reader convenience function
# =============================================================================
#' Load cached fundamentals for a single ticker
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit CIK (optional if only one match in cache).
#' @param cache_dir Character. Cache directory.
#' @return data.table or NULL if not cached.
get_fundamentals <- function(ticker, cik = NULL,
                             cache_dir = "cache/fundamentals") {

  if (!is.null(cik)) {
    path <- file.path(cache_dir, sprintf("%s_%s.parquet", cik, ticker))
    if (file.exists(path)) {
      return(as.data.table(arrow::read_parquet(path)))
    }
    return(NULL)
  }

  # Search for matching file
  pattern <- sprintf("*_%s.parquet", ticker)
  files <- list.files(cache_dir, pattern = glob2rx(pattern), full.names = TRUE)

  if (length(files) == 0) return(NULL)
  if (length(files) > 1) {
    warning(sprintf("get_fundamentals: multiple files for %s, using most recent",
                    ticker), call. = FALSE)
    files <- files[order(file.mtime(files), decreasing = TRUE)]
  }

  as.data.table(arrow::read_parquet(files[1]))
}


# =============================================================================
# 11. prototype_20_tickers()
# =============================================================================
#' Run the 20-ticker prototype for Session C
#'
#' Selects 20 representative tickers spanning sectors, market caps,
#' and edge cases. Fetches, caches, and runs coverage report.
#'
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param cache_dir Character. Cache directory.
#' @param force_refresh Logical. Re-fetch even if cached.
#' @return List with $batch_result and $coverage.
prototype_20_tickers <- function(
    master_path = "cache/lookups/constituent_master.parquet",
    sector_path = "cache/lookups/sector_industry.parquet",
    cache_dir   = "cache/fundamentals",
    force_refresh = FALSE) {

  message("prototype_20_tickers: Session C prototype starting...")

  # Curated 20 tickers spanning sectors and edge cases:
  # Tech:        AAPL, MSFT, NVDA (large cap, different reporting patterns)
  # Financials:  JPM, BRK.B (banks lack COGS/inventory)
  # Healthcare:  JNJ, UNH
  # Consumer:    AMZN, WMT, PG
  # Industrial:  CAT, HON
  # Energy:      XOM, CVX
  # Utilities:   NEE
  # Real Estate: PLD
  # Comm Svc:    GOOGL, FB (listed as FB in constituent master; CIK 1326801)
  # Materials:   LIN
  # Cons Def:    KO
  prototype_tickers <- c(
    "AAPL", "MSFT", "NVDA",
    "JPM", "BRK.B",
    "JNJ", "UNH",
    "AMZN", "WMT", "PG",
    "CAT", "HON",
    "XOM", "CVX",
    "NEE",
    "PLD",
    "GOOGL", "FB",
    "LIN",
    "KO"
  )

  # Fetch
  batch_result <- build_fundamentals(
    master_path   = master_path,
    cache_dir     = cache_dir,
    tickers       = prototype_tickers,
    force_refresh = force_refresh
  )

  # Coverage report
  cov <- coverage_report(batch_result$results)

  message(sprintf("\nprototype_20_tickers: complete -- %d/%d tickers fetched",
                  length(batch_result$success), length(prototype_tickers)))

  list(batch_result = batch_result, coverage = cov)
}
