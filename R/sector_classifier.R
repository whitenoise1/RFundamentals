# ============================================================================
# sector_classifier.R  --  Finviz Sector/Industry Lookup (Module 3)
# ============================================================================
# Fetch sector and industry classification for S&P 500 constituents.
# Primary source: finviz.com ticker pages.
# Fallback: static CSV at data/sector_industry_fallback.csv.
# Output: cache/lookups/sector_industry.parquet
#
# Output schema (SCD Type-2 since the daily-update wave, fix D2/B):
#   ticker     chr   Ticker symbol
#   sector     chr   Finviz sector (11 sectors)
#   industry   chr   Finviz industry (~150 industries)
#   source     chr   "finviz" | "fallback" | "override"
#   valid_from date  Row valid from this date. Migration floor-stamped
#                    all pre-existing rows (best current estimate applied
#                    backward, docs/KNOWN_LIMITATIONS.md L1); the update
#                    tiers append a new-dated row only when a ticker's
#                    value changes. Readers resolve via .sector_asof().
#
# Dependencies: data.table, arrow, httr
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(httr)
})


# =============================================================================
# CONSTANTS
# =============================================================================

.FINVIZ_BASE     <- "https://finviz.com/quote.ashx"
.FINVIZ_RATE_SEC <- 0.5

# Ticker -> c(sector, industry) for constituents finviz cannot classify:
# symbols reused by an unrelated listing after delisting (finviz returns
# the wrong company) and dead symbols finviz no longer serves. Only
# data-bearing constituents need entries; symbols with no fundamentals
# never enter snapshots. Values mirror the finviz row of the surviving
# sibling listing where one exists (DISCA/DISCK -> WBD).
.SECTOR_OVERRIDES <- list(
  INFO  = c("Industrials", "Specialty Business Services"),  # IHS Markit
  DISCA = c("Communication Services", "Entertainment"),     # Discovery A
  DISCK = c("Communication Services", "Entertainment"),     # Discovery C
  FRC   = c("Financial", "Banks - Regional")                # First Republic
)
.FINVIZ_UA       <- paste0(
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ",
  "AppleWebKit/537.36 (KHTML, like Gecko) ",
  "Chrome/120.0.0.0 Safari/537.36")

# The 11 finviz sectors, verbatim. .SECTOR_NA_INDICATORS keys on these
# literals ("Financial", NOT "Financial Services"), so every static
# override and crosswalk output MUST be one of these exact strings -- a
# near-miss silently disables the bank/utility/REIT NA-mask (the D3
# failure mode). tests/test_sector_classifier.R asserts this for every
# override CSV row.
.FINVIZ_SECTORS <- c(
  "Basic Materials", "Communication Services", "Consumer Cyclical",
  "Consumer Defensive", "Energy", "Financial", "Healthcare",
  "Industrials", "Real Estate", "Technology", "Utilities"
)

# Static override CSVs applied by build_sector_industry step 4c, in
# order (later files win on ticker collision).
.SECTOR_OVERRIDE_CSVS <- c(
  "data/sector_overrides_oldcik.csv",
  "data/sector_overrides_delisted.csv"
)

# valid_from stamp for rows that predate the dated dimension: the best
# current estimate applied back to the start of history (2010 grid).
# Documented approximation, KNOWN_LIMITATIONS.md L1.
.SECTOR_VALID_FROM_FLOOR <- as.Date("2010-01-01")

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

#' Fetch a single finviz quote page and extract sector + industry
#' @param ticker Character. Ticker symbol.
#' @return Named list with sector and industry, or NULL on failure.
.finviz_fetch_one <- function(ticker) {
  # Finviz uses dashes for class tickers (BRK-B, not BRK.B)
  url_ticker <- gsub("\\.", "-", ticker)
  url <- sprintf("%s?t=%s&ty=c&p=d&b=1", .FINVIZ_BASE, url_ticker)

  resp <- tryCatch({
    httr::GET(url,
              httr::add_headers(`User-Agent` = .FINVIZ_UA),
              httr::timeout(15))
  }, error = function(e) NULL)

  Sys.sleep(.FINVIZ_RATE_SEC)

  if (is.null(resp) || httr::status_code(resp) != 200) return(NULL)

  html <- httr::content(resp, as = "text", encoding = "UTF-8")
  .extract_finviz_sector_industry(html)
}

#' Extract sector and industry from finviz HTML
#' @param html Character. Raw HTML content.
#' @return Named list with sector and industry, or NULL.
.extract_finviz_sector_industry <- function(html) {
  # Finviz uses screener links with sec_ and ind_ prefixes. Layouts seen:
  #   <a href="screener.ashx?v=111&f=sec_technology" class="tab-link">Technology</a>
  #   <a href="screener?v=111&f=ind_consumerelectronics" class="quote-header_category"
  #     title="Consumer Electronics"><span class="min-w-0 truncate">Consumer Electronics</span></a>
  # The 2026 layout wraps the link text in a <span>, so allow one nested tag
  # between the anchor and its text.
  sector   <- .finviz_link_text(html, "sec")
  industry <- .finviz_link_text(html, "ind")
  if (is.null(sector) || is.null(industry)) return(NULL)

  list(sector = sector, industry = industry)
}

#' Extract the text of the first finviz screener link with the given prefix
#' @param html Character. Raw HTML content.
#' @param prefix Character. "sec" or "ind".
#' @return Character scalar, or NULL if no link found.
.finviz_link_text <- function(html, prefix) {
  pat <- sprintf('f=%s_[^"]*"[^>]*>(?:<span[^>]*>)?([^<]+)', prefix)
  m <- regmatches(html, regexpr(pat, html, perl = TRUE))
  if (length(m) == 0) return(NULL)

  txt <- .decode_html_entities(trimws(sub(pat, "\\1", m, perl = TRUE)))
  if (nchar(txt) == 0) return(NULL)
  txt
}

#' Decode common HTML entities to plain text
#' @param x Character scalar.
#' @return Character with entities decoded.
.decode_html_entities <- function(x) {
  x <- gsub("&amp;",  "&",  x, fixed = TRUE)
  x <- gsub("&lt;",   "<",  x, fixed = TRUE)
  x <- gsub("&gt;",   ">",  x, fixed = TRUE)
  x <- gsub("&quot;", "\"", x, fixed = TRUE)
  x <- gsub("&#39;",  "'",  x, fixed = TRUE)
  x
}


# =============================================================================
# 1. load_sector_fallback()
# =============================================================================
#' Load static CSV fallback for sector/industry mapping
#' @param path Character. Path to fallback CSV.
#' @return data.table with ticker, sector, industry columns, or NULL if missing.
load_sector_fallback <- function(path = "data/sector_industry_fallback.csv") {
  if (!file.exists(path)) return(NULL)
  dt <- fread(path, stringsAsFactors = FALSE, na.strings = c("NA", ""))
  if (!all(c("ticker", "sector", "industry") %in% names(dt))) return(NULL)
  dt[, ticker := trimws(toupper(ticker))]
  dt[, source := "fallback"]
  dt[, .(ticker, sector, industry, source)]
}


# =============================================================================
# 2. load_cached_sectors()
# =============================================================================
#' Load previously cached sector/industry data
#' @param cache_path Character. Path to cached parquet.
#' @return data.table or NULL if no cache exists.
load_cached_sectors <- function(cache_path = "cache/lookups/sector_industry.parquet") {
  if (!file.exists(cache_path)) return(NULL)
  tryCatch(
    as.data.table(arrow::read_parquet(cache_path)),
    error = function(e) NULL)
}


# =============================================================================
# 2b. .sector_asof()  --  SCD Type-2 reader (fix D2/B)
# =============================================================================
#' Resolve the sector table to one row per ticker as of a date
#'
#' SCD semantics: for each ticker, the row with the largest valid_from
#' <= as_of. Tickers whose earliest row postdates as_of resolve to that
#' earliest row -- the best estimate applied backward, the same
#' documented approximation as the migration floor-stamp (a missing row
#' would put the ticker in the Unknown bucket and kill the NA-mask,
#' which is strictly worse). Legacy tables without valid_from (and rows
#' checkpointed mid-scrape with NA valid_from) pass through as current.
#'
#' @param sec_dt data.table. Sector table (any vintage of the schema).
#' @param as_of Date or character. Resolution date (default today).
#' @return data.table, one row per ticker.
.sector_asof <- function(sec_dt, as_of = Sys.Date()) {
  if (is.null(sec_dt) || nrow(sec_dt) == 0) return(sec_dt)
  if (!"valid_from" %in% names(sec_dt)) return(sec_dt)

  d <- as.Date(as_of)
  x <- copy(sec_dt)
  x[, valid_from := as.Date(valid_from)]
  x[is.na(valid_from), valid_from := .SECTOR_VALID_FROM_FLOOR]
  setorder(x, ticker, valid_from)

  live <- x[valid_from <= d, .SD[.N], by = ticker]
  first_seen_later <- x[!ticker %in% live$ticker, .SD[1], by = ticker]
  out <- rbind(live, first_seen_later)
  setorder(out, ticker)
  out
}


# =============================================================================
# 2c. merge_sector_scd()  --  SCD Type-2 writer (fix D2/B)
# =============================================================================
#' Merge freshly-resolved current values into the dated sector table
#'
#' For each ticker in `current`: no history -> append with valid_from =
#' as_of (readers backfill pre-dates via the earliest-row rule); value
#' unchanged vs the latest historical row -> keep history untouched;
#' value changed -> append a new row dated as_of. Tickers present only
#' in history are preserved (a delisted name keeps its classification).
#' Re-running the same day replaces that day's row instead of stacking.
#'
#' @param prev data.table or NULL. Existing table (legacy or dated).
#' @param current data.table. Fresh current view: ticker/sector/industry/source.
#' @param as_of Date. Stamp for appended rows (default today).
#' @param floor Date. Stamp for a bootstrap from scratch.
#' @return data.table with valid_from, one row per (ticker, valid_from).
merge_sector_scd <- function(prev, current, as_of = Sys.Date(),
                             floor = .SECTOR_VALID_FROM_FLOOR) {
  cur <- copy(as.data.table(current))[, .(ticker, sector, industry, source)]
  as_of <- as.Date(as_of)

  if (is.null(prev) || nrow(prev) == 0) {
    cur[, valid_from := as.Date(floor)]
    return(cur)
  }

  hist <- copy(as.data.table(prev))
  if (!"valid_from" %in% names(hist)) hist[, valid_from := as.Date(floor)]
  hist[, valid_from := as.Date(valid_from)]
  hist[is.na(valid_from), valid_from := as.Date(floor)]
  hist <- hist[, .(ticker, sector, industry, source, valid_from)]
  setorder(hist, ticker, valid_from)

  latest <- hist[, .SD[.N], by = ticker]
  cmp <- merge(cur, latest[, .(ticker, old_sector = sector,
                               old_industry = industry)],
               by = "ticker", all.x = TRUE)
  add <- cmp[is.na(old_sector) | sector != old_sector |
               industry != old_industry,
             .(ticker, sector, industry, source)]
  if (nrow(add) > 0) {
    add[, valid_from := as_of]
    out <- rbind(hist, add)
    # same-day re-run: the newer resolution replaces that day's row
    out <- out[!duplicated(out[, .(ticker, valid_from)], fromLast = TRUE)]
  } else {
    out <- hist
  }
  setorder(out, ticker, valid_from)
  out
}


# =============================================================================
# 3. fetch_finviz_sectors()
# =============================================================================
#' Fetch sector/industry from finviz for a vector of tickers
#'
#' Resumes from cache: only fetches tickers not already cached.
#' Rate-limited. Logs warnings on failure, never stops.
#'
#' @param tickers Character vector of ticker symbols.
#' @param cache_path Character. Path for parquet cache.
#' @param batch_size Integer. Save cache every N tickers.
#' @return data.table with ticker, sector, industry, source columns.
fetch_finviz_sectors <- function(tickers,
                                 cache_path = "cache/lookups/sector_industry.parquet",
                                 batch_size = 50L) {

  tickers <- unique(trimws(toupper(tickers)))

  # Load existing cache
  cached <- load_cached_sectors(cache_path)
  if (!is.null(cached)) {
    already_done <- cached[source == "finviz", ticker]
    remaining <- setdiff(tickers, already_done)
    message(sprintf("fetch_finviz_sectors: %d cached, %d remaining",
                    length(already_done), length(remaining)))
  } else {
    cached <- data.table(
      ticker = character(), sector = character(),
      industry = character(), source = character())
    remaining <- tickers
  }

  if (length(remaining) == 0) {
    message("fetch_finviz_sectors: all tickers already cached")
    return(cached[ticker %in% tickers])
  }

  # Fetch remaining tickers
  new_rows <- list()
  n_success <- 0L
  n_fail <- 0L

  for (i in seq_along(remaining)) {
    tk <- remaining[i]

    if (i %% 50 == 0 || i == 1) {
      message(sprintf("  fetching %d/%d: %s", i, length(remaining), tk))
    }

    result <- tryCatch(.finviz_fetch_one(tk), error = function(e) NULL)

    if (!is.null(result)) {
      new_rows[[length(new_rows) + 1]] <- data.table(
        ticker = tk, sector = result$sector,
        industry = result$industry, source = "finviz")
      n_success <- n_success + 1L
    } else {
      warning(sprintf("fetch_finviz_sectors: failed for %s", tk),
              call. = FALSE)
      n_fail <- n_fail + 1L
    }

    # Checkpoint: save cache every batch_size tickers
    if (length(new_rows) > 0 && (i %% batch_size == 0 || i == length(remaining))) {
      batch_dt <- rbindlist(new_rows)
      cached <- rbind(cached, batch_dt)
      .write_sector_cache(cached, cache_path)
      new_rows <- list()
    }
  }

  message(sprintf("fetch_finviz_sectors: done -- %d success, %d failed",
                  n_success, n_fail))

  cached[ticker %in% tickers]
}


# =============================================================================
# 4. .write_sector_cache()
# =============================================================================
.write_sector_cache <- function(dt, cache_path) {
  out_dir <- dirname(cache_path)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  arrow::write_parquet(dt, cache_path)
}


# =============================================================================
# 5. build_sector_industry()  --  Orchestrator
# =============================================================================
#' Build the sector/industry lookup table
#'
#' Strategy:
#' 1. Load constituent master to get ticker list.
#' 2. Attempt finviz scraping for all tickers.
#' 3. Fill gaps from static CSV fallback if available.
#' 4. Write final parquet.
#'
#' @param master_path Character. Path to constituent_master.parquet.
#' @param output_path Character. Path for output parquet.
#' @param fallback_path Character. Path to fallback CSV.
#' @return data.table with ticker, sector, industry, source (invisibly).
build_sector_industry <- function(
    master_path   = "cache/lookups/constituent_master.parquet",
    output_path   = "cache/lookups/sector_industry.parquet",
    fallback_path = "data/sector_industry_fallback.csv") {

  message("build_sector_industry: starting...")

  # Step 0: Hold the existing dated table (SCD history) in memory NOW --
  # the finviz fetch checkpoints into output_path and would clobber it.
  prev <- load_cached_sectors(output_path)

  # Step 1: Load constituent master
  if (!file.exists(master_path)) {
    stop("build_sector_industry: constituent_master.parquet not found. Run Session A first.")
  }
  master <- as.data.table(arrow::read_parquet(master_path))
  tickers <- unique(master$ticker)
  message(sprintf("  %d unique tickers from constituent master", length(tickers)))

  # Step 2: Fetch from finviz
  result <- fetch_finviz_sectors(tickers, cache_path = output_path)

  # Steps 3-4 operate on the CURRENT view (one row per ticker); a cache
  # resume may have returned dated history -- collapse it first. The
  # history is re-attached from `prev` at write time (step 6b).
  result <- .sector_asof(result, Sys.Date())
  if ("valid_from" %in% names(result)) result[, valid_from := NULL]

  # Step 3: Fill gaps from fallback
  covered <- result$ticker
  missing <- setdiff(tickers, covered)

  if (length(missing) > 0) {
    message(sprintf("  %d tickers missing after finviz, trying fallback...",
                    length(missing)))
    fb <- load_sector_fallback(fallback_path)
    if (!is.null(fb)) {
      fb_match <- fb[ticker %in% missing]
      if (nrow(fb_match) > 0) {
        result <- rbind(result, fb_match)
        message(sprintf("  filled %d from fallback", nrow(fb_match)))
      }
    }
  }

  # Step 4: Deduplicate (keep finviz over fallback)
  result <- result[order(ticker, source != "finviz")]
  result <- result[!duplicated(ticker)]

  # Step 4b: Manual overrides. Finviz serves the CURRENT listing at a
  # ticker; when a symbol is reused (or redirected) after a constituent
  # delists, the scraped sector describes the wrong company. Override
  # only data-bearing cases (constituent has fundamentals in its era).
  # Precedent: resolve_all_ciks pass-3 manual CIK overrides.
  for (tk in names(.SECTOR_OVERRIDES)) {
    ov <- .SECTOR_OVERRIDES[[tk]]
    if (tk %in% result$ticker) {
      result[ticker == tk, `:=`(sector = ov[1], industry = ov[2],
                                source = "override")]
    } else {
      result <- rbind(result, data.table(
        ticker = tk, sector = ov[1], industry = ov[2], source = "override"))
    }
  }

  # Step 4c: static assignments from data files. Delisted names are dead
  # on finviz (or their reused symbol serves an unrelated ETF/company);
  # their era-correct classifications live in data files rather than
  # code literals. Same precedence as .SECTOR_OVERRIDES: always wins
  # over a scrape of the reused symbol.
  #   sector_overrides_oldcik.csv   -- old-CIK wave Tier 2 (2026-07),
  #                                    ~145 recovered delisted names
  #   sector_overrides_delisted.csv -- daily-update wave D2/A (2026-07),
  #                                    2021-24 index removals purged
  #                                    from finviz after the last scrape
  for (ov_csv in .SECTOR_OVERRIDE_CSVS) {
    if (!file.exists(ov_csv)) next
    ov_dt <- fread(ov_csv)
    n_new <- 0L; n_repl <- 0L
    for (i in seq_len(nrow(ov_dt))) {
      tk <- ov_dt$ticker[i]
      if (tk %in% result$ticker) {
        result[ticker == tk, `:=`(sector = ov_dt$sector[i],
                                  industry = ov_dt$industry[i],
                                  source = "override")]
        n_repl <- n_repl + 1L
      } else {
        result <- rbind(result, data.table(
          ticker = tk, sector = ov_dt$sector[i],
          industry = ov_dt$industry[i], source = "override"))
        n_new <- n_new + 1L
      }
    }
    message(sprintf("  %s: %d added, %d replaced",
                    basename(ov_csv), n_new, n_repl))
  }

  # Step 5: Final summary
  still_missing <- setdiff(tickers, result$ticker)
  if (length(still_missing) > 0) {
    message(sprintf("  WARNING: %d tickers still missing sector data: %s",
                    length(still_missing),
                    paste(head(still_missing, 20), collapse = ", ")))
  }

  # Step 6: Validate the current view
  .assert_output(result, "build_sector_industry", list(
    "is data.table" = is.data.table,
    "has 4 columns" = function(x) ncol(x) == 4,
    "has ticker col" = function(x) "ticker" %in% names(x),
    "has sector col" = function(x) "sector" %in% names(x),
    "has industry col" = function(x) "industry" %in% names(x),
    "no duplicate tickers" = function(x) !anyDuplicated(x$ticker)
  ))

  # Step 6b: Re-attach the dated dimension (D2/B). Unchanged tickers
  # keep their history; changed values append a row dated today.
  dated <- merge_sector_scd(prev, result)
  .assert_output(dated, "build_sector_industry", list(
    "has valid_from" = function(x) "valid_from" %in% names(x),
    "no duplicate (ticker, valid_from)" =
      function(x) !anyDuplicated(x[, .(ticker, valid_from)]),
    "current view intact" = function(x) {
      now <- .sector_asof(x, Sys.Date())
      nrow(now) >= nrow(result) &&
        all(result$ticker %in% now$ticker)
    }
  ))

  # Step 7: Write final parquet
  .write_sector_cache(dated, output_path)

  n_finviz   <- result[source == "finviz", .N]
  n_fallback <- result[source == "fallback", .N]
  message(sprintf(
    "build_sector_industry: wrote %d rows / %d tickers (%d finviz, %d fallback) to %s",
    nrow(dated), uniqueN(dated$ticker), n_finviz, n_fallback, output_path))

  # Sector distribution (current view)
  if (nrow(result) > 0) {
    sector_counts <- result[, .N, by = sector][order(-N)]
    message("  Sector distribution:")
    for (i in seq_len(nrow(sector_counts))) {
      message(sprintf("    %s: %d", sector_counts$sector[i], sector_counts$N[i]))
    }
  }

  invisible(dated)
}


# =============================================================================
# 6. get_sector_lookup()  --  Reader convenience function
# =============================================================================
#' Load the sector/industry lookup table
#'
#' @param cache_path Character. Path to cached parquet.
#' @return data.table with ticker, sector, industry, source.
get_sector_lookup <- function(cache_path = "cache/lookups/sector_industry.parquet") {
  if (!file.exists(cache_path)) {
    stop("get_sector_lookup: sector_industry.parquet not found. Run build_sector_industry() first.")
  }
  dt <- as.data.table(arrow::read_parquet(cache_path))
  .assert_output(dt, "get_sector_lookup", list(
    "is data.table" = is.data.table,
    "has ticker col" = function(x) "ticker" %in% names(x),
    "has sector col" = function(x) "sector" %in% names(x)
  ))
  dt
}
