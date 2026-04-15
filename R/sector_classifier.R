# ============================================================================
# sector_classifier.R  --  Finviz Sector/Industry Lookup (Module 3)
# ============================================================================
# Fetch sector and industry classification for S&P 500 constituents.
# Primary source: finviz.com ticker pages.
# Fallback: static CSV at data/sector_industry_fallback.csv.
# Output: cache/lookups/sector_industry.parquet
#
# Output schema:
#   ticker    chr   Ticker symbol
#   sector    chr   Finviz sector (11 sectors)
#   industry  chr   Finviz industry (~150 industries)
#   source    chr   "finviz" | "fallback"
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
.FINVIZ_UA       <- paste0(
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ",
  "AppleWebKit/537.36 (KHTML, like Gecko) ",
  "Chrome/120.0.0.0 Safari/537.36")

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
  # Finviz uses screener links with sec_ and ind_ prefixes:
  #   <a href="screener.ashx?v=111&f=sec_technology" class="tab-link">Technology</a>
  #   <a href="screener.ashx?v=111&f=ind_consumerelectronics" class="tab-link"...>Consumer Electronics</a>
  sector_m <- regmatches(html,
    regexpr('f=sec_[^"]*"[^>]*>([^<]+)</a>', html, perl = TRUE))
  industry_m <- regmatches(html,
    regexpr('f=ind_[^"]*"[^>]*>([^<]+)</a>', html, perl = TRUE))

  if (length(sector_m) == 0 || length(industry_m) == 0) return(NULL)

  sector <- sub('.*>([^<]+)</a>$', "\\1", sector_m, perl = TRUE)
  industry <- sub('.*>([^<]+)</a>$', "\\1", industry_m, perl = TRUE)

  sector   <- .decode_html_entities(trimws(sector))
  industry <- .decode_html_entities(trimws(industry))
  if (nchar(sector) == 0 || nchar(industry) == 0) return(NULL)

  list(sector = sector, industry = industry)
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

  # Step 1: Load constituent master
  if (!file.exists(master_path)) {
    stop("build_sector_industry: constituent_master.parquet not found. Run Session A first.")
  }
  master <- as.data.table(arrow::read_parquet(master_path))
  tickers <- unique(master$ticker)
  message(sprintf("  %d unique tickers from constituent master", length(tickers)))

  # Step 2: Fetch from finviz
  result <- fetch_finviz_sectors(tickers, cache_path = output_path)

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

  # Step 5: Final summary
  still_missing <- setdiff(tickers, result$ticker)
  if (length(still_missing) > 0) {
    message(sprintf("  WARNING: %d tickers still missing sector data: %s",
                    length(still_missing),
                    paste(head(still_missing, 20), collapse = ", ")))
  }

  # Step 6: Validate
  .assert_output(result, "build_sector_industry", list(
    "is data.table" = is.data.table,
    "has 4 columns" = function(x) ncol(x) == 4,
    "has ticker col" = function(x) "ticker" %in% names(x),
    "has sector col" = function(x) "sector" %in% names(x),
    "has industry col" = function(x) "industry" %in% names(x),
    "no duplicate tickers" = function(x) !anyDuplicated(x$ticker)
  ))

  # Step 7: Write final parquet
  .write_sector_cache(result, output_path)

  n_finviz   <- result[source == "finviz", .N]
  n_fallback <- result[source == "fallback", .N]
  message(sprintf(
    "build_sector_industry: wrote %d rows (%d finviz, %d fallback) to %s",
    nrow(result), n_finviz, n_fallback, output_path))

  # Sector distribution
  if (nrow(result) > 0) {
    sector_counts <- result[, .N, by = sector][order(-N)]
    message("  Sector distribution:")
    for (i in seq_len(nrow(sector_counts))) {
      message(sprintf("    %s: %d", sector_counts$sector[i], sector_counts$N[i]))
    }
  }

  invisible(result)
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
