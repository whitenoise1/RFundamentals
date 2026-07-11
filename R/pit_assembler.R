# ============================================================================
# pit_assembler.R  --  Point-in-Time Cross-Sectional Assembler (Module 5)
# ============================================================================
# Assembles point-in-time fundamental snapshots for the S&P 500 universe.
# For any snapshot date d: determines universe membership, retrieves most
# recent annual filing per ticker (filed <= d), fetches closing price on
# filing date, computes all indicators, and produces cross-sectional matrices.
#
# Output (per snapshot date, two parquet files):
#   cache/snapshots/pit_{YYYY-MM-DD}_raw.parquet
#   cache/snapshots/pit_{YYYY-MM-DD}_zscore.parquet
#
# Output schema (both files):
#   date       Date    Snapshot date
#   ticker     chr     Ticker symbol
#   industry   chr     Finviz industry
#   sector     chr     Finviz sector
#   [57 indicator columns]  num  Raw or z-scored values
#
# Public API:
#   get_universe_at_date(snapshot_date, master_dt)
#   fetch_ticker_prices(ticker, from, cache_dir, retries)
#   get_price_on_date(price_xts, target_date)
#   get_latest_annual_filing(fund_dt, as_of_date)
#   assemble_snapshot(snapshot_date, ...)
#   build_historical_snapshots(start_date, end_date, ...)
#
# Dependencies: data.table, arrow, quantmod, xts, zoo
# Requires: constituent_master.R, sector_classifier.R,
#           fundamental_fetcher.R, indicator_compute.R (already sourced)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(quantmod)
  library(xts)
  library(zoo)
})


# =============================================================================
# CONSTANTS
# =============================================================================

.PRICE_CACHE_DIR <- "cache/prices"
.SNAPSHOT_DIR     <- "cache/snapshots"


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


# =============================================================================
# 1. get_universe_at_date()
# =============================================================================
#' Determine which tickers were in the S&P 500 on a given date
#'
#' A ticker is in the universe on date d if:
#'   date_added <= d (or date_added is NA, meaning original member)
#'   AND date_removed > d (or date_removed is NA, meaning still in index)
#'
#' @param snapshot_date Date or character coercible to Date.
#' @param master_dt data.table. Constituent master (from constituent_master.parquet).
#' @return data.table subset of master_dt for tickers in the universe at that date.
get_universe_at_date <- function(snapshot_date, master_dt) {

  d <- as.Date(snapshot_date)

  universe <- master_dt[
    (is.na(date_added) | as.Date(date_added) <= d) &
    (is.na(date_removed) | as.Date(date_removed) > d)
  ]

  .assert_output(universe, "get_universe_at_date", list(
    "is data.table"    = is.data.table,
    "has ticker col"   = function(x) "ticker" %in% names(x),
    "has cik col"      = function(x) "cik" %in% names(x)
  ))

  universe
}


# =============================================================================
# 2. fetch_ticker_prices()
# =============================================================================

# Session-level memo of tickers whose Yahoo download failed after all
# retries (mostly delisted symbols). A multi-date backfill calls
# fetch_ticker_prices for the same dead tickers at every snapshot date;
# without the memo each date re-pays retries + exponential backoff
# (~10s per dead ticker). Session-scoped on purpose: a fresh R process
# retries everything, so a transient outage cannot poison future runs.
.PRICE_FETCH_FAILED <- new.env(parent = emptyenv())

# =============================================================================
# Wave P: provider symbol map + Tiingo fallback for Yahoo-purged names
# (design: docs/DESIGN_SECOND_PRICE_SOURCE.md)
# =============================================================================

# Roster ticker -> provider symbol for renamed chains where BOTH Yahoo and
# Tiingo key the full continuous history under the CURRENT symbol only.
# Verified against Tiingo 2026-07-08 (first prints line up with the old
# series). Duplicated in ttm_eps.R for getSplits -- keep in sync.
.PROVIDER_SYMBOL_MAP <- c(
  COG   = "CTRA",   # Cabot Oil & Gas -> Coterra (2021)
  GPS   = "GAP",    # Gap Inc ticker change (2024)
  FBHS  = "FBIN",   # Fortune Brands Home -> Innovations (2022)
  DISCA = "WBD",    # Discovery series A -> Warner Bros. Discovery (2022)
  VIAC  = "PARA",   # Viacom/ViacomCBS -> Paramount (2022)
  CDAY  = "DAY",    # Ceridian -> Dayforce (2024)
  # old-CIK wave Tier 2 (2026-07): same-share-line renames whose full
  # chain both providers key under the CURRENT symbol. Corporate
  # mechanics verified per name -- conversions with share RATIOS (e.g.
  # WPX->DVN) must never be mapped.
  ACE   = "CB",     # ACE Ltd renamed Chubb (2016)
  ADS   = "BFH",    # Alliance Data renamed Bread Financial (2022)
  ARNC  = "HWM",    # Arconic Inc renamed Howmet (2020; ARNC reused by spinco)
  CCE   = "CCEP",   # new CCE -> Coca-Cola European Partners (2016); Tiingo
                    # chain reaches the pre-2010 old-CCE line too
  DLPH  = "APTV",   # Delphi Automotive renamed Aptiv (2017; DLPH reused)
  HFC   = "DINO",   # HollyFrontier -> HF Sinclair (2022)
  HRS   = "LHX",    # Harris renamed L3Harris (2019)
  JEC   = "J",      # Jacobs Engineering ticker change (2019)
  KFT   = "MDLZ",   # Kraft Foods Inc renamed Mondelez (2012; KRFT spun off)
  SAI   = "LDOS",   # SAIC Inc renamed Leidos (2013; new SAIC spun off)
  TSO   = "ANDV",   # Tesoro renamed Andeavor (2017; chain ends 2018)
  TYC   = "JCI",    # Tyco plc = surviving entity, renamed JCI plc (2016)
  WPI   = "AGN"     # Watson -> Actavis -> Allergan plc (chain ends 2020)
)

.provider_symbol <- function(ticker) {
  mapped <- .PROVIDER_SYMBOL_MAP[ticker]
  if (!is.na(mapped)) as.character(mapped) else ticker
}

# Dead constituents whose symbol Yahoo has REUSED for an unrelated
# listing: a Yahoo fetch SUCCEEDS with the wrong company's prices (COL
# served a $0.15 penny stock across Rockwell Collins' membership window).
# For these, skip Yahoo entirely -- Tiingo keys the true delisted chain
# under the same symbol. Mirrored in ttm_eps.R .SPLITS_YAHOO_BLOCKLIST.
.YAHOO_REUSED_BLOCKLIST <- c("COL", "CAM", "PCL", "PCLN", "PCS")

# Tiingo token from env or ~/.Renviron (either case variant).
.tiingo_token <- function() {
  for (nm in c("TIINGO_TOKEN", "tiingo_token")) {
    v <- Sys.getenv(nm)
    if (nzchar(v)) return(v)
  }
  if (file.exists("~/.Renviron")) {
    readRenviron("~/.Renviron")
    for (nm in c("TIINGO_TOKEN", "tiingo_token")) {
      v <- Sys.getenv(nm)
      if (nzchar(v)) return(v)
    }
  }
  ""
}

# Resolve the cache file for a ticker: Yahoo first (primary source), then
# Tiingo (fallback, honest provenance in the name). Returns the existing
# file, or the Yahoo path when neither exists (the write target for a
# fresh fetch).
.price_cache_path <- function(ticker, from = "2009-01-01",
                              cache_dir = .PRICE_CACHE_DIR) {
  for (src in c("yahoo", "tiingo")) {
    f <- file.path(cache_dir, sprintf("%s_%s_%s.parquet", ticker, src, from))
    if (file.exists(f)) return(f)
  }
  file.path(cache_dir, sprintf("%s_yahoo_%s.parquet", ticker, from))
}

#' Download daily prices from Tiingo and convert to the Yahoo cache basis
#'
#' Tiingo's `close` is the RAW as-traded print with splitFactor/divCash
#' event columns -- the inverse of Yahoo's split-adjusted Close. The
#' adapter converts at write time so every existing reader works
#' unchanged: cached_close = raw_close * .split_factor(date, splits),
#' and the derived TRUE split events are written to cache/splits/
#' {ticker}.parquet (quantmod ratio convention, 2:1 -> 0.5).
#'
#' splitFactor events with |log(f)| < log(1.25) are NOT splits: spinoffs
#' and special distributions are encoded there too (K 2023-10-02 carries
#' splitFactor 1.065 for the WK Kellogg spinoff) and would corrupt the
#' share-issuance and TTM-EPS split adjustments. Real splits are 3:2 or
#' coarser. Tiingo adjClose is never used (dividend-rebased -- the same
#' defect class as Yahoo Adjusted, see the NVDA 2015 P/E postmortem).
#'
#' @param ticker Character. ROSTER ticker (provider symbol resolved
#'   internally via .PROVIDER_SYMBOL_MAP).
#' @param from Character. Start date.
#' @param split_dir Character. Splits cache directory.
#' @param retries Integer. Download attempts.
#' @return xts with Open/High/Low/Close/Volume on the Yahoo cache basis,
#'   or NULL on failure / missing token.
.fetch_prices_tiingo <- function(ticker, from = "2009-01-01",
                                 split_dir = "cache/splits",
                                 retries = 3L) {

  token <- .tiingo_token()
  if (token == "") return(NULL)

  # Tiingo keys class shares with hyphens (brk-b), same as Yahoo
  sym <- gsub("\\.", "-", .provider_symbol(ticker))
  url <- sprintf(
    "https://api.tiingo.com/tiingo/daily/%s/prices?startDate=%s&token=%s",
    tolower(sym), from, token)

  dat <- NULL
  for (attempt in seq_len(retries)) {
    dat <- tryCatch({
      resp <- httr::GET(url, httr::timeout(30))
      if (httr::status_code(resp) == 429) { Sys.sleep(5 * attempt); stop("429") }
      if (httr::status_code(resp) != 200) stop(httr::status_code(resp))
      d <- as.data.table(jsonlite::fromJSON(
        httr::content(resp, as = "text", encoding = "UTF-8")))
      if (nrow(d) == 0) stop("empty")
      d
    }, error = function(e) {
      if (attempt < retries) Sys.sleep(2^attempt)
      NULL
    })
    if (!is.null(dat)) break
  }
  if (is.null(dat)) return(NULL)

  dat[, date := as.Date(substr(date, 1, 10))]
  setorder(dat, date)

  conv <- .tiingo_convert(dat, ticker)

  # Never clobber a NON-EMPTY (Yahoo-derived) splits cache. An EMPTY one
  # may be a stale artifact of a failed getSplits on this purged symbol
  # (load_ticker_splits caches empties) -- upgrade it when Tiingo carries
  # real events, else the cached close (multiplied by real factors) would
  # be divided by 1 at read time and pre-split as-traded prices come out
  # wrong.
  split_file <- file.path(split_dir, paste0(ticker, ".parquet"))
  existing <- if (file.exists(split_file)) {
    tryCatch(nrow(arrow::read_parquet(split_file)), error = function(e) 0L)
  } else -1L
  if (existing <= 0L) {
    if (!dir.exists(split_dir)) dir.create(split_dir, recursive = TRUE)
    tryCatch(arrow::write_parquet(conv$splits, split_file),
             error = function(e) NULL)
  }

  conv$px
}


# TRUE split factor test, shared semantics with ttm_eps.R .tiingo_splits
# (duplicated there for standalone sourcing -- keep in sync). A real
# split is a clean small-integer ratio (2, 3, 3:2, 6:5, 1:8...) away
# from 1; spinoffs and special distributions are encoded in splitFactor
# too but as arbitrary decimals (K 2023-10-02 = 1.065 for the WK Kellogg
# spinoff) and must NOT become split events -- they would corrupt the
# share-issuance and TTM-EPS adjustments. Nonpositive factors (dirty
# data) are rejected before log().
.is_true_split_factor <- function(f) {
  vapply(f, function(x) {
    if (is.na(x) || x <= 0 || x == 1) return(FALSE)
    if (abs(log(x)) < log(1.15)) return(FALSE)
    for (q in 1:10) {
      p <- round(x * q)
      if (p >= 1 && p != q && abs(x - p / q) / x < 0.002) return(TRUE)
    }
    FALSE
  }, logical(1))
}

# Pure conversion: Tiingo raw daily rows -> list(px = xts on the Yahoo
# cache basis, splits = data.table(ex_date, ratio)). Split of the two
# concerns from the HTTP layer so the basis math is unit-testable.
.tiingo_convert <- function(dat, ticker) {

  # true split events only; quantmod convention ratio = 1/f
  splits <- data.table(ex_date = as.Date(character()), ratio = numeric())
  if ("splitFactor" %in% names(dat)) {
    ev <- dat[.is_true_split_factor(splitFactor)]
    if (nrow(ev) > 0) {
      splits <- ev[, .(ex_date = date, ratio = 1 / splitFactor)]
    }
  }

  # raw as-traded -> Yahoo-style split-adjusted cache basis. Adjusted =
  # Tiingo adjClose passthrough (already split+dividend adjusted to the
  # current basis, same semantics as Yahoo's column 6) -- kept ONLY for
  # column-shape parity with Yahoo files (feature_compute_rF expects it);
  # never a valid level for ratios.
  fct <- .split_factor(dat$date, splits)
  out <- data.frame(
    Open     = dat$open  * fct,
    High     = dat$high  * fct,
    Low      = dat$low   * fct,
    Close    = dat$close * fct,
    Volume   = dat$volume / fct,
    Adjusted = if ("adjClose" %in% names(dat)) dat$adjClose else
      dat$close * fct
  )
  colnames(out) <- paste(gsub("\\.", "-", ticker),
                         c("Open", "High", "Low", "Close", "Volume",
                           "Adjusted"),
                         sep = ".")
  list(px = xts(out, order.by = dat$date), splits = splits)
}

#' Download OHLCV price data for a single ticker with parquet cache
#'
#' Adapted from download_ohlcv() in ref/data_loader.R.
#' Returns xts object. Caches to cache/prices/{ticker}_yahoo_{from}.parquet.
#' Tickers that fail all retries are memoized for the session and return
#' NULL immediately on subsequent calls.
#'
#' @param ticker Character. Ticker symbol.
#' @param from Character. Start date (default "2009-01-01").
#' @param cache_dir Character. Cache directory.
#' @param retries Integer. Number of download attempts.
#' @return xts OHLCV object, or NULL on failure.
fetch_ticker_prices <- function(ticker, from = "2009-01-01",
                                cache_dir = .PRICE_CACHE_DIR,
                                retries = 3L) {

  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  # Try cache first (Yahoo primary, Tiingo fallback file)
  cache_file <- .price_cache_path(ticker, from, cache_dir)
  if (file.exists(cache_file)) {
    cached <- tryCatch({
      df <- as.data.frame(arrow::read_parquet(cache_file))
      xts(df[, -1, drop = FALSE], order.by = as.Date(df$date))
    }, error = function(e) NULL)
    if (!is.null(cached) && nrow(cached) > 0) return(cached)
  }

  # Known-dead this session: skip the download entirely. The key encodes
  # whether a Tiingo token was available: a failure memoized WITHOUT the
  # token must not block a retry after the user exports one mid-session.
  memo_key <- sprintf("%s_%s%s", ticker, from,
                      if (.tiingo_token() == "") "_notoken" else "")
  if (isTRUE(.PRICE_FETCH_FAILED[[memo_key]])) return(NULL)

  # Download from Yahoo. Renamed chains resolve to the CURRENT symbol
  # (COG -> CTRA: Yahoo purges the old symbol and keys the continuous
  # history under the new one). Yahoo uses dashes for class tickers
  # (BRK-B), while the index roster uses dots (BRK.B); without the
  # conversion the request 404s. Reused dead symbols never ask Yahoo --
  # it would answer with the WRONG company (see .YAHOO_REUSED_BLOCKLIST).
  yahoo_symbol <- gsub("\\.", "-", .provider_symbol(ticker))
  px <- NULL
  retries_yahoo <- if (ticker %in% .YAHOO_REUSED_BLOCKLIST) 0L else retries
  for (attempt in seq_len(retries_yahoo)) {
    px <- tryCatch({
      p <- getSymbols(yahoo_symbol, src = "yahoo", from = from,
                      to = Sys.Date(), auto.assign = FALSE)
      if (is.null(p) || nrow(p) == 0) stop("empty result")
      p
    }, error = function(e) {
      if (attempt < retries) Sys.sleep(2^attempt)
      NULL
    })
    if (!is.null(px)) break
  }

  src <- "yahoo"
  if (!is.null(px)) {
    # normalize column prefixes to the ROSTER ticker (dash form): mapped
    # chains fetch under the current symbol (COG -> CTRA) but consumers
    # that match columns BY NAME (feature_compute_rF) key on the roster
    colnames(px) <- sub("^[^.]+\\.",
                        paste0(gsub("\\.", "-", ticker), "."),
                        colnames(px))
  } else {
    # Tiingo fallback (Wave P): purged delistings. The adapter returns
    # data already on the Yahoo cache basis; NULL when no token is set
    # or the symbol is missing there too.
    if (.tiingo_token() == "") {
      if (!isTRUE(.PRICE_FETCH_FAILED[["__tiingo_token_warned"]])) {
        warning("fetch_ticker_prices: no TIINGO_TOKEN -- purged delistings stay NA this run",
                call. = FALSE)
        .PRICE_FETCH_FAILED[["__tiingo_token_warned"]] <- TRUE
      }
    } else {
      px <- .fetch_prices_tiingo(ticker, from = from)
      src <- "tiingo"
      if (!is.null(px)) {
        message(sprintf("  fetch_ticker_prices: %s recovered via Tiingo (%d rows)",
                        ticker, nrow(px)))
      }
    }
  }

  if (is.null(px)) {
    .PRICE_FETCH_FAILED[[memo_key]] <- TRUE
    return(NULL)
  }

  # Cache write (provenance in the filename)
  cache_file <- file.path(cache_dir,
                          sprintf("%s_%s_%s.parquet", ticker, src, from))
  df <- data.frame(date = as.character(index(px)), coredata(px),
                   check.names = FALSE)
  tryCatch(arrow::write_parquet(df, cache_file), error = function(e) NULL)

  px
}


# =============================================================================
# 3. get_price_on_date()
# =============================================================================
#' Cumulative split factor applied after a date
#'
#' Yahoo's Close column is retroactively split-adjusted to today's share
#' basis: Close(d) = as_traded(d) * prod(ratio of splits with ex_date > d),
#' where a 4:1 split has ratio 0.25. Dividing Close by this factor recovers
#' the as-traded print. Returns 1 where no later splits exist.
#' (Duplicated from ttm_eps.R so both modules source standalone.)
#'
#' @param filed Date vector.
#' @param splits data.table with ex_date, ratio; NULL/empty allowed.
#' @return Numeric vector, same length as filed.
.split_factor <- function(filed, splits = NULL) {
  if (is.null(splits) || !NROW(splits)) return(rep(1, length(filed)))
  ex <- as.Date(splits$ex_date); rt <- as.numeric(splits$ratio)
  vapply(filed, function(f) {
    if (is.na(f)) return(1)
    sel <- ex > f & !is.na(rt) & rt > 0
    if (!any(sel)) 1 else prod(rt[sel])
  }, numeric(1))
}


#' Look up the as-traded closing price on a specific date
#'
#' If the target date is not a trading day, uses the most recent prior
#' trading day (up to 10 calendar days back).
#'
#' Basis: Yahoo's Close is split-adjusted to today's basis and Adjusted
#' additionally removes dividends -- both bake future corporate events
#' into historical levels, mis-scaling ratios against as-reported
#' fundamentals (a stock with a later 4:1 split looked 4x cheaper).
#' The as-traded print is Close divided by the post-date split factor.
#' Without splits this equals Close: exact for never-split tickers,
#' still dividend-clean for the rest.
#'
#' @param price_xts xts. OHLCV price data for one ticker.
#' @param target_date Date or character. The date to look up.
#' @param splits data.table with ex_date, ratio (load_ticker_splits
#'   output), or NULL for tickers without split history.
#' @return Numeric scalar (as-traded close), or NA_real_ if unavailable.
get_price_on_date <- function(price_xts, target_date, splits = NULL) {

  if (is.null(price_xts) || nrow(price_xts) == 0) return(NA_real_)

  d <- as.Date(target_date)
  dates <- index(price_xts)
  valid <- dates[dates <= d]
  if (length(valid) == 0) return(NA_real_)

  closest <- max(valid)
  # Don't reach back more than 10 calendar days
  if (as.numeric(d - closest) > 10) return(NA_real_)

  row <- coredata(price_xts[closest, ])
  nc <- ncol(price_xts)

  # Close is column 4; single-column series fall back to column 1
  px <- if (nc >= 4) as.numeric(row[1, 4]) else if (nc >= 1)
    as.numeric(row[1, 1]) else NA_real_

  px / .split_factor(closest, splits)
}


# =============================================================================
# 4. get_latest_annual_filing()
# =============================================================================
#' Find the most recent annual filing available as of a given date
#'
#' Scans long-format fundamentals for FY rows where filed <= as_of_date.
#' Returns the fiscal year and the latest filed date for that year.
#'
#' @param fund_dt data.table. Long-format fundamentals for one ticker.
#' @param as_of_date Date or character. Point-in-time cutoff.
#' @return List with $fiscal_year (integer) and $filed_date (Date),
#'   or NULL if no annual filing is available.
get_latest_annual_filing <- function(fund_dt, as_of_date,
                                     min_concepts = 5L) {

  if (is.null(fund_dt) || nrow(fund_dt) == 0) return(NULL)

  d <- as.Date(as_of_date)

  # Point-in-time: only data that was filed on or before the snapshot date
  available <- fund_dt[!is.na(filed) & as.Date(filed) <= d]
  if (nrow(available) == 0) return(NULL)

  # Annual filings only
  fy_rows <- available[!is.na(period_type) & period_type == "FY"]
  if (nrow(fy_rows) == 0) return(NULL)

  # Walk backwards from most recent FY until we find one with sufficient data.
  # Non-December fiscal year companies can have sparse FY entries where only
  # one or two concepts are classified as that fiscal year.
  fy_candidates <- sort(unique(fy_rows$fiscal_year), decreasing = TRUE)

  for (fy in fy_candidates) {
    fy_data <- fy_rows[fiscal_year == fy]
    n_concepts <- length(unique(fy_data$concept))

    if (n_concepts >= min_concepts) {
      filed_date <- max(fy_data$filed, na.rm = TRUE)
      return(list(fiscal_year = as.integer(fy),
                  filed_date  = as.Date(filed_date)))
    }
  }

  # No fiscal year with sufficient data
  NULL
}


# =============================================================================
# 5. .generate_snapshot_dates()  --  Quarterly end dates
# =============================================================================
.generate_snapshot_dates <- function(start_date, end_date) {

  start <- as.Date(start_date)
  end   <- as.Date(end_date)

  start_year <- as.integer(format(start, "%Y"))
  end_year   <- as.integer(format(end, "%Y"))

  qe_months <- c(3L, 6L, 9L, 12L)
  qe_days   <- c(31L, 30L, 30L, 31L)

  dates <- as.Date(character(0))
  for (y in start_year:end_year) {
    for (q in seq_along(qe_months)) {
      d <- as.Date(sprintf("%d-%02d-%02d", y, qe_months[q], qe_days[q]))
      if (d >= start && d <= end) {
        dates <- c(dates, d)
      }
    }
  }

  sort(dates)
}


# =============================================================================
# 6. .prefetch_universe_prices()  --  Batch price download
# =============================================================================
#' Download and cache prices for all tickers in a universe
#'
#' Only fetches tickers not already cached. Logs progress.
#'
#' @param tickers Character vector.
#' @param cache_dir Character. Price cache directory.
#' @return Invisible NULL. Side effect: populates cache.
.prefetch_universe_prices <- function(tickers, from = "2009-01-01",
                                      cache_dir = .PRICE_CACHE_DIR) {

  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  # Check which tickers need fetching (either provider's cache counts)
  need_fetch <- character(0)
  for (tk in tickers) {
    if (!file.exists(.price_cache_path(tk, from, cache_dir))) {
      need_fetch <- c(need_fetch, tk)
    }
  }

  if (length(need_fetch) == 0) {
    message("  prices: all cached")
    return(invisible(NULL))
  }

  message(sprintf("  prices: fetching %d tickers (of %d total)...",
                  length(need_fetch), length(tickers)))

  n_ok <- 0L
  n_fail <- 0L

  for (i in seq_along(need_fetch)) {
    tk <- need_fetch[i]
    if (i %% 50 == 0 || i == 1) {
      message(sprintf("    price fetch %d/%d: %s", i, length(need_fetch), tk))
    }

    px <- tryCatch(
      fetch_ticker_prices(tk, cache_dir = cache_dir),
      error = function(e) NULL
    )

    if (!is.null(px)) {
      n_ok <- n_ok + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  message(sprintf("  prices: %d fetched, %d failed", n_ok, n_fail))
  invisible(NULL)
}


# =============================================================================
# 7. assemble_snapshot()  --  Core assembler
# =============================================================================
#' Assemble a point-in-time cross-sectional snapshot for a single date
#'
#' For each ticker in the S&P 500 on the snapshot date:
#'   1. Loads cached fundamentals
#'   2. Filters to filings known as of snapshot date (point-in-time)
#'   3. Identifies the most recent annual filing
#'   4. Gets the closing price on the filing date
#'   5. Computes all indicators (get_indicator_names(), 116 as of Wave 4)
#'   6. Stacks into cross-section and z-scores
#'
#' @param snapshot_date Date or character. The snapshot date.
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @param prefetch_prices Logical. Pre-download prices for full universe.
#' @param zscore_mode Character. "cross_section" (default) standardizes each
#'   indicator against the current snapshot's cross-section (per CLAUDE.md
#'   point-in-time rule 4). "expanding_window" instead pools winsorized values
#'   from all prior pit_*_raw snapshots plus the current one -- an opt-in
#'   research variant, not the default output contract.
#' @param zscore_clip Numeric length-2 or NULL. Post-standardization clip
#'   bounds, honored in both modes. Default c(-5, 5); NULL disables.
#' @param .master_dt data.table or NULL. Pre-loaded master (avoids re-reading).
#' @param .sector_dt data.table or NULL. Pre-loaded sectors (avoids re-reading).
#' @return List with $raw, $zscored (data.tables), $stats, or NULL on failure.
assemble_snapshot <- function(snapshot_date,
                              master_path     = "cache/lookups/constituent_master.parquet",
                              sector_path     = "cache/lookups/sector_industry.parquet",
                              fund_dir        = "cache/fundamentals",
                              price_cache_dir = "cache/prices",
                              output_dir      = "cache/snapshots",
                              prefetch_prices = TRUE,
                              zscore_mode     = c("cross_section", "expanding_window"),
                              zscore_clip     = c(-5, 5),
                              .master_dt      = NULL,
                              .sector_dt      = NULL) {

  zscore_mode <- match.arg(zscore_mode)

  snapshot_date <- as.Date(snapshot_date)
  message(sprintf("assemble_snapshot: %s", snapshot_date))

  # Splits are needed for the as-traded price basis and split-adjusted
  # share issuance. Do not fail standalone use, but never degrade silently:
  # the regression harness bug of 2026-07-06 showed silent gating produces
  # plausible-but-wrong split-sensitive indicators.
  if (!exists("load_ticker_splits")) {
    warning(paste0("assemble_snapshot: ttm_eps.R not sourced -- prices fall ",
                   "back to split-adjusted Close and share issuance is ",
                   "computed unadjusted"), call. = FALSE)
  }

  # -- Load lookups (use pre-loaded if provided) --
  if (!is.null(.master_dt)) {
    master <- .master_dt
  } else {
    if (!file.exists(master_path)) {
      stop("assemble_snapshot: constituent_master.parquet not found")
    }
    master <- as.data.table(arrow::read_parquet(master_path))
  }

  if (!is.null(.sector_dt)) {
    sectors <- .sector_dt
  } else {
    if (!file.exists(sector_path)) {
      stop("assemble_snapshot: sector_industry.parquet not found")
    }
    sectors <- as.data.table(arrow::read_parquet(sector_path))
  }

  # -- Universe at snapshot date --
  universe <- get_universe_at_date(snapshot_date, master)
  message(sprintf("  universe: %d tickers", nrow(universe)))

  # Merge sector/industry
  universe <- merge(universe, sectors[, .(ticker, sector, industry)],
                    by = "ticker", all.x = TRUE)

  # -- Prefetch prices --
  if (prefetch_prices) {
    .prefetch_universe_prices(universe$ticker, cache_dir = price_cache_dir)
  }

  # -- Process each ticker --
  n_total <- nrow(universe)

  # Pre-allocate collection lists (avoid O(n^2) vector growth)
  indicator_list   <- vector("list", n_total)
  ticker_buf       <- vector("character", n_total)
  sector_buf       <- vector("character", n_total)
  industry_buf     <- vector("character", n_total)
  n_success  <- 0L
  n_no_fund  <- 0L
  n_no_filing <- 0L
  n_no_price <- 0L

  for (i in seq_len(n_total)) {
    tk  <- universe$ticker[i]
    cik <- universe$cik[i]
    sec <- universe$sector[i]
    ind <- universe$industry[i]

    if (i %% 100 == 0 || i == 1) {
      message(sprintf("  computing %d/%d: %s", i, n_total, tk))
    }

    # Load raw vintages (one row per concept x period x filing)
    fund_dt <- tryCatch(
      get_fundamentals(tk, cik, cache_dir = fund_dir, vintages = TRUE),
      error = function(e) NULL
    )
    if (is.null(fund_dt) || nrow(fund_dt) == 0) {
      n_no_fund <- n_no_fund + 1L
      next
    }

    # Point-in-time resolution: drop rows filed after the snapshot date and
    # collapse vintages to the row that was authoritative on that date
    # (original filing until an amendment/re-report becomes public).
    fund_dt <- pit_dedup(fund_dt, as_of = snapshot_date)
    if (nrow(fund_dt) == 0) {
      n_no_filing <- n_no_filing + 1L
      next
    }

    # Find latest annual filing
    filing <- get_latest_annual_filing(fund_dt, snapshot_date)
    if (is.null(filing)) {
      n_no_filing <- n_no_filing + 1L
      next
    }

    # Split events: needed for the as-traded price basis and for
    # share-issuance adjustment. load_ticker_splits lives in ttm_eps.R;
    # a startup warning is emitted when it is absent (see loop preamble).
    # fetch = FALSE: no network in the snapshot loop; the splits cache is
    # populated by the timeseries TTM augment.
    splits <- if (exists("load_ticker_splits")) {
      tryCatch(load_ticker_splits(tk, fetch = FALSE), error = function(e) NULL)
    } else NULL

    # Get as-traded price on filing date
    price_data <- tryCatch(
      fetch_ticker_prices(tk, cache_dir = price_cache_dir),
      error = function(e) NULL
    )
    price <- get_price_on_date(price_data, filing$filed_date, splits)
    if (is.na(price)) n_no_price <- n_no_price + 1L

    # Compute indicators (pass point-in-time-filtered fundamentals)
    sector_label <- if (is.na(sec)) "Unknown" else sec
    indicators <- tryCatch(
      compute_ticker_indicators(fund_dt, price, sector_label,
                                target_fy = filing$fiscal_year,
                                splits = splits),
      error = function(e) {
        warning(sprintf("  indicators failed for %s: %s", tk, e$message),
                call. = FALSE)
        NULL
      }
    )
    if (is.null(indicators)) next

    n_success <- n_success + 1L
    indicator_list[[n_success]] <- indicators
    ticker_buf[n_success]       <- tk
    sector_buf[n_success]       <- sector_label
    industry_buf[n_success]     <- if (is.na(ind)) "Unknown" else ind
  }

  # Trim pre-allocated buffers to actual size
  indicator_list   <- indicator_list[seq_len(n_success)]
  valid_tickers    <- ticker_buf[seq_len(n_success)]
  valid_sectors    <- sector_buf[seq_len(n_success)]
  valid_industries <- industry_buf[seq_len(n_success)]

  message(sprintf(
    "  results: %d success (%d without price), %d no fundamentals, %d no filing (of %d)",
    n_success, n_no_price, n_no_fund, n_no_filing, n_total))

  if (n_success == 0) {
    warning("assemble_snapshot: no tickers computed successfully")
    return(NULL)
  }

  # -- Build cross-section --
  # compute_cross_section produces the per-snapshot (cross-sectional) z-score,
  # which is the default output. It is overwritten below only when the caller
  # opts into zscore_mode = "expanding_window".
  cs <- compute_cross_section(indicator_list, valid_tickers, valid_sectors,
                              industries = valid_industries,
                              clip = zscore_clip)

  # Add metadata columns to raw
  raw_dt <- copy(cs$raw)
  raw_dt[, date     := snapshot_date]
  raw_dt[, industry := valid_industries]
  raw_dt[, sector   := valid_sectors]
  setcolorder(raw_dt, c("date", "ticker", "industry", "sector"))

  # -- Z-score path selection --
  # Expanding-window pools winsorized values from all pit_*_raw snapshots
  # with date < snapshot_date plus the current snapshot, and standardizes
  # via median / MAD of the pool. Cross-section mode uses only the current
  # snapshot.
  if (zscore_mode == "expanding_window") {
    zscored_dt <- zscore_expanding_window(raw_dt,
                                          history_dt_list = NULL,
                                          output_dir = output_dir,
                                          clip = zscore_clip)
  } else {
    zscored_dt <- copy(cs$zscored)
    zscored_dt[, date     := snapshot_date]
    zscored_dt[, industry := valid_industries]
    zscored_dt[, sector   := valid_sectors]
    setcolorder(zscored_dt, c("date", "ticker", "industry", "sector"))
  }

  # -- Validate --
  .assert_output(raw_dt, "assemble_snapshot$raw", list(
    "is data.table"     = is.data.table,
    "has date col"      = function(x) "date" %in% names(x),
    "has ticker col"    = function(x) "ticker" %in% names(x),
    "has sector col"    = function(x) "sector" %in% names(x),
    "has indicator cols" = function(x) {
      all(get_indicator_names() %in% names(x))
    }
  ))

  # -- Write parquet --
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  raw_path    <- file.path(output_dir,
                           sprintf("pit_%s_raw.parquet", snapshot_date))
  zscore_path <- file.path(output_dir,
                           sprintf("pit_%s_zscore.parquet", snapshot_date))

  arrow::write_parquet(raw_dt, raw_path)
  arrow::write_parquet(zscored_dt, zscore_path)

  message(sprintf("  wrote: %s (%d tickers x %d indicators)",
                  basename(raw_path), nrow(raw_dt),
                  length(get_indicator_names())))

  list(
    raw     = raw_dt,
    zscored = zscored_dt,
    stats   = list(
      snapshot_date = snapshot_date,
      n_universe    = n_total,
      n_success     = n_success,
      n_no_fund     = n_no_fund,
      n_no_filing   = n_no_filing,
      n_no_price    = n_no_price
    )
  )
}


# =============================================================================
# 8. build_historical_snapshots()  --  Quarterly cadence builder
# =============================================================================
#' Build point-in-time snapshots at quarterly cadence
#'
#' Generates snapshots for quarter-end dates (Mar 31, Jun 30, Sep 30, Dec 31)
#' between start_date and end_date. Resumable: skips dates with existing
#' parquet files unless force_refresh = TRUE.
#'
#' @param start_date Character. First quarter-end date (default "2010-03-31").
#' @param end_date Character or Date. Last date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @param force_refresh Logical. Rebuild existing snapshots.
#' @return List of stats per snapshot date (invisibly).
build_historical_snapshots <- function(
    start_date      = "2010-03-31",
    end_date        = Sys.Date(),
    master_path     = "cache/lookups/constituent_master.parquet",
    sector_path     = "cache/lookups/sector_industry.parquet",
    fund_dir        = "cache/fundamentals",
    price_cache_dir = "cache/prices",
    output_dir      = "cache/snapshots",
    force_refresh   = FALSE) {

  dates <- .generate_snapshot_dates(start_date, end_date)
  message(sprintf(
    "build_historical_snapshots: %d quarterly dates (%s to %s)",
    length(dates), min(dates), max(dates)))

  # Load lookups once, pass to each snapshot call
  master_dt <- as.data.table(arrow::read_parquet(master_path))
  sector_dt <- as.data.table(arrow::read_parquet(sector_path))

  results  <- list()
  n_built  <- 0L
  n_skip   <- 0L
  n_fail   <- 0L

  for (i in seq_along(dates)) {
    d <- dates[i]

    # Check if already exists
    raw_path <- file.path(output_dir,
                          sprintf("pit_%s_raw.parquet", d))
    if (file.exists(raw_path) && !force_refresh) {
      n_skip <- n_skip + 1L
      next
    }

    message(sprintf("\n[%d/%d] Snapshot: %s", i, length(dates), d))

    result <- tryCatch(
      assemble_snapshot(d, master_path, sector_path, fund_dir,
                        price_cache_dir, output_dir,
                        .master_dt = master_dt,
                        .sector_dt = sector_dt),
      error = function(e) {
        warning(sprintf("  snapshot failed for %s: %s", d, e$message),
                call. = FALSE)
        NULL
      }
    )

    if (!is.null(result)) {
      results[[as.character(d)]] <- result$stats
      n_built <- n_built + 1L
    } else {
      n_fail <- n_fail + 1L
    }
  }

  message(sprintf(
    "\nbuild_historical_snapshots: done -- %d built, %d skipped, %d failed (of %d)",
    n_built, n_skip, n_fail, length(dates)))

  invisible(results)
}


# =============================================================================
# 8b. zscore_expanding_window()  --  Expanding-window robust z-scoring
# =============================================================================
# Pooled-history standardization. For each indicator:
#   1. Winsorize every snapshot's cross-section at its own [p2.5, p97.5].
#   2. Pool the winsorized values from all historical snapshots plus the
#      current snapshot (expanding window up to snapshot_date).
#   3. Standardize the current snapshot using median / MAD of the pool
#      (via .robust_zscore, which applies the per-snapshot winsorization
#      to x independently).
#
# Rationale (see docs/research/06_indicator_verification_report.md §12):
#   - Per-snapshot winsorization bounds keep each cross-section's regime
#     intact (a firm in the 2nd percentile this quarter gets pulled to p2.5
#     of this quarter's distribution, not of a 10-year pool).
#   - Expanding-window median / MAD stabilizes the location / scale as
#     evidence accumulates, avoiding single-quarter sampling noise.
#   - Burn-in: at T_1 the pool = current snapshot only; expanding-window
#     equals per-snapshot z. This matches the user's agreed Q4 semantics
#     (emit z anyway; "expanding" just means "use everything you have").

# Concat pool of winsorized per-snapshot values for one indicator column.
# history_dt_list: list of prior raw snapshots (strictly < current date).
# raw_dt: current snapshot. Both are included in the pool. Rows whose
# sector NA-masks the indicator (.SECTOR_NA_INDICATORS) are excluded.
.build_pooled_calib <- function(ind_col, raw_dt, history_dt_list) {
  snapshots <- c(history_dt_list, list(raw_dt))
  pool <- numeric(0)
  for (s in snapshots) {
    if (is.null(s) || !(ind_col %in% names(s))) next
    vals <- s[[ind_col]]
    if ("sector" %in% names(s)) {
      vals[.sector_na_mask(ind_col, s[["sector"]])] <- NA_real_
    }
    vals <- vals[!is.na(vals)]
    if (length(vals) < 3L) next  # too small to winsorize meaningfully
    q <- quantile(vals, c(0.025, 0.975), na.rm = TRUE)
    pool <- c(pool, pmin(pmax(vals, q[1]), q[2]))
  }
  pool
}

# Read prior raw snapshots from disk (dates strictly < snapshot_date).
# Returns a list of data.tables; empty list if directory is missing or
# no prior files exist.
.load_history_snapshots <- function(snapshot_date,
                                    output_dir = .SNAPSHOT_DIR) {
  if (!dir.exists(output_dir)) return(list())
  files <- list.files(output_dir, pattern = "^pit_.*_raw\\.parquet$",
                      full.names = TRUE)
  if (length(files) == 0L) return(list())
  dates <- as.Date(sub("^pit_(.*)_raw\\.parquet$", "\\1", basename(files)))
  keep  <- !is.na(dates) & dates < as.Date(snapshot_date)
  files <- files[keep]
  if (length(files) == 0L) return(list())
  lapply(files, function(f) {
    tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
  })
}

#' Expanding-window robust z-scoring of a point-in-time snapshot
#'
#' @param raw_dt data.table. Current snapshot in raw form. Must carry
#'   indicator columns; may include metadata (date, ticker, industry,
#'   sector) which is passed through unchanged.
#' @param history_dt_list List of data.tables. Prior raw snapshots
#'   (strictly before raw_dt's date). If NULL, loaded from output_dir
#'   using raw_dt$date[1] as the as-of date.
#' @param output_dir Character. Directory of pit_*_raw.parquet files.
#'   Ignored if history_dt_list is non-NULL.
#' @param clip numeric(2) or NULL. Post-standardization clip bounds;
#'   default c(-5, 5). See .robust_zscore in R/indicator_compute.R and
#'   tools/diag_zscore_tails.R for the tail-diagnostic evidence behind
#'   the choice of 5 over 3. NULL drops the clip entirely.
#' @return data.table of the same shape as raw_dt with indicator columns
#'   replaced by expanding-window z-scores. Metadata preserved.
zscore_expanding_window <- function(raw_dt,
                                    history_dt_list = NULL,
                                    output_dir = .SNAPSHOT_DIR,
                                    clip = c(-5, 5)) {

  if (is.null(raw_dt) || nrow(raw_dt) == 0) return(raw_dt)

  if (is.null(history_dt_list)) {
    snap_date <- if ("date" %in% names(raw_dt)) raw_dt[["date"]][1] else NA
    if (is.na(snap_date)) {
      history_dt_list <- list()
    } else {
      history_dt_list <- .load_history_snapshots(snap_date, output_dir)
    }
  }

  dt <- copy(raw_dt)
  meta_cols <- intersect(c("date", "ticker", "industry", "sector"),
                         names(dt))
  sectors_vec <- if ("sector" %in% names(dt)) {
    dt[["sector"]]
  } else {
    rep(NA_character_, nrow(dt))
  }
  ind_cols <- setdiff(names(dt), meta_cols)

  for (col_name in ind_cols) {
    vals <- dt[[col_name]]
    masked <- .sector_na_mask(col_name, sectors_vec)

    pool <- .build_pooled_calib(col_name, raw_dt, history_dt_list)

    if (any(masked)) {
      vals_in <- vals
      vals_in[masked] <- NA_real_
      z <- .robust_zscore(vals_in, calib_sample = pool, clip = clip)
      z[masked] <- NA_real_
    } else {
      z <- .robust_zscore(vals, calib_sample = pool, clip = clip)
    }
    dt[, (col_name) := z]
  }

  .assert_output(dt, "zscore_expanding_window", list(
    "is data.table"   = is.data.table,
    "row count preserved" = function(x) nrow(x) == nrow(raw_dt)
  ))

  dt
}


# =============================================================================
# 9. load_snapshot()  --  Reader convenience function
# =============================================================================
#' Load a previously assembled snapshot
#'
#' @param snapshot_date Date or character.
#' @param type Character. "raw" or "zscore".
#' @param output_dir Character.
#' @return data.table, or NULL if file does not exist.
load_snapshot <- function(snapshot_date, type = "raw",
                          output_dir = "cache/snapshots") {

  stopifnot(type %in% c("raw", "zscore"))
  path <- file.path(output_dir,
                    sprintf("pit_%s_%s.parquet", as.Date(snapshot_date), type))

  if (!file.exists(path)) return(NULL)

  dt <- tryCatch(
    as.data.table(arrow::read_parquet(path)),
    error = function(e) NULL
  )

  if (!is.null(dt)) {
    .assert_output(dt, "load_snapshot", list(
      "is data.table"  = is.data.table,
      "has ticker col" = function(x) "ticker" %in% names(x)
    ))
  }

  dt
}
