# ============================================================================
# update_runner.R  --  Incremental Daily Update (Module 8)
# ============================================================================
# Keeps the database current via cheap append operations instead of
# reprocessing history (docs/DESIGN_DAILY_UPDATE.md sections 4-6).
# PIT-safe by construction: appends never recompute a value for a date
# on or before the information's public date.
#
# Cadence tiers:
#   Tier D (daily):   split guard FIRST -> price append (or full price
#                     re-fetch + daily-layer rebuild when a new true
#                     split re-based the Yahoo cache) -> daily-layer
#                     append -> per-ticker TTM augment.
#   Tier W (weekly):  EDGAR submissions freshness probe (captures SIC);
#                     changed CIKs refetch companyfacts and rebuild the
#                     fund + per-share layers (daily layer untouched --
#                     old rows used the data public at their own dates).
#   Tier M (monthly): roster diff (new constituent = full backfill,
#                     removal = stop updating); dated sector snapshot
#                     (SCD append via merge_sector_scd); extend the
#                     quarterly pit_* snapshot grid.
#
# State: cache/lookups/update_manifest.parquet, one row per ticker plus
# a __WATERMARK__ row holding per-tier last-run dates. Every tier reads
# it, does minimal work, writes it back -- idempotent and resumable.
#
# Public API:
#   bootstrap_update_manifest(...)
#   load_update_manifest(path) / save_update_manifest(manifest, path)
#   run_tier_daily(as_of, ...)
#   run_tier_weekly(as_of, ...)
#   run_tier_monthly(as_of, ...)
#   run_incremental_update(as_of, ...)
#
# Dependencies: data.table, arrow, quantmod, xts
# Requires: fundamental_fetcher.R, sector_classifier.R, ttm_eps.R,
#           indicator_compute.R, pit_assembler.R, timeseries_builder.R,
#           pipeline_runner.R (already sourced)
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})


# =============================================================================
# SECTION 1: CONSTANTS AND HELPERS
# =============================================================================

.MANIFEST_PATH  <- "cache/lookups/update_manifest.parquet"
.UPDATE_LOG_CSV <- "cache/update_run_log.csv"
.WATERMARK_TK   <- "__WATERMARK__"

# Manifest statuses:
#   ok             active constituent, all tiers apply
#   removed        left the index; stop updating (PIT membership keeps
#                  its historical presence via get_universe_at_date)
#   price-purged   fundamentals update, price/daily appends are skipped
#                  (no provider carries the chain -- KNOWN_LIMITATIONS L4)
#   needs-backfill new constituent whose layers are not built yet
.MANIFEST_STATUSES <- c("ok", "removed", "price-purged", "needs-backfill")

.assert_output_ur <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}

# Manifest invariants, shared by save_update_manifest and every tier's
# return (each tier mutates the manifest and is public API).
.assert_manifest <- function(manifest, fn) {
  .assert_output_ur(manifest, fn, list(
    "is data.table"  = is.data.table,
    "has ticker"     = function(x) "ticker" %in% names(x),
    "has watermark"  = function(x) .WATERMARK_TK %in% x$ticker,
    "no dup tickers" = function(x) !anyDuplicated(x$ticker),
    "valid statuses" = function(x)
      all(x[ticker != .WATERMARK_TK, status] %in% .MANIFEST_STATUSES)
  ))
}

# Deterministic fingerprint of a splits table; a changed hash is the
# Tier D signal that Yahoo re-based the whole price history.
.splits_hash <- function(s) {
  if (is.null(s) || nrow(s) == 0) return("0")
  s <- as.data.table(s)[order(as.Date(ex_date))]
  paste(sprintf("%s:%.8g", as.Date(s$ex_date), as.numeric(s$ratio)),
        collapse = ";")
}

# Max of a parquet column read pruned, or NA. Used by the bootstrap.
.parquet_col_max <- function(path, col) {
  if (!file.exists(path)) return(NA)
  v <- tryCatch(
    arrow::read_parquet(path, col_select = tidyselect::any_of(col))[[col]],
    error = function(e) NULL)
  if (is.null(v) || length(v) == 0) return(NA)
  suppressWarnings(max(v, na.rm = TRUE))
}


# =============================================================================
# SECTION 2: MANIFEST I/O + BOOTSTRAP
# =============================================================================

#' Load the update manifest
#' @param path Character. Manifest parquet path.
#' @return data.table, or NULL when no manifest exists yet.
load_update_manifest <- function(path = .MANIFEST_PATH) {
  if (!file.exists(path)) return(NULL)
  dt <- tryCatch(as.data.table(arrow::read_parquet(path)),
                 error = function(e) NULL)
  if (is.null(dt) || nrow(dt) == 0) return(NULL)
  for (dc in c("last_filed_date", "last_companyfacts_fetch",
               "last_price_date", "last_daily_date")) {
    dt[, (dc) := as.Date(get(dc))]
  }
  dt
}

#' Save the update manifest
#' @param manifest data.table.
#' @param path Character. Manifest parquet path.
save_update_manifest <- function(manifest, path = .MANIFEST_PATH) {
  .assert_manifest(manifest, "save_update_manifest")
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(manifest, path)
  invisible(manifest)
}

#' Bootstrap the manifest from current cache state
#'
#' First run only: derives per-ticker cursors from the price caches,
#' _daily layers, and fundamentals caches, then proceeds normally.
#' Universe = every roster ticker with a timeseries fund layer, plus
#' ACTIVE roster tickers without one (status needs-backfill).
#'
#' @return data.table (the manifest, also written to path).
bootstrap_update_manifest <- function(
    path        = .MANIFEST_PATH,
    master_path = "cache/lookups/constituent_master.parquet",
    ts_dir      = "cache/timeseries",
    price_dir   = "cache/prices",
    fund_dir    = "cache/fundamentals",
    split_dir   = "cache/splits") {

  message("bootstrap_update_manifest: deriving state from cache...")
  t0 <- Sys.time()
  master <- as.data.table(arrow::read_parquet(master_path))

  fund_tks <- unique(gsub("_fund\\.parquet$", "",
                          list.files(ts_dir, pattern = "_fund\\.parquet$")))
  active_tks <- unique(master[status == "ACTIVE", ticker])
  tks <- sort(union(fund_tks, active_tks))

  # newest roster row per ticker (occurrence splits: latest entity);
  # fund_dir listed ONCE -- per-ticker list.files on the Drive mount
  # would turn the bootstrap into a network-metadata stall
  mm <- master[order(ticker, date_added)][, .SD[.N], by = ticker]
  cik_by_tk <- setNames(mm$cik, mm$ticker)
  fund_all  <- list.files(fund_dir, pattern = "\\.parquet$",
                          full.names = TRUE)
  fund_base <- basename(fund_all)

  rows <- vector("list", length(tks))
  for (i in seq_along(tks)) {
    tk <- tks[i]
    cik <- cik_by_tk[[tk]] %||% NA_character_

    # fundamentals cache: newest filed + accession (CIK-keyed file with
    # the shared-CIK alias fallback handled by get_fundamentals; here a
    # cheap prefix match mirrors it)
    fund_file <- NULL
    if (!is.na(cik)) {
      hit <- fund_all[startsWith(fund_base, paste0(cik, "_"))]
      if (length(hit)) fund_file <- hit[1]
    }
    if (is.null(fund_file)) {
      hit <- fund_all[endsWith(fund_base, paste0("_", tk, ".parquet"))]
      if (length(hit)) fund_file <- hit[1]
    }
    last_filed <- NA; last_accn <- NA_character_; cf_fetch <- NA
    if (!is.null(fund_file)) {
      fd <- tryCatch(as.data.table(arrow::read_parquet(
        fund_file,
        col_select = tidyselect::any_of(c("filed", "accession", "form")))),
        error = function(e) NULL)
      if (!is.null(fd) && nrow(fd)) {
        # restrict to the same form family the Tier W probe compares
        # against -- an 8-K comparative filed after the latest 10-Q
        # would otherwise force a spurious refetch on the first run
        if ("form" %in% names(fd)) fd <- fd[form %in% .PERSHARE_FORMS]
      }
      if (!is.null(fd) && nrow(fd)) {
        last_filed <- suppressWarnings(max(as.Date(fd$filed), na.rm = TRUE))
        if ("accession" %in% names(fd)) {
          la <- fd[as.Date(filed) == last_filed & !is.na(accession), accession]
          if (length(la)) last_accn <- la[1]
        }
      }
      cf_fetch <- as.Date(file.mtime(fund_file))
    }

    price_file <- .price_cache_path(tk, cache_dir = price_dir)
    last_price <- if (file.exists(price_file)) {
      as.Date(.parquet_col_max(price_file, "date"))
    } else as.Date(NA)
    last_daily <- as.Date(.parquet_col_max(
      file.path(ts_dir, sprintf("%s_daily.parquet", tk)), "date"))

    splits <- tryCatch(load_ticker_splits(tk, split_dir, fetch = FALSE),
                       error = function(e) NULL)

    status <- if (!tk %in% active_tks) "removed"
    else if (!tk %in% fund_tks) "needs-backfill"
    else if (is.na(last_price)) "price-purged"
    else "ok"

    rows[[i]] <- data.table(
      cik = cik, ticker = tk,
      last_filing_accession = last_accn,
      last_filed_date = as.Date(last_filed),
      last_companyfacts_fetch = as.Date(cf_fetch),
      last_price_date = last_price,
      last_daily_date = last_daily,
      splits_hash = .splits_hash(splits),
      sic = NA_character_,
      status = status)

    if (i %% 100 == 0) message(sprintf("  [%d/%d] %s", i, length(tks), tk))
  }

  manifest <- rbindlist(rows)
  manifest <- rbind(manifest, data.table(
    cik = NA_character_, ticker = .WATERMARK_TK,
    last_filing_accession = NA_character_,
    last_filed_date = as.Date(NA),          # last Tier W run
    last_companyfacts_fetch = as.Date(NA),  # last Tier M run
    last_price_date = as.Date(NA),
    last_daily_date = as.Date(NA),          # last Tier D run
    splits_hash = NA_character_, sic = NA_character_,
    status = NA_character_))

  save_update_manifest(manifest, path)
  message(sprintf(
    "bootstrap_update_manifest: %d tickers (%s) in %.1f min",
    nrow(manifest) - 1L,
    paste(sprintf("%s=%d", names(table(manifest$status)),
                  as.integer(table(manifest$status))), collapse = ", "),
    as.numeric(difftime(Sys.time(), t0, units = "mins"))))
  manifest
}


# =============================================================================
# SECTION 3: PRICE APPEND
# =============================================================================

#' Append new rows to a Yahoo price cache (no-split fast path)
#'
#' ONLY valid when the Tier D split guard verified no new split since
#' the cache was written: Yahoo's Close is split-adjusted to fetch-time
#' basis, so a new split re-bases the entire history and appending would
#' create a mixed-basis series. Tiingo-sourced caches (purged
#' delistings) never grow and are skipped.
#'
#' @param ticker Character. Roster ticker.
#' @param through_date Date. Append up to this date.
#' @param price_dir Character. Price cache directory.
#' @return List: $appended (rows appended; 0L on no-op; NA_integer_ on
#'   fetch failure, cache untouched) and $last_date (max cached date
#'   after the call, NA on failure).
.append_ticker_prices <- function(ticker, through_date = Sys.Date(),
                                  price_dir = "cache/prices") {
  fail <- list(appended = NA_integer_, last_date = as.Date(NA))
  cache_file <- .price_cache_path(ticker, cache_dir = price_dir)
  if (!file.exists(cache_file)) return(fail)

  old <- tryCatch(as.data.frame(arrow::read_parquet(cache_file)),
                  error = function(e) NULL)
  if (is.null(old) || nrow(old) == 0) return(fail)
  last_cached <- max(as.Date(old$date), na.rm = TRUE)
  noop <- list(appended = 0L, last_date = last_cached)

  if (grepl("_tiingo_", basename(cache_file))) return(noop)
  if (ticker %in% .YAHOO_REUSED_BLOCKLIST) return(noop)
  if (last_cached >= as.Date(through_date)) return(noop)

  yahoo_symbol <- gsub("\\.", "-", .provider_symbol(ticker))
  px <- NULL
  for (attempt in 1:2) {
    px <- tryCatch({
      p <- quantmod::getSymbols(yahoo_symbol, src = "yahoo",
                                from = last_cached + 1L,
                                to = as.Date(through_date) + 1L,
                                auto.assign = FALSE)
      if (is.null(p) || nrow(p) == 0) NULL else p
    }, error = function(e) NULL)
    if (!is.null(px)) break
    if (attempt < 2) Sys.sleep(2)
  }
  if (is.null(px)) return(fail)

  colnames(px) <- sub("^[^.]+\\.",
                      paste0(gsub("\\.", "-", ticker), "."), colnames(px))
  new_df <- data.frame(date = as.character(zoo::index(px)),
                       zoo::coredata(px), check.names = FALSE)
  new_df <- new_df[as.Date(new_df$date) > last_cached, , drop = FALSE]
  if (nrow(new_df) == 0) return(noop)
  if (!identical(names(old), names(new_df))) return(fail)

  arrow::write_parquet(rbind(old, new_df), cache_file)
  list(appended = nrow(new_df), last_date = max(as.Date(new_df$date)))
}


# =============================================================================
# SECTION 4: TIER D -- daily
# =============================================================================

#' Tier D: split guard, price append, daily-layer append, TTM augment
#'
#' @param as_of Date. Update through this date.
#' @param manifest data.table. The update manifest (modified by reference
#'   semantics are avoided; the updated manifest is returned).
#' @param refresh_splits Logical. Run the split guard (network; TRUE for
#'   production runs, FALSE only in offline tests).
#' @return List with $manifest (updated) and $stats.
run_tier_daily <- function(as_of = Sys.Date(),
                           manifest,
                           ts_dir     = "cache/timeseries",
                           price_dir  = "cache/prices",
                           fund_dir   = "cache/fundamentals",
                           split_dir  = "cache/splits",
                           refresh_splits = TRUE) {

  as_of <- as.Date(as_of)
  t0 <- Sys.time()
  work <- manifest[status == "ok" & ticker != .WATERMARK_TK]
  message(sprintf("run_tier_daily: %s, %d tickers", as_of, nrow(work)))

  n_split_rebuilds <- 0L; n_appended <- 0L; n_price_fail <- 0L
  changed_tks <- character(0)

  for (i in seq_len(nrow(work))) {
    tk <- work$ticker[i]
    res <- tryCatch({
      # -- 1. split guard FIRST --
      split_rebased <- FALSE
      guard_failed  <- FALSE
      if (refresh_splits) {
        s_new <- load_ticker_splits(tk, split_dir, fetch = TRUE,
                                    refresh = TRUE)
        if (isTRUE(attr(s_new, "refresh_failed"))) {
          # could not CHECK for a new split: "no change" is unproven,
          # so the append path (which assumes a stable Yahoo basis) is
          # not safe this run -- the daily layer still advances on the
          # existing same-basis cache
          guard_failed <- TRUE
        } else {
          h_new <- .splits_hash(s_new)
          h_old <- manifest[ticker == tk, splits_hash]
          if (!identical(h_new, h_old)) {
            split_rebased <- TRUE
            manifest[ticker == tk, splits_hash := h_new]
          }
        }
      }

      new_price_last <- as.Date(NA)
      if (split_rebased) {
        # new true split: the whole Yahoo history re-based -> full price
        # re-fetch + full daily-layer rebuild for this ticker. No append
        # path this run.
        message(sprintf("  %s: new split event -- full re-fetch + rebuild", tk))
        for (f in list.files(price_dir,
                             pattern = sprintf("^%s_(yahoo|tiingo)_",
                                               gsub("\\.", "\\\\.", tk)),
                             full.names = TRUE)) file.remove(f)
        px <- fetch_ticker_prices(tk, cache_dir = price_dir)
        if (!is.null(px)) new_price_last <- max(as.Date(zoo::index(px)))
        daily_f <- file.path(ts_dir, sprintf("%s_daily.parquet", tk))
        if (file.exists(daily_f)) file.remove(daily_f)
        n_split_rebuilds <- n_split_rebuilds + 1L
      } else if (!guard_failed) {
        # -- 2. price append (no-split path, guard verified) --
        ap <- .append_ticker_prices(tk, through_date = as_of,
                                    price_dir = price_dir)
        if (is.na(ap$appended)) n_price_fail <- n_price_fail + 1L
        else new_price_last <- ap$last_date
      }

      # -- 3. daily-layer append (10-Q per-share stubs, D1-fixed) --
      before <- manifest[ticker == tk, last_daily_date]
      d <- update_ticker_daily(tk, through_date = as_of,
                               price_dir = price_dir, ts_dir = ts_dir)
      if (!is.null(d) && nrow(d) > 0) {
        new_last <- max(as.Date(d$date))
        manifest[ticker == tk, last_daily_date := new_last]
        if (!is.na(new_price_last)) {
          manifest[ticker == tk, last_price_date := new_price_last]
        }
        if (is.na(before) || new_last > before || split_rebased) {
          changed_tks <- c(changed_tks, tk)
          n_appended <- n_appended + 1L
        }
      }
      TRUE
    }, error = function(e) {
      warning(sprintf("run_tier_daily: %s failed -- %s", tk,
                      conditionMessage(e)), call. = FALSE)
      FALSE
    })

    if (i %% 100 == 0) message(sprintf("  [%d/%d] %s", i, nrow(work), tk))
  }

  # -- 4. per-ticker TTM augment (only changed layers) --
  if (length(changed_tks) > 0 && exists("augment_daily_ttm")) {
    message(sprintf("  TTM augment for %d changed tickers",
                    length(changed_tks)))
    tryCatch(augment_daily_ttm(fund_dir = fund_dir, ts_dir = ts_dir,
                               tickers = changed_tks, verbose = FALSE),
             error = function(e)
               warning(sprintf("augment_daily_ttm failed: %s",
                               conditionMessage(e)), call. = FALSE))
  }

  manifest[ticker == .WATERMARK_TK, last_daily_date := as_of]
  .assert_manifest(manifest, "run_tier_daily")
  stats <- list(n = nrow(work), appended = n_appended,
                split_rebuilds = n_split_rebuilds,
                price_failures = n_price_fail,
                elapsed_min = round(as.numeric(
                  difftime(Sys.time(), t0, units = "mins")), 1))
  message(sprintf(
    "run_tier_daily: done in %.1f min -- %d layers advanced, %d split rebuilds, %d price failures",
    stats$elapsed_min, n_appended, n_split_rebuilds, n_price_fail))
  list(manifest = manifest, stats = stats)
}


# =============================================================================
# SECTION 5: TIER W -- weekly / event-driven
# =============================================================================

#' Probe EDGAR submissions for filing freshness (one rate-limited call)
#'
#' @param cik Character. 10-digit CIK.
#' @return List(accession, form, filed, sic) for the newest 10-K/10-Q
#'   family filing, or NULL on fetch failure.
.probe_edgar_freshness <- function(cik) {
  url <- sprintf("%s/submissions/CIK%s.json", .EDGAR_BASE, cik)
  txt <- .edgar_fetch(url)
  if (is.null(txt)) return(NULL)
  subs <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (is.null(subs)) return(NULL)
  b <- subs$filings$recent
  if (is.null(b) || is.null(b$accessionNumber)) return(NULL)
  dt <- data.table(accession = b$accessionNumber, form = b$form,
                   filed = as.Date(b$filingDate))
  # same statement-bearing form family the per-share layer keys on --
  # one shared constant so the two lists cannot drift
  dt <- dt[form %in% .PERSHARE_FORMS]
  if (nrow(dt) == 0) return(NULL)
  newest <- dt[order(-filed)][1]
  list(accession = newest$accession, form = newest$form,
       filed = newest$filed, sic = subs$sic %||% NA_character_)
}

#' Tier W: freshness probe + refetch/rebuild for changed CIKs
#'
#' @param as_of Date.
#' @param manifest data.table.
#' @return List with $manifest and $stats.
run_tier_weekly <- function(as_of = Sys.Date(),
                            manifest,
                            ts_dir      = "cache/timeseries",
                            fund_dir    = "cache/fundamentals",
                            sector_path = "cache/lookups/sector_industry.parquet") {

  as_of <- as.Date(as_of)
  t0 <- Sys.time()
  work <- manifest[status %in% c("ok", "price-purged") &
                     ticker != .WATERMARK_TK & !is.na(cik)]
  # shared-CIK listings: one probe + one refetch serves every listing
  ciks <- unique(work$cik)
  message(sprintf("run_tier_weekly: %s, %d CIKs", as_of, length(ciks)))

  sectors <- .sector_asof(
    as.data.table(arrow::read_parquet(sector_path)), as_of)

  # one CIK end-to-end; returns "fresh" / "unchanged" / "probe-fail"
  # (a plain function, because `next` inside tryCatch expr is caught as
  # an error instead of advancing the loop)
  refresh_one_cik <- function(ck) {
    probe <- .probe_edgar_freshness(ck)
    if (is.null(probe)) return("probe-fail")
    tks <- work[cik == ck, ticker]
    manifest[ticker %in% tks & is.na(sic), sic := probe$sic]

    known <- manifest[ticker == tks[1], last_filing_accession]
    if (identical(probe$accession, known)) return("unchanged")

    # new filing: refetch companyfacts (vintage rows reproduce from
    # the blob; Wave M instances resume from the accession log), then
    # rebuild the fund + per-share layers. The daily layer stays: its
    # existing rows used the data public on their own dates, and the
    # next Tier D append picks up the new stubs.
    message(sprintf("  CIK %s (%s): new filing %s %s", ck,
                    paste(tks, collapse = "/"), probe$form, probe$filed))
    # refetch under the CANONICAL cache ticker: one file per CIK is a
    # design invariant (shared-CIK aliasing keys off it); writing
    # {cik}_{tks[1]} would mint a second, competing cache for filers
    # whose file uses the sibling/old ticker suffix (WBD -> _DISCA)
    exist <- list.files(fund_dir, pattern = paste0("^", ck, "_"))
    canon_tk <- if (length(exist)) {
      sub("^\\d+_(.+)\\.parquet$", "\\1", exist[1])
    } else tks[1]
    invisible(fetch_and_cache_ticker(canon_tk, ck, cache_dir = fund_dir,
                                     force_refresh = TRUE))
    for (tk in tks) {
      sec <- sectors[ticker == tk, sector]
      sec <- if (length(sec)) sec[1] else "Unknown"
      invisible(build_ticker_fundamentals(tk, ck, sec,
                                          fund_dir = fund_dir,
                                          ts_dir = ts_dir, force = TRUE))
      invisible(build_ticker_pershare(tk, ck, fund_dir = fund_dir,
                                      ts_dir = ts_dir, force = TRUE))
    }
    manifest[ticker %in% tks, `:=`(
      last_filing_accession = probe$accession,
      last_filed_date = probe$filed,
      last_companyfacts_fetch = as_of)]
    "fresh"
  }

  n_fresh <- 0L; n_probe_fail <- 0L
  for (ck in ciks) {
    res <- tryCatch(refresh_one_cik(ck), error = function(e) {
      warning(sprintf("run_tier_weekly: CIK %s failed -- %s", ck,
                      conditionMessage(e)), call. = FALSE)
      "error"
    })
    if (res == "fresh")      n_fresh <- n_fresh + 1L
    if (res == "probe-fail") n_probe_fail <- n_probe_fail + 1L
  }

  manifest[ticker == .WATERMARK_TK, last_filed_date := as_of]
  .assert_manifest(manifest, "run_tier_weekly")
  stats <- list(n_ciks = length(ciks), refreshed = n_fresh,
                probe_failures = n_probe_fail,
                elapsed_min = round(as.numeric(
                  difftime(Sys.time(), t0, units = "mins")), 1))
  message(sprintf(
    "run_tier_weekly: done in %.1f min -- %d CIKs refreshed, %d probe failures",
    stats$elapsed_min, n_fresh, n_probe_fail))
  list(manifest = manifest, stats = stats)
}


# =============================================================================
# SECTION 6: TIER M -- monthly / structural
# =============================================================================

#' Refresh the dated sector dimension (SCD append)
#'
#' Re-scrapes finviz for ACTIVE tickers into a throwaway cache, applies
#' the static overrides, and appends a dated row for every changed value
#' via merge_sector_scd. Non-scraped (delisted) tickers keep history.
#'
#' @return Number of appended/changed rows.
update_sector_dimension <- function(as_of = Sys.Date(),
                                    master_path = "cache/lookups/constituent_master.parquet",
                                    sector_path = "cache/lookups/sector_industry.parquet") {
  master <- as.data.table(arrow::read_parquet(master_path))
  active <- unique(master[status == "ACTIVE", ticker])

  # persistent sidecar checkpoint (never the canonical SCD file): a
  # mid-scrape failure resumes instead of redoing ~500 rate-limited
  # requests; dropped only after a successful merge+write
  scrape_path <- paste0(sector_path, ".scrape_",
                        format(as.Date(as_of), "%Y%m"))
  scraped <- fetch_finviz_sectors(active, cache_path = scrape_path)
  if (is.null(scraped) || nrow(scraped) == 0) {
    warning("update_sector_dimension: scrape returned nothing; skipping",
            call. = FALSE)
    return(0L)
  }

  # static overrides always win (reused/dead symbols); exact-string
  # validation lives inside the helper (hard stop, never a silent
  # mask-killing near-miss). Scrape refresh corrects only tickers
  # actually scraped -- delisted names keep their SCD history via
  # merge_sector_scd.
  scraped <- apply_sector_overrides(scraped, add_missing = FALSE)

  prev <- as.data.table(arrow::read_parquet(sector_path))
  merged <- merge_sector_scd(prev, scraped, as_of = as_of)
  n_changed <- nrow(merged) - nrow(prev)
  arrow::write_parquet(merged, sector_path)
  unlink(scrape_path)
  message(sprintf("update_sector_dimension: %d dated rows appended",
                  max(n_changed, 0L)))
  max(n_changed, 0L)
}

#' Tier M: roster diff, dated sector snapshot, quarterly grid extension
#'
#' @param as_of Date.
#' @param manifest data.table.
#' @param scrape_sectors Logical. Run the finviz sector refresh
#'   (network-heavy; FALSE only in offline tests).
#' @return List with $manifest and $stats.
run_tier_monthly <- function(as_of = Sys.Date(),
                             manifest,
                             roster_csv   = "data/sp500_constituents_.csv",
                             master_path  = "cache/lookups/constituent_master.parquet",
                             sector_path  = "cache/lookups/sector_industry.parquet",
                             ts_dir       = "cache/timeseries",
                             fund_dir     = "cache/fundamentals",
                             price_dir    = "cache/prices",
                             snapshot_dir = "cache/snapshots",
                             scrape_sectors = TRUE) {

  as_of <- as.Date(as_of)
  t0 <- Sys.time()
  message(sprintf("run_tier_monthly: %s", as_of))

  # -- 1. roster diff --
  n_new <- 0L; n_removed <- 0L
  if (file.exists(roster_csv)) {
    roster <- fread(roster_csv)
    roster[, ticker := trimws(toupper(ticker))]
    known <- manifest[ticker != .WATERMARK_TK, ticker]

    new_tks <- setdiff(unique(roster$ticker), known)
    for (tk in new_tks) {
      ck <- roster[ticker == tk][.N]$cik
      ck <- if (length(ck) && !is.na(ck) && nzchar(as.character(ck))) {
        .pad_cik(ck)
      } else NA_character_
      manifest <- rbind(manifest, data.table(
        cik = ck, ticker = tk,
        last_filing_accession = NA_character_,
        last_filed_date = as.Date(NA),
        last_companyfacts_fetch = as.Date(NA),
        last_price_date = as.Date(NA), last_daily_date = as.Date(NA),
        splits_hash = "0", sic = NA_character_,
        status = "needs-backfill"))
      n_new <- n_new + 1L
    }
    if (n_new) message(sprintf("  new constituents: %s",
                               paste(new_tks, collapse = ", ")))

    # removals: stop updating (historical presence stays PIT via
    # get_universe_at_date)
    removed_now <- roster[!is.na(date_removed) &
                            as.Date(date_removed) <= as_of,
                          unique(ticker)]
    still_active <- roster[is.na(date_removed) |
                             as.Date(date_removed) > as_of, unique(ticker)]
    removed_now <- setdiff(removed_now, still_active)
    flip <- manifest[ticker %in% removed_now & status == "ok", ticker]
    if (length(flip)) {
      manifest[ticker %in% flip, status := "removed"]
      n_removed <- length(flip)
      message(sprintf("  removals flipped to 'removed': %s",
                      paste(flip, collapse = ", ")))
    }
  }

  # -- 2. dated sector snapshot (B) -- BEFORE the backfill: a brand-new
  # roster ticker has no row in the old table, and building its layers
  # with sector "Unknown" would skip the masked-sector NA stubs (D3)
  n_sector_rows <- 0L
  if (scrape_sectors) {
    n_sector_rows <- tryCatch(
      update_sector_dimension(as_of, master_path, sector_path),
      error = function(e) {
        warning(sprintf("update_sector_dimension failed: %s",
                        conditionMessage(e)), call. = FALSE)
        0L
      })
  }

  # -- 3. backfill new constituents (full: fundamentals -> layers) --
  todo <- manifest[status == "needs-backfill" & !is.na(cik) &
                     ticker != .WATERMARK_TK]
  sectors_now <- .sector_asof(
    as.data.table(arrow::read_parquet(sector_path)), as_of)
  n_backfilled <- 0L
  for (i in seq_len(nrow(todo))) {
    tk <- todo$ticker[i]; ck <- todo$cik[i]
    ok <- tryCatch({
      invisible(fetch_and_cache_ticker(tk, ck, cache_dir = fund_dir))
      sec <- sectors_now[ticker == tk, sector]
      sec <- if (length(sec)) sec[1] else "Unknown"
      # splits cache BEFORE any layer build: update_ticker_daily loads
      # splits with fetch = FALSE, so building first would reconstruct
      # every pre-split as-traded price with factor 1 -- and the hash
      # written below would blind the Tier D split guard to it forever
      splits <- tryCatch(load_ticker_splits(tk, fetch = TRUE),
                         error = function(e) NULL)
      invisible(build_ticker_fundamentals(tk, ck, sec, fund_dir = fund_dir,
                                          ts_dir = ts_dir))
      d <- update_ticker_daily(tk, through_date = as_of,
                               price_dir = price_dir, ts_dir = ts_dir)
      if (!is.null(d) && nrow(d)) {
        manifest[ticker == tk, `:=`(
          status = "ok",
          last_daily_date = max(as.Date(d$date)),
          splits_hash = .splits_hash(splits))]
        n_backfilled <- n_backfilled + 1L
        TRUE
      } else FALSE
    }, error = function(e) {
      warning(sprintf("run_tier_monthly: backfill %s failed -- %s", tk,
                      conditionMessage(e)), call. = FALSE)
      FALSE
    })
  }

  # -- 4. extend the quarterly pit_* grid --
  # quarter-end convention: last calendar day of Mar/Jun/Sep/Dec
  existing <- list_snapshots(snapshot_dir)
  yrs <- 2010:as.integer(format(as_of, "%Y"))
  q_ends <- as.Date(paste0(rep(yrs, each = 4),
                           c("-03-31", "-06-30", "-09-30", "-12-31")))
  missing_q <- sort(setdiff(as.character(q_ends[q_ends <= as_of]), existing))
  n_snaps <- 0L
  for (d in missing_q) {
    ok <- tryCatch({
      # prefetch_prices = TRUE: Tier M runs BEFORE Tier D's price
      # append (structural-first ordering), so filing-date prices near
      # the new quarter end may not be cached yet -- and missing_q only
      # builds snapshots that do not exist, so a defective one would
      # never be reassembled
      r <- assemble_snapshot(d, output_dir = snapshot_dir,
                             prefetch_prices = TRUE)
      !is.null(r)
    }, error = function(e) {
      warning(sprintf("run_tier_monthly: snapshot %s failed -- %s", d,
                      conditionMessage(e)), call. = FALSE)
      FALSE
    })
    if (ok) {
      v <- tryCatch(suppressMessages(validate_snapshot(d)),
                    error = function(e) NULL)
      if (is.null(v) || !isTRUE(v$pass)) {
        warning(sprintf("run_tier_monthly: snapshot %s FAILED validation", d),
                call. = FALSE)
      }
      n_snaps <- n_snaps + 1L
    }
  }

  manifest[ticker == .WATERMARK_TK, last_companyfacts_fetch := as_of]
  .assert_manifest(manifest, "run_tier_monthly")
  stats <- list(new_constituents = n_new, backfilled = n_backfilled,
                removed = n_removed, sector_rows = n_sector_rows,
                new_snapshots = n_snaps,
                elapsed_min = round(as.numeric(
                  difftime(Sys.time(), t0, units = "mins")), 1))
  message(sprintf(
    "run_tier_monthly: done in %.1f min -- +%d constituents (%d backfilled), %d removed, %d sector rows, %d snapshots",
    stats$elapsed_min, n_new, n_backfilled, n_removed, n_sector_rows, n_snaps))
  list(manifest = manifest, stats = stats)
}


# =============================================================================
# SECTION 7: ORCHESTRATOR
# =============================================================================

#' Run the incremental update (Tier D always; W/M when due or forced)
#'
#' Never stops on a single ticker failure; writes the updated manifest
#' and appends a row to the run log.
#'
#' @param as_of Date. Update through this date (default today).
#' @param force_weekly Logical. Run Tier W regardless of cadence.
#' @param force_monthly Logical. Run Tier M regardless of cadence.
#' @param refresh_splits Logical. Tier D split guard (see run_tier_daily).
#' @return Invisible list of per-tier stats.
run_incremental_update <- function(as_of = Sys.Date(),
                                   force_weekly  = FALSE,
                                   force_monthly = FALSE,
                                   manifest_path = .MANIFEST_PATH,
                                   refresh_splits = TRUE,
                                   scrape_sectors = TRUE,
                                   master_path  = "cache/lookups/constituent_master.parquet",
                                   sector_path  = "cache/lookups/sector_industry.parquet",
                                   roster_csv   = "data/sp500_constituents_.csv",
                                   ts_dir       = "cache/timeseries",
                                   price_dir    = "cache/prices",
                                   fund_dir     = "cache/fundamentals",
                                   split_dir    = "cache/splits",
                                   snapshot_dir = "cache/snapshots") {

  as_of <- as.Date(as_of)
  t0 <- Sys.time()
  message(sprintf("run_incremental_update: %s", as_of))

  manifest <- load_update_manifest(manifest_path)
  if (is.null(manifest)) {
    message("  no manifest -- bootstrapping from cache state")
    manifest <- bootstrap_update_manifest(
      path = manifest_path, master_path = master_path, ts_dir = ts_dir,
      price_dir = price_dir, fund_dir = fund_dir, split_dir = split_dir)
  }
  wm <- manifest[ticker == .WATERMARK_TK]

  weekly_due <- force_weekly || is.na(wm$last_filed_date) ||
    as_of - wm$last_filed_date >= 7
  monthly_due <- force_monthly || is.na(wm$last_companyfacts_fetch) ||
    format(as_of, "%Y-%m") != format(wm$last_companyfacts_fetch, "%Y-%m")

  stats <- list()

  # structural changes first (roster/sectors feed the other tiers), then
  # fundamentals freshness, then the daily fast path
  if (monthly_due) {
    r <- run_tier_monthly(as_of, manifest, roster_csv = roster_csv,
                          master_path = master_path,
                          sector_path = sector_path, ts_dir = ts_dir,
                          fund_dir = fund_dir, price_dir = price_dir,
                          snapshot_dir = snapshot_dir,
                          scrape_sectors = scrape_sectors)
    manifest <- r$manifest; stats$monthly <- r$stats
    save_update_manifest(manifest, manifest_path)
  }
  if (weekly_due) {
    r <- run_tier_weekly(as_of, manifest, ts_dir = ts_dir,
                         fund_dir = fund_dir, sector_path = sector_path)
    manifest <- r$manifest; stats$weekly <- r$stats
    save_update_manifest(manifest, manifest_path)
  }
  r <- run_tier_daily(as_of, manifest, ts_dir = ts_dir,
                      price_dir = price_dir, fund_dir = fund_dir,
                      split_dir = split_dir,
                      refresh_splits = refresh_splits)
  manifest <- r$manifest; stats$daily <- r$stats
  save_update_manifest(manifest, manifest_path)

  # multi-dimensional feature layer (carried over from the old
  # run_daily_update body; existence-gated like the pipeline's
  # build_all_features hook -- feature_standardizer is optional)
  if (exists("update_features", mode = "function")) {
    tryCatch(
      update_features(through_date = as_of, sector_path = sector_path),
      error = function(e) {
        warning(sprintf("update_features failed: %s", conditionMessage(e)),
                call. = FALSE)
      }
    )
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  log_row <- data.table(
    run_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    as_of = as.character(as_of),
    tiers = paste(c("D", if (weekly_due) "W", if (monthly_due) "M"),
                  collapse = "+"),
    layers_advanced = stats$daily$appended %||% 0L,
    split_rebuilds = stats$daily$split_rebuilds %||% 0L,
    ciks_refreshed = stats$weekly$refreshed %||% 0L,
    new_snapshots = stats$monthly$new_snapshots %||% 0L,
    elapsed_min = elapsed)
  dir.create(dirname(.UPDATE_LOG_CSV), recursive = TRUE, showWarnings = FALSE)
  fwrite(log_row, .UPDATE_LOG_CSV, append = file.exists(.UPDATE_LOG_CSV))

  message(sprintf("run_incremental_update: complete in %.1f min (tiers %s)",
                  elapsed, log_row$tiers))
  invisible(stats)
}
