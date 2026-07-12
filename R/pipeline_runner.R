# ============================================================================
# pipeline_runner.R  --  Pipeline Orchestration & Validation (Module 6)
# ============================================================================
# Orchestrates the full pipeline from constituent master through snapshot
# assembly. Provides validation and coverage reporting.
#
# Public API:
#   run_full_build(start_date, end_date, ...)
#   run_daily_update(date, ...)
#   validate_snapshot(snapshot_date, output_dir, ...)
#   validate_daily_layer(ts_dir, sector_path, split_dir)
#   summarize_coverage(snapshot_date, output_dir)
#
# Dependencies: data.table, arrow
# Requires: all other modules already sourced
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})


# =============================================================================
# CONSTANTS
# =============================================================================

.DEFAULT_MASTER_PATH  <- "cache/lookups/constituent_master.parquet"
.DEFAULT_SECTOR_PATH  <- "cache/lookups/sector_industry.parquet"
.DEFAULT_FUND_DIR     <- "cache/fundamentals"
.DEFAULT_PRICE_DIR    <- "cache/prices"
.DEFAULT_SNAPSHOT_DIR <- "cache/snapshots"

# Indicators with multi-year lookback windows that are structurally all-NA
# on snapshot dates near the ~2009 start of XBRL data (5y issuance/rd_cap
# windows, 16q volatility panels, 3y capex growth). validate_snapshot
# exempts exactly this set from the >99%-NA check before 2013-06-30; the
# set is empirically fully populated from 2013-03-31 onward.
.EARLY_HISTORY_NA_OK <- c(
  "fcf_stability", "composite_debt_issuance", "share_iss_5y",
  "grcapx3y", "rd_cap", "ms_roa_vol", "ms_rev_vol", "ms_score"
)

# Session-level memo for validate_daily_layer. The daily layer is one
# shared artifact -- validating it once per session is enough even when
# validate_snapshot runs over 66 dates. Keyed by ts_dir; assign NULL (or
# restart R) after rebuilding the timeseries layers to force a re-check.
.DAILY_GATE_CACHE <- new.env(parent = emptyenv())

# Split events smaller than this band are spinoff/dividend artifacts, not
# true splits (same band as the Wave P Tiingo filter in pit_assembler.R).
.DAILY_GATE_SPLIT_BAND <- log(1.25)

# Tolerance for the implied-shares jump across a split event and for
# pe_trailing continuity. Organic share drift (buybacks/issuance) inside
# the +/-14 day event window is a few percent at most; a missed split
# adjustment is off by the full ratio (>= 1.25 by the band above).
.DAILY_GATE_JUMP_TOL <- log(1.2)


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

.check_prerequisite <- function(path, label) {
  if (!file.exists(path)) {
    stop(sprintf("run_full_build: prerequisite missing: %s (%s)", label, path),
         call. = FALSE)
  }
  message(sprintf("  [ok] %s", label))
}


# =============================================================================
# 1. run_full_build()
# =============================================================================
#' Run the full end-to-end pipeline
#'
#' Verifies that all prerequisites exist (Sessions A-D outputs),
#' then builds historical snapshots at quarterly cadence.
#'
#' Does NOT re-run Sessions A-D. Those must be completed before calling this.
#'
#' @param start_date Character. First snapshot date (default "2010-03-31").
#' @param end_date Character or Date. Last snapshot date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @param force_refresh Logical. Rebuild existing snapshots.
#' @return List of snapshot stats (invisibly).
run_full_build <- function(start_date      = "2010-03-31",
                           end_date        = Sys.Date(),
                           master_path     = .DEFAULT_MASTER_PATH,
                           sector_path     = .DEFAULT_SECTOR_PATH,
                           fund_dir        = .DEFAULT_FUND_DIR,
                           price_cache_dir = .DEFAULT_PRICE_DIR,
                           output_dir      = .DEFAULT_SNAPSHOT_DIR,
                           force_refresh   = FALSE) {

  message("run_full_build: starting pipeline...")
  t0 <- Sys.time()

  # -- Check prerequisites --
  message("  checking prerequisites...")
  .check_prerequisite(master_path, "constituent_master.parquet")
  .check_prerequisite(sector_path, "sector_industry.parquet")

  # Check fundamentals cache has files
  fund_files <- list.files(fund_dir, pattern = "\\.parquet$")
  if (length(fund_files) == 0) {
    stop("run_full_build: no fundamentals cached. Run Sessions C+D first.",
         call. = FALSE)
  }
  message(sprintf("  [ok] fundamentals cache: %d files", length(fund_files)))

  # -- Build snapshots --
  results <- build_historical_snapshots(
    start_date      = start_date,
    end_date        = end_date,
    master_path     = master_path,
    sector_path     = sector_path,
    fund_dir        = fund_dir,
    price_cache_dir = price_cache_dir,
    output_dir      = output_dir,
    force_refresh   = force_refresh
  )

  # -- Build multi-dimensional features (if feature_standardizer is loaded) --
  if (exists("build_all_features", mode = "function")) {
    message("\n  building multi-dimensional features...")
    tryCatch(
      build_all_features(from_date = start_date, to_date = end_date,
                         sector_path = sector_path),
      error = function(e) {
        warning(sprintf("Feature build failed: %s", e$message), call. = FALSE)
      }
    )
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("\nrun_full_build: complete in %.1f minutes", elapsed))

  # -- Summary --
  snapshot_files <- list.files(output_dir, pattern = "pit_.*_raw\\.parquet$")
  message(sprintf("  total snapshots on disk: %d", length(snapshot_files)))

  invisible(results)
}


# =============================================================================
# 2. run_daily_update()
# =============================================================================
#' Run an incremental update for a single date
#'
#' Assembles (or reassembles) the snapshot for the given date.
#' Useful for daily or weekly refreshes of the latest snapshot.
#'
#' @param date Date or character. Snapshot date (default today).
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param fund_dir Character. Fundamentals cache directory.
#' @param price_cache_dir Character. Price cache directory.
#' @param output_dir Character. Snapshot output directory.
#' @return Snapshot result (list with $raw, $zscored, $stats), or NULL.
run_daily_update <- function(date            = Sys.Date(),
                             master_path     = .DEFAULT_MASTER_PATH,
                             sector_path     = .DEFAULT_SECTOR_PATH,
                             fund_dir        = .DEFAULT_FUND_DIR,
                             price_cache_dir = .DEFAULT_PRICE_DIR,
                             output_dir      = .DEFAULT_SNAPSHOT_DIR) {

  message(sprintf("run_daily_update: %s", as.Date(date)))
  t0 <- Sys.time()

  result <- assemble_snapshot(
    snapshot_date   = date,
    master_path     = master_path,
    sector_path     = sector_path,
    fund_dir        = fund_dir,
    price_cache_dir = price_cache_dir,
    output_dir      = output_dir,
    prefetch_prices = TRUE
  )

  # Update multi-dimensional features (if feature_standardizer is loaded)
  if (exists("update_features", mode = "function")) {
    tryCatch(
      update_features(through_date = date, sector_path = sector_path),
      error = function(e) {
        warning(sprintf("Feature update failed: %s", e$message), call. = FALSE)
      }
    )
  }

  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)
  message(sprintf("run_daily_update: done in %.1f minutes", elapsed))

  invisible(result)
}


# =============================================================================
# 3. validate_snapshot()
# =============================================================================
#' Validate a snapshot against sanity checks
#'
#' Checks:
#'   1. Ticker count is plausible (~400-505 for S&P 500)
#'   2. Z-scores have median 0 and MAD 1 (construction invariants)
#'   3. No indicator is >99% NA
#'   4. Key metadata columns present
#'   5. No duplicate tickers
#'   6. Piotroski F-Score is 0-9
#'   7. Zero tickers with sector "Unknown"/NA (D3 gate: an Unknown bucket
#'      silently disables the sector NA-mask)
#'   8. For every sector in .SECTOR_NA_INDICATORS, its masked indicators
#'      are NA for all its members (mask-fires gate)
#'   9. Daily-layer integrity via validate_daily_layer() (sector coverage,
#'      on-disk mask, split-consistent market cap -- the D1 gate). Heavy,
#'      so memoized per session; disable with check_daily = FALSE only in
#'      unit tests that fabricate snapshots without a timeseries layer.
#'
#' @param snapshot_date Date or character.
#' @param output_dir Character. Snapshot directory.
#' @param check_daily Logical. Run the daily-layer check (default TRUE).
#' @param ts_dir Character. Timeseries directory for the daily-layer check.
#' @param sector_path Character. Sector lookup for the daily-layer check.
#' @param split_dir Character. Splits cache for the daily-layer check.
#' @return List with $pass (logical), $checks (named logical vector),
#'   $details (list of diagnostic info).
validate_snapshot <- function(snapshot_date,
                              output_dir  = .DEFAULT_SNAPSHOT_DIR,
                              check_daily = TRUE,
                              ts_dir      = "cache/timeseries",
                              sector_path = .DEFAULT_SECTOR_PATH,
                              split_dir   = "cache/splits") {

  snapshot_date <- as.Date(snapshot_date)
  message(sprintf("validate_snapshot: %s", snapshot_date))

  raw_path    <- file.path(output_dir,
                           sprintf("pit_%s_raw.parquet", snapshot_date))
  zscore_path <- file.path(output_dir,
                           sprintf("pit_%s_zscore.parquet", snapshot_date))

  checks  <- c()
  details <- list()

  # -- File existence --
  checks["raw_file_exists"]    <- file.exists(raw_path)
  checks["zscore_file_exists"] <- file.exists(zscore_path)

  if (!checks["raw_file_exists"]) {
    message("  FAIL: raw parquet not found")
    return(list(pass = FALSE, checks = checks, details = details))
  }

  raw <- tryCatch(
    as.data.table(arrow::read_parquet(raw_path)),
    error = function(e) {
      message(sprintf("  FAIL: corrupt raw parquet: %s", e$message))
      NULL
    }
  )
  if (is.null(raw)) {
    checks["raw_readable"] <- FALSE
    return(list(pass = FALSE, checks = checks, details = details))
  }

  zsc <- if (checks["zscore_file_exists"]) {
    tryCatch(
      as.data.table(arrow::read_parquet(zscore_path)),
      error = function(e) NULL
    )
  } else NULL

  n_tickers <- nrow(raw)
  details$n_tickers <- n_tickers
  ind_names <- get_indicator_names()

  # -- Ticker count --
  checks["ticker_count_plausible"] <- n_tickers >= 100 && n_tickers <= 600
  message(sprintf("  tickers: %d %s", n_tickers,
                  if (checks["ticker_count_plausible"]) "[ok]" else "[WARN]"))

  # -- Required columns --
  required <- c("date", "ticker", "industry", "sector", ind_names)
  checks["all_columns_present"] <- all(required %in% names(raw))

  # -- No duplicate tickers --
  checks["no_duplicate_tickers"] <- !anyDuplicated(raw$ticker)

  # -- NA rates per indicator --
  na_rates <- sapply(ind_names, function(col) {
    if (col %in% names(raw)) mean(is.na(raw[[col]])) else 1
  })
  details$na_rates <- na_rates

  # Multi-year-lookback indicators cannot exist in the earliest snapshots:
  # XBRL data starts ~2009, so 3-5y windows and 16q volatility panels are
  # structurally empty before ~2013 (the set shrinks each year and is fully
  # populated from 2013-03-31 on). Exempt exactly that named set on early
  # dates; everywhere else the check stays strict.
  high_na <- na_rates[na_rates > 0.99]
  if (snapshot_date < as.Date("2013-06-30")) {
    high_na <- high_na[!names(high_na) %in% .EARLY_HISTORY_NA_OK]
  }
  checks["no_99pct_na_indicators"] <- length(high_na) == 0
  if (length(high_na) > 0) {
    message(sprintf("  WARNING: %d indicators >99%% NA: %s",
                    length(high_na), paste(names(high_na), collapse = ", ")))
  }

  # Median NA rate
  details$median_na_rate <- median(na_rates)
  message(sprintf("  median NA rate: %.1f%%", details$median_na_rate * 100))

  # -- Piotroski F-Score range --
  if ("f_score" %in% names(raw)) {
    fs <- raw$f_score[!is.na(raw$f_score)]
    checks["f_score_range_valid"] <- length(fs) == 0 ||
      (min(fs) >= 0 && max(fs) <= 9)
    details$f_score_mean <- if (length(fs) > 0) mean(fs) else NA
  }

  # -- Z-score validation --
  if (!is.null(zsc)) {
    # .robust_zscore centers on the median and scales by 1.4826*MAD, so
    # median(z) = 0 and mad(z) = 1 hold EXACTLY by construction (symmetric
    # clipping moves neither). The former mean/sd checks assumed mean-
    # centering and were permanently red on ~25 skewed level indicators;
    # these invariants are strict and distribution-free instead.
    z_meds <- sapply(ind_names, function(col) {
      if (col %in% names(zsc)) median(zsc[[col]], na.rm = TRUE) else NA
    })
    z_mads <- sapply(ind_names, function(col) {
      if (col %in% names(zsc)) mad(zsc[[col]], na.rm = TRUE) else NA
    })

    valid_z <- !is.na(z_meds) & !is.na(z_mads)
    if (sum(valid_z) > 0) {
      checks["zscore_median_zero"] <- all(abs(z_meds[valid_z]) < 0.05)
      checks["zscore_mad_one"]     <- all(z_mads[valid_z] > 0.9 &
                                          z_mads[valid_z] < 1.1)
      details$z_median_range <- range(z_meds[valid_z])
      details$z_mad_range    <- range(z_mads[valid_z])

      message(sprintf("  z-score medians: [%.3f, %.3f]",
                      details$z_median_range[1], details$z_median_range[2]))
      message(sprintf("  z-score MADs:    [%.3f, %.3f]",
                      details$z_mad_range[1], details$z_mad_range[2]))
    }
  }

  # -- Sector distribution --
  if ("sector" %in% names(raw)) {
    sector_dist <- raw[, .N, by = sector][order(-N)]
    details$sector_distribution <- sector_dist
    checks["multiple_sectors"] <- nrow(sector_dist) >= 5
    message("  sectors:")
    for (j in seq_len(nrow(sector_dist))) {
      message(sprintf("    %s: %d", sector_dist$sector[j], sector_dist$N[j]))
    }

    # -- Zero Unknown sectors (D3 gate) --
    # "Unknown" is not a key in .SECTOR_NA_INDICATORS, so the sector
    # NA-mask silently does not fire for that bucket: delisted banks kept
    # non-NA net_debt_price in production (measured 2026-07-09). Any
    # Unknown-sector ticker is therefore a hard failure, not a warning.
    unk <- raw[is.na(sector) | sector == "Unknown", ticker]
    checks["no_unknown_sector"] <- length(unk) == 0
    details$unknown_sector_tickers <- unk
    if (length(unk) > 0) {
      message(sprintf("  FAIL: %d Unknown-sector tickers: %s",
                      length(unk), paste(sort(unk), collapse = ", ")))
    }

    # -- Mask fires for every masked sector --
    # For each sector in .SECTOR_NA_INDICATORS, every masked indicator
    # must be NA across all its members. A single non-NA cell means the
    # mask did not fire at compute/stub time and a nonsensical value
    # reached the cross-section.
    for (sec in names(.SECTOR_NA_INDICATORS)) {
      key <- paste0("mask_fires_", gsub("[^a-z]+", "_", tolower(sec)))
      masked_cols <- intersect(.SECTOR_NA_INDICATORS[[sec]], names(raw))
      members <- raw[sector == sec]
      if (nrow(members) == 0 || length(masked_cols) == 0) {
        checks[key] <- TRUE
        next
      }
      leaks <- masked_cols[vapply(masked_cols, function(cn) {
        any(!is.na(members[[cn]]))
      }, logical(1))]
      checks[key] <- length(leaks) == 0
      if (length(leaks) > 0) {
        leak_tks <- unique(unlist(lapply(leaks, function(cn) {
          members[!is.na(get(cn)), ticker]
        })))
        details[[paste0("mask_leaks_", sec)]] <-
          list(indicators = leaks, tickers = leak_tks)
        message(sprintf("  FAIL: %s mask not firing for %s (tickers: %s)",
                        sec, paste(leaks, collapse = ", "),
                        paste(head(sort(leak_tks), 10), collapse = ", ")))
      }
    }
  }

  # -- Daily-layer integrity (D1 gate; memoized per session) --
  if (check_daily) {
    memo_key <- paste(ts_dir, sector_path, split_dir, sep = "|")
    daily_res <- .DAILY_GATE_CACHE[[memo_key]]
    if (is.null(daily_res)) {
      daily_res <- validate_daily_layer(ts_dir = ts_dir,
                                        sector_path = sector_path,
                                        split_dir = split_dir)
      .DAILY_GATE_CACHE[[memo_key]] <- daily_res
    } else {
      message("  daily layer: using memoized result (once per session)")
    }
    checks["daily_layer_valid"] <- isTRUE(daily_res$pass)
    details$daily_layer <- daily_res
    message(sprintf("  daily layer: %s",
                    if (isTRUE(daily_res$pass)) "[ok]" else "FAIL"))
  }

  # -- Overall pass --
  all_pass <- all(checks)
  message(sprintf("  result: %d/%d checks passed -- %s",
                  sum(checks), length(checks),
                  if (all_pass) "PASS" else "FAIL"))

  list(pass = all_pass, checks = checks, details = details)
}


# =============================================================================
# 3b. validate_daily_layer()
# =============================================================================
#' Validate the daily timeseries layer (whole-history, all tickers)
#'
#' The daily layer is one shared artifact spanning all dates, so this runs
#' per layer, not per snapshot date. Three check families:
#'
#'   1. Sector coverage: every ticker with a timeseries layer (_fund or
#'      _daily) has a known, non-"Unknown" sector row. Fund layers are
#'      built WITH the sector (the .MASKED_STUB_OF stub NA-ing), so a
#'      missing sector at build time is the D3 root cause.
#'   2. On-disk mask: for tickers in masked sectors, every masked
#'      indicator column in their _daily parquet is all-NA. Catches
#'      layers built before the ticker's sector resolved.
#'   3. Split-consistent market cap (the D1 gate): across every true
#'      split event, implied shares (market_cap / price) must jump by
#'      the split multiple, and pe_trailing must be continuous. A daily
#'      layer that pairs the as-traded price with unadjusted per-share
#'      stubs shows a jump of 1 and a P/E discontinuity of the split
#'      ratio -- the exact D1 signature.
#'
#' Reads are column-pruned; only masked-sector members and split tickers
#' are opened. No network access.
#'
#' @param ts_dir Character. Timeseries directory.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param split_dir Character. Splits cache directory.
#' @return List with $pass, $checks (named logical), $details.
validate_daily_layer <- function(ts_dir      = "cache/timeseries",
                                 sector_path = .DEFAULT_SECTOR_PATH,
                                 split_dir   = "cache/splits") {

  t0 <- Sys.time()
  message(sprintf("validate_daily_layer: %s", ts_dir))
  checks  <- c()
  details <- list()

  daily_files <- list.files(ts_dir, pattern = "_daily\\.parquet$",
                            full.names = TRUE)
  fund_files  <- list.files(ts_dir, pattern = "_fund\\.parquet$",
                            full.names = TRUE)
  daily_tks <- gsub("_daily\\.parquet$", "", basename(daily_files))
  ts_tks    <- union(daily_tks,
                     gsub("_fund\\.parquet$", "", basename(fund_files)))

  checks["daily_files_present"] <- length(daily_files) >= 100
  message(sprintf("  layers: %d daily, %d fund", length(daily_files),
                  length(fund_files)))

  # -- 1. sector coverage over the timeseries universe --
  # current view: layers are built with the current classification, so
  # the on-disk mask is judged against it (SCD table resolved via
  # .sector_asof; legacy tables pass through)
  sec_dt <- if (file.exists(sector_path)) {
    tryCatch(.sector_asof(as.data.table(arrow::read_parquet(sector_path)),
                          Sys.Date()),
             error = function(e) NULL)
  } else NULL

  checks["sector_lookup_exists"] <- !is.null(sec_dt)
  if (!is.null(sec_dt)) {
    known <- sec_dt[!is.na(sector) & sector != "Unknown", ticker]
    no_sec <- sort(setdiff(ts_tks, known))
    checks["sector_coverage_complete"] <- length(no_sec) == 0
    details$no_sector_tickers <- no_sec
    if (length(no_sec) > 0) {
      message(sprintf("  FAIL: %d timeseries tickers lack a sector: %s",
                      length(no_sec), paste(no_sec, collapse = ", ")))
    }
  }

  # -- Plan column-pruned reads: masked cols + split-check cols per ticker --
  need_cols <- list()
  if (!is.null(sec_dt)) {
    masked_secs <- sec_dt[sector %in% names(.SECTOR_NA_INDICATORS) &
                            ticker %in% daily_tks]
    for (j in seq_len(nrow(masked_secs))) {
      tk <- masked_secs$ticker[j]
      need_cols[[tk]] <- .SECTOR_NA_INDICATORS[[masked_secs$sector[j]]]
    }
  }

  split_events <- list()
  for (tk in daily_tks) {
    sf <- file.path(split_dir, paste0(tk, ".parquet"))
    if (!file.exists(sf)) next
    s <- tryCatch(as.data.table(arrow::read_parquet(sf)),
                  error = function(e) NULL)
    if (is.null(s) || nrow(s) == 0) next
    s <- s[!is.na(ratio) & ratio > 0 &
             abs(log(1 / ratio)) >= .DAILY_GATE_SPLIT_BAND]
    if (nrow(s) == 0) next
    split_events[[tk]] <- s
    need_cols[[tk]] <- c(need_cols[[tk]],
                         "price", "market_cap", "pe_trailing")
  }

  # -- 2 + 3. one pruned read per flagged ticker --
  mask_leaks  <- character(0)
  jump_fails  <- list()
  n_events    <- 0L

  for (tk in names(need_cols)) {
    f <- file.path(ts_dir, sprintf("%s_daily.parquet", tk))
    if (!file.exists(f)) next
    cols <- unique(c("date", need_cols[[tk]]))
    d <- tryCatch(
      as.data.table(arrow::read_parquet(
        f, col_select = tidyselect::any_of(cols))),
      error = function(e) NULL
    )
    if (is.null(d) || nrow(d) == 0 || !"date" %in% names(d)) next
    d[, date := as.Date(date)]

    # on-disk mask
    mcols <- intersect(setdiff(need_cols[[tk]],
                               c("price", "market_cap", "pe_trailing")),
                       names(d))
    leaked <- mcols[vapply(mcols, function(cn) any(!is.na(d[[cn]])),
                           logical(1))]
    if (length(leaked) > 0) {
      mask_leaks <- c(mask_leaks,
                      sprintf("%s:%s", tk, paste(leaked, collapse = "+")))
    }

    # split-consistency
    ev <- split_events[[tk]]
    if (!is.null(ev) && all(c("price", "market_cap") %in% names(d))) {
      for (j in seq_len(nrow(ev))) {
        ex <- as.Date(ev$ex_date[j])
        n_mult <- 1 / ev$ratio[j]
        b <- d[date >= ex - 14 & date < ex &
                 !is.na(market_cap) & !is.na(price)]
        a <- d[date >= ex & date <= ex + 14 &
                 !is.na(market_cap) & !is.na(price)]
        if (nrow(b) == 0 || nrow(a) == 0) next
        b <- b[.N]; a <- a[1]
        n_events <- n_events + 1L
        jump <- (a$market_cap / a$price) / (b$market_cap / b$price)
        ok_mc <- abs(log(jump / n_mult)) < .DAILY_GATE_JUMP_TOL
        ok_pe <- TRUE
        if ("pe_trailing" %in% names(d) &&
            !is.na(b$pe_trailing) && !is.na(a$pe_trailing) &&
            a$pe_trailing > 0 && b$pe_trailing > 0) {
          ok_pe <- abs(log(b$pe_trailing / a$pe_trailing)) <
            .DAILY_GATE_JUMP_TOL
        }
        if (!ok_mc || !ok_pe) {
          jump_fails[[length(jump_fails) + 1]] <- data.table(
            ticker = tk, ex_date = ex, split_mult = n_mult,
            shares_jump = jump,
            pe_before = b$pe_trailing, pe_after = a$pe_trailing)
        }
      }
    }
  }

  checks["mask_fires_on_disk"] <- length(mask_leaks) == 0
  details$mask_leaks <- mask_leaks
  if (length(mask_leaks) > 0) {
    message(sprintf("  FAIL: on-disk mask leaks for %d tickers: %s",
                    length(mask_leaks),
                    paste(head(mask_leaks, 10), collapse = ", ")))
  }

  jump_dt <- if (length(jump_fails) > 0) rbindlist(jump_fails) else NULL
  checks["split_consistent_market_cap"] <- is.null(jump_dt)
  details$split_jump_failures <- jump_dt
  details$n_split_events_checked <- n_events
  if (!is.null(jump_dt)) {
    message(sprintf(
      "  FAIL: %d/%d split events with split-inconsistent mc/pe (D1): %s",
      nrow(jump_dt), n_events,
      paste(head(sprintf("%s@%s x%.3g jump %.3g", jump_dt$ticker,
                         jump_dt$ex_date, jump_dt$split_mult,
                         jump_dt$shares_jump), 8), collapse = "; ")))
  } else {
    message(sprintf("  split events checked: %d, all consistent", n_events))
  }

  all_pass <- all(checks)
  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  message(sprintf("  result: %d/%d checks passed -- %s (%.1fs)",
                  sum(checks), length(checks),
                  if (all_pass) "PASS" else "FAIL", elapsed))

  list(pass = all_pass, checks = checks, details = details)
}


# =============================================================================
# 4. summarize_coverage()
# =============================================================================
#' Generate a coverage report for a snapshot
#'
#' Shows the percentage of tickers with non-NA values for each indicator,
#' broken down by sector.
#'
#' @param snapshot_date Date or character.
#' @param output_dir Character. Snapshot directory.
#' @return data.table with indicator, overall coverage, and per-sector coverage.
summarize_coverage <- function(snapshot_date,
                               output_dir = .DEFAULT_SNAPSHOT_DIR) {

  snapshot_date <- as.Date(snapshot_date)

  raw <- load_snapshot(snapshot_date, type = "raw", output_dir = output_dir)
  if (is.null(raw)) {
    stop(sprintf("summarize_coverage: no snapshot found for %s", snapshot_date),
         call. = FALSE)
  }

  ind_names <- get_indicator_names()
  sectors   <- unique(raw$sector)

  # Overall coverage per indicator
  overall <- data.table(
    indicator = ind_names,
    n_total   = nrow(raw),
    n_valid   = sapply(ind_names, function(col) {
      if (col %in% names(raw)) sum(!is.na(raw[[col]])) else 0L
    }),
    pct_valid = sapply(ind_names, function(col) {
      if (col %in% names(raw)) {
        round(mean(!is.na(raw[[col]])) * 100, 1)
      } else 0
    })
  )

  # Per-sector coverage (wide format: one column per sector)
  for (sec in sort(sectors)) {
    sec_dt <- raw[sector == sec]
    col_name <- gsub(" ", "_", tolower(sec))
    overall[, (col_name) := sapply(ind_names, function(col) {
      if (col %in% names(sec_dt) && nrow(sec_dt) > 0) {
        round(mean(!is.na(sec_dt[[col]])) * 100, 1)
      } else 0
    })]
  }

  # Print summary
  message(sprintf("summarize_coverage: %s (%d tickers, %d indicators)",
                  snapshot_date, nrow(raw), length(ind_names)))

  # Group by coverage tier
  high   <- overall[pct_valid >= 90]
  medium <- overall[pct_valid >= 50 & pct_valid < 90]
  low    <- overall[pct_valid < 50]

  message(sprintf("  high coverage (>=90%%): %d indicators", nrow(high)))
  message(sprintf("  medium coverage (50-89%%): %d indicators", nrow(medium)))
  message(sprintf("  low coverage (<50%%): %d indicators", nrow(low)))

  if (nrow(low) > 0) {
    message("  low coverage indicators:")
    for (j in seq_len(nrow(low))) {
      message(sprintf("    %s: %.1f%%", low$indicator[j], low$pct_valid[j]))
    }
  }

  .assert_output(overall, "summarize_coverage", list(
    "is data.table"        = is.data.table,
    "has indicator col"    = function(x) "indicator" %in% names(x),
    "has pct_valid col"    = function(x) "pct_valid" %in% names(x),
    "covers all indicators" = function(x) nrow(x) == length(ind_names)
  ))

  overall
}


# =============================================================================
# 5. list_snapshots()
# =============================================================================
#' List all available snapshot dates
#'
#' @param output_dir Character. Snapshot directory.
#' @return Character vector of snapshot dates (sorted), or character(0).
list_snapshots <- function(output_dir = .DEFAULT_SNAPSHOT_DIR) {

  if (!dir.exists(output_dir)) return(character(0))

  files <- list.files(output_dir, pattern = "pit_.*_raw\\.parquet$")
  dates <- gsub("pit_(.*)_raw\\.parquet", "\\1", files)
  sort(dates)
}
