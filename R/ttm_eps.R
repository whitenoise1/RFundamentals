# ============================================================================
# ttm_eps.R  --  Trailing-twelve-month diluted EPS + TTM P/E (PIT)
# ============================================================================
# Adds two daily columns to the existing _daily timeseries WITHOUT rebuilding
# the FY-cadence fundamentals layer or touching indicator_compute.R:
#   eps_ttm  : trailing-12m diluted EPS, de-cumulated from quarterly XBRL,
#              refreshed on every 10-Q/10-K filing (PIT by filed_date).
#   pe_ttm   : price / eps_ttm  (the honest trailing P/E; |eps_ttm| >= 0.01).
#
# WHY TTM and not the existing pe_trailing: build_ticker_fundamentals keys the
# fund layer to FY (annual) rows, so pe_trailing uses annual EPS and refreshes
# only once a year on the 10-K. eps_ttm sums the latest four reported quarters
# and refreshes each quarter -- the freshest honest earnings base.
#
# WHY NOT a forecast: the Holt damped-trend forward-EPS estimator was validated
# out-of-sample (analytics/forward_eps_methodology.md §6) and tuned by grid
# search; best PIT MASE = 0.9995 ~= 1.0 (indistinguishable from the random
# walk; "Higgledy Piggledy Growth", Little 1962 / Ball-Watts 1972). Per §6 the
# model is NOT adopted: Forward P/E ~= trailing TTM P/E, stated honestly.
#
# De-cumulation: EarningsPerShareDiluted is reported cumulative YTD within a
# fiscal-year period_start group (Q1~90d, Q2~181d/H1, Q3~272d/9m, FY~363d).
# TTM at an interim quarter q = YTD_q + FY_prev - YTD_prev,q ; at FY-end = FY.
# This is the accounting-standard TTM, robust to a missing middle quarter.
#
# Dependencies: data.table, arrow
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

# Cumulative split-adjustment factor that maps an as-filed per-share value onto
# the CURRENT split basis. GAAP restates EPS for splits across every period a
# filing presents, so an as-filed value's split basis is the split state at its
# FILING date. Multiply by the product of the ratios of all splits with
# ex_date > filed (getSplits convention: an n:1 split is 1/n, e.g. 50:1 -> 0.02)
# to express it on today's share count -- matching the split-adjusted price.
# `splits`: data.frame/data.table with columns ex_date (Date), ratio (numeric).
# NULL/empty splits, or NA filed, -> factor 1 (no adjustment).
.split_factor <- function(filed, splits = NULL) {
  if (is.null(splits) || !NROW(splits)) return(rep(1, length(filed)))
  ex <- as.Date(splits$ex_date); rt <- as.numeric(splits$ratio)
  vapply(filed, function(f) {
    if (is.na(f)) return(1)
    sel <- ex > f & !is.na(rt) & rt > 0
    if (!any(sel)) 1 else prod(rt[sel])
  }, numeric(1))
}

# --- quarterly TTM diluted-EPS series for one ticker -------------------------
# fund_dt: raw long-format XBRL (cache/fundamentals/{cik}_{ticker}.parquet).
# splits : per-ticker split events (ex_date, ratio); NULL = no split adjustment.
# Returns data.table(qend, filed, eps_ttm) ordered by qend, or NULL. eps_ttm is
# expressed on the CURRENT split basis so price/eps_ttm is consistent at every date.
build_ttm_eps_series <- function(fund_dt, splits = NULL) {
  e <- fund_dt[concept == "eps_diluted" &
                 !is.na(period_start) & !is.na(period_end) & !is.na(value)]
  if (!nrow(e)) return(NULL)
  e[, `:=`(ps = as.Date(period_start), pe = as.Date(period_end),
           fd = as.Date(filed))]
  # Normalize every as-filed EPS to the current split basis BEFORE de-cumulation,
  # so a TTM window spanning a split never blends pre- and post-split quarters.
  e[, value := value * .split_factor(fd, splits)]
  e[, dur := as.integer(pe - ps)]
  # classify cumulative period by duration
  e[, pt := fifelse(dur <= 135, "Q1",
             fifelse(dur <= 225, "Q2",
              fifelse(dur <= 315, "Q3", "FY")))]
  # dedup (period_start, period_type): prefer non-8-K, then EARLIEST filed.
  # The vintage-preserving cache keeps every filing that reported a period;
  # the original report (earliest filed) is the point-in-time correct pick --
  # later rows are comparatives whose filed date would delay availability.
  # Split basis stays consistent because .split_factor keys off each row's
  # own filed date. (On old collapsed caches this is a no-op.)
  e[, is8k := as.integer(form == "8-K")]
  setorder(e, ps, pt, is8k, fd)
  e <- e[, .SD[1], by = .(ps, pt)]

  starts <- sort(unique(e$ps))
  if (length(starts) < 2) return(NULL)
  cum <- dcast(e, ps ~ pt, value.var = "value")
  fld <- dcast(e, ps ~ pt, value.var = "fd",
               fun.aggregate = function(x) if (length(x)) max(x) else as.Date(NA))
  for (cc in c("Q1", "Q2", "Q3", "FY")) {
    if (!cc %in% names(cum)) cum[[cc]] <- NA_real_
    if (!cc %in% names(fld)) fld[[cc]] <- as.Date(NA)
  }
  setkey(cum, ps); setkey(fld, ps)

  out <- list()
  for (i in seq_along(starts)) {
    s <- starts[i]
    row <- cum[.(s)]; frow <- fld[.(s)]
    prev <- if (i >= 2) cum[.(starts[i - 1])] else NULL
    for (q in c("Q1", "Q2", "Q3")) {
      ytd <- row[[q]]
      if (is.na(ytd) || is.null(prev)) next
      fy_prev <- prev[["FY"]]; ytd_prev <- prev[[q]]
      if (is.na(fy_prev) || is.na(ytd_prev)) next
      out[[length(out) + 1]] <- list(
        qend = e[ps == s & pt == q, pe][1],
        filed = frow[[q]],
        eps_ttm = ytd + fy_prev - ytd_prev)
    }
    if (!is.na(row[["FY"]])) {
      out[[length(out) + 1]] <- list(
        qend = e[ps == s & pt == "FY", pe][1],
        filed = frow[["FY"]],
        eps_ttm = row[["FY"]])
    }
  }
  if (!length(out)) return(NULL)
  r <- rbindlist(out)
  r <- r[!is.na(qend) & !is.na(eps_ttm) & !is.na(filed)]
  setorder(r, qend)
  r <- r[!duplicated(qend)]
  if (!nrow(r)) return(NULL)
  r
}

# --- PIT map of eps_ttm onto a vector of dates ------------------------------
# For each date d, pick the most recent TTM record with filed <= d (greedy by
# period end, mirroring update_ticker_daily's non-monotonic-filing-safe lookup).
.pit_eps_ttm <- function(ttm, dates) {
  n <- length(dates)
  out <- rep(NA_real_, n)
  if (is.null(ttm) || !nrow(ttm)) return(out)
  o <- order(-as.integer(ttm$qend))           # most-recent period first
  qe_filed <- as.Date(ttm$filed)[o]
  qe_eps   <- ttm$eps_ttm[o]
  for (k in seq_along(qe_eps)) {
    elig <- is.na(out) & dates >= qe_filed[k]
    out[elig] <- qe_eps[k]
  }
  out
}

# --- per-ticker split events (cached) ----------------------------------------
# cache/splits/{tk}.parquet (ex_date, ratio). If absent and fetch=TRUE, pull once
# via quantmod::getSplits and cache -- including an EMPTY table for no-split
# tickers, so we never refetch them. Returns data.table(ex_date, ratio); 0 rows
# = no splits. Same Yahoo provider that split-adjusted the price, so the EPS and
# price split bases stay consistent.
# Renamed chains where Yahoo keys the continuous history (and splits)
# under the CURRENT symbol only. Duplicated from pit_assembler.R's
# .PROVIDER_SYMBOL_MAP so this module sources standalone -- keep in sync
# (tests/test_price_fallback.R asserts equality).
.SPLITS_SYMBOL_MAP <- c(
  COG = "CTRA", GPS = "GAP", FBHS = "FBIN",
  DISCA = "WBD", VIAC = "PARA", CDAY = "DAY",
  # old-CIK wave Tier 2 same-share-line chains (see pit_assembler.R)
  ACE = "CB", ADS = "BFH", ARNC = "HWM", CCE = "CCEP", DLPH = "APTV",
  HFC = "DINO", HRS = "LHX", JEC = "J", KFT = "MDLZ", SAI = "LDOS",
  TSO = "ANDV", TYC = "JCI", WPI = "AGN"
)

# mirror of pit_assembler.R .YAHOO_REUSED_BLOCKLIST -- keep in sync
.SPLITS_YAHOO_BLOCKLIST <- c("COL", "CAM", "PCL", "PCLN", "PCS")

# Fundamentals-cache aliases for roster tickers that share a CIK with the
# cached listing (build_fundamentals dedups the fetch list by CIK, so one
# file serves every listing of the filer). Mirrors get_fundamentals'
# CIK-prefix fallback, which this module cannot use because daily files
# carry no CIK -- keep in sync with the shared-CIK pairs in
# constituent_master (tests assert each alias resolves to a cache file).
.FUND_CACHE_ALIASES <- c(
  DISCK = "DISCA", WBD = "DISCA", UAA = "UA", NWSA = "NWS", FOXA = "FOX",
  # old-CIK wave Tier 2: recovered delisted names whose filer's cache
  # lives under the current-ticker sibling (same CIK, no second fetch)
  ACE = "CB", ARNC = "HWM", CHK = "EXE", CMCSK = "CMCSA", DLPH = "APTV",
  DPS = "KDP", DWDP = "DD", HRS = "LHX", JEC = "J", KFT = "MDLZ",
  KORS = "CPRI", LUK = "JEF", PCLN = "BKNG", PCS = "TMUS", SAI = "LDOS",
  TYC = "JCI", ANDV = "TSO"
)

# Tiingo split-event fallback for Yahoo-PURGED symbols (Wave P): getSplits
# errors on a purged symbol, but the split events are needed regardless of
# whether the Tiingo PRICE fetch has run yet (the fund layer loads splits
# before any price fetch). Derives TRUE split events from the daily
# splitFactor column with the same spinoff band filter as pit_assembler's
# .tiingo_convert -- keep the band in sync. Returns data.table, or NULL
# when Tiingo is unreachable (no token / HTTP failure) so the caller can
# avoid caching a false empty.
.tiingo_splits <- function(sym) {
  tok <- ""
  for (nm in c("TIINGO_TOKEN", "tiingo_token")) {
    v <- Sys.getenv(nm); if (nzchar(v)) { tok <- v; break }
  }
  if (tok == "" && file.exists("~/.Renviron")) {
    readRenviron("~/.Renviron")
    for (nm in c("TIINGO_TOKEN", "tiingo_token")) {
      v <- Sys.getenv(nm); if (nzchar(v)) { tok <- v; break }
    }
  }
  if (tok == "" || !requireNamespace("httr", quietly = TRUE) ||
      !requireNamespace("jsonlite", quietly = TRUE)) return(NULL)
  d <- tryCatch({
    resp <- httr::GET(sprintf(
      "https://api.tiingo.com/tiingo/daily/%s/prices?startDate=2009-01-01&token=%s",
      tolower(sym), tok), httr::timeout(30))
    if (httr::status_code(resp) != 200) stop(httr::status_code(resp))
    as.data.table(jsonlite::fromJSON(
      httr::content(resp, as = "text", encoding = "UTF-8")))
  }, error = function(e) NULL)
  if (is.null(d)) return(NULL)
  empty <- data.table(ex_date = as.Date(character()), ratio = numeric())
  if (nrow(d) == 0 || !"splitFactor" %in% names(d)) return(empty)
  d[, date := as.Date(substr(date, 1, 10))]
  # duplicated from pit_assembler.R .is_true_split_factor -- keep in sync:
  # clean small-integer ratio away from 1; spinoffs (arbitrary decimals)
  # and nonpositive dirty factors rejected
  is_split <- vapply(d$splitFactor, function(x) {
    if (is.na(x) || x <= 0 || x == 1) return(FALSE)
    if (abs(log(x)) < log(1.15)) return(FALSE)
    for (q in 1:10) {
      p <- round(x * q)
      if (p >= 1 && p != q && abs(x - p / q) / x < 0.002) return(TRUE)
    }
    FALSE
  }, logical(1))
  ev <- d[is_split]
  if (nrow(ev) == 0) return(empty)
  ev[, .(ex_date = date, ratio = 1 / splitFactor)]
}

load_ticker_splits <- function(tk, split_dir = "cache/splits", fetch = TRUE,
                               refresh = FALSE) {
  cf <- file.path(split_dir, paste0(tk, ".parquet"))
  cached <- if (file.exists(cf)) {
    tryCatch(as.data.table(arrow::read_parquet(cf)), error = function(e) NULL)
  } else NULL
  # refresh = TRUE (Tier D split guard): bypass the cache read and fetch
  # fresh; a failed fetch falls back to the cached table rather than
  # caching a false empty.
  if (!refresh && !is.null(cached)) return(cached)
  empty <- data.table(ex_date = as.Date(character()), ratio = numeric())
  if (!fetch || !requireNamespace("quantmod", quietly = TRUE)) {
    return(if (!is.null(cached)) cached else empty)
  }
  # Renamed chains resolve to the current symbol; Yahoo uses dashes for
  # class tickers (BRK-B), the roster uses dots. Reused dead symbols
  # (mirror of pit_assembler.R .YAHOO_REUSED_BLOCKLIST) never ask Yahoo:
  # it would answer with the reusing company's split history.
  sym <- .SPLITS_SYMBOL_MAP[tk]
  sym <- if (!is.na(sym)) as.character(sym) else tk
  yerr <- FALSE
  sx <- if (tk %in% .SPLITS_YAHOO_BLOCKLIST) { yerr <- TRUE; NULL } else
    tryCatch(quantmod::getSplits(gsub("\\.", "-", sym)),
             error = function(e) { yerr <<- TRUE; NULL })
  s <- if (is.null(sx) || !length(sx)) empty else
    data.table(ex_date = as.Date(zoo::index(sx)), ratio = as.numeric(sx))
  s <- s[!is.na(ratio) & ratio > 0]
  # A getSplits ERROR (vs a clean no-splits answer) means Yahoo does not
  # know the symbol -- purged delisting. Ask Tiingo before caching, and
  # do NOT cache when Tiingo is unreachable (a false empty poisons the
  # as-traded reconstruction of a split-adjusted Tiingo price cache).
  if (yerr && nrow(s) == 0) {
    ts <- .tiingo_splits(gsub("\\.", "-", sym))
    if (is.null(ts)) return(if (!is.null(cached)) cached else empty)
    s <- ts
  }
  if (!dir.exists(split_dir)) dir.create(split_dir, recursive = TRUE)
  tryCatch(arrow::write_parquet(s, cf), error = function(e) NULL)
  s
}

# --- augment all _daily parquets in place with eps_ttm + pe_ttm --------------
# fund_dir : raw XBRL cache (cik_ticker.parquet); ts_dir : timeseries cache.
# Every _daily parquet gets both columns (NA where no positive-EPS TTM exists),
# so the ingest's canonical column set always includes them.
augment_daily_ttm <- function(fund_dir  = "cache/fundamentals",
                              ts_dir    = if (exists(".TS_DIR")) .TS_DIR else "cache/timeseries",
                              split_dir = "cache/splits",
                              fetch_splits = TRUE,
                              verbose   = TRUE,
                              tickers   = NULL) {
  daily_files <- list.files(ts_dir, pattern = "_daily\\.parquet$", full.names = TRUE)
  # per-ticker scope (daily-update wave): the incremental tiers pass only
  # the tickers whose layers changed, so a daily run rewrites a handful
  # of parquets instead of all ~750
  if (!is.null(tickers)) {
    daily_files <- daily_files[
      sub("_daily\\.parquet$", "", basename(daily_files)) %in% tickers]
  }
  fund_files  <- list.files(fund_dir, pattern = "\\.parquet$", full.names = TRUE)
  # ticker -> fundamentals path via a NAMED VECTOR (not a keyed data.table: a
  # keyed join `dt[.(tk), ]` whose lookup variable shares the key column's name
  # `tk` silently resolves `.(tk)` to the whole column, returning row 1 for all).
  fund_tks <- sub("^[0-9]+_(.*)\\.parquet$", "\\1", basename(fund_files))
  # Occurrence-split tickers (old-CIK wave Tier 2: CCE, ESRX) have TWO
  # cache files with the same ticker suffix under different CIKs. The
  # daily layer runs to the ticker's final trading window, so it must use
  # the LATEST entity's fundamentals: keep the file whose newest filed
  # date is most recent (alphabetical CIK order would pick the OLD one).
  if (anyDuplicated(fund_tks)) {
    for (tk_dup in unique(fund_tks[duplicated(fund_tks)])) {
      idx <- which(fund_tks == tk_dup)
      last_filed <- vapply(fund_files[idx], function(f) {
        d <- tryCatch(arrow::read_parquet(f, col_select = "filed"),
                      error = function(e) NULL)
        if (is.null(d) || !nrow(d)) return(-Inf)
        as.numeric(max(as.Date(d$filed), na.rm = TRUE))
      }, numeric(1))
      drop <- idx[-which.max(last_filed)]
      fund_files <- fund_files[-drop]
      fund_tks   <- fund_tks[-drop]
    }
  }
  path_by_tk <- stats::setNames(fund_files, fund_tks)
  # Shared-CIK listings resolve to the sibling ticker's cache file
  # (DISCK/WBD -> DISCA, ...); never shadow a ticker's own file.
  for (al in names(.FUND_CACHE_ALIASES)) {
    tgt <- .FUND_CACHE_ALIASES[[al]]
    if (!al %in% names(path_by_tk) && tgt %in% names(path_by_tk)) {
      path_by_tk[al] <- path_by_tk[[tgt]]
    }
  }

  n_ok <- 0L; n_ttm <- 0L; n_fail <- 0L
  for (f in daily_files) {
    tk <- sub("_daily\\.parquet$", "", basename(f))
    d <- tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
    if (is.null(d) || !nrow(d) || !all(c("date", "price") %in% names(d))) {
      n_fail <- n_fail + 1L; next
    }
    d[, date := as.Date(date)]

    splits <- tryCatch(load_ticker_splits(tk, split_dir, fetch = fetch_splits),
                       error = function(e) NULL)

    ttm <- NULL
    fp <- unname(path_by_tk[tk])           # NA if this ticker has no fundamentals file
    if (!is.na(fp) && file.exists(fp)) {
      fd <- tryCatch(as.data.table(arrow::read_parquet(fp)), error = function(e) NULL)
      if (!is.null(fd)) ttm <- tryCatch(build_ttm_eps_series(fd, splits), error = function(e) NULL)
    }

    eps_ttm <- .pit_eps_ttm(ttm, d$date)
    # Convert from today's split basis (build_ttm_eps_series normalizes all
    # quarters there so TTM sums can span splits) back to the as-traded
    # basis of each date, matching the as-traded daily `price`. The split
    # factors cancel, so pe_ttm = Close / eps_ttm_today_basis either way;
    # storing eps_ttm as-traded keeps Price / eps_ttm reconciling with the
    # shown pe_ttm.
    eps_ttm <- eps_ttm / .split_factor(d$date, splits)
    # Guard: positive trailing EPS only -- eps_ttm <= 0 (TTM loss) is "not meaningful"
    # for P/E and is surfaced as N/M by the frontend (pe_ttm = NA).
    pe_ttm  <- ifelse(!is.na(d$price) & !is.na(eps_ttm) & eps_ttm >= 0.01,
                      d$price / eps_ttm, NA_real_)
    set(d, j = "eps_ttm", value = eps_ttm)
    set(d, j = "pe_ttm",  value = pe_ttm)

    arrow::write_parquet(d, f)
    n_ok <- n_ok + 1L
    if (any(!is.na(eps_ttm))) n_ttm <- n_ttm + 1L
  }
  if (verbose) {
    message(sprintf("augment_daily_ttm: %d daily files updated (%d with TTM EPS), %d skipped",
                    n_ok, n_ttm, n_fail))
  }
  invisible(list(updated = n_ok, with_ttm = n_ttm, failed = n_fail))
}

# tiny null-coalescing helper (only if not already defined by the package)
`%||%` <- function(a, b) if (is.null(a)) b else a
