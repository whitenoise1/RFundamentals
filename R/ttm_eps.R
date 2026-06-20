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

# --- quarterly TTM diluted-EPS series for one ticker -------------------------
# fund_dt: raw long-format XBRL (cache/fundamentals/{cik}_{ticker}.parquet).
# Returns data.table(qend, filed, eps_ttm) ordered by qend, or NULL.
build_ttm_eps_series <- function(fund_dt) {
  e <- fund_dt[concept == "eps_diluted" &
                 !is.na(period_start) & !is.na(period_end) & !is.na(value)]
  if (!nrow(e)) return(NULL)
  e[, `:=`(ps = as.Date(period_start), pe = as.Date(period_end),
           fd = as.Date(filed))]
  e[, dur := as.integer(pe - ps)]
  # classify cumulative period by duration
  e[, pt := fifelse(dur <= 135, "Q1",
             fifelse(dur <= 225, "Q2",
              fifelse(dur <= 315, "Q3", "FY")))]
  # dedup (period_start, period_type): prefer non-8-K, then latest filed
  e[, is8k := as.integer(form == "8-K")]
  setorder(e, ps, pt, is8k, -fd)
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

# --- augment all _daily parquets in place with eps_ttm + pe_ttm --------------
# fund_dir : raw XBRL cache (cik_ticker.parquet); ts_dir : timeseries cache.
# Every _daily parquet gets both columns (NA where no positive-EPS TTM exists),
# so the ingest's canonical column set always includes them.
augment_daily_ttm <- function(fund_dir = "cache/fundamentals",
                              ts_dir   = if (exists(".TS_DIR")) .TS_DIR else "cache/timeseries",
                              verbose  = TRUE) {
  daily_files <- list.files(ts_dir, pattern = "_daily\\.parquet$", full.names = TRUE)
  fund_files  <- list.files(fund_dir, pattern = "\\.parquet$", full.names = TRUE)
  # ticker -> fundamentals path via a NAMED VECTOR (not a keyed data.table: a
  # keyed join `dt[.(tk), ]` whose lookup variable shares the key column's name
  # `tk` silently resolves `.(tk)` to the whole column, returning row 1 for all).
  path_by_tk <- stats::setNames(fund_files,
                  sub("^[0-9]+_(.*)\\.parquet$", "\\1", basename(fund_files)))

  n_ok <- 0L; n_ttm <- 0L; n_fail <- 0L
  for (f in daily_files) {
    tk <- sub("_daily\\.parquet$", "", basename(f))
    d <- tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
    if (is.null(d) || !nrow(d) || !all(c("date", "price") %in% names(d))) {
      n_fail <- n_fail + 1L; next
    }
    d[, date := as.Date(date)]

    ttm <- NULL
    fp <- unname(path_by_tk[tk])           # NA if this ticker has no fundamentals file
    if (!is.na(fp) && file.exists(fp)) {
      fd <- tryCatch(as.data.table(arrow::read_parquet(fp)), error = function(e) NULL)
      if (!is.null(fd)) ttm <- tryCatch(build_ttm_eps_series(fd), error = function(e) NULL)
    }

    eps_ttm <- .pit_eps_ttm(ttm, d$date)
    pe_ttm  <- ifelse(!is.na(d$price) & !is.na(eps_ttm) & abs(eps_ttm) >= 0.01,
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
