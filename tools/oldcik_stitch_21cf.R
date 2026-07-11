# Old-CIK wave Tier 2: stitch the old News Corp / Twenty-First Century
# Fox price chain (Tiingo TFCFA / TFCF, the final tickers) into the
# NWSA / FOXA / FOX caches so the occurrence-1 (old entity) snapshot
# windows price correctly:
#   NWSA <- TFCFA rows  < 2013-06-19  (old News Corp class A)
#   FOXA <- TFCFA rows [2013-06-19, cache head)   (21CF class A)
#   FOX  <- TFCF  rows [2013-06-19, cache head)   (21CF class B)
#
# Basis invariant: cached Close(d) = as_traded(d) x prod(split ratios
# ex-dated AFTER d in cache/splits/<ticker>). The NWSA/FOXA/FOX splits
# caches are empty, so prepended rows must be RAW as-traded prints and
# the prepended chain must contain NO true split events -- verified
# loudly below (a spurious splitFactor at the 2013 separation would
# corrupt every pre-2013 price).
#
# Idempotent: a cache whose head already predates the segment is skipped.
# Usage: Rscript tools/oldcik_stitch_21cf.R

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(httr); library(jsonlite)
})
suppressMessages(source("R/pit_assembler.R"))  # .is_true_split_factor

readRenviron("~/.Renviron")
tok <- Sys.getenv("tiingo_token", Sys.getenv("TIINGO_TOKEN"))
stopifnot(nzchar(tok))

tiingo_raw <- function(sym, from = "2009-01-01") {
  url <- sprintf(
    "https://api.tiingo.com/tiingo/daily/%s/prices?startDate=%s&token=%s",
    tolower(sym), from, tok)
  for (a in 1:3) {
    r <- tryCatch(GET(url, timeout(60)), error = function(e) NULL)
    if (!is.null(r) && status_code(r) == 200) {
      d <- as.data.table(fromJSON(content(r, as = "text", encoding = "UTF-8")))
      if (nrow(d)) { d[, date := as.Date(substr(date, 1, 10))]; setorder(d, date); return(d) }
    }
    Sys.sleep(5 * a)
  }
  NULL
}

stitch <- function(roster_tk, src, seg_from, seg_to) {
  cache <- sprintf("cache/prices/%s_yahoo_2009-01-01.parquet", roster_tk)
  stopifnot(file.exists(cache))
  cur <- as.data.table(read_parquet(cache))
  cur[, date := as.Date(date)]
  head_date <- min(cur$date)
  if (head_date <= as.Date(seg_from) + 10) {
    message(roster_tk, ": already stitched (head ", head_date, ") -- skip")
    return(invisible(NULL))
  }
  seg <- src[date >= as.Date(seg_from) & date <= min(as.Date(seg_to),
                                                     head_date - 1)]
  stopifnot(nrow(seg) > 100)

  # no true split events inside the segment, else the empty splits cache
  # cannot reconstruct as-traded prints
  if ("splitFactor" %in% names(seg)) {
    ev <- seg[.is_true_split_factor(splitFactor)]
    if (nrow(ev)) stop(roster_tk, ": true split events inside segment: ",
                       paste(ev$date, collapse = ","))
  }

  pre <- data.table(
    date = as.character(seg$date),
    Open = seg$open, High = seg$high, Low = seg$low,
    Close = seg$close, Volume = seg$volume,
    Adjusted = if ("adjClose" %in% names(seg)) seg$adjClose else seg$close)
  setnames(pre, c("Open", "High", "Low", "Close", "Volume", "Adjusted"),
           paste(gsub("[.]", "-", roster_tk),
                 c("Open", "High", "Low", "Close", "Volume", "Adjusted"),
                 sep = "."))
  # align to the existing cache's column set
  cur_out <- copy(cur); cur_out[, date := as.character(date)]
  stopifnot(identical(sort(names(pre)), sort(names(cur_out))))
  out <- rbind(pre[, names(cur_out), with = FALSE], cur_out)
  write_parquet(out, cache)
  message(sprintf("%s: prepended %d rows (%s..%s) ahead of %s",
                  roster_tk, nrow(pre), min(seg$date), max(seg$date),
                  head_date))
}

tfcfa <- tiingo_raw("TFCFA"); stopifnot(!is.null(tfcfa))
tfcf  <- tiingo_raw("TFCF");  stopifnot(!is.null(tfcf))
message(sprintf("TFCFA %d rows %s..%s | TFCF %d rows %s..%s",
                nrow(tfcfa), min(tfcfa$date), max(tfcfa$date),
                nrow(tfcf), min(tfcf$date), max(tfcf$date)))

stitch("NWSA", tfcfa, "2009-01-01", "2013-06-18")
stitch("FOXA", tfcfa, "2013-06-19", "2019-03-19")
stitch("FOX",  tfcf,  "2013-06-19", "2019-03-19")
message("stitch complete")
