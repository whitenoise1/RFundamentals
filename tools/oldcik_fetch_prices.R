# Old-CIK wave Tier 2: price recovery for resolved names.
#   1. delete stale/reused-symbol caches that a correct chain will replace
#   2. fetch via fetch_ticker_prices (Yahoo under the mapped current
#      symbol first, Tiingo fallback for dead chains)
#   3. verify: window coverage + implied market cap sanity per name
#      (reused-symbol poison shows up as absurd market cap -- the COL
#      lesson: Yahoo COL served a $0.15 penny stock over Rockwell
#      Collins' membership window)
#
# Rate note: Tiingo hourly quota is finite; rerun the script to resume --
# names with a verified cache are skipped.
# Usage: Rscript tools/oldcik_fetch_prices.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })
suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/ttm_eps.R")
  source("R/pit_assembler.R")
})

# caches to replace: poison-with-overlap (COL) or inert junk from reused
# symbols where a true chain is recoverable via the provider map / Tiingo
REPLACE <- c("COL", "CAM", "PCL", "PCLN", "PCS", "DLPH", "ARNC")

# fetch list: chains via provider map + dead own-symbol chains + probe
# candidates + tail-stub partials. LUK already has a good Tiingo cache.
FETCH <- c(
  # provider-map chains (Yahoo serves the live current symbol)
  "ACE", "ADS", "CCE", "HFC", "JEC", "KFT", "SAI", "TSO", "TYC", "WPI",
  "DLPH", "HRS", "ARNC",
  # dead symbols with verified full Tiingo chains
  "CAM", "COL", "ESRX", "JDSU", "LO", "NYX", "PCL", "PCLN", "PCS",
  "WPX", "YHOO",
  # probe-then-fetch candidates (round-2 probes were quota-blocked)
  "ESV", "IGT", "BIG", "MNK", "ENDP", "JCP",
  # tail stubs: Tiingo only reaches back to 2016 for these dead names --
  # partial recovery of the window tail
  "HAR", "LLTC", "CVC", "HOT", "PCP", "BRCM"
)

res <- as.data.table(read_parquet("cache/lookups/oldcik_cik_resolution.parquet"))[verdict == "RESOLVED"]
ff  <- list.files("cache/fundamentals")

verify_cache <- function(tk) {
  # returns list(ok, why, px, mcap, from, to)
  ph <- list.files("cache/prices",
                   pattern = sprintf("^%s_(yahoo|tiingo)_", tk),
                   full.names = TRUE)
  if (!length(ph)) return(list(ok = FALSE, why = "no-cache"))
  d <- as.data.table(read_parquet(ph[1]))
  dc <- intersect(c("date", "Date"), names(d))[1]
  d[, date := as.Date(get(dc))]
  cc <- grep("Close", names(d), value = TRUE)
  cc <- setdiff(cc, grep("Adjusted", cc, value = TRUE))[1]
  wf <- as.Date(res[ticker == tk, win_from][1])
  wt <- as.Date(res[ticker == tk, win_to][1])
  idx <- which(d$date <= wt & d$date >= wf)
  if (!length(idx)) return(list(ok = FALSE, why = "no-window-overlap",
                                from = min(d$date), to = max(d$date)))
  px <- d[[cc]][max(idx)]; pdt <- d$date[max(idx)]
  fh <- ff[endsWith(ff, paste0("_", tk, ".parquet"))]
  # shared-CIK names keep fundamentals under the sibling suffix
  if (!length(fh) && tk %in% names(.FUND_CACHE_ALIASES)) {
    fh <- ff[endsWith(ff, paste0("_", .FUND_CACHE_ALIASES[[tk]], ".parquet"))]
  }
  mcap <- NA_real_
  if (length(fh)) {
    fd <- as.data.table(read_parquet(file.path("cache/fundamentals", fh[1])))
    sh <- fd[concept == "shares_outstanding" & as.Date(filed) <= pdt + 200 &
             value > 1e6]
    if (nrow(sh)) { sh <- sh[order(-as.Date(filed))]; mcap <- px * sh$value[1] }
  }
  bad <- px < 2.0 | (!is.na(mcap) & (mcap < 2e8 | mcap > 3e12))
  cov_head <- min(d$date) <= (wf + 45)
  list(ok = !bad, why = if (bad) "implausible" else
       if (cov_head) "full" else "tail-only",
       px = px, mcap = mcap, from = min(d$date), to = max(d$date))
}

for (tk in REPLACE) {
  for (f in list.files("cache/prices",
                       pattern = sprintf("^%s_(yahoo|tiingo)_", tk),
                       full.names = TRUE)) {
    file.remove(f)
    message("removed stale cache: ", basename(f))
  }
  # a stale splits cache derived from the reused symbol must go too
  sf <- file.path("cache/splits", paste0(tk, ".parquet"))
  if (file.exists(sf)) { file.remove(sf); message("removed splits: ", tk) }
}

summary_rows <- list()
for (tk in FETCH) {
  v0 <- verify_cache(tk)
  if (isTRUE(v0$ok)) {
    message(sprintf("%-6s cached+verified (%s)", tk, v0$why))
    summary_rows[[length(summary_rows) + 1]] <- data.table(
      ticker = tk, outcome = paste0("cached-", v0$why))
    next
  }
  px <- tryCatch(fetch_ticker_prices(tk), error = function(e) NULL)
  v <- verify_cache(tk)
  outcome <- if (is.null(px)) "fetch-failed"
    else if (isTRUE(v$ok)) paste0("fetched-", v$why)
    else paste0("BAD-", v$why)
  message(sprintf("%-6s %s  px=%s mcap=%s range=%s..%s", tk, outcome,
                  if (is.null(v$px)) "NA" else round(v$px, 2),
                  if (is.null(v$mcap) || is.na(v$mcap)) "NA" else signif(v$mcap, 3),
                  if (is.null(v$from)) "" else format(v$from),
                  if (is.null(v$to)) "" else format(v$to)))
  summary_rows[[length(summary_rows) + 1]] <- data.table(
    ticker = tk, outcome = outcome)
}
s <- rbindlist(summary_rows)
message("\n==== price fetch summary ====")
print(s[, .N, by = outcome][order(-N)])
print(s[grepl("BAD|failed", outcome)])
