# Old-CIK wave Tier 2: reused-symbol poison audit over price caches.
# Yahoo purges dead symbols; when a symbol is reused, a blind fetch caches
# the WRONG company's prices under a recovered constituent's ticker (found
# live: COL_yahoo = a $0.15 penny stock overlapping Rockwell Collins'
# window). Flag every cache whose in-window close is implausible for an
# S&P 500 member: price < $2.50 or implied market cap outside [3e8, 3e12].
# Usage: Rscript tools/oldcik_audit_price_caches.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })

r  <- as.data.table(read_parquet("cache/lookups/oldcik_cik_resolution.parquet"))[verdict == "RESOLVED"]
pf <- list.files("cache/prices")
ff <- list.files("cache/fundamentals")

out <- list()
for (i in seq_len(nrow(r))) {
  tk <- r$ticker[i]; wt <- as.Date(r$win_to[i]); wf <- as.Date(r$win_from[i])
  ph <- pf[grepl(sprintf("^%s_(yahoo|tiingo)_", tk), pf)]
  if (!length(ph)) next
  d <- as.data.table(read_parquet(file.path("cache/prices", ph[1])))
  dc <- intersect(c("date", "Date"), names(d))[1]
  d[, date := as.Date(get(dc))]
  cc <- grep("Close", names(d), value = TRUE)
  cc <- setdiff(cc, grep("Adjusted", cc, value = TRUE))[1]
  idx <- which(d$date <= wt & d$date >= wf)
  if (!length(idx)) {
    out[[length(out) + 1]] <- data.table(ticker = tk, file = ph[1],
      overlap = FALSE, px = NA_real_, mcap = NA_real_)
    next
  }
  px <- d[[cc]][max(idx)]; pdt <- d$date[max(idx)]
  fh <- ff[endsWith(ff, paste0("_", tk, ".parquet"))]
  mcap <- NA_real_
  if (length(fh)) {
    fd <- as.data.table(read_parquet(file.path("cache/fundamentals", fh[1])))
    sh <- fd[concept == "shares_outstanding" & as.Date(filed) <= pdt + 200]
    if (nrow(sh)) { sh <- sh[order(-as.Date(filed))]; mcap <- px * sh$value[1] }
  }
  out[[length(out) + 1]] <- data.table(ticker = tk, file = ph[1],
    overlap = TRUE, px = round(px, 2), mcap = signif(mcap, 3))
}
a <- rbindlist(out)
write_parquet(a, "cache/lookups/oldcik_price_cache_audit.parquet")

flag <- a[overlap == TRUE &
          (px < 2.5 | (!is.na(mcap) & (mcap < 3e8 | mcap > 3e12)))]
cat("caches with window overlap:", a[overlap == TRUE, .N],
    " flagged:", nrow(flag), "\n")
print(flag, nrows = 50)
cat("\nno-overlap caches (inert for the window):", a[overlap == FALSE, .N],
    "->", paste(a[overlap == FALSE, ticker], collapse = ","), "\n")
