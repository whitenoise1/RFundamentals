# Wave P phase 0: Tiingo coverage probe for Yahoo-purged names.
# Design: docs/DESIGN_SECOND_PRICE_SOURCE.md. Run BEFORE any pipeline code.
#
# Usage: TIINGO_TOKEN=... Rscript tools/probe_tiingo_coverage.R
#
# For each candidate ticker, reports: available date range, row count,
# split events, first/last raw close, and whether the range covers the
# constituent gap window. GO/NO-GO is decided per name from this output.

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(data.table)
})

.TOKEN <- Sys.getenv("TIINGO_TOKEN")
if (.TOKEN == "") stop("Set TIINGO_TOKEN (free account at tiingo.com)")

# Gap census 2026-07-08 (docs/DESIGN_SECOND_PRICE_SOURCE.md section 1):
# ticker -> gap window observed in snapshots (first..last NA date).
.CANDIDATES <- list(
  COG  = c("2010-03-31", "2026-06-30"), K    = c("2010-03-31", "2026-06-30"),
  VIAC = c("2010-03-31", "2026-06-30"), HES  = c("2010-03-31", "2025-06-30"),
  IPG  = c("2011-03-31", "2026-06-30"), JNPR = c("2010-03-31", "2025-06-30"),
  MRO  = c("2010-03-31", "2024-09-30"), CTXS = c("2010-03-31", "2022-09-30"),
  SEE  = c("2011-03-31", "2023-09-30"), GPS  = c("2010-03-31", "2021-12-31"),
  CERN = c("2011-03-31", "2022-03-31"), WBA  = c("2015-12-31", "2026-06-30"),
  XLNX = c("2011-06-30", "2021-12-31"), HOLX = c("2016-03-31", "2026-06-30"),
  NLSN = c("2013-09-30", "2022-09-30"), PXD  = c("2015-03-31", "2024-03-31"),
  KSU  = c("2013-06-30", "2021-09-30"), ANSS = c("2017-06-30", "2025-06-30"),
  ATVI = c("2015-09-30", "2023-09-30"), WRK  = c("2018-12-31", "2026-06-30"),
  HBI  = c("2015-03-31", "2021-09-30"), FBHS = c("2016-06-30", "2022-09-30"),
  DISH = c("2017-03-31", "2023-03-31"), CMA  = c("2019-03-31", "2024-03-31"),
  DRE  = c("2017-09-30", "2022-09-30"), CDAY = c("2021-09-30", "2026-06-30"),
  DISCA= c("2010-03-31", "2022-03-31"), ABMD = c("2018-06-30", "2022-09-30"),
  TWTR = c("2018-06-30", "2022-09-30"), CTLT = c("2020-09-30", "2024-09-30"),
  SIVB = c("2019-03-31", "2022-12-31"), MXIM = c("2018-12-31", "2021-06-30"),
  DFS  = c("2025-03-31", "2025-03-31"),
  # ticker-reuse cases: Tiingo history under these symbols may be the NEW
  # entity; compare the range against the gap window before trusting.
  BTU  = c("2010-03-31", "2014-06-30"),
  INFO = c("2017-06-30", "2021-12-31")
)

.probe <- function(tk) {
  url <- sprintf(
    "https://api.tiingo.com/tiingo/daily/%s/prices?startDate=2009-01-01&token=%s",
    tolower(tk), .TOKEN)
  resp <- tryCatch(
    httr::GET(url, httr::timeout(30)),
    error = function(e) NULL)
  if (is.null(resp) || httr::status_code(resp) != 200) {
    return(data.table(ticker = tk, status = sprintf(
      "HTTP %s", if (is.null(resp)) "ERR" else httr::status_code(resp))))
  }
  dat <- tryCatch(
    as.data.table(jsonlite::fromJSON(
      httr::content(resp, as = "text", encoding = "UTF-8"))),
    error = function(e) NULL)
  if (is.null(dat) || nrow(dat) == 0) {
    return(data.table(ticker = tk, status = "EMPTY"))
  }
  dat[, date := as.Date(substr(date, 1, 10))]
  win <- as.Date(.CANDIDATES[[tk]])
  covered <- min(dat$date) <= win[1] + 95 & max(dat$date) >= win[2] - 95
  data.table(
    ticker      = tk,
    status      = if (covered) "GO" else "PARTIAL",
    from        = min(dat$date),
    to          = max(dat$date),
    n           = nrow(dat),
    n_splits    = if ("splitFactor" %in% names(dat)) sum(dat$splitFactor != 1) else NA_integer_,
    first_close = dat[which.min(date), close],
    last_close  = dat[which.max(date), close],
    gap_from    = win[1],
    gap_to      = win[2]
  )
}

res <- list()
for (tk in names(.CANDIDATES)) {
  res[[tk]] <- .probe(tk)
  message(sprintf("%-5s %s", tk, res[[tk]]$status[1]))
  Sys.sleep(0.5)
}
out <- rbindlist(res, fill = TRUE)
print(out, nrows = 50)

if (!dir.exists("cache/lookups")) dir.create("cache/lookups", recursive = TRUE)
arrow::write_parquet(out, "cache/lookups/tiingo_coverage_probe.parquet")
message("\nwritten: cache/lookups/tiingo_coverage_probe.parquet")
message("Verify last_close for acquisition names against known final prints")
message("(e.g. ATVI ~94-95 cash 2023-10, TWTR ~53.7-54.2 2022-10, CERN ~94.9 2022-06)")
