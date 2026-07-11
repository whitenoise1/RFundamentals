# Old-CIK wave Tier 2: Tiingo coverage probe for resolved names whose
# price caches miss their membership window. For each name, probe the
# roster symbol AND (where the company merely renamed / re-tickered on a
# continuous share line) the candidate current symbol.
#
# Output: cache/lookups/oldcik_price_probe.parquet
# Usage: Rscript tools/oldcik_probe_prices.R

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(httr); library(jsonlite)
})

tok <- Sys.getenv("tiingo_token", Sys.getenv("TIINGO_TOKEN"))
if (!nzchar(tok)) { readRenviron("~/.Renviron"); tok <- Sys.getenv("tiingo_token", Sys.getenv("TIINGO_TOKEN")) }
stopifnot(nzchar(tok))

# candidate current-symbol chains (roster ticker -> provider symbol).
# Only same-share-line renames belong here (corporate mechanics verified
# per name); conversions/mergers with share RATIOS must NOT be mapped.
CHAIN <- c(
  ACE  = "CB",    # ACE Ltd renamed Chubb Ltd 2016, same shares
  KFT  = "MDLZ",  # Kraft Foods Inc renamed Mondelez 2012 (KRFT was the spinoff)
  TSO  = "ANDV",  # Tesoro renamed Andeavor 2017, same shares
  TYC  = "JCI",   # Tyco plc = surviving listed entity, renamed JCI plc 2016
  SAI  = "LDOS",  # SAIC Inc renamed Leidos 2013 (new SAIC was the spinoff)
  JEC  = "J",     # Jacobs Engineering ticker change 2019
  HFC  = "DINO",  # HollyFrontier -> HF Sinclair 2022, 1:1
  ADS  = "BFH",   # Alliance Data renamed Bread Financial 2022
  WPI  = "AGN",   # Watson -> Actavis (ACT) -> Allergan plc: continuous line
  CCE  = "CCEP",  # NEW CCE (2010-2016) -> Coca-Cola European Partners
  IGT  = "IGT"    # old IGT -> IGT plc 2015: ticker continued
)

probe <- function(sym) {
  r <- tryCatch(GET(sprintf("https://api.tiingo.com/tiingo/daily/%s", tolower(sym)),
                    add_headers(Authorization = paste("Token", tok)),
                    timeout(30)), error = function(e) NULL)
  Sys.sleep(0.25)
  if (is.null(r) || status_code(r) != 200) return(NULL)
  j <- tryCatch(fromJSON(content(r, as = "text", encoding = "UTF-8")),
                error = function(e) NULL)
  if (is.null(j) || is.null(j$startDate)) return(NULL)
  list(from = as.Date(j$startDate), to = as.Date(j$endDate),
       name = j$name %||% "")
}
`%||%` <- function(a, b) if (is.null(a)) b else a

res <- as.data.table(read_parquet("cache/lookups/oldcik_cik_resolution.parquet"))[verdict == "RESOLVED"]
pf <- list.files("cache/prices")

out <- list()
for (i in seq_len(nrow(res))) {
  tk <- res$ticker[i]
  wf <- as.Date(res$win_from[i]); wt <- as.Date(res$win_to[i])

  # current cache coverage
  hits <- pf[grepl(sprintf("^%s_(yahoo|tiingo)_", tk), pf)]
  cov <- FALSE
  if (length(hits)) {
    d <- as.data.table(read_parquet(file.path("cache/prices", hits[1])))
    dc <- intersect(c("date", "Date"), names(d))[1]
    rng <- range(as.Date(d[[dc]]))
    cov <- rng[1] <= (wf + 45) & rng[2] >= (wt - 45)
  }
  if (cov) {
    out[[length(out) + 1L]] <- data.table(ticker = tk, status = "CACHED",
      symbol = tk, t_from = as.Date(NA), t_to = as.Date(NA), t_name = "")
    next
  }

  # probe own symbol, then chain candidate
  best <- NULL; best_sym <- NA_character_
  for (sym in unique(c(tk, unname(CHAIN[tk])))) {
    if (is.na(sym)) next
    p <- probe(gsub("\\.", "-", sym))
    if (is.null(p)) next
    covers <- p$from <= (wf + 45) & p$to >= (wt - 45)
    if (covers) { best <- p; best_sym <- sym; break }
    if (is.null(best) || (p$from < best$from)) { best <- p; best_sym <- sym }
  }

  status <- if (is.null(best)) "NO_TIINGO"
    else if (best$from <= (wf + 45) & best$to >= (wt - 45)) "RECOVERABLE"
    else "PARTIAL_TIINGO"
  out[[length(out) + 1L]] <- data.table(ticker = tk, status = status,
    symbol = best_sym, t_from = if (is.null(best)) as.Date(NA) else best$from,
    t_to = if (is.null(best)) as.Date(NA) else best$to,
    t_name = if (is.null(best)) "" else best$name)
  message(sprintf("%-6s %s via %s %s..%s %s", tk, status, best_sym,
                  if (is.null(best)) "" else format(best$from),
                  if (is.null(best)) "" else format(best$to),
                  if (is.null(best)) "" else substr(best$t_name, 1, 30)))
}

final <- rbindlist(out)
final <- merge(final, res[, .(ticker, win_from, win_to)], by = "ticker")
write_parquet(final, "cache/lookups/oldcik_price_probe.parquet")
message("\n==== probe summary ====")
print(final[, .N, by = status])
