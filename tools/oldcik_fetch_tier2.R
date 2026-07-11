# Old-CIK wave Tier 2: batch-fetch fundamentals for the resolved census
# names + the three old-entity windows (21CF serving NWSA/FOXA/FOX pre-
# split dates; old CCE; old Express Scripts).
#
# Shared-CIK names whose cache already exists under the current-ticker
# sibling (ACE->CB, TYC->JCI, KFT->MDLZ, ...) are EXCLUDED: the loader's
# CIK-prefix glob serves them without a fetch (Tier 1 mechanism).
#
# Resumable: fetch_fundamentals_batch skips existing cache files.
# Post-fetch: cover-share/EPS coverage scan to triage Wave-M-symptom
# multi-class filers (companyfacts drops dimensional facts).
# Usage: Rscript tools/oldcik_fetch_tier2.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })
source("R/fundamental_fetcher.R")

res <- as.data.table(read_parquet("cache/lookups/oldcik_cik_resolution.parquet"))
res <- res[verdict == "RESOLVED"]

# exclude names already served by an existing cache file for the same CIK
files <- list.files("cache/fundamentals")
res[, cache_file := vapply(cik, function(ck) {
  h <- files[startsWith(files, paste0(ck, "_"))]
  if (length(h)) h[1] else NA_character_
}, character(1))]
alias <- res[!is.na(cache_file)]
fetch <- res[is.na(cache_file)]
message(sprintf("tier2 fetch: %d to fetch, %d served by existing caches (%s)",
                nrow(fetch), nrow(alias),
                paste(alias$ticker, collapse = ",")))

# de-dup within the fetch list itself (TSO/ANDV share CIK 50104: fetch
# once under the earlier/longer window ticker)
setorder(fetch, cik, win_from)
dup <- duplicated(fetch$cik)
if (any(dup)) {
  message("shared-CIK within fetch list, fetching once: ",
          paste(sprintf("%s->%s", fetch$ticker[dup],
                        fetch$ticker[match(fetch$cik[dup], fetch$cik)]),
                collapse = ", "))
  fetch <- fetch[!dup]
}

tickers <- fetch$ticker
ciks    <- fetch$cik

# old-entity windows (task 2): 21CF cached under the non-roster suffix
# TFCFA (its final ticker) so the daily-layer keying never collides with
# the live FOXA layer; old CCE / old Express Scripts cache under their
# own tickers with distinct CIK prefixes.
old_ents <- data.table(
  ticker = c("TFCFA", "CCE", "ESRX"),
  cik    = c("0001308161", "0000804055", "0000885721"))
tickers <- c(tickers, old_ents$ticker)
ciks    <- c(ciks, old_ents$cik)

message(sprintf("fetching %d (ticker, CIK) pairs", length(tickers)))
res_batch <- fetch_fundamentals_batch(tickers, ciks,
                                      cache_dir = "cache/fundamentals",
                                      chunk_size = 25L)

# ---- multiclass triage: which fetched names lack cover shares / EPS ----
message("\n==== multiclass triage (missing shares/eps in window) ====")
suspects <- list()
for (i in seq_along(tickers)) {
  tk <- tickers[i]; ck <- ciks[i]
  f <- file.path("cache/fundamentals", sprintf("%s_%s.parquet", ck, tk))
  if (!file.exists(f)) next
  dt <- as.data.table(read_parquet(f))
  n_sh  <- dt[concept == "shares_outstanding", .N]
  n_eps <- dt[concept == "eps_diluted", .N]
  n_rev <- dt[concept == "revenue", .N]
  if (n_rev > 0 && (n_sh == 0 || n_eps == 0)) {
    suspects[[length(suspects) + 1L]] <- data.table(
      ticker = tk, cik = ck, n_shares = n_sh, n_eps = n_eps, n_rev = n_rev)
  }
}
if (length(suspects)) {
  print(rbindlist(suspects))
} else message("none -- every fetched cache carries shares and EPS")

message(sprintf("\nDONE: %d success, %d failed (%s)",
                length(res_batch$success), length(res_batch$failed),
                paste(res_batch$failed, collapse = ",")))
