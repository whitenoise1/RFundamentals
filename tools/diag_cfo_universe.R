#!/usr/bin/env Rscript
# -- Full-universe CFO cumulation sweep --
# Walks every cached parquet under cache/fundamentals/ and classifies each
# operating_cashflow row by period_days bucket, then rolls up to a per-ticker
# verdict. Output lists:
#   - global bucket distribution (flags any band not seen in the 60-ticker sample)
#   - fiscal_qtr x kind cross-tab (flags any new off-diagonal cell)
#   - per-ticker verdict tally and list of non-conformers
#   - tickers missing CFO rows entirely (ITT class)
# Writes a machine-readable summary to
# cache/lookups/cfo_universe_sweep_<YYYY-MM-DD>.parquet
# so subsequent sessions can diff it.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

cache_dir <- "cache/fundamentals"
out_dir   <- "cache/lookups"
sweep_date <- Sys.Date()
out_path <- file.path(out_dir, sprintf("cfo_universe_sweep_%s.parquet", sweep_date))

all_files <- list.files(cache_dir, pattern = "\\.parquet$", full.names = TRUE)
cat(sprintf("Sweeping %d cached ticker files...\n", length(all_files)))

# -- Bucket logic identical to diag_cfo_wide.R --
bucket <- function(d) {
  fifelse(is.na(d),                       "instant",
  fifelse(d <  60,                        "short",
  fifelse(d >= 60  & d <= 120,            "3mo",   # 60-120 covers 13-wk and 16-wk Q1
  fifelse(d >= 121 & d <= 140,            "4mo",
  fifelse(d >= 160 & d <= 200,            "2Q_YTD",
  fifelse(d >= 201 & d <= 249,            "7-8mo",
  fifelse(d >= 250 & d <= 290,            "3Q_YTD",
  fifelse(d >= 291 & d <= 329,            "10-11mo",
  fifelse(d >= 330 & d <= 380,            "FY",
                                          "other")))))))))
}

# Pattern classes known to 2026-04-20 (from docs/research/07_cfo_cumulation_issue.md)
KNOWN_BUCKETS <- c("instant", "short", "3mo", "4mo", "2Q_YTD",
                   "7-8mo", "3Q_YTD", "10-11mo", "FY", "other")

# In the 60-ticker sample, only 3mo / 2Q_YTD / 3Q_YTD / FY appeared. Any row
# in another bucket is a novel pattern that deserves inspection.
EXPECTED_BUCKETS_FROM_SAMPLE <- c("3mo", "2Q_YTD", "3Q_YTD", "FY")

diag_one <- function(f) {
  ticker <- sub("^[0-9]+_", "", sub("\\.parquet$", "", basename(f)))
  dt <- tryCatch(as.data.table(read_parquet(f)), error = function(e) NULL)
  if (is.null(dt) || nrow(dt) == 0) {
    return(list(ticker = ticker, rows = NULL, no_cfo = TRUE, n_cfo = 0L))
  }
  dt <- dt[concept == "operating_cashflow"]
  if (nrow(dt) == 0) {
    return(list(ticker = ticker, rows = NULL, no_cfo = TRUE, n_cfo = 0L))
  }
  dt[, period_days := as.integer(period_end - period_start)]
  dt[, kind := bucket(period_days)]
  dt[, ticker := ticker]
  list(
    ticker = ticker,
    rows = dt[, .(ticker, fiscal_year, fiscal_qtr, period_start, period_end,
                  period_days, kind, form, value)],
    no_cfo = FALSE,
    n_cfo = nrow(dt)
  )
}

res_list <- lapply(all_files, diag_one)

no_cfo_tickers <- vapply(res_list, function(r) if (r$no_cfo) r$ticker else NA_character_,
                         character(1))
no_cfo_tickers <- no_cfo_tickers[!is.na(no_cfo_tickers)]

rows <- rbindlist(lapply(res_list, function(r) r$rows), use.names = TRUE, fill = TRUE)

cat(sprintf("Tickers with CFO rows: %d\n", length(unique(rows$ticker))))
cat(sprintf("Tickers with zero CFO rows: %d\n", length(no_cfo_tickers)))
if (length(no_cfo_tickers) > 0) {
  cat(sprintf("  -> %s\n", paste(no_cfo_tickers, collapse = ", ")))
}
cat(sprintf("Total CFO rows: %d\n", nrow(rows)))

# -- 1. Global bucket distribution + novel-bucket flag --
cat("\n=== [1] Global bucket distribution ===\n")
bkt <- rows[, .N, by = kind][order(-N)]
print(bkt, row.names = FALSE)
novel_buckets <- setdiff(bkt$kind, EXPECTED_BUCKETS_FROM_SAMPLE)
if (length(novel_buckets) > 0) {
  cat(sprintf("  !! Novel buckets (not seen in 60-ticker sample): %s\n",
              paste(novel_buckets, collapse = ", ")))
}

# -- 2. fiscal_qtr x kind cross-tab --
cat("\n=== [2] fiscal_qtr x kind (rows across all tickers) ===\n")
m <- dcast(rows[, .N, by = .(fiscal_qtr, kind)],
           fiscal_qtr ~ kind, value.var = "N", fill = 0L)
print(m, row.names = FALSE)

# -- 3. Per-(ticker, fp) verdict --
dom <- rows[, .(
  n_rows  = .N,
  n_3mo   = sum(kind == "3mo"),
  n_YTD2  = sum(kind == "2Q_YTD"),
  n_YTD3  = sum(kind == "3Q_YTD"),
  n_FY    = sum(kind == "FY"),
  n_other = sum(!(kind %in% c("3mo","2Q_YTD","3Q_YTD","FY"))),
  kinds   = paste(sort(unique(kind)), collapse = "|")
), by = .(ticker, fiscal_qtr)]

classify_pair <- function(fp, k_3mo, k_Y2, k_Y3, k_FY, k_other) {
  if (is.na(fp) || fp == "")                       return("MISSING_FP")
  if (k_other > 0 && (k_3mo + k_Y2 + k_Y3 + k_FY) == 0) return("ALL_ANOMALOUS")
  if (fp == "FY" && k_FY > 0 && k_other == 0 &&
      k_3mo == 0 && k_Y2 == 0 && k_Y3 == 0)        return("OK_FY")
  if (fp == "Q1" && k_3mo > 0 &&
      k_Y2 == 0 && k_Y3 == 0 && k_FY == 0)         return("OK_Q1_3mo")
  if (fp == "Q2" && k_Y2 > 0 &&
      k_3mo == 0 && k_Y3 == 0 && k_FY == 0)        return("OK_Q2_YTD")
  if (fp == "Q3" && k_Y3 > 0 &&
      k_3mo == 0 && k_Y2 == 0 && k_FY == 0)        return("OK_Q3_YTD")
  if (fp == "Q4" && k_FY > 0)                      return("OK_Q4_asFY")
  "NONCONFORMING"
}
dom[, verdict := mapply(classify_pair, fiscal_qtr,
                         n_3mo, n_YTD2, n_YTD3, n_FY, n_other)]

cat("\n=== [3] Per-(ticker, fiscal_qtr) verdict tally ===\n")
print(dom[, .N, by = verdict][order(-N)], row.names = FALSE)

KNOWN_VERDICTS <- c("OK_FY","OK_Q1_3mo","OK_Q2_YTD","OK_Q3_YTD","OK_Q4_asFY",
                    "NONCONFORMING","ALL_ANOMALOUS","MISSING_FP")
novel_verdicts <- setdiff(unique(dom$verdict), KNOWN_VERDICTS)
if (length(novel_verdicts) > 0) {
  cat(sprintf("  !! Novel verdicts: %s\n", paste(novel_verdicts, collapse = ", ")))
}

# -- 4. Ticker-level overall verdict --
ticker_v <- dom[, .(
  anomalous = any(verdict == "ALL_ANOMALOUS"),
  noncon    = sum(verdict == "NONCONFORMING"),
  missing   = sum(verdict == "MISSING_FP"),
  ok        = sum(startsWith(verdict, "OK_"))
), by = ticker]
ticker_v[, overall := fifelse(anomalous, "ANOMALOUS_TICKER",
                       fifelse(noncon > 0 | missing > 0, "HAS_NONCONFORMING",
                                                         "CLEAN"))]

cat("\n=== [4] Ticker-level overall verdict ===\n")
tally <- ticker_v[, .N, by = overall][order(-N)]
print(tally, row.names = FALSE)
n_total <- nrow(ticker_v)
pct_clean <- round(100 * tally[overall == "CLEAN", N] / n_total, 1)
cat(sprintf("  CLEAN pct: %.1f%% (%d / %d)\n",
            pct_clean, tally[overall == "CLEAN", N], n_total))

if (tally[overall == "ANOMALOUS_TICKER", .N] > 0) {
  cat("\n  Anomalous tickers (TTM-leak class):\n")
  print(ticker_v[overall == "ANOMALOUS_TICKER", .(ticker, ok, noncon, missing)],
        row.names = FALSE)
}

if (tally[overall == "HAS_NONCONFORMING", .N] > 0) {
  hn <- ticker_v[overall == "HAS_NONCONFORMING"]
  cat(sprintf("\n  HAS_NONCONFORMING tickers (%d):\n", nrow(hn)))
  print(hn[, .(ticker, ok, noncon, missing)], row.names = FALSE)
}

# -- 5. Unusual forms in non-standard buckets --
cat("\n=== [5] Forms for non-standard buckets ===\n")
odd <- rows[kind %in% c("short","4mo","7-8mo","10-11mo","other")]
if (nrow(odd) > 0) {
  print(odd[, .N, by = .(kind, form)][order(kind, -N)], row.names = FALSE)
} else {
  cat("  (none)\n")
}

# -- 6. Persist sweep summary --
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
sweep_out <- ticker_v[, .(sweep_date = sweep_date, ticker, ok, noncon,
                          missing, overall)]
# Append no-CFO tickers (not in dom because they have zero operating_cashflow rows)
if (length(no_cfo_tickers) > 0) {
  sweep_out <- rbind(sweep_out,
                     data.table(sweep_date = sweep_date,
                                ticker = no_cfo_tickers,
                                ok = 0L, noncon = 0L, missing = 0L,
                                overall = "NO_CFO_ROWS"))
}
write_parquet(sweep_out, out_path)
cat(sprintf("\nWrote sweep summary: %s (%d rows)\n", out_path, nrow(sweep_out)))

# -- 7. Novelty gate for the session log --
cat("\n=== [6] Novelty gate ===\n")
if (length(novel_buckets) == 0 && length(novel_verdicts) == 0) {
  cat("PASS: no novel buckets or verdicts. Three-class model still complete.\n")
} else {
  cat("FAIL: novel patterns detected. Log in docs/research/07_cfo_cumulation_issue.md.\n")
}

invisible(NULL)
