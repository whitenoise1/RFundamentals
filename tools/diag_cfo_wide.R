#!/usr/bin/env Rscript
# -- Wider CFO-pattern diagnostic (60 tickers) --
# Goal: enumerate every period-length pattern in cached operating_cashflow
# rows, not just confirm the two we already know (AAPL-class YTD, AMZN TTM).
# Classifies each quarterly row's period length into buckets:
#   instant, 3mo (60-110), 2Q_YTD (160-200), 3Q_YTD (250-290),
#   FY (330-380), TTM_off (other 12-month-ish), short (< 60), other
# Then, per ticker, computes:
#   - dominant_kind by fiscal_qtr
#   - number of rows per (fiscal_qtr, kind)
#   - flags anomalies: missing Q1, non-conforming fp vs kind,
#     duplicate fiscal_qtr with different kinds, etc.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

cache_dir <- "cache/fundamentals"

top_tickers <- c("AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
                 "LLY", "JPM", "V", "XOM", "UNH",
                 "MA", "AVGO", "HD", "WMT", "JNJ")

all_files <- list.files(cache_dir, pattern = "\\.parquet$", full.names = FALSE)
all_tkrs  <- sub("^[0-9]+_", "", sub("\\.parquet$", "", all_files))
non_top   <- setdiff(all_tkrs, top_tickers)
set.seed(42)
rand_tickers <- sample(non_top, 45)   # 15 top + 45 random = 60

sample_tickers <- c(top_tickers, rand_tickers)
cat(sprintf("Sampling %d tickers\n", length(sample_tickers)))

# Bucket assignment
bucket <- function(d) {
  fifelse(is.na(d), "instant",
  fifelse(d <  60,                    "short",
  fifelse(d >= 60  & d <= 110,        "3mo",
  fifelse(d >= 111 & d <= 140,        "4mo",    # fiscal-year boundary anomalies
  fifelse(d >= 160 & d <= 200,        "2Q_YTD",
  fifelse(d >= 201 & d <= 249,        "7-8mo",
  fifelse(d >= 250 & d <= 290,        "3Q_YTD",
  fifelse(d >= 291 & d <= 329,        "10-11mo",
  fifelse(d >= 330 & d <= 380,        "FY",
                                      "other")))))))))
}

diag_one <- function(ticker) {
  files <- list.files(cache_dir, pattern = paste0("_", ticker, "\\.parquet$"),
                      full.names = TRUE)
  if (length(files) == 0) return(NULL)
  dt <- as.data.table(read_parquet(files[1]))
  if (nrow(dt) == 0) return(NULL)

  dt <- dt[concept == "operating_cashflow"]
  if (nrow(dt) == 0) return(NULL)

  dt[, period_days := as.integer(period_end - period_start)]
  dt[, kind := bucket(period_days)]
  dt[, ticker := ticker]
  dt[, .(ticker, fiscal_year, fiscal_qtr, period_start, period_end,
         period_days, kind, form, value)]
}

rows <- rbindlist(lapply(sample_tickers, diag_one),
                  use.names = TRUE, fill = TRUE)

cat(sprintf("Total CFO rows across %d tickers: %d\n",
            length(unique(rows$ticker)), nrow(rows)))
cat(sprintf("Tickers with no CFO rows: %s\n",
            paste(setdiff(sample_tickers, unique(rows$ticker)),
                  collapse = ", ")))

# -- 1. Global bucket distribution --
cat("\n=== [1] Global bucket distribution ===\n")
print(rows[, .N, by = kind][order(-N)], row.names = FALSE)

# -- 2. By fiscal_qtr x kind (what the dedup produced) --
cat("\n=== [2] fiscal_qtr x kind (rows, across all tickers) ===\n")
m <- dcast(rows[, .N, by = .(fiscal_qtr, kind)],
           fiscal_qtr ~ kind, value.var = "N", fill = 0L)
print(m, row.names = FALSE)

# -- 3. Per-ticker dominant kind per fiscal_qtr --
dom <- rows[, .(n_rows = .N,
                n_3mo   = sum(kind == "3mo"),
                n_YTD2  = sum(kind == "2Q_YTD"),
                n_YTD3  = sum(kind == "3Q_YTD"),
                n_FY    = sum(kind == "FY"),
                n_other = sum(!(kind %in%
                                c("3mo","2Q_YTD","3Q_YTD","FY"))),
                kinds   = paste(sort(unique(kind)), collapse = "|")
               ),
             by = .(ticker, fiscal_qtr)]

# Classify each (ticker, fp)
classify_pair <- function(fp, k_3mo, k_Y2, k_Y3, k_FY, k_other) {
  if (is.na(fp) || fp == "") return("MISSING_FP")
  if (k_other > 0 && (k_3mo + k_Y2 + k_Y3 + k_FY) == 0) return("ALL_ANOMALOUS")
  if (fp == "FY" && k_FY > 0 && k_other == 0 &&
      k_3mo == 0 && k_Y2 == 0 && k_Y3 == 0)         return("OK_FY")
  if (fp == "Q1" && k_3mo > 0 &&
      k_Y2 == 0 && k_Y3 == 0 && k_FY == 0)          return("OK_Q1_3mo")
  if (fp == "Q2" && k_Y2 > 0 &&
      k_3mo == 0 && k_Y3 == 0 && k_FY == 0)         return("OK_Q2_YTD")
  if (fp == "Q3" && k_Y3 > 0 &&
      k_3mo == 0 && k_Y2 == 0 && k_FY == 0)         return("OK_Q3_YTD")
  if (fp == "Q4" && k_FY > 0)                       return("OK_Q4_asFY")
  "NONCONFORMING"
}
dom[, verdict := mapply(classify_pair, fiscal_qtr,
                         n_3mo, n_YTD2, n_YTD3, n_FY, n_other)]

cat("\n=== [3] Per-(ticker, fiscal_qtr) verdict tally ===\n")
print(dom[, .N, by = verdict][order(-N)], row.names = FALSE)

# -- 4. List all non-OK ticker-qtr pairs --
cat("\n=== [4] Non-conforming (ticker, fiscal_qtr) pairs ===\n")
nc <- dom[!(verdict %in% c("OK_FY","OK_Q1_3mo","OK_Q2_YTD",
                            "OK_Q3_YTD","OK_Q4_asFY"))]
print(nc, row.names = FALSE)

# -- 5. Ticker-level overall verdict --
ticker_v <- dom[, .(
  anomalous = any(verdict == "ALL_ANOMALOUS"),
  noncon    = sum(verdict == "NONCONFORMING"),
  ok        = sum(startsWith(verdict, "OK_"))
), by = ticker]
ticker_v[, overall := fifelse(anomalous, "ANOMALOUS_TICKER",
                       fifelse(noncon > 0, "HAS_NONCONFORMING",
                                           "CLEAN"))]

cat("\n=== [5] Ticker-level overall verdict ===\n")
print(ticker_v[, .N, by = overall], row.names = FALSE)
cat("\nAnomalous tickers:\n")
print(ticker_v[overall == "ANOMALOUS_TICKER", .(ticker, ok, noncon)],
      row.names = FALSE)
cat("\nTickers with some non-conforming (ticker, fp) pairs:\n")
print(ticker_v[overall == "HAS_NONCONFORMING",
                .(ticker, ok, noncon)], row.names = FALSE)

# -- 6. Forms seen in "other"/"short"/"4mo"/"7-8mo"/"10-11mo" rows --
cat("\n=== [6] Forms for non-standard buckets ===\n")
odd <- rows[kind %in% c("short","4mo","7-8mo","10-11mo","other")]
print(odd[, .N, by = .(kind, form)][order(kind, -N)], row.names = FALSE)

# -- 7. Missing-quarter diagnostic: which tickers lack Q1 entirely? --
cat("\n=== [7] Tickers missing an entire fiscal_qtr in CFO rows ===\n")
have <- rows[, .(qtrs = paste(sort(unique(fiscal_qtr)), collapse = ",")),
             by = ticker]
print(have[!grepl("Q1", qtrs) | !grepl("Q2", qtrs) |
           !grepl("Q3", qtrs) | !grepl("FY", qtrs)], row.names = FALSE)

# -- 8. Spot-check: pick up to 5 NONCONFORMING pairs and dump rows --
cat("\n=== [8] Sample rows from non-conforming (ticker, fp) pairs ===\n")
picks <- head(nc, 5)
for (i in seq_len(nrow(picks))) {
  tk <- picks$ticker[i]; fp <- picks$fiscal_qtr[i]
  cat(sprintf("\n-- %s / fp=%s --\n", tk, fp))
  print(rows[ticker == tk & fiscal_qtr == fp,
             .(fiscal_year, period_start, period_end, period_days,
               kind, form, value_bn = round(value/1e9, 2))],
        row.names = FALSE)
}

invisible(NULL)
