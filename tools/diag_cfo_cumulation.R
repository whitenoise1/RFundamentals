#!/usr/bin/env Rscript
# -- Diagnostic: quarterly CFO cumulation in cached fundamentals --
# For each sampled ticker, inspects rows where concept == "operating_cashflow"
# and classifies each quarterly observation as:
#   "3mo"  -> period_days in [60, 120]   (standalone quarter)
#   "YTD"  -> period_days in [160, 380]  (cumulative year-to-date)
#   "other" otherwise
# Reports:
#   1. For each ticker: count of 3mo vs YTD vs other, by fiscal_qtr (Q1-Q4, FY).
#   2. Whether the ticker is "consistent" (all quarterly rows are one type)
#      or "mixed" (a ticker has e.g. Q2 as YTD but Q3 as 3mo).
#   3. Cross-sample summary.

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

cache_dir <- "cache/fundamentals"

# Top ~15 mega caps known to be in cache
top_tickers <- c("AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
                 "LLY", "JPM", "V", "XOM", "UNH",
                 "MA", "AVGO", "HD", "WMT", "JNJ")

# All cached files, then pick 15 random from the non-top set
all_files  <- list.files(cache_dir, pattern = "\\.parquet$", full.names = FALSE)
all_tkrs   <- sub("^[0-9]+_", "", sub("\\.parquet$", "", all_files))
non_top    <- setdiff(all_tkrs, top_tickers)
set.seed(42)
rand_tickers <- sample(non_top, 15)

sample_tickers <- c(top_tickers, rand_tickers)

# --- Per-ticker diagnostic ---
diag_one <- function(ticker) {
  file <- list.files(cache_dir, pattern = paste0("_", ticker, "\\.parquet$"),
                     full.names = TRUE)
  if (length(file) == 0) return(NULL)
  dt <- as.data.table(read_parquet(file[1]))
  if (nrow(dt) == 0) return(NULL)

  dt <- dt[concept == "operating_cashflow"]
  if (nrow(dt) == 0) return(NULL)

  dt[, period_days := as.integer(period_end - period_start)]
  dt[, kind := fifelse(
    is.na(period_days), "instant",
    fifelse(period_days >= 60  & period_days <= 120, "3mo",
    fifelse(period_days >= 160 & period_days <= 200, "2Q_YTD",
    fifelse(period_days >= 250 & period_days <= 290, "3Q_YTD",
    fifelse(period_days >= 330 & period_days <= 380, "FY",     "other"))))
  )]
  dt[, ticker := ticker]
  dt[, .(ticker, fiscal_year, fiscal_qtr, period_start, period_end,
         period_days, kind, form, tag, value)]
}

rows <- rbindlist(lapply(sample_tickers, diag_one), use.names = TRUE, fill = TRUE)
setorder(rows, ticker, period_end, fiscal_qtr)

# --- Summary 1: per-ticker x fiscal_qtr x kind ---
summ <- rows[, .N, by = .(ticker, fiscal_qtr, kind)]
summ_wide <- dcast(summ, ticker + fiscal_qtr ~ kind, value.var = "N", fill = 0L)

cat("\n=== Per ticker x fiscal_qtr, count of period-length kinds ===\n")
print(summ_wide, row.names = FALSE)

# --- Summary 2: for Q2/Q3 specifically (the YTD-vs-3mo contested cells) ---
cat("\n=== Q2 / Q3 rows only: what survived dedup ===\n")
q23 <- rows[fiscal_qtr %in% c("Q2", "Q3")]
q23_summ <- q23[, .N, by = .(ticker, fiscal_qtr, kind)]
q23_wide <- dcast(q23_summ, ticker + fiscal_qtr ~ kind,
                  value.var = "N", fill = 0L)
print(q23_wide, row.names = FALSE)

# --- Summary 3: is each ticker "consistent" on Q2/Q3? ---
cat("\n=== Per-ticker verdict on Q2/Q3 consistency ===\n")
ticker_verdict <- q23[, .(
  n_rows      = .N,
  n_3mo       = sum(kind == "3mo"),
  n_2Q_YTD    = sum(kind == "2Q_YTD"),
  n_3Q_YTD    = sum(kind == "3Q_YTD"),
  n_other     = sum(!(kind %in% c("3mo", "2Q_YTD", "3Q_YTD")))
), by = ticker]

ticker_verdict[, verdict := fifelse(
  n_3mo > 0 & (n_2Q_YTD + n_3Q_YTD) == 0, "all_3mo",
  fifelse(n_3mo == 0 & (n_2Q_YTD + n_3Q_YTD) > 0, "all_YTD",
          "MIXED"))]
print(ticker_verdict, row.names = FALSE)

cat("\n=== Cross-sample tally ===\n")
print(ticker_verdict[, .N, by = verdict])

# --- Summary 4: for an "all_YTD" ticker, show the effect on an SD(CFO/Assets) calc ---
cat("\n=== Illustrative impact: AAPL last 16 Q CFO values as stored ===\n")
aapl_cfo <- rows[ticker == "AAPL" & fiscal_qtr %in% c("Q1","Q2","Q3","Q4")]
setorder(aapl_cfo, period_end)
print(tail(aapl_cfo[, .(period_end, fiscal_qtr, period_days, kind,
                         value_bn = round(value/1e9, 2))], 16),
      row.names = FALSE)

invisible(NULL)
