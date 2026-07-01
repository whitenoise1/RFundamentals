# Read-only e2e verification of the split-adjusted pe_ttm fix. Computes the NEW
# eps_ttm/pe_ttm from real cache data WITHOUT writing parquets, and prints
# before(DB)/after(fix) at key dates for CMG (split), INTC (TTM loss), KO (div payer).
suppressPackageStartupMessages({ library(data.table); library(arrow) })
source("R/ttm_eps.R")

fund_dir <- "cache/fundamentals"; ts_dir <- "cache/timeseries"; split_dir <- "cache/splits"
fund_files <- list.files(fund_dir, pattern = "\\.parquet$", full.names = TRUE)
path_by_tk <- stats::setNames(fund_files, sub("^[0-9]+_(.*)\\.parquet$", "\\1", basename(fund_files)))

new_series <- function(tk) {
  fp <- unname(path_by_tk[tk]); fd <- as.data.table(read_parquet(fp))
  splits <- load_ticker_splits(tk, split_dir, fetch = TRUE)
  ttm <- build_ttm_eps_series(fd, splits)
  d <- as.data.table(read_parquet(file.path(ts_dir, paste0(tk, "_daily.parquet"))))
  d[, date := as.Date(date)]
  eps <- .pit_eps_ttm(ttm, d$date)
  pe  <- ifelse(!is.na(d$price) & !is.na(eps) & eps >= 0.01, d$price / eps, NA_real_)
  list(splits = splits, dt = data.table(date = d$date, price = d$price,
       eps_ttm_new = eps, pe_ttm_new = pe))
}
show <- function(tk, dates) {
  cat(sprintf("\n===== %s =====\n", tk))
  r <- new_series(tk)
  cat("splits:\n"); print(r$splits)
  print(r$dt[date %in% as.Date(dates)])
  cat(sprintf("post-2024-07 median pe_ttm_new: %.1f | min eps_ttm_new: %.3f | any neg pe: %s\n",
      median(r$dt[date >= as.Date("2024-07-01"), pe_ttm_new], na.rm = TRUE),
      min(r$dt$eps_ttm_new, na.rm = TRUE),
      any(r$dt$pe_ttm_new < 0, na.rm = TRUE)))
}

show("CMG",  c("2024-06-20","2025-06-10","2026-01-05","2026-04-13"))
show("INTC", c("2025-06-10","2026-04-13"))
show("KO",   c("2015-06-15","2024-06-14","2026-04-13"))
cat("\n(DB-current for comparison: CMG 2024-06-20 pe_ttm=1.45; INTC latest=-1086; gate wants CMG 10-120x, no neg)\n")
