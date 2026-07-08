# Unit tests for Wave P second price source (pit_assembler.R Tiingo
# adapter + provider symbol map). Standalone, no network.
# Usage: Rscript tests/test_price_fallback.R
suppressWarnings(suppressMessages({
  source("R/fundamental_fetcher.R")
  source("R/ttm_eps.R")
  source("R/indicator_compute.R")
  source("R/pit_assembler.R")
}))

np <- 0L; nf <- 0L
ok <- function(cond, name) {
  if (isTRUE(cond)) { np <<- np + 1L; cat(sprintf("  PASS: %s\n", name)) }
  else { nf <<- nf + 1L; cat(sprintf("  FAIL: %s\n", name)) }
}
approx <- function(a, b, tol = 1e-9) !is.na(a) && !is.na(b) && abs(a - b) < tol

cat("=== .provider_symbol ===\n")
ok(.provider_symbol("COG") == "CTRA", "COG -> CTRA")
ok(.provider_symbol("DISCA") == "WBD", "DISCA -> WBD")
ok(.provider_symbol("AAPL") == "AAPL", "unmapped passes through")
ok(.provider_symbol("BRK.B") == "BRK.B", "dot ticker unmapped (dash conversion is separate)")
ok(identical(.SPLITS_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)],
             .PROVIDER_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)]) ||
     all(.SPLITS_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)] ==
           .PROVIDER_SYMBOL_MAP[names(.PROVIDER_SYMBOL_MAP)]),
   "ttm_eps .SPLITS_SYMBOL_MAP in sync with .PROVIDER_SYMBOL_MAP")

cat("\n=== .price_cache_path resolution ===\n")
td <- file.path(tempdir(), "px_test"); dir.create(td, showWarnings = FALSE)
ok(basename(.price_cache_path("XXX", "2009-01-01", td)) == "XXX_yahoo_2009-01-01.parquet",
   "neither exists -> yahoo write target")
file.create(file.path(td, "XXX_tiingo_2009-01-01.parquet"))
ok(basename(.price_cache_path("XXX", "2009-01-01", td)) == "XXX_tiingo_2009-01-01.parquet",
   "tiingo file resolves when yahoo absent")
file.create(file.path(td, "XXX_yahoo_2009-01-01.parquet"))
ok(basename(.price_cache_path("XXX", "2009-01-01", td)) == "XXX_yahoo_2009-01-01.parquet",
   "yahoo preferred when both exist")
unlink(td, recursive = TRUE)

cat("\n=== .tiingo_convert: split filter + basis round trip ===\n")
# synthetic series: 100 raw close, a 2:1 split on day 3 (raw halves), a
# spinoff-style 1.065 factor on day 5 (must NOT become a split event)
dat <- data.table(
  date = as.Date("2020-01-01") + 0:5,
  open = c(100, 101, 50.5, 51, 51.5, 48.5),
  high = c(101, 102, 51, 52, 52, 49),
  low  = c(99, 100, 50, 50.5, 51, 48),
  close = c(100.5, 101.5, 50.75, 51.5, 51.75, 48.25),
  volume = c(1e6, 1.1e6, 2.2e6, 2.1e6, 2.0e6, 2.3e6),
  splitFactor = c(1, 1, 2, 1, 1.065, 1),
  divCash = c(0, 0, 0, 0, 3.5, 0)
)
dat[, adjClose := close * 0.9]   # arbitrary adjusted series (passthrough)
cv <- .tiingo_convert(dat, "TST")
ok(nrow(cv$splits) == 1 && cv$splits$ex_date == as.Date("2020-01-03") &&
     approx(cv$splits$ratio, 0.5),
   "one true split kept (2:1 -> ratio 0.5), spinoff 1.065 excluded")
ok(all(colnames(cv$px) == paste0("TST.", c("Open","High","Low","Close","Volume","Adjusted"))),
   "yahoo-style column names incl Adjusted, Close at position 4")
ok(approx(as.numeric(cv$px[2, 6]), 101.5 * 0.9), "Adjusted is adjClose passthrough")
# round trip: reader divides cached close by .split_factor -> raw close
cached <- as.numeric(cv$px[, 4])
recovered <- cached / .split_factor(as.Date(index(cv$px)), cv$splits)
ok(all(abs(recovered - dat$close) < 1e-9), "as-traded round trip exact on all days")
# pre-split day cached on today's basis: 100.5 * 0.5 = 50.25
ok(approx(cached[1], 50.25), "pre-split cached close is split-adjusted (Yahoo basis)")
ok(approx(cached[4], 51.5), "post-split cached close unadjusted")
# get_price_on_date recovers the as-traded print
ok(approx(get_price_on_date(cv$px, "2020-01-02", cv$splits), 101.5, 1e-6),
   "get_price_on_date recovers pre-split as-traded print")

cat("\n=== .is_true_split_factor ===\n")
ok(identical(.is_true_split_factor(c(2, 3, 1.5, 1.2, 0.5, 0.125)),
             rep(TRUE, 6)),
   "clean ratios kept: 2, 3, 3:2, 6:5, 1:2, 1:8")
ok(identical(.is_true_split_factor(c(1, 1.065, 1.032, 1.1)),
             rep(FALSE, 4)),
   "spinoff-style decimals and near-1 factors excluded")
ok(identical(.is_true_split_factor(c(0, -2, NA_real_)), rep(FALSE, 3)),
   "nonpositive and NA factors rejected (no log() poisoning)")

cat("\n=== .tiingo_convert: reverse split + no splitFactor column ===\n")
dat2 <- data.table(
  date = as.Date("2020-01-01") + 0:2,
  open = c(1, 8.1, 8.2), high = c(1.1, 8.3, 8.4), low = c(0.9, 8, 8.1),
  close = c(1, 8.2, 8.3), volume = c(9e6, 1e6, 1e6),
  splitFactor = c(1, 0.125, 1)
)
cv2 <- .tiingo_convert(dat2, "RVS")
ok(approx(cv2$splits$ratio, 8), "1:8 reverse split -> ratio 8")
ok(approx(as.numeric(cv2$px[1, 4]), 8), "pre-reverse-split close scaled up")
dat3 <- dat2[, .(date, open, high, low, close, volume)]
cv3 <- .tiingo_convert(dat3, "NSF")
ok(nrow(cv3$splits) == 0 && approx(as.numeric(cv3$px[1, 4]), 1),
   "missing splitFactor column -> no splits, close passthrough")

cat(sprintf("\n%d passed, %d failed\n", np, nf))
if (nf > 0) stop("test_price_fallback: failures")
