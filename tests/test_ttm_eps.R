# Unit tests for ttm_eps.R — TTM diluted-EPS de-cumulation + PIT mapping.
# Standalone (synthetic fixture, no cache). Usage: Rscript tests/test_ttm_eps.R
suppressMessages(library(data.table))
source("R/ttm_eps.R")

np <- 0L; nf <- 0L
ok <- function(cond, name) {
  if (isTRUE(cond)) { np <<- np + 1L; cat(sprintf("  PASS: %s\n", name)) }
  else { nf <<- nf + 1L; cat(sprintf("  FAIL: %s\n", name)) }
}
approx <- function(a, b, tol = 1e-6) !is.na(a) && !is.na(b) && abs(a - b) < tol

# --- synthetic XBRL long frame: two fiscal years of cumulative YTD diluted EPS -
# FY1 (start 2022-01-01): Q1=1.0, Q2(H1)=2.0, Q3(9m)=3.0, FY=4.0
# FY2 (start 2023-01-01): Q1=1.2, Q2=2.4, Q3=3.6, FY=5.0
mk <- function(ps, pe, val, filed, form = "10-Q")
  data.table(concept = "eps_diluted", period_start = ps, period_end = pe,
             value = val, filed = filed, form = form)
fd <- rbindlist(list(
  mk("2022-01-01","2022-04-01",1.0,"2022-05-01"),
  mk("2022-01-01","2022-07-01",2.0,"2022-08-01"),
  mk("2022-01-01","2022-10-01",3.0,"2022-11-01"),
  mk("2022-01-01","2022-12-31",4.0,"2023-02-15","10-K"),
  mk("2023-01-01","2023-04-01",1.2,"2023-05-01"),
  mk("2023-01-01","2023-07-01",2.4,"2023-08-01"),
  mk("2023-01-01","2023-10-01",3.6,"2023-11-01"),
  mk("2023-01-01","2023-12-31",5.0,"2024-02-15","10-K")
))

r <- build_ttm_eps_series(fd)
cat("\n=== build_ttm_eps_series (de-cumulation) ===\n")
ok(!is.null(r) && nrow(r) == 5, "emits 5 TTM points (FY1 annual + FY2 Q1/Q2/Q3/FY)")
get <- function(d) r[as.character(qend) == d, eps_ttm]
ok(approx(get("2022-12-31"), 4.0), "FY1 year-end TTM = annual (4.0)")
ok(approx(get("2023-04-01"), 4.2), "FY2 Q1 TTM = 1.2 + 4.0 - 1.0 = 4.2")
ok(approx(get("2023-07-01"), 4.4), "FY2 Q2 TTM = 2.4 + 4.0 - 2.0 = 4.4")
ok(approx(get("2023-10-01"), 4.6), "FY2 Q3 TTM = 3.6 + 4.0 - 3.0 = 4.6")
ok(approx(get("2023-12-31"), 5.0), "FY2 year-end TTM = annual (5.0)")

cat("\n=== .pit_eps_ttm (point-in-time by filed_date) ===\n")
# NB: FY1's interim quarters emit no TTM point (no prior-year anchor), so the
# earliest available TTM is the FY1 10-K (filed 2023-02-15) — before that, NA.
dts <- as.Date(c("2023-01-01","2023-03-01","2023-05-15","2023-12-15","2024-03-01"))
ev <- .pit_eps_ttm(r, dts)
ok(is.na(ev[1]), "as-of 2023-01-01 -> NA (FY1 10-K not yet filed; no interim anchor)")
ok(approx(ev[2], 4.0), "as-of 2023-03-01 -> FY1 annual 4.0 (10-K filed 2023-02-15)")
ok(approx(ev[3], 4.2), "as-of 2023-05-15 -> FY2 Q1 4.2 (Q1 filed 2023-05-01)")
ok(approx(ev[4], 4.6), "as-of 2023-12-15 -> FY2 Q3 4.6 (Q3 filed, FY not yet)")
ok(approx(ev[5], 5.0), "as-of 2024-03-01 -> FY2 annual 5.0 (10-K filed 2024-02-15)")

cat("\n=== edge cases ===\n")
ok(all(is.na(.pit_eps_ttm(NULL, dts))), "NULL series -> all NA")
ok(all(.pit_eps_ttm(r, as.Date("2020-01-01")) %in% NA), "date before any filing -> NA")
single <- build_ttm_eps_series(fd[period_start == "2022-01-01"])
ok(is.null(single) || nrow(single) == 1, "single fiscal year -> only its FY-end TTM (no interim anchor)")

cat("\n=== split adjustment (CMG-style 50:1 mid-stream) ===\n")
# .split_factor: as-filed value -> current basis (product of ratios, ex_date > filed)
sp <- data.table(ex_date = as.Date("2023-03-01"), ratio = 0.02)   # 50:1 split
ok(approx(.split_factor(as.Date("2023-02-15"), sp), 0.02), "split_factor: filed before split -> 0.02 (1/50)")
ok(approx(.split_factor(as.Date("2023-05-01"), sp), 1.0),  "split_factor: filed after split -> 1")
ok(approx(.split_factor(as.Date(NA), sp), 1.0),            "split_factor: NA filed -> 1")
ok(approx(.split_factor(as.Date("2022-05-01"), NULL), 1.0),"split_factor: no splits -> 1")

# Same FY1/FY2 economics, but FY1 filed PRE-split (values x50) and FY2 filed POST-split.
# After normalization the series must equal the no-split fixture (4.0/4.2/4.4/4.6/5.0).
fd_sp <- rbindlist(list(
  mk("2022-01-01","2022-04-01", 50,  "2022-05-01"),            # pre-split (x50)
  mk("2022-01-01","2022-07-01", 100, "2022-08-01"),
  mk("2022-01-01","2022-10-01", 150, "2022-11-01"),
  mk("2022-01-01","2022-12-31", 200, "2023-02-15","10-K"),     # pre-split 10-K
  mk("2023-01-01","2023-04-01", 1.2, "2023-05-01"),            # post-split filings
  mk("2023-01-01","2023-07-01", 2.4, "2023-08-01"),
  mk("2023-01-01","2023-10-01", 3.6, "2023-11-01"),
  mk("2023-01-01","2023-12-31", 5.0, "2024-02-15","10-K")
))
rs <- build_ttm_eps_series(fd_sp, sp)
gs <- function(d) rs[as.character(qend) == d, eps_ttm]
ok(approx(gs("2022-12-31"), 4.0), "split: FY1 annual normalized to current basis (200*0.02 = 4.0)")
ok(approx(gs("2023-04-01"), 4.2), "split: FY2 Q1 TTM spans split, no blend (1.2 + 4.0 - 1.0 = 4.2)")
ok(approx(gs("2023-10-01"), 4.6), "split: FY2 Q3 TTM consistent basis (4.6)")
ok(approx(gs("2023-12-31"), 5.0), "split: FY2 annual (5.0)")

# Bug repro: WITHOUT split adjustment the Q1 TTM blends pre+post-split (1.2+200-50=151.2)
rb <- build_ttm_eps_series(fd_sp, NULL)
ok(rb[as.character(qend) == "2023-04-01", eps_ttm] > 100,
   "split: WITHOUT adjustment FY2 Q1 TTM blends bases (>100) -- the original CMG bug")

cat(sprintf("\nResults: %d PASS, %d FAIL\n", np, nf))
if (nf > 0L) quit(status = 1L)
