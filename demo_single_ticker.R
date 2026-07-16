# demo_single_ticker.R -- Daily indicator time series for a single ticker
#
# Usage:
#   source("demo_single_ticker.R")
#   dt <- demo_ticker("AAPL")
#   dt <- demo_ticker("AAPL", from = "2020-01-01")
#   dt <- demo_ticker("AAPL", families = "valuation")
#   dt <- demo_ticker("AAPL", families = c("valuation", "growth"))

source("R/fundamental_fetcher.R")
source("R/sector_classifier.R")
source("R/indicator_compute.R")

suppressPackageStartupMessages({ library(data.table); library(arrow); library(xts) })


demo_ticker <- function(ticker, from = "2010-01-01", families = NULL) {

  from <- as.Date(from)

  # -- Lookups (sector table is a dated SCD since D2/B; get_sector_lookup
  # returns the current one-row-per-ticker view) --
  master  <- as.data.table(read_parquet("cache/lookups/constituent_master.parquet"))
  sectors <- get_sector_lookup()
  tk <- ticker
  cik    <- master[ticker == tk]$cik[1]
  sector <- sectors[ticker == tk]$sector[1]
  if (is.na(cik)) stop(sprintf("%s not in constituent master.", tk))
  if (is.na(sector)) sector <- "Unknown"

  # -- Fundamentals: compute per-FY indicators + extract stubs --
  # allow_restated: this demo shows the latest restated view (RF-2 opt-in);
  # per-FY indicator computation applies its own as-of filing filter downstream.
  fund_dt <- get_fundamentals(ticker, cik, allow_restated = TRUE)
  if (is.null(fund_dt) || nrow(fund_dt) == 0) {
    message("  fetching from EDGAR...")
    fund_dt <- fetch_and_cache_ticker(ticker, cik)
  }
  if (is.null(fund_dt) || nrow(fund_dt) == 0) stop("No fundamental data.")

  wide <- pivot_fundamentals(fund_dt)
  wide <- .derive_quantities(wide)
  fy_rows <- wide[period_type == "FY"]
  if (nrow(fy_rows) == 0) stop("No annual filings.")

  fund_only_names <- setdiff(get_indicator_names(),
    c("pe_trailing","peg","pb","ps","pfcf","ev_ebitda","ev_revenue",
      "earnings_yield","dividend_yield","buyback_yield","market_cap",
      "enterprise_value"))

  fy_list <- list()
  for (fy in sort(unique(fy_rows$fiscal_year))) {
    curr <- fy_rows[fiscal_year == fy]
    filed <- as.Date(max(curr$filed, na.rm = TRUE))
    ind <- tryCatch(compute_ticker_indicators(fund_dt, NA_real_, sector,
                                              target_fy = fy),
                    error = function(e) NULL)
    if (is.null(ind)) next
    fy_list[[length(fy_list) + 1L]] <- as.data.table(c(
      list(fiscal_year = as.integer(fy), filed_date = filed),
      as.list(ind[fund_only_names]),
      list(stub_shares = .col(curr[1], "shares_outstanding"),
           stub_eps    = .col(curr[1], "eps_diluted"),
           stub_equity = .col(curr[1], "stockholders_equity"),
           stub_rev    = .col(curr[1], "revenue"),
           stub_fcf    = .col(curr[1], "fcf"),
           stub_ebitda = .col(curr[1], "ebitda"),
           stub_td     = .col(curr[1], "total_debt"),
           stub_nd     = .col(curr[1], "net_debt"),
           stub_div    = .col(curr[1], "dividends_paid"),
           stub_buy    = .col(curr[1], "buybacks"))
    ))
  }
  fund <- rbindlist(fy_list, fill = TRUE)
  setorder(fund, -fiscal_year)

  # -- Prices from cache --
  pf <- sprintf("cache/prices/%s_yahoo_2009-01-01.parquet", ticker)
  if (!file.exists(pf)) stop("Price not cached. Run pit_assembler first.")
  pdf <- as.data.frame(read_parquet(pf))
  px  <- xts(pdf[, -1, drop = FALSE], order.by = as.Date(pdf$date))
  nc  <- ncol(px)
  pcol <- if (nc >= 6) 6 else if (nc >= 4) 4 else 1
  dates  <- as.Date(index(px))
  prices <- as.numeric(coredata(px[, pcol]))

  # Filter to requested range
  mask   <- dates >= from
  dates  <- dates[mask]
  prices <- prices[mask]

  # -- Point-in-time: map each day to its latest filed FY --
  fund_idx <- rep(NA_integer_, length(dates))
  for (k in seq_len(nrow(fund))) {
    eligible <- is.na(fund_idx) & dates >= fund$filed_date[k]
    fund_idx[eligible] <- k
  }
  valid <- !is.na(fund_idx) & !is.na(prices)
  dates <- dates[valid]; prices <- prices[valid]; fund_idx <- fund_idx[valid]

  # -- Assemble daily data.table --
  dt <- data.table(date = dates, price = prices)

  # Fundamental-only indicators: carry forward from filing
  for (col in fund_only_names) {
    if (col %in% names(fund)) set(dt, j = col, value = as.numeric(fund[[col]][fund_idx]))
    else set(dt, j = col, value = NA_real_)
  }

  # Price-sensitive indicators: recompute daily
  s <- function(nm) as.numeric(fund[[nm]][fund_idx])
  mc <- prices * s("stub_shares")
  nd <- s("stub_nd"); td <- s("stub_td")
  ev <- ifelse(!is.na(mc), ifelse(!is.na(nd), mc+nd, ifelse(!is.na(td), mc+td, NA)), NA)
  eps <- s("stub_eps"); eq <- s("stub_equity"); rev <- s("stub_rev")
  fcf <- s("stub_fcf"); ebitda <- s("stub_ebitda")
  div <- abs(s("stub_div")); buy <- abs(s("stub_buy"))
  eps_g <- dt$eps_growth_yoy

  set(dt, j = "market_cap",       value = mc)
  set(dt, j = "enterprise_value", value = ev)
  set(dt, j = "pe_trailing",      value = ifelse(abs(eps)>=0.01, prices/eps, NA))
  set(dt, j = "peg",              value = ifelse(!is.na(eps_g)&eps_g>0, (prices/eps)/(eps_g*100), NA))
  set(dt, j = "pb",               value = ifelse(!is.na(eq)&eq>0, mc/eq, NA))
  set(dt, j = "ps",               value = ifelse(abs(rev)>=1e-9, mc/rev, NA))
  set(dt, j = "pfcf",             value = ifelse(!is.na(fcf)&fcf>0, mc/fcf, NA))
  set(dt, j = "ev_ebitda",        value = ifelse(!is.na(ebitda)&ebitda>0, ev/ebitda, NA))
  set(dt, j = "ev_revenue",       value = ifelse(abs(rev)>=1e-9, ev/rev, NA))
  set(dt, j = "earnings_yield",   value = ifelse(abs(prices)>=0.01, eps/prices, NA))
  set(dt, j = "dividend_yield",   value = ifelse(abs(mc)>=1e-9, div/mc, NA))
  set(dt, j = "buyback_yield",    value = ifelse(abs(mc)>=1e-9, buy/mc, NA))

  # Reorder columns: date, price, then canonical indicator order
  ind_names <- get_indicator_names()
  setcolorder(dt, c("date", "price", intersect(ind_names, names(dt))))

  # Family filter
  if (!is.null(families)) {
    keep <- indicator_names(families)
    drop <- setdiff(ind_names, keep)
    for (col in drop) set(dt, j = col, value = NULL)
    ind_names <- keep
  }

  message(sprintf("%s: [%d days x %d indicators]  %s to %s",
                  ticker, nrow(dt), length(ind_names),
                  min(dt$date), max(dt$date)))
  dt[]
}


if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  tk   <- if (length(args) >= 1) toupper(args[1]) else "AAPL"
  from <- if (length(args) >= 2) args[2] else "2010-01-01"
  dt <- demo_ticker(tk, from)
  print(head(dt, 5))
  print(tail(dt, 5))
}
