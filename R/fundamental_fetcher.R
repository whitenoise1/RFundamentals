# ============================================================================
# fundamental_fetcher.R  --  EDGAR XBRL Fundamental Fetcher (Module 2)
# ============================================================================
# Fetch companyfacts JSON from SEC EDGAR, parse XBRL data into long-format
# data.tables, resolve tag aliases, deduplicate, and cache as parquet.
#
# Output schema (per-ticker parquet, long format):
#   ticker       chr   Ticker symbol
#   cik          chr   10-digit zero-padded CIK
#   concept      chr   Canonical concept name (e.g., "revenue")
#   tag          chr   Original XBRL tag used
#   value        num   Reported value
#   period_end   Date  Period end date
#   period_start Date  Period start date (NA for instant tags)
#   filed        Date  SEC filing date (point-in-time stamp)
#   form         chr   Filing form (10-K, 10-Q, 10-K/A, etc.)
#   accession    chr   Accession number (unique filing ID)
#   fiscal_year  int   Fiscal year
#   fiscal_qtr   chr   Quarter label (Q1-Q4 or FY)
#   unit         chr   Unit (USD, shares, etc.)
#   period_type  chr   Classified period (FY, Q1-Q4, or NA if unclassifiable)
#
# Dependencies: data.table, arrow, jsonlite, httr
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(jsonlite)
  library(httr)
  library(xml2)     # Wave M: per-filing XBRL instance parsing
})


# =============================================================================
# CONSTANTS
# =============================================================================

.EDGAR_BASE     <- "https://data.sec.gov"
.EDGAR_UA       <- Sys.getenv("EDGAR_UA", "BSTAR/1.0 contact@email.com")
.EDGAR_RATE_SEC <- as.numeric(Sys.getenv("EDGAR_RATE_SEC", "0.11"))

# Form priority for deduplication (lower = preferred).
# Annual and amended-annual share the same priority so that -accession
# sort picks the amendment (later accession) over the original.
# Same logic for quarterly forms.
.FORM_PRIORITY <- c(
  "10-K"   = 1L,
  "10-K/A" = 1L,   # amendment supersedes original via later accession
  "20-F"   = 1L,   # foreign private issuers (annual)
  "20-F/A" = 1L,
  "40-F"   = 1L,   # Canadian cross-listed (annual)
  "40-F/A" = 1L,
  "10-Q"   = 2L,
  "10-Q/A" = 2L
)

# Expected period_days bands per fp label. Used by dedup_fundamentals to
# tie-break on duration match when multiple rows share (concept, period_end,
# fp). Balance-sheet (instant) concepts have NA period_start -> NA period_days
# and are handled by treating NA as a neutral match (rank 0).
.FP_EXPECTED_DAYS <- list(
  Q1 = c(60L, 120L),   # 3-month standalone; upper 120 accommodates 16-week
                       # Q1 used by Kroger/AAP fiscal calendars (12-12-12-16)
  Q2 = c(160L, 200L),  # 6-month YTD
  Q3 = c(250L, 290L),  # 9-month YTD
  Q4 = c(330L, 380L),  # 12-month (when filers tag annual as Q4)
  FY = c(330L, 380L)
)

# Return 0L if period_days falls in the expected band for fp (preferred),
# 1L otherwise. NA period_days -> 0L (balance-sheet instants). NA / unknown
# fp -> 0L (no basis to discriminate).
.duration_match_rank <- function(period_days, fp) {
  out <- integer(length(period_days))
  for (i in seq_along(period_days)) {
    d <- period_days[i]; f <- fp[i]
    if (is.na(d) || is.na(f) || is.null(.FP_EXPECTED_DAYS[[f]])) {
      out[i] <- 0L
    } else {
      band <- .FP_EXPECTED_DAYS[[f]]
      out[i] <- if (d >= band[1] && d <= band[2]) 0L else 1L
    }
  }
  out
}


# =============================================================================
# XBRL TAG ALIAS MAP
# =============================================================================
# Maps canonical concept names to ordered vectors of XBRL tags (us-gaap).
# First match wins during resolution. Order reflects prevalence.

.TAG_ALIASES <- list(

  # --- Income Statement ---
  revenue = c(
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax"
  ),

  cogs = c(
    "CostOfGoodsAndServicesSold",
    "CostOfRevenue",
    "CostOfGoodsSold",
    "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization"
  ),

  operating_income = c(
    "OperatingIncomeLoss"
  ),

  net_income = c(
    "NetIncomeLoss"
  ),

  # Multiclass* tags are synthetic (Wave M, see section 4b): per-class facts
  # recovered from filing instances and combined into the priced class.
  # Ranked first so dedup prefers them over legacy single-class fragments.
  eps_basic = c(
    "MulticlassEPSBasic",
    "EarningsPerShareBasic"
  ),

  eps_diluted = c(
    "MulticlassEPSDiluted",
    "EarningsPerShareDiluted",
    # single-class filers that report one combined figure (TIE, WIN --
    # old-CIK wave Tier 2); ranked last so a real diluted tag always wins
    "EarningsPerShareBasicAndDiluted"
  ),

  interest_expense = c(
    "InterestExpense",
    "InterestExpenseDebt"
  ),

  sga = c(
    "SellingGeneralAndAdministrativeExpense"
  ),

  rnd = c(
    "ResearchAndDevelopmentExpense",
    "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"
  ),

  depreciation = c(
    "DepreciationDepletionAndAmortization",
    "DepreciationAndAmortization",
    "Depreciation"
  ),

  # Phase 0 OpenAP expansion (docs/research/09_openap_indicator_expansion.md):
  # tax concepts for Tax, ChTax, ETR (Lev-Nissim 2004, Thomas-Zhang 2011)
  income_tax_expense = c(
    "IncomeTaxExpenseBenefit"
  ),

  # Consolidated tags only. The ...BeforeIncomeTaxesDomestic tag is deliberately
  # excluded: it is the domestic-only portion and won dedup for multinationals
  # (PG 2010-2019) when tested, silently understating pretax income. Firms
  # lacking a consolidated tag get NA; downstream may fall back to
  # income_tax_expense + net_income.
  pretax_income = c(
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"
  ),

  # Sparsely tagged (often text-only disclosure); coverage decides whether
  # GrAdExp / Mohanram component 8 are buildable. NA-heavy is expected.
  advertising = c(
    "AdvertisingExpense",
    "MarketingAndAdvertisingExpense"
  ),

  # --- Balance Sheet ---
  total_assets = c(
    "Assets"
  ),

  stockholders_equity = c(
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"
  ),

  long_term_debt = c(
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "LongTermDebtAndCapitalLeaseObligations"
  ),

  short_term_debt = c(
    "ShortTermBorrowings",
    "ShortTermDebtCurrent",
    "DebtCurrent",
    "ShortTermBankLoansAndNotesPayable"
  ),

  current_assets = c(
    "AssetsCurrent"
  ),

  current_liabilities = c(
    "LiabilitiesCurrent"
  ),

  total_liabilities = c(
    "Liabilities"
  ),

  accounts_receivable = c(
    "AccountsReceivableNetCurrent",
    "AccountsReceivableNet"
  ),

  inventory = c(
    "InventoryNet",
    "InventoryFinishedGoodsNetOfReserves"
  ),

  cash = c(
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsAndShortTermInvestments",
    "Cash"
  ),

  # MulticlassSharesOutstanding = synthesized from per-class DEI cover
  # counts (authoritative); ...BS = synthesized from per-class balance-
  # sheet counts. BS ranks above the organic balance-sheet line (a whole-
  # company sum beats a possibly single-class fragment on a same-date
  # collision) but the organic DEI cover keeps priority at the selector
  # level (.SHARES_TAG_PREF in indicator_compute.R): cover dates never
  # collide with balance-sheet dates in dedup, so this rank only decides
  # the same-date contest.
  shares_outstanding = c(
    "MulticlassSharesOutstanding",
    "MulticlassSharesOutstandingBS",
    "CommonStockSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
    "WeightedAverageNumberOfDilutedSharesOutstanding"
  ),

  # DEI cover-page public float (instant; reported by ~70-80% of filers, NA otherwise)
  public_float = c(
    "EntityPublicFloat"
  ),

  accounts_payable = c(
    "AccountsPayableCurrent",
    "AccountsPayableAndAccruedLiabilitiesCurrent"
  ),

  accrued_liabilities = c(
    "AccruedLiabilitiesCurrent"
  ),

  deferred_revenue = c(
    "DeferredRevenueCurrent",
    "ContractWithCustomerLiabilityCurrent",
    "DeferredRevenueCurrentAndNoncurrent"
  ),

  prepaid_expenses = c(
    "PrepaidExpenseAndOtherAssetsCurrent",
    "PrepaidExpenseAndOtherAssets",
    "PrepaidExpenseCurrent"
  ),

  # Phase 0 OpenAP expansion: balance-sheet concepts for the change family
  # (Richardson 2005, Hirshleifer 2004, Soliman 2008) and level ratios
  # (tang, ZScore, Penman net debt).
  ppe_net = c(
    "PropertyPlantAndEquipmentNet",
    "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization"
  ),

  ppe_gross = c(
    "PropertyPlantAndEquipmentGross",
    "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetBeforeAccumulatedDepreciationAndAmortization"
  ),

  # Financial assets split (FNA in Richardson decomposition). LongTermInvestments
  # is the aggregate; EquityMethodInvestments is a component listed last so the
  # aggregate wins when both are reported.
  st_investments = c(
    "ShortTermInvestments",
    "MarketableSecuritiesCurrent",
    "AvailableForSaleSecuritiesCurrent"
  ),

  lt_investments = c(
    "LongTermInvestments",
    "MarketableSecuritiesNoncurrent",
    "AvailableForSaleSecuritiesNoncurrent",
    "EquityMethodInvestments"
  ),

  retained_earnings = c(
    "RetainedEarningsAccumulatedDeficit"
  ),

  # Near-zero for most S&P 500 industrials; downstream treats missing as 0.
  preferred_stock = c(
    "PreferredStockValue",
    "PreferredStockValueOutstanding"
  ),

  # Goodwill and ex-goodwill intangibles kept separate so they can be summed
  # without double counting. The IncludingGoodwill aggregate tag is deliberately
  # excluded from both.
  goodwill = c(
    "Goodwill"
  ),

  intangibles = c(
    "IntangibleAssetsNetExcludingGoodwill",
    "FiniteLivedIntangibleAssetsNet"
  ),

  minority_interest = c(
    "MinorityInterest"
  ),

  # --- Cash Flow Statement ---
  operating_cashflow = c(
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByOperatingActivities",
    # Filers with (current or historical) discontinued operations tag the
    # continuing-operations line separately. Used by FMC, TFX, ITT, DRI and
    # similar. Listed last so firms that report both the total and the
    # continuing-ops line still resolve to the total.
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
  ),

  capex = c(
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets"
  ),

  buybacks = c(
    "PaymentsForRepurchaseOfCommonStock",
    "PaymentsForRepurchaseOfEquity"
  ),

  dividends_paid = c(
    "PaymentsOfDividendsCommonStock",
    "PaymentsOfDividends",
    "Dividends"
  ),

  # Phase 0 OpenAP expansion: financing flows for NetDebtFinance,
  # NetEquityFinance, XFIN, NetPayoutYield (Bradshaw 2006, Boudoukh 2007).
  # Long-term only by design: first-alias-wins dedup cannot sum LT and ST
  # lines, so the short-term component comes from the short_term_debt
  # balance-sheet delta instead (Bradshaw's own construction).
  debt_issuance = c(
    "ProceedsFromIssuanceOfLongTermDebt",
    "ProceedsFromIssuanceOfSeniorLongTermDebt",
    "ProceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet",
    # Generic / firm-specific styles found in prototype (KO, GOOGL, MSFT, CAT).
    # MaturingInMoreThanThreeMonths includes some short-term paper; acceptable
    # proxy since the short-term component is also captured via the
    # short_term_debt balance-sheet delta.
    "ProceedsFromIssuanceOfDebt",
    "ProceedsFromDebtNetOfIssuanceCosts",
    "ProceedsFromDebtMaturingInMoreThanThreeMonths"
  ),

  debt_repayment = c(
    "RepaymentsOfLongTermDebt",
    "RepaymentsOfSeniorDebt",
    "RepaymentsOfLongTermDebtAndCapitalSecurities",
    "RepaymentsOfDebtAndCapitalLeaseObligations",
    "RepaymentsOfDebt",
    "RepaymentsOfDebtMaturingInMoreThanThreeMonths"
  ),

  # SSTK analog. Option-plan proceeds tag listed last: it is the common line
  # for mature firms that issue shares only through compensation plans.
  equity_issuance = c(
    "ProceedsFromIssuanceOfCommonStock",
    "ProceedsFromIssuanceOrSaleOfEquity",
    "ProceedsFromIssuanceOfSharesUnderIncentiveAndShareBasedCompensationPlansIncludingStockOptions",
    "ProceedsFromStockOptionsExercised"
  )
)

# Reverse lookup: XBRL tag -> canonical concept
.TAG_TO_CONCEPT <- {
  out <- list()
  for (concept in names(.TAG_ALIASES)) {
    for (tag in .TAG_ALIASES[[concept]]) {
      out[[tag]] <- concept
    }
  }
  out
}

# All target tags as a flat vector (for filtering companyfacts JSON)
.ALL_TARGET_TAGS <- unlist(.TAG_ALIASES, use.names = FALSE)


# =============================================================================
# PRIVATE HELPERS
# =============================================================================

.assert_output <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}

.pad_cik <- function(cik) {
  formatC(as.integer(cik), width = 10, flag = "0")
}

.edgar_fetch <- function(url, retries = 3L, timeout_s = 30) {
  for (attempt in seq_len(retries)) {
    resp <- tryCatch({
      httr::GET(url, httr::add_headers(`User-Agent` = .EDGAR_UA),
                httr::timeout(timeout_s))
    }, error = function(e) NULL)

    if (!is.null(resp) && httr::status_code(resp) == 200) {
      Sys.sleep(.EDGAR_RATE_SEC)
      return(httr::content(resp, as = "text", encoding = "UTF-8"))
    }

    if (!is.null(resp) && httr::status_code(resp) == 429) {
      Sys.sleep(2^attempt)
      next
    }

    if (attempt < retries) Sys.sleep(2^attempt)
  }
  NULL
}

# Null-coalesce operator (available in base R >= 4.4.0, define for older versions)
if (!exists("%||%", mode = "function", envir = baseenv())) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
}


# =============================================================================
# 1. fetch_companyfacts()
# =============================================================================
#' Fetch EDGAR companyfacts JSON for a single CIK
#'
#' @param cik Character. 10-digit zero-padded CIK.
#' @return Parsed list from JSON, or NULL on failure.
fetch_companyfacts <- function(cik) {
  url <- sprintf("%s/api/xbrl/companyfacts/CIK%s.json", .EDGAR_BASE, cik)
  json_text <- .edgar_fetch(url)
  if (is.null(json_text)) return(NULL)
  tryCatch(
    jsonlite::fromJSON(json_text, simplifyVector = FALSE),
    error = function(e) {
      warning(sprintf("fetch_companyfacts: JSON parse failed for CIK %s: %s",
                      cik, e$message), call. = FALSE)
      NULL
    }
  )
}


# =============================================================================
# 2. parse_companyfacts()
# =============================================================================
#' Parse companyfacts JSON into a long-format data.table
#'
#' Extracts all target XBRL tags (from .TAG_ALIASES) found in the JSON.
#' Each row is one (tag, period, filing) observation.
#'
#' @param facts List. Parsed companyfacts JSON from fetch_companyfacts().
#' @param ticker Character. Ticker symbol (for output).
#' @param cik Character. 10-digit CIK (for output).
#' @return data.table in long format, or NULL if no target tags found.
parse_companyfacts <- function(facts, ticker, cik) {

  if (is.null(facts)) return(NULL)

  # companyfacts JSON structure:
  # facts -> us-gaap -> {TagName} -> units -> {USD|shares|...} -> [array of obs]
  # Each observation has: val, end, start, filed, form, accn, fy, fp, frame
  us_gaap <- facts[["facts"]][["us-gaap"]]
  dei     <- facts[["facts"]][["dei"]]

  if (is.null(us_gaap) && is.null(dei)) return(NULL)

  rows <- list()
  row_idx <- 0L

  # Helper to process one namespace
  process_namespace <- function(ns_data) {
    if (is.null(ns_data)) return()
    for (tag_name in names(ns_data)) {
      if (!(tag_name %in% .ALL_TARGET_TAGS)) next

      concept <- .TAG_TO_CONCEPT[[tag_name]]
      tag_data <- ns_data[[tag_name]]
      units_data <- tag_data[["units"]]
      if (is.null(units_data)) next

      for (unit_name in names(units_data)) {
        obs_list <- units_data[[unit_name]]
        if (length(obs_list) == 0) next

        for (obs in obs_list) {
          val <- obs[["val"]]
          if (is.null(val)) next

          row_idx <<- row_idx + 1L
          rows[[row_idx]] <<- list(
            ticker       = ticker,
            cik          = cik,
            concept      = concept,
            tag          = tag_name,
            value        = as.numeric(val),
            period_end   = as.character(obs[["end"]]   %||% NA_character_),
            period_start = as.character(obs[["start"]] %||% NA_character_),
            filed        = as.character(obs[["filed"]] %||% NA_character_),
            form         = as.character(obs[["form"]]  %||% NA_character_),
            accession    = as.character(obs[["accn"]]  %||% NA_character_),
            fiscal_year  = as.integer(obs[["fy"]]      %||% NA_integer_),
            fiscal_qtr   = as.character(obs[["fp"]]    %||% NA_character_),
            unit         = unit_name
          )
        }
      }
    }
  }

  process_namespace(us_gaap)
  process_namespace(dei)

  if (row_idx == 0L) return(NULL)

  dt <- rbindlist(rows)

  # Type conversions
  dt[, period_end   := as.Date(period_end)]
  dt[, period_start := as.Date(period_start)]
  dt[, filed        := as.Date(filed)]

  dt
}


# =============================================================================
# 3. dedup_fundamentals() + pit_dedup()
# =============================================================================

# Attach the ranking helper columns shared by dedup_fundamentals and
# pit_dedup: form_priority, tag_rank, duration_match. Mutates a copy.
.add_dedup_ranks <- function(dt) {

  dt <- copy(dt)

  # Assign form priority (unrecognized forms get lowest priority)
  dt[, form_priority := .FORM_PRIORITY[form]]
  dt[is.na(form_priority), form_priority := 99L]

  # Assign tag rank within concept (lower = preferred alias)
  dt[, tag_rank := mapply(function(concept, tag) {
    aliases <- .TAG_ALIASES[[concept]]
    if (is.null(aliases)) return(99L)
    idx <- match(tag, aliases)
    if (is.na(idx)) 99L else idx
  }, concept, tag)]

  # Duration-match rank: 0 if period length matches fp-expected band, else 1.
  # Instant tags (balance sheet) have NA period_start and are unaffected.
  dt[, period_days := as.integer(period_end - period_start)]
  dt[, duration_match := .duration_match_rank(period_days, fiscal_qtr)]

  dt
}

.RANK_COLS <- c("form_priority", "tag_rank", "period_days", "duration_match")

#' Deduplicate fundamental observations WITHIN each filing (vintage-preserving)
#'
#' Keeps one row per (concept, period_end, fiscal_qtr, accession): within a
#' single filing, resolves tag aliases (lower alias rank wins) and drops
#' TTM / prior-period comparatives that share the same key (duration-match
#' tie-break). Rows for the same (concept, period_end, fiscal_qtr) reported
#' by DIFFERENT filings are all retained, each with its own filed date.
#'
#' This preserves reporting vintages: when a later 10-K re-reports last
#' year's balance sheet as a comparative, both the original row (early filed
#' date) and the comparative (late filed date) survive. Point-in-time
#' consumers resolve to a single row per period with pit_dedup(), which
#' honors filed <= as_of. Without vintages, every cache refresh silently
#' shifted historical values to the latest re-report and pushed their filed
#' dates forward, degrading PIT snapshots (see docs/research/
#' 09_openap_indicator_expansion.md, Phase 0 verification).
#'
#' @param dt data.table from parse_companyfacts().
#' @return data.table, one row per concept x period x filing.
dedup_fundamentals <- function(dt) {

  if (is.null(dt) || nrow(dt) == 0) return(dt)

  dt <- .add_dedup_ranks(dt)

  # Within a filing there is no accession/form variation, so order by
  # duration match (drop comparatives/TTM under the same key) then alias rank.
  setorder(dt, concept, period_end, fiscal_qtr, accession,
           duration_match, form_priority, tag_rank)

  # Keep first row per (concept, period_end, fiscal_qtr, accession).
  # fiscal_qtr distinguishes FY from Q4 when period_end is the same.
  dt <- dt[!duplicated(dt[, .(concept, period_end, fiscal_qtr, accession)])]

  dt[, (.RANK_COLS) := NULL]

  dt
}

#' Resolve vintages to one row per period, as of a point-in-time date
#'
#' For each (concept, period_end, fiscal_qtr) combination:
#' 1. If as_of is given, drop rows filed after as_of (and rows with NA filed),
#'    so the view contains only what was public on that date.
#' 2. Apply form priority: annual (10-K/10-K/A/20-F/40-F) > quarterly (10-Q/10-Q/A).
#'    Within a group, amendments share priority with originals so that accession
#'    sort picks the amendment (later accession number) over the original.
#' 3. Within same form priority, keep most recent accession number (latest
#'    amendment or re-report available as of the date).
#' 4. Prefer rows whose period length matches the expected duration for the
#'    `fp` label, then lower tag alias rank.
#'
#' With as_of = NULL this reproduces the pre-vintage cache content exactly
#' (latest view across all filings) -- the compatible default for consumers
#' that do not reconstruct history.
#'
#' @param dt data.table with vintage rows (dedup_fundamentals output / cache).
#' @param as_of Date or character. Point-in-time cutoff; NULL = latest view.
#' @return Deduplicated data.table (one row per concept x period).
pit_dedup <- function(dt, as_of = NULL) {

  if (is.null(dt) || nrow(dt) == 0) return(dt)

  if (!is.null(as_of)) {
    d <- as.Date(as_of)
    dt <- dt[!is.na(filed) & as.Date(filed) <= d]
    if (nrow(dt) == 0) return(dt)
  }

  dt <- .add_dedup_ranks(dt)

  # Sort: concept + period + fiscal_qtr, duration-match first (so good rows
  # win against TTM/prior-period comparatives), then form priority, accession
  # (desc so amendments beat originals), then tag rank.
  setorder(dt, concept, period_end, fiscal_qtr,
           duration_match, form_priority, -accession, tag_rank)

  dt <- dt[!duplicated(dt[, .(concept, period_end, fiscal_qtr)])]

  dt[, (.RANK_COLS) := NULL]

  dt
}


# =============================================================================
# 4. classify_period()
# =============================================================================
#' Classify observations as annual (FY) or quarterly (Q1-Q4)
#'
#' Uses filing form and fiscal period to determine granularity.
#' Filters out observations that cannot be classified (e.g., fragments).
#'
#' @param dt data.table from dedup_fundamentals().
#' @return data.table with period_type column added (FY or Q1-Q4).
classify_period <- function(dt) {

  if (is.null(dt) || nrow(dt) == 0) return(dt)

  dt <- copy(dt)

  # Determine period length in days (for duration tags)
  dt[, period_days := as.integer(period_end - period_start)]

  # Classify based on fiscal_qtr field from EDGAR
  # fp values: FY, Q1, Q2, Q3, Q4
  dt[, period_type := fifelse(
    fiscal_qtr %in% c("FY"), "FY",
    fifelse(fiscal_qtr %in% c("Q1", "Q2", "Q3", "Q4"), fiscal_qtr,
            NA_character_)
  )]

  # For unclassified rows, infer from period length
  # Annual: > 300 days, Quarterly: 60-120 days
  dt[is.na(period_type) & !is.na(period_days) & period_days > 300,
     period_type := "FY"]
  dt[is.na(period_type) & !is.na(period_days) &
       period_days >= 60 & period_days <= 120,
     period_type := {
       # Infer quarter from period_end month
       m <- as.integer(format(period_end, "%m"))
       fifelse(m %in% 1:3, "Q1",
               fifelse(m %in% 4:6, "Q2",
                       fifelse(m %in% 7:9, "Q3", "Q4")))
     }]

  # Instant tags (balance sheet) have no period_start -> NA period_days
  # Keep these; they are classified by their fiscal_qtr from EDGAR.
  # Only assign valid period_type values.
  valid_fp <- c("FY", "Q1", "Q2", "Q3", "Q4")
  dt[is.na(period_type) & is.na(period_start) & fiscal_qtr %in% valid_fp,
     period_type := fiscal_qtr]

  # Drop helper column
  dt[, period_days := NULL]

  dt
}


# =============================================================================
# 4b. MULTI-CLASS SHARE / EPS EXTRACTION (Wave M)
# =============================================================================
# The companyfacts API omits every fact that carries a dimension. Multi-class
# filers tag cover-page share counts (dei:EntityCommonStockSharesOutstanding)
# and per-class EPS with StatementClassOfStockAxis members, so those facts
# never reach the cache: shares_outstanding / eps_* are NA for these names
# and market_cap plus every ratio built on it dies.
#
# This section recovers per-class facts from the per-filing XBRL instances
# and synthesizes canonical rows:
#   shares_outstanding = sum over classes of cover shares x conversion into
#                        the priced class (as-traded close x count = whole-
#                        company market cap)
#   eps_basic/diluted  = the priced class's own reported EPS
# Synthetic rows carry first-ranked Multiclass* tags so existing dedup
# prefers them over legacy single-class fragments, and real accession/filed
# dates so PIT semantics are identical to organic rows.
# Design: docs/DESIGN_MULTICLASS_SHARES.md

.MULTICLASS_RAW_DIR <- "cache/multiclass_raw"

.MULTICLASS_SHARES_TAG      <- "MulticlassSharesOutstanding"
.MULTICLASS_SHARES_BS_TAG   <- "MulticlassSharesOutstandingBS"
.MULTICLASS_EPS_BASIC_TAG   <- "MulticlassEPSBasic"
.MULTICLASS_EPS_DILUTED_TAG <- "MulticlassEPSDiluted"

# Instance fact tags extracted (matched by local name, namespace-year agnostic)
.MC_INSTANCE_TAGS <- c(
  "EntityCommonStockSharesOutstanding",
  "CommonStockSharesOutstanding",
  "EarningsPerShareBasic",
  "EarningsPerShareDiluted",
  "EarningsPerShareBasicAndDiluted"
)

# Registry of multi-class CIKs. Fields:
#   ticker      roster ticker (priced listing)
#   priced      canonical class letter of the priced listing
#   conv        per non-priced class: list(mode = "eps_ratio"|"fixed",
#               fallback = numeric). eps_ratio derives the conversion from
#               per-class EPS in the same filing (handles V's floating
#               class-B rate and BRK's 1500:1); fallback applies when the
#               ratio is not derivable. fixed uses fallback directly.
#               NA fallback = skip that class with a warning.
#   default_conv conversion for class members not in conv (triage names
#               whose single class is tagged dimensionally). NA = skip.
#   member_map  optional ordered list(pattern = class) tried before the
#               generic Class([A-Z]) regex (MKC voting/non-voting).
#   organic_eps_class class letter that NON-dimensional instance EPS
#               represents when dimensional priced EPS is absent (BRK
#               pre-2014 tagged Class A equivalent EPS undimensioned);
#               converted into the priced class via conv.
#   drop_organic_shares / drop_organic_eps  drop ALL organic rows of that
#               concept for this CIK (legacy fragments are garbage on the
#               priced basis: BRK 1.6M Class-A weighted counts against a
#               Class B price).
#   window      c(from, to) filing-date bounds for instance fetching;
#               NA side = open. NULL = full history.
.MULTICLASS_REGISTRY <- list(

  # Berkshire: A converts into B at 30:1 before the 2010-01-21 B 50:1
  # split, 1500:1 after. All synthetic values are AS-FILED basis, so the
  # fallback must match the conversion in force on the filing date
  # (downstream .split_factor rescales to the current basis). Legacy
  # non-dimensional EPS rows are Class A values and legacy share rows are
  # Class A fragments -> drop both.
  "0001067983" = list(
    ticker = "BRK.B", priced = "B",
    conv = list(A = list(mode = "eps_ratio",
                         fallback = function(filed) {
                           if (!is.na(filed) && filed < as.Date("2010-01-21")) 30 else 1500
                         })),
    organic_eps_class = "A",
    drop_organic_shares = TRUE, drop_organic_eps = TRUE,
    window = c("2009-08-01", NA)
  ),

  # Visa: class B floating conversion (~1.6, litigation escrow), class C
  # 1:1 pre-2015 split then 4:1. eps_ratio tracks both regimes.
  "0001403161" = list(
    ticker = "V", priced = "A",
    conv = list(B = list(mode = "eps_ratio", fallback = NA_real_),
                C = list(mode = "eps_ratio", fallback = NA_real_)),
    drop_organic_shares = TRUE,
    window = c("2009-06-01", NA)
  ),

  # Constellation Brands: class B 1:1 economics, retired Nov 2022.
  "0000016918" = list(
    ticker = "STZ", priced = "A",
    conv = list(B = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2010-07-01", NA)
  ),

  # GoDaddy Up-C: class B has voting only, no economic rights. Window
  # ends where dimensional cover tagging stops (last per-class cover
  # filed 2025-05-02, verified in the terminal-rebuild residual check):
  # later filings tag the cover NON-dimensionally and reach companyfacts
  # organically, so the drop must not touch them.
  "0001609711" = list(
    ticker = "GDDY", priced = "A",
    conv = list(B = list(mode = "fixed", fallback = 0)),
    drop_organic_shares = TRUE,
    window = c("2015-01-01", "2025-06-30")
  ),

  # Baker Hughes (BHGE era) Up-C: GE-held class B non-economic.
  "0001701605" = list(
    ticker = "BKR", priced = "A",
    conv = list(B = list(mode = "fixed", fallback = 0)),
    drop_organic_shares = TRUE,
    window = c("2017-01-01", "2023-12-31")
  ),

  # Under Armour: A/B/C equal economics; roster ticker UA = class C
  # after the 2016-03 split, class A before it.
  "0001336917" = list(
    ticker = "UA", priced = "C",
    conv = list(A = list(mode = "eps_ratio", fallback = 1),
                B = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2016-01-01", "2021-12-31")
  ),

  # Erie Indemnity: class A listed; class B unlisted. Fixed 2400:1
  # CONVERSION basis (market-value convention), not eps_ratio: the
  # reported two-class EPS allocates participation at ~150:1, but B's
  # market value follows its conversion right into 2400 A shares.
  "0000922621" = list(
    ticker = "ERIE", priced = "A",
    conv = list(B = list(mode = "fixed", fallback = 2400)),
    drop_organic_shares = TRUE,
    window = c("2023-01-01", NA)
  ),

  # Kinder Morgan post-IPO structure: P public; A/B/C investor-retained
  # stock converting into P near 1:1. Filings tag EPS only for P and A
  # (verified 2012 10-Q), so B/C carry fixed ~parity fallbacks (B 94M,
  # C 2.3M of ~800M total -- bounded approximation).
  "0001506307" = list(
    ticker = "KMI", priced = "P",
    conv = list(A = list(mode = "eps_ratio", fallback = 1),
                B = list(mode = "fixed", fallback = 1),
                C = list(mode = "fixed", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2011-01-01", "2015-12-31")
  ),

  # Lennar: class B ~1:1 economics (trades at a small discount; conv 1
  # slightly overstates ~2% of B's ~10% weight -- accepted).
  "0000920760" = list(
    ticker = "LEN", priced = "A",
    conv = list(B = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2013-02-07", NA)
  ),

  # McCormick (MKC) is deliberately ABSENT: its 2011 share gap is
  # unrecoverable point-in-time -- the H2-2011 10-Qs tag no share counts
  # at all and the first per-class counts arrive with the 10-K filed
  # 2012-01-27, after every gap snapshot date. 2012+ is already covered
  # by the organic weighted-average fallback. A wide-window recipe made
  # things WORSE (replaced good organic totals with an NV-only
  # undercount at the 2012 golden date).

  # Nike: class A unlisted, class B listed, equal economics.
  "0000320187" = list(
    ticker = "NKE", priced = "B",
    conv = list(A = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2009-01-01", "2013-12-31")
  ),

  # Discovery: series A priced (DISCA); B and C (DISCK) equal economics.
  "0001437107" = list(
    ticker = "DISCA", priced = "A",
    conv = list(B = list(mode = "eps_ratio", fallback = 1),
                C = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2009-01-01", "2022-06-30")
  ),

  # News Corp (post-2013 spin entity): class B voting (NWS, the cached
  # roster ticker), class A non-voting (NWSA, served via CIK alias);
  # equal economics, single combined EPS. Cover shares are tagged
  # dimensionally in every filing to date (zero organic
  # EntityCommonStockSharesOutstanding rows in companyfacts), so the
  # window stays open. The two 2013 organic CommonStockSharesOutstanding
  # fragments are dropped. Synthesis warns on nws:SeriesCommonStockMember
  # every run: an authorized-but-unissued series line, 0 shares in every
  # filing, correctly skipped.
  "0001564708" = list(
    ticker = "NWS", priced = "B",
    conv = list(A = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2013-06-01", NA)
  ),

  # Fox Corp (post-2019 spin entity): class B voting (FOX, the cached
  # roster ticker), class A non-voting (FOXA, served via CIK alias);
  # equal economics, single combined EPS. Dimensional cover tagging
  # throughout; the lone organic cover row is a registration-statement
  # placeholder (value 1, 2019-03-18) and is dropped.
  "0001754301" = list(
    ticker = "FOX", priced = "B",
    conv = list(A = list(mode = "eps_ratio", fallback = 1)),
    drop_organic_shares = TRUE,
    window = c("2019-01-01", NA)
  ),

  # Triage names: single class, cover shares tagged dimensionally in some
  # vintages. default_conv 1 counts whatever single member appears.
  "0001137774" = list(
    ticker = "PRU", priced = "A", conv = list(), default_conv = 1,
    drop_organic_shares = TRUE,
    window = c("2010-01-01", "2014-12-31")
  ),
  # Mosaic post-Cargill split (2011-2015): listed common is tagged with a
  # custom NoClassCommonStockMember; restricted Class A/B convert 1:1.
  "0001285785" = list(
    ticker = "MOS", priced = "NC",
    conv = list(A = list(mode = "fixed", fallback = 1),
                B = list(mode = "fixed", fallback = 1)),
    member_map = list("NoClassCommonStock" = "NC"),
    drop_organic_shares = TRUE,
    window = c("2014-01-01", "2016-12-31")
  ),
  "0000080424" = list(
    ticker = "PG", priced = "A", conv = list(), default_conv = 1,
    drop_organic_shares = TRUE,
    window = c("2009-01-01", "2011-12-31")
  )
)


# -- Canonical class letter for an explicit member qname.
# member_map patterns first (ordered), then Class([A-Z]) / Series([A-Z]).
# Preferred members return NA (not common equity). Unknown members return
# the raw local member name so recipes can decide via default_conv.
.mc_canonical_class <- function(member, member_map = NULL) {
  local <- sub("^[^:]*:", "", member)
  if (grepl("Preferred", local)) return(NA_character_)
  if (!is.null(member_map)) {
    for (pat in names(member_map)) {
      if (grepl(pat, local)) return(member_map[[pat]])
    }
  }
  m <- regmatches(local, regexpr("(Class|Series)[A-Z](?![a-z])", local,
                                 perl = TRUE))
  if (length(m) == 1) return(substr(m, nchar(m), nchar(m)))
  local
}


# -- Filing index (10-K / 10-Q family) from the submissions API, including
# older paginated files. window = c(from, to) skips paginated pages whose
# filingFrom/filingTo range falls entirely outside it (each page is an
# extra rate-limited round trip). Returns data.table(accession, form,
# filed, primary_doc) sorted by filed, or NULL on fetch failure.
fetch_filing_index <- function(cik, window = NULL) {

  url <- sprintf("%s/submissions/CIK%s.json", .EDGAR_BASE, cik)
  txt <- .edgar_fetch(url)
  if (is.null(txt)) return(NULL)
  subs <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (is.null(subs)) return(NULL)

  block_to_dt <- function(b) {
    if (is.null(b) || is.null(b$accessionNumber)) return(NULL)
    data.table(accession   = b$accessionNumber,
               form        = b$form,
               filed       = as.Date(b$filingDate),
               primary_doc = b$primaryDocument %||% NA_character_)
  }

  win_from <- if (!is.null(window) && !is.na(window[1])) as.Date(window[1]) else NULL
  win_to   <- if (!is.null(window) && length(window) > 1 && !is.na(window[2])) {
    as.Date(window[2])
  } else NULL

  parts <- list(block_to_dt(subs$filings$recent))
  extra <- subs$filings$files
  if (!is.null(extra) && NROW(extra) > 0) {
    for (i in seq_len(NROW(extra))) {
      pg_from <- suppressWarnings(as.Date(extra$filingFrom[i]))
      pg_to   <- suppressWarnings(as.Date(extra$filingTo[i]))
      if (!is.null(win_from) && !is.na(pg_to)   && pg_to   < win_from) next
      if (!is.null(win_to)   && !is.na(pg_from) && pg_from > win_to)   next
      txt2 <- .edgar_fetch(sprintf("%s/submissions/%s", .EDGAR_BASE,
                                   extra$name[i]))
      if (is.null(txt2)) next
      older <- tryCatch(jsonlite::fromJSON(txt2), error = function(e) NULL)
      parts[[length(parts) + 1L]] <- block_to_dt(older)
    }
  }

  dt <- rbindlist(Filter(Negate(is.null), parts))
  if (nrow(dt) == 0) return(NULL)
  dt <- dt[form %in% names(.FORM_PRIORITY)]
  setorder(dt, filed)

  .assert_output(dt, "fetch_filing_index", list(
    "is data.table"  = is.data.table,
    "has accession"  = function(x) "accession" %in% names(x),
    "filed is Date"  = function(x) inherits(x$filed, "Date")
  ))
  dt
}


# -- Locate the XBRL instance document inside one filing. Inline-XBRL era
# filings extract to {doc}_htm.xml; older filings ship a schema-named
# {prefix}-{yyyymmdd}.xml. Returns the full URL, "" when the filing index
# was fetched but holds no instance (permanent: pre-XBRL filing), or NULL
# on fetch failure (transient: retry later).
locate_xbrl_instance <- function(cik, accession) {

  accn <- gsub("-", "", accession)
  base <- sprintf("https://www.sec.gov/Archives/edgar/data/%d/%s",
                  as.integer(cik), accn)
  txt <- .edgar_fetch(paste0(base, "/index.json"))
  if (is.null(txt)) return(NULL)
  idx <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (is.null(idx)) return(NULL)
  items <- idx$directory$item
  if (is.null(items) || NROW(items) == 0) return("")

  nm <- items$name
  cand <- nm[grepl("_htm\\.xml$", nm)]
  if (length(cand) == 0) {
    cand <- nm[grepl("^[a-z0-9]+-[0-9]{8}\\.xml$", nm, ignore.case = TRUE)]
  }
  if (length(cand) == 0) {
    excl <- grepl("(_cal|_def|_lab|_pre|_ref)\\.xml$|\\.xsd$|FilingSummary",
                  nm, ignore.case = TRUE)
    xml_only <- grepl("\\.xml$", nm, ignore.case = TRUE) & !excl
    if (any(xml_only)) {
      sz <- suppressWarnings(as.numeric(items$size[xml_only]))
      cand <- nm[xml_only][which.max(fifelse(is.na(sz), 0, sz))]
    }
  }
  if (length(cand) == 0) return("")
  paste0(base, "/", cand[1])
}


# -- Parse one instance document. Keeps facts whose context is either
# non-dimensional or dimensioned ONLY on a ClassOfStock axis. Returns
# data.table(tag, member, value, period_start, period_end) or NULL.
# Facts are collected first so only referenced contexts are parsed --
# large instances (BRK 10-K ~30MB) carry thousands of contexts.
parse_instance_classes <- function(xml_text) {

  # HUGE lifts libxml2's input-size safety limit: >10MB instances (DISCA
  # FY2018 10-K, 11.7MB) fail with "Huge input lookup" without it
  doc <- tryCatch(xml2::read_xml(xml_text, options = c("HUGE")),
                  error = function(e) NULL)
  if (is.null(doc)) return(NULL)

  facts <- list()
  for (tag in .MC_INSTANCE_TAGS) {
    fact_nodes <- xml2::xml_find_all(doc,
      sprintf("//*[local-name()='%s']", tag))
    if (length(fact_nodes) == 0) next
    facts[[tag]] <- data.table(
      tag   = tag,
      cref  = xml2::xml_attr(fact_nodes, "contextRef"),
      value = suppressWarnings(as.numeric(xml2::xml_text(fact_nodes)))
    )
  }
  if (length(facts) == 0) return(NULL)
  fdt <- rbindlist(facts)[!is.na(cref) & !is.na(value)]
  if (nrow(fdt) == 0) return(NULL)

  # one document pass over contexts, filtered to those the facts reference
  # (the local-name() XPath visits every element; per-ref queries would
  # rescan a ~30MB BRK instance dozens of times)
  refs <- unique(fdt$cref)
  ctx_nodes <- xml2::xml_find_all(doc, "//*[local-name()='context']")
  ctx_ids <- xml2::xml_attr(ctx_nodes, "id")
  sel <- which(ctx_ids %in% refs)
  ctx <- vector("list", length(sel))
  names(ctx) <- ctx_ids[sel]
  for (j in seq_along(sel)) {
    node <- ctx_nodes[[sel[j]]]
    mem_nodes <- xml2::xml_find_all(node,
      ".//*[local-name()='explicitMember']")
    dims <- xml2::xml_attr(mem_nodes, "dimension")
    mems <- xml2::xml_text(mem_nodes)
    class_ok <- length(dims) == 0 || all(grepl("ClassOfStock", dims))
    inst  <- xml2::xml_text(xml2::xml_find_first(node,
      ".//*[local-name()='instant']"))
    start <- xml2::xml_text(xml2::xml_find_first(node,
      ".//*[local-name()='startDate']"))
    end   <- xml2::xml_text(xml2::xml_find_first(node,
      ".//*[local-name()='endDate']"))
    ctx[[j]] <- list(
      ok     = class_ok,
      member = if (length(mems)) mems[1] else "",
      ps     = if (!is.na(inst)) NA_character_ else start,
      pe     = if (!is.na(inst)) inst else end
    )
  }

  keep <- vapply(fdt$cref, function(r)
    !is.null(ctx[[r]]) && isTRUE(ctx[[r]]$ok), logical(1))
  fdt <- fdt[keep]
  if (nrow(fdt) == 0) return(NULL)

  fdt[, member       := vapply(cref, function(r) ctx[[r]]$member, character(1))]
  fdt[, period_start := as.Date(vapply(cref, function(r) ctx[[r]]$ps, character(1)))]
  fdt[, period_end   := as.Date(vapply(cref, function(r) ctx[[r]]$pe, character(1)))]
  fdt[, cref := NULL]
  unique(fdt)
}


# -- Fetch + parse all instances for one registry CIK. Resumable: extracted
# rows cached in {raw_dir}/{cik}_{ticker}.parquet, processed accessions in
# {raw_dir}/{cik}_{ticker}_accessions.parquet (n_rows = -1 marks a failed
# fetch/parse, retried on the next run). Never stops on a single filing.
build_multiclass_raw <- function(ticker, cik,
                                 raw_dir = .MULTICLASS_RAW_DIR,
                                 force_refresh = FALSE) {

  rec <- .MULTICLASS_REGISTRY[[cik]]
  if (is.null(rec)) return(NULL)

  if (!dir.exists(raw_dir)) dir.create(raw_dir, recursive = TRUE)
  raw_file <- file.path(raw_dir, sprintf("%s_%s.parquet", cik, ticker))
  acc_file <- file.path(raw_dir, sprintf("%s_%s_accessions.parquet",
                                         cik, ticker))

  raw_dt <- if (!force_refresh && file.exists(raw_file)) {
    tryCatch(as.data.table(arrow::read_parquet(raw_file)),
             error = function(e) NULL)
  } else NULL
  acc_dt <- if (!force_refresh && file.exists(acc_file)) {
    tryCatch(as.data.table(arrow::read_parquet(acc_file)),
             error = function(e) NULL)
  } else NULL

  win <- rec$window
  idx <- fetch_filing_index(cik, window = win)
  if (is.null(idx) || nrow(idx) == 0) {
    warning(sprintf("build_multiclass_raw: no filing index for %s (CIK %s)",
                    ticker, cik), call. = FALSE)
    return(raw_dt)
  }

  # XBRL mandate floor: nothing to extract from pre-2009 filings
  idx <- idx[filed >= as.Date("2009-01-01")]
  if (!is.null(win)) {
    if (!is.na(win[1])) idx <- idx[filed >= as.Date(win[1])]
    if (length(win) > 1 && !is.na(win[2])) idx <- idx[filed <= as.Date(win[2])]
  }

  # n_rows >= 0: extracted. n_rows == -2: permanent (filing carries no
  # XBRL instance: pre-mandate or amendment without exhibits) -- never
  # retried. n_rows == -1: transient fetch/parse failure -- retried.
  done <- if (!is.null(acc_dt)) {
    acc_dt[n_rows >= 0 | n_rows == -2L, accession]
  } else character(0)
  todo <- idx[!accession %in% done]

  if (nrow(todo) > 0) {
    message(sprintf("build_multiclass_raw: %s -- %d filings to extract",
                    ticker, nrow(todo)))
  }

  new_raw <- list(); new_acc <- list()
  for (i in seq_len(nrow(todo))) {
    accn <- todo$accession[i]
    inst_url <- locate_xbrl_instance(cik, accn)
    if (!is.null(inst_url) && identical(inst_url, "")) {
      new_acc[[length(new_acc) + 1L]] <- data.table(
        accession = accn, filed = todo$filed[i], form = todo$form[i],
        n_rows = -2L)
      next
    }
    parsed <- NULL
    if (!is.null(inst_url)) {
      # instances run to tens of MB (DISCA FY2018 10-K broke the default
      # 30s ceiling for good, not transiently)
      xml_text <- .edgar_fetch(inst_url, timeout_s = 180)
      if (!is.null(xml_text)) parsed <- parse_instance_classes(xml_text)
    }
    if (is.null(parsed)) {
      warning(sprintf("build_multiclass_raw: extraction failed for %s %s",
                      ticker, accn), call. = FALSE)
      new_acc[[length(new_acc) + 1L]] <- data.table(
        accession = accn, filed = todo$filed[i], form = todo$form[i],
        n_rows = -1L)
      next
    }
    parsed[, `:=`(accession = accn, form = todo$form[i],
                  filed = todo$filed[i])]
    new_raw[[length(new_raw) + 1L]] <- parsed
    new_acc[[length(new_acc) + 1L]] <- data.table(
      accession = accn, filed = todo$filed[i], form = todo$form[i],
      n_rows = nrow(parsed))
    if (i %% 10 == 0) {
      message(sprintf("build_multiclass_raw: %s -- %d/%d filings",
                      ticker, i, nrow(todo)))
    }
  }

  if (length(new_raw) > 0) {
    raw_dt <- unique(rbindlist(c(list(raw_dt), new_raw), use.names = TRUE,
                               fill = TRUE))
    arrow::write_parquet(raw_dt, raw_file)
  }
  if (length(new_acc) > 0) {
    keep <- if (!is.null(acc_dt)) acc_dt[n_rows >= 0 | n_rows == -2L] else NULL
    acc_dt <- unique(rbindlist(c(list(keep), new_acc), use.names = TRUE,
                               fill = TRUE))
    arrow::write_parquet(acc_dt, acc_file)
  }

  if (!is.null(raw_dt) && nrow(raw_dt) > 0) {
    .assert_output(raw_dt, "build_multiclass_raw", list(
      "is data.table" = is.data.table,
      "has tag/value/accession" = function(x)
        all(c("tag", "value", "accession", "filed") %in% names(x))
    ))
  }
  raw_dt
}


# -- Conversion factors into the priced class for one filing's rows.
# Returns named numeric vector (class -> factor), priced class = 1.
# eps_ratio = median of per-period EPS_class / EPS_priced within the
# filing (basic first, diluted fallback), guarded against tiny EPS.
# A fallback may be a function(filed) -> numeric for conversions that
# changed over time on the as-filed basis (BRK pre/post the 2010 split).
.mc_conversions <- function(sub, rec) {

  conv <- c(1); names(conv) <- rec$priced
  filed <- suppressWarnings(min(sub$filed, na.rm = TRUE))

  ratio_from_eps <- function(cl) {
    for (tg in c("EarningsPerShareBasic", "EarningsPerShareDiluted")) {
      pr <- sub[tag == tg & class == rec$priced & !is.na(value)]
      ot <- sub[tag == tg & class == cl & !is.na(value)]
      if (nrow(pr) == 0 || nrow(ot) == 0) next
      m <- merge(pr[, .(period_start, period_end, v_pr = value)],
                 ot[, .(period_start, period_end, v_ot = value)],
                 by = c("period_start", "period_end"))
      m <- m[abs(v_pr) >= 0.05]
      if (nrow(m) > 0) return(stats::median(m$v_ot / m$v_pr))
    }
    NA_real_
  }

  for (cl in names(rec$conv)) {
    spec <- rec$conv[[cl]]
    fb <- spec$fallback
    if (is.function(fb)) fb <- fb(filed)
    r <- NA_real_
    if (identical(spec$mode, "eps_ratio")) r <- ratio_from_eps(cl)
    if (is.na(r)) r <- fb
    conv[cl] <- r
  }
  conv
}


# -- Combine raw per-class rows into canonical cache rows for one CIK.
# organic_dt supplies the filer's own fy/fp labels per accession.
synthesize_multiclass_rows <- function(raw_dt, cik, ticker,
                                       organic_dt = NULL) {

  rec <- .MULTICLASS_REGISTRY[[cik]]
  if (is.null(rec) || is.null(raw_dt) || nrow(raw_dt) == 0) return(NULL)

  dt <- copy(raw_dt)
  dt[, class := vapply(member, .mc_canonical_class, character(1),
                       member_map = rec$member_map)]
  # invariant: member == "" <=> class == "" (non-dimensional facts)
  dt <- dt[!is.na(class)]                       # drop preferred-stock members

  # fy/fp per accession: modal organic label. Accessions absent from the
  # organic frame are SKIPPED (rare: amendments that never reached
  # companyfacts) -- calendar inference mislabels non-calendar fiscal
  # years (NKE May FYE, STZ Feb FYE) and a mislabeled FY row corrupts the
  # timeseries filed_date anchor. NA beats a wrong label.
  meta <- NULL
  if (!is.null(organic_dt) && nrow(organic_dt) > 0 &&
      all(c("accession", "fiscal_year", "fiscal_qtr") %in%
          names(organic_dt))) {
    meta <- organic_dt[!is.na(accession),
                       .N, by = .(accession, fiscal_year, fiscal_qtr)]
    setorder(meta, accession, -N)
    meta <- meta[!duplicated(accession), .(accession, fiscal_year,
                                           fiscal_qtr)]
  }
  if (is.null(meta)) {
    warning(sprintf(
      "synthesize_multiclass_rows: %s -- no organic fy/fp metadata, skipping",
      ticker), call. = FALSE)
    return(NULL)
  }
  no_meta <- setdiff(unique(dt$accession), meta$accession)
  if (length(no_meta) > 0) {
    warning(sprintf(
      "synthesize_multiclass_rows: %s -- %d accession(s) lack organic fy/fp, skipped",
      ticker, length(no_meta)), call. = FALSE)
    dt <- dt[!accession %in% no_meta]
  }
  if (nrow(dt) == 0) return(NULL)

  # accessions whose organic rows already carry genuine diluted EPS: do
  # NOT synthesize basic-as-diluted there, the organic row must win
  org_dil_accn <- if (isTRUE(rec$drop_organic_eps)) character(0) else {
    unique(organic_dt[concept == "eps_diluted" & !is.na(accession),
                      accession])
  }

  default_conv <- rec$default_conv %||% NA_real_
  known_classes <- unique(c(rec$priced, names(rec$conv)))
  unknown_seen <- character(0)
  skipped_na_conv <- character(0)

  out <- list()
  for (accn in unique(dt$accession)) {
    sub <- dt[accession == accn]
    conv <- .mc_conversions(sub, rec)

    conv_of <- function(cl) {
      if (cl %in% names(conv)) {
        if (is.na(conv[[cl]])) {
          skipped_na_conv <<- union(skipped_na_conv, cl)
        }
        return(conv[[cl]])
      }
      if (cl == "") return(NA_real_)
      unknown_seen <<- union(unknown_seen, cl)
      default_conv
    }

    # ---- shares_outstanding: candidates from the dei cover (authoritative
    # tag) and the balance sheet (demoted tag; may carry the issued-count
    # defect). Per source: use the period_end carrying the most classes
    # (comparative instants and conversion-date one-offs carry partial
    # class sets); a known-class member beats default_conv umbrellas at
    # the same date (no double count); any NA conversion at the chosen
    # date kills the candidate -- NA beats an understated sum. The cover
    # source wins unless the balance sheet covers MORE classes (a cover
    # tagging only one class must not shadow a complete per-class BS).
    share_candidate <- function(src_tag) {
      cov <- sub[tag == src_tag & !is.na(value)]
      if (nrow(cov) == 0) return(NULL)
      dim_cov <- cov[class != ""]
      dim_cov <- dim_cov[!duplicated(paste(class, period_end))]
      if (nrow(dim_cov) > 0) {
        n_cls <- dim_cov[, .(n = uniqueN(class)), by = period_end]
        setorder(n_cls, -n, -period_end)
        pe_use <- n_cls$period_end[1]
        cur <- dim_cov[period_end == pe_use]
        cur_known <- cur[class %in% known_classes]
        if (nrow(cur_known) > 0) {
          unknown_seen <<- union(unknown_seen,
                                 setdiff(cur$class, known_classes))
          cur <- cur_known
        }
        cur[, cf := vapply(class, conv_of, numeric(1))]
        if (nrow(cur) == 0 || anyNA(cur$cf)) return(NULL)
        return(list(value = sum(cur$value * cur$cf), pe = pe_use,
                    n_classes = nrow(cur)))
      }
      if (length(rec$conv) == 0) {
        # undimensioned counts are trusted only for single-class triage
        # names; for true multi-class filers they are one class's count
        # on the wrong basis (BRK Class-A fragments)
        nod <- cov[class == ""]
        pe_use <- max(nod$period_end)
        return(list(value = nod[period_end == pe_use, value[1]],
                    pe = pe_use, n_classes = 1L))
      }
      NULL
    }
    cand_dei <- share_candidate("EntityCommonStockSharesOutstanding")
    cand_bs  <- share_candidate("CommonStockSharesOutstanding")
    pick <- NULL; pick_tag <- NULL
    if (!is.null(cand_dei) &&
        (is.null(cand_bs) || cand_dei$n_classes >= cand_bs$n_classes)) {
      pick <- cand_dei; pick_tag <- .MULTICLASS_SHARES_TAG
    } else if (!is.null(cand_bs)) {
      pick <- cand_bs; pick_tag <- .MULTICLASS_SHARES_BS_TAG
    }
    # zero-value cover facts occur in the wild (BRK 2010-04-30 dei = 0,
    # retired classes); a zero count must never win dedup
    if (!is.null(pick) && !is.na(pick$value) && pick$value > 0) {
      out[[length(out) + 1L]] <- data.table(
        concept = "shares_outstanding", tag = pick_tag,
        value = pick$value, period_end = pick$pe,
        period_start = as.Date(NA), accession = accn, unit = "shares")
    }

    # ---- eps: priced-class dimensional first; non-dimensional converted
    # via organic_eps_class when configured. Source tags are tried in
    # order; basic is the last-resort diluted source (filers with no
    # dilutive securities report only basic and diluted == basic), but
    # only when the filing's organic rows carry no genuine diluted EPS.
    eps_rows <- function(srcs) {
      for (src in srcs) {
        eps <- sub[tag == src & class == rec$priced & !is.na(value)]
        if (nrow(eps) > 0) return(eps)
        if (!is.null(rec$organic_eps_class)) {
          cf <- conv_of(rec$organic_eps_class)
          nod <- sub[tag == src & class == "" & !is.na(value)]
          if (nrow(nod) > 0 && !is.na(cf) && cf != 0) {
            return(copy(nod)[, value := value / cf])
          }
        }
      }
      NULL
    }
    dil_srcs <- c("EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted")
    if (!accn %in% org_dil_accn) {
      dil_srcs <- c(dil_srcs, "EarningsPerShareBasic")
    }
    for (spec in list(list(srcs = c("EarningsPerShareBasic",
                                    "EarningsPerShareBasicAndDiluted"),
                           out_tag = .MULTICLASS_EPS_BASIC_TAG,
                           concept = "eps_basic"),
                      list(srcs = dil_srcs,
                           out_tag = .MULTICLASS_EPS_DILUTED_TAG,
                           concept = "eps_diluted"))) {
      eps <- eps_rows(spec$srcs)
      if (is.null(eps)) next
      out[[length(out) + 1L]] <- eps[, .(
        concept = spec$concept, tag = spec$out_tag, value,
        period_end, period_start, accession = accn,
        unit = "USD/shares")]
    }
  }

  if (length(unknown_seen) > 0) {
    warning(sprintf(
      "synthesize_multiclass_rows: %s -- unmapped class members: %s",
      ticker, paste(unknown_seen, collapse = ", ")), call. = FALSE)
  }
  if (length(skipped_na_conv) > 0) {
    warning(sprintf(
      "synthesize_multiclass_rows: %s -- classes skipped (no conversion): %s",
      ticker, paste(skipped_na_conv, collapse = ", ")), call. = FALSE)
  }
  if (length(out) == 0) return(NULL)

  syn <- rbindlist(out, use.names = TRUE)
  form_filed <- unique(dt[, .(accession, form, filed)])
  syn <- merge(syn, form_filed, by = "accession", all.x = TRUE)
  syn <- merge(syn, meta, by = "accession", all.x = TRUE)

  syn[, `:=`(ticker = ticker, cik = cik)]
  setcolorder(syn, c("ticker", "cik", "concept", "tag", "value",
                     "period_end", "period_start", "filed", "form",
                     "accession", "fiscal_year", "fiscal_qtr", "unit"))

  .assert_output(syn, "synthesize_multiclass_rows", list(
    "is data.table"       = is.data.table,
    "synthetic tags only" = function(x) all(grepl("^Multiclass", x$tag)),
    "no NA filed"         = function(x) !anyNA(x$filed),
    "no NA fiscal labels" = function(x) !anyNA(x$fiscal_qtr),
    "positive share counts" = function(x)
      x[concept == "shares_outstanding", all(value > 0)]
  ))
  syn
}


# -- Merge hook: drop configured legacy rows, append synthetic rows.
# Used by fetch_and_cache_ticker (rebuild path) and patch_multiclass_cache.
.merge_multiclass <- function(dt, ticker, cik,
                              raw_dir = .MULTICLASS_RAW_DIR) {

  rec <- .MULTICLASS_REGISTRY[[cik]]
  if (is.null(rec)) return(dt)

  syn <- tryCatch({
    raw_dt <- build_multiclass_raw(ticker, cik, raw_dir = raw_dir)
    synthesize_multiclass_rows(raw_dt, cik, ticker, organic_dt = dt)
  }, error = function(e) {
    warning(sprintf(".merge_multiclass: synthesis failed for %s: %s",
                    ticker, e$message), call. = FALSE)
    NULL
  })

  # No synthesis -> return dt UNTOUCHED. Applying the drop flags without
  # replacement rows would gut shares/eps coverage on a transient EDGAR
  # failure (legacy garbage beats an empty cache that used to be good).
  if (is.null(syn) || nrow(syn) == 0) {
    warning(sprintf(
      ".merge_multiclass: %s -- no synthetic rows, organic rows kept as-is",
      ticker), call. = FALSE)
    return(dt)
  }

  # Drops are scoped to the registry window (by filed date): outside the
  # window synthesis has no coverage and organic rows must survive.
  # Inside it, organic share rows are unreliable mixtures of totals and
  # single-class fragments that would win pit_dedup's accession-first
  # ordering when a later filing re-reports a comparative instant.
  in_window <- function(filed) {
    win <- rec$window
    if (is.null(win)) return(rep(TRUE, length(filed)))
    ok <- rep(TRUE, length(filed))
    if (!is.na(win[1])) ok <- ok & filed >= as.Date(win[1])
    if (length(win) > 1 && !is.na(win[2])) ok <- ok & filed <= as.Date(win[2])
    ok & !is.na(filed)
  }
  if (isTRUE(rec$drop_organic_shares)) {
    dt <- dt[!(concept == "shares_outstanding" &
                 !grepl("^Multiclass", tag) & in_window(filed))]
  }
  if (isTRUE(rec$drop_organic_eps)) {
    dt <- dt[!(concept %in% c("eps_basic", "eps_diluted") &
                 !grepl("^Multiclass", tag) & in_window(filed))]
  }

  message(sprintf(".merge_multiclass: %s -- %d synthetic rows (%d shares, %d eps)",
                  ticker, nrow(syn),
                  syn[concept == "shares_outstanding", .N],
                  syn[concept != "shares_outstanding", .N]))
  # fill: the patch path carries period_type already; classify_period
  # recomputes it for the appended rows downstream
  rbindlist(list(dt, syn), use.names = TRUE, fill = TRUE)
}


# -- Patch existing per-ticker caches in place (no companyfacts re-fetch).
# Strips previous synthetic rows, re-synthesizes, re-dedups, re-writes.
patch_multiclass_cache <- function(cache_dir = "cache/fundamentals",
                                   raw_dir = .MULTICLASS_RAW_DIR,
                                   tickers = NULL) {

  results <- list()
  for (cik in names(.MULTICLASS_REGISTRY)) {
    rec <- .MULTICLASS_REGISTRY[[cik]]
    if (!is.null(tickers) && !(rec$ticker %in% tickers)) next

    cache_file <- file.path(cache_dir,
                            sprintf("%s_%s.parquet", cik, rec$ticker))
    if (!file.exists(cache_file)) {
      warning(sprintf("patch_multiclass_cache: no cache for %s", rec$ticker),
              call. = FALSE)
      next
    }
    dt <- tryCatch(as.data.table(arrow::read_parquet(cache_file)),
                   error = function(e) NULL)
    if (is.null(dt) || nrow(dt) == 0) next

    had_syn <- dt[grepl("^Multiclass", tag), .N]
    dt <- dt[!grepl("^Multiclass", tag)]
    dt <- .merge_multiclass(dt, rec$ticker, cik, raw_dir = raw_dir)

    # never trade previously-good synthetic rows for nothing (transient
    # EDGAR failure must not gut the cache)
    if (had_syn > 0 && dt[grepl("^Multiclass", tag), .N] == 0) {
      warning(sprintf(
        "patch_multiclass_cache: %s -- re-synthesis produced no rows, cache left untouched",
        rec$ticker), call. = FALSE)
      next
    }

    dt <- dedup_fundamentals(dt)
    dt <- classify_period(dt)

    .assert_output(dt, "patch_multiclass_cache", list(
      "is data.table"      = is.data.table,
      "has concept column" = function(x) "concept" %in% names(x),
      "no duplicate concept+period+qtr+accession" = function(x)
        !anyDuplicated(x[, .(concept, period_end, fiscal_qtr, accession)])
    ))

    arrow::write_parquet(dt, cache_file)
    n_syn <- dt[grepl("^Multiclass", tag), .N]
    message(sprintf("patch_multiclass_cache: %s -- %d synthetic rows in cache",
                    rec$ticker, n_syn))
    results[[rec$ticker]] <- n_syn
  }
  invisible(results)
}


# =============================================================================
# 5. fetch_and_cache_ticker()
# =============================================================================
#' Fetch, parse, dedup, and cache fundamentals for a single ticker
#'
#' Idempotent: if cache parquet exists for this ticker, returns cached data.
#' Use force_refresh = TRUE to re-fetch.
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit zero-padded CIK.
#' @param cache_dir Character. Directory for per-ticker parquet files.
#' @param force_refresh Logical. If TRUE, re-fetch even if cached.
#' @return data.table in long format, or NULL on failure.
fetch_and_cache_ticker <- function(ticker, cik,
                                   cache_dir = "cache/fundamentals",
                                   force_refresh = FALSE) {

  cache_file <- file.path(cache_dir, sprintf("%s_%s.parquet", cik, ticker))

  # Return cached if exists and not forcing refresh
  if (!force_refresh && file.exists(cache_file)) {
    dt <- tryCatch(
      as.data.table(arrow::read_parquet(cache_file)),
      error = function(e) NULL)
    if (!is.null(dt) && nrow(dt) > 0) {
      # Self-heal (Wave M): a true multi-class cache without synthetic
      # rows is a poisoned artifact of a transient synthesis failure --
      # the idempotent cached path would otherwise never retry.
      rec <- .MULTICLASS_REGISTRY[[cik]]
      if (!is.null(rec) && length(rec$conv) > 0 &&
          !any(grepl("^Multiclass", dt$tag))) {
        message(sprintf(
          "fetch_and_cache_ticker: %s -- no synthetic multiclass rows in cache, re-synthesizing",
          ticker))
        healed <- .merge_multiclass(dt, ticker, cik)
        if (any(grepl("^Multiclass", healed$tag))) {
          healed <- classify_period(dedup_fundamentals(healed))
          arrow::write_parquet(healed, cache_file)
          return(healed)
        }
      }
      return(dt)
    }
  }

  # Fetch from EDGAR
  facts <- fetch_companyfacts(cik)
  if (is.null(facts)) {
    warning(sprintf("fetch_and_cache_ticker: EDGAR fetch failed for %s (CIK %s)",
                    ticker, cik), call. = FALSE)
    return(NULL)
  }

  # Parse
  dt <- parse_companyfacts(facts, ticker, cik)
  if (is.null(dt) || nrow(dt) == 0) {
    warning(sprintf("fetch_and_cache_ticker: no target tags found for %s (CIK %s)",
                    ticker, cik), call. = FALSE)
    return(NULL)
  }

  # Multi-class recovery (Wave M): registry CIKs get per-class facts from
  # filing instances; companyfacts drops all dimensional facts
  if (cik %in% names(.MULTICLASS_REGISTRY)) {
    dt <- .merge_multiclass(dt, ticker, cik)
  }

  # Dedup
  dt <- dedup_fundamentals(dt)

  # Classify periods
  dt <- classify_period(dt)

  # Validate before caching
  .assert_output(dt, "fetch_and_cache_ticker", list(
    "is data.table"        = is.data.table,
    "has concept column"   = function(x) "concept" %in% names(x),
    "has filed column"     = function(x) "filed" %in% names(x),
    "has value column"     = function(x) "value" %in% names(x),
    "no duplicate concept+period+qtr+accession" = function(x)
      !anyDuplicated(x[, .(concept, period_end, fiscal_qtr, accession)])
  ))

  # Cache
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  arrow::write_parquet(dt, cache_file)

  dt
}


# =============================================================================
# 6. fetch_fundamentals_batch()
# =============================================================================
#' Fetch fundamentals for a batch of tickers
#'
#' Resumable: skips tickers already cached (unless force_refresh).
#' Chunked: processes tickers in groups of chunk_size with progress
#' summaries between chunks. Logs progress. Never stops on single-ticker failure.
#'
#' @param tickers Character vector. Ticker symbols.
#' @param ciks Character vector. Matching 10-digit CIKs (same order as tickers).
#' @param cache_dir Character. Directory for per-ticker parquet files.
#' @param force_refresh Logical. Re-fetch all, even if cached.
#' @param chunk_size Integer. Number of tickers per chunk. NULL = no chunking.
#' @return List with $results (named list of data.tables), $success (tickers),
#'         $failed (tickers), $cached (tickers loaded from cache).
fetch_fundamentals_batch <- function(tickers, ciks,
                                     cache_dir = "cache/fundamentals",
                                     force_refresh = FALSE,
                                     chunk_size = NULL) {

  stopifnot(length(tickers) == length(ciks))

  results <- list()
  success <- character()
  failed  <- character()
  cached  <- character()
  n <- length(tickers)

  # Determine chunk boundaries
  if (is.null(chunk_size) || chunk_size >= n) {
    chunk_starts <- 1L
    chunk_ends   <- n
  } else {
    chunk_starts <- seq(1L, n, by = chunk_size)
    chunk_ends   <- pmin(chunk_starts + chunk_size - 1L, n)
  }
  n_chunks <- length(chunk_starts)

  for (ch in seq_len(n_chunks)) {
    idx_from <- chunk_starts[ch]
    idx_to   <- chunk_ends[ch]

    if (n_chunks > 1) {
      message(sprintf("\n--- Chunk %d/%d (tickers %d-%d of %d) ---",
                      ch, n_chunks, idx_from, idx_to, n))
    }

    for (i in idx_from:idx_to) {
      tk  <- tickers[i]
      cik <- ciks[i]

      if (i %% 25 == 1 || i == idx_from || i == n) {
        message(sprintf("  [%d/%d] %s (CIK %s)", i, n, tk, cik))
      }

      cache_file <- file.path(cache_dir, sprintf("%s_%s.parquet", cik, tk))
      was_cached <- !force_refresh && file.exists(cache_file)

      dt <- tryCatch(
        fetch_and_cache_ticker(tk, cik, cache_dir, force_refresh),
        error = function(e) {
          warning(sprintf("fetch_fundamentals_batch: error for %s: %s",
                          tk, e$message), call. = FALSE)
          NULL
        }
      )

      if (!is.null(dt) && nrow(dt) > 0) {
        results[[tk]] <- dt
        success <- c(success, tk)
        if (was_cached) cached <- c(cached, tk)
      } else {
        failed <- c(failed, tk)
      }
    }

    # Chunk summary
    if (n_chunks > 1) {
      message(sprintf("  Chunk %d done: %d total success, %d total failed so far",
                      ch, length(success), length(failed)))
    }
  }

  message(sprintf("fetch_fundamentals_batch: %d success (%d from cache), %d failed",
                  length(success), length(cached), length(failed)))
  if (length(failed) > 0) {
    message("  Failed: ", paste(failed, collapse = ", "))
  }

  list(results = results, success = success, failed = failed, cached = cached)
}


# =============================================================================
# 7. coverage_report()
# =============================================================================
#' Generate XBRL tag coverage report across fetched tickers
#'
#' Inspects which canonical concepts are covered, which XBRL tags were
#' actually used (alias resolution), and which tickers have gaps.
#'
#' @param results Named list of data.tables (output from fetch_fundamentals_batch).
#' @return data.table with coverage statistics.
coverage_report <- function(results) {

  if (length(results) == 0) {
    message("coverage_report: no results to analyze")
    return(NULL)
  }

  all_concepts <- names(.TAG_ALIASES)
  tickers <- names(results)

  # Build coverage matrix: concept x ticker
  coverage <- data.table(
    concept = rep(all_concepts, each = length(tickers)),
    ticker  = rep(tickers, times = length(all_concepts))
  )

  # Check which concepts have data per ticker
  coverage[, has_data := FALSE]
  coverage[, tag_used := NA_character_]
  coverage[, n_obs    := 0L]

  for (tk in tickers) {
    dt_tk <- results[[tk]]
    if (is.null(dt_tk) || nrow(dt_tk) == 0) next

    # Compute per concept
    concept_summary <- dt_tk[, .(
      n_obs    = .N,
      tag_used = tag[1]
    ), by = concept]

    for (j in seq_len(nrow(concept_summary))) {
      cn <- concept_summary$concept[j]
      coverage[concept == cn & ticker == tk,
               `:=`(has_data = TRUE,
                    tag_used = concept_summary$tag_used[j],
                    n_obs    = concept_summary$n_obs[j])]
    }
  }

  # Aggregate per concept
  concept_agg <- coverage[, .(
    n_tickers_with_data = sum(has_data),
    pct_coverage = round(sum(has_data) / .N * 100, 1),
    tags_used = paste(unique(na.omit(tag_used)), collapse = " | ")
  ), by = concept]

  setorder(concept_agg, -pct_coverage)

  # Print report
  message("\n=== XBRL Tag Coverage Report ===")
  message(sprintf("Tickers analyzed: %d", length(tickers)))
  message(sprintf("Concepts defined: %d", length(all_concepts)))
  message("")

  for (i in seq_len(nrow(concept_agg))) {
    row <- concept_agg[i]
    message(sprintf("  %-25s %3d/%d (%5.1f%%)  tags: %s",
                    row$concept, row$n_tickers_with_data,
                    length(tickers), row$pct_coverage, row$tags_used))
  }

  # Ticker-level summary
  ticker_agg <- coverage[, .(
    n_concepts = sum(has_data),
    pct_concepts = round(sum(has_data) / .N * 100, 1)
  ), by = ticker]

  setorder(ticker_agg, pct_concepts)

  message("\n=== Per-Ticker Coverage ===")
  for (i in seq_len(nrow(ticker_agg))) {
    row <- ticker_agg[i]
    message(sprintf("  %-6s %2d/%d concepts (%5.1f%%)",
                    row$ticker, row$n_concepts,
                    length(all_concepts), row$pct_concepts))
  }

  invisible(list(
    concept_coverage = concept_agg,
    ticker_coverage  = ticker_agg,
    detail           = coverage
  ))
}


# =============================================================================
# 8. build_fundamentals()  --  Orchestrator
# =============================================================================
#' Build fundamental data for S&P 500 constituents
#'
#' Reads the constituent master, selects tickers with CIKs,
#' and fetches fundamentals in batch. Handles duplicate tickers:
#' - SAME_COMPANY: one CIK, highest occurrence wins (single fetch).
#' - DIFFERENT_COMPANY: if both occurrences share a CIK, one fetch covers
#'   both. If they have distinct CIKs, both are fetched (rare in practice).
#'
#' @param master_path Character. Path to constituent_master.parquet.
#' @param cache_dir Character. Directory for per-ticker parquet files.
#' @param tickers Character vector. If NULL, fetches all tickers with CIK.
#'   If provided, fetches only these tickers.
#' @param force_refresh Logical. Re-fetch all, even if cached.
#' @param chunk_size Integer. Tickers per chunk for progress logging. NULL = no chunking.
#' @return List from fetch_fundamentals_batch().
build_fundamentals <- function(
    master_path = "cache/lookups/constituent_master.parquet",
    cache_dir   = "cache/fundamentals",
    tickers     = NULL,
    force_refresh = FALSE,
    chunk_size  = 50L) {

  message("build_fundamentals: starting...")

  # Load constituent master
  if (!file.exists(master_path)) {
    stop("build_fundamentals: constituent_master.parquet not found. Run Session A first.")
  }
  master <- as.data.table(arrow::read_parquet(master_path))

  # Filter to tickers with CIK
  master <- master[!is.na(cik)]

  if (!is.null(tickers)) {
    master <- master[ticker %in% tickers]
    if (nrow(master) == 0) {
      stop("build_fundamentals: none of the requested tickers found in master with CIK")
    }
  }

  # Deduplicate to unique (ticker, cik) pairs.
  # SAME_COMPANY duplicates share a CIK -> one fetch per ticker.
  # DIFFERENT_COMPANY duplicates may have distinct CIKs -> fetch both.
  # Within same (ticker, cik), keep highest occurrence (most recent stint).
  master <- master[order(ticker, cik, -occurrence)]
  master <- master[!duplicated(master[, .(ticker, cik)])]

  # Multiple rows with the same CIK but different tickers can't happen
  # (CIK is unique per filer). Multiple rows with the same ticker but
  # different CIKs are DIFFERENT_COMPANY cases -- each CIK needs its own
  # fetch. The cache filename {CIK}_{ticker}.parquet keeps them distinct.
  # However, the same CIK appearing under the same ticker (SAME_COMPANY
  # re-additions) is already collapsed above. Check for same-CIK collisions
  # across different tickers (shouldn't happen, but guard against it).
  fetch_master <- copy(master)
  n_dup_cik <- sum(duplicated(fetch_master$cik))
  if (n_dup_cik > 0) {
    message(sprintf("  %d duplicate CIK(s) across different tickers -- fetching once each",
                    n_dup_cik))
    fetch_master <- fetch_master[!duplicated(cik)]
  }

  message(sprintf("  %d unique CIKs to fetch (%d ticker-CIK pairs)",
                  nrow(fetch_master), nrow(master)))

  # Fetch
  result <- fetch_fundamentals_batch(
    tickers       = fetch_master$ticker,
    ciks          = fetch_master$cik,
    cache_dir     = cache_dir,
    force_refresh = force_refresh,
    chunk_size    = chunk_size
  )

  result
}


# =============================================================================
# 9. validate_fundamentals_build()
# =============================================================================
#' Post-build validation and summary for the full fundamental corpus
#'
#' Scans all cached parquet files and reports: total tickers, concept coverage,
#' date ranges, file sizes, and any anomalies.
#'
#' @param cache_dir Character. Cache directory.
#' @param master_path Character. Path to constituent_master.parquet (for gap check).
#' @return Invisible list with $summary, $concept_coverage, $ticker_stats.
validate_fundamentals_build <- function(
    cache_dir   = "cache/fundamentals",
    master_path = "cache/lookups/constituent_master.parquet") {

  if (!dir.exists(cache_dir)) {
    message("validate_fundamentals_build: no parquet files found")
    return(invisible(NULL))
  }
  files <- list.files(cache_dir, pattern = "[.]parquet$", full.names = TRUE)
  if (length(files) == 0) {
    message("validate_fundamentals_build: no parquet files found")
    return(invisible(NULL))
  }

  message(sprintf("\n=== Fundamentals Build Validation ==="))
  message(sprintf("Cache dir: %s", cache_dir))
  message(sprintf("Files found: %d", length(files)))

  # Aggregate stats across all files
  all_concepts <- names(.TAG_ALIASES)
  ticker_stats <- list()
  concept_ticker_count <- setNames(integer(length(all_concepts)), all_concepts)
  total_rows <- 0L
  total_bytes <- 0L
  min_filed <- as.Date("2099-01-01")
  max_filed <- as.Date("1900-01-01")

  for (f in files) {
    dt <- tryCatch(as.data.table(arrow::read_parquet(f)), error = function(e) NULL)
    if (is.null(dt) || nrow(dt) == 0) next

    tk <- dt$ticker[1]
    fsize <- file.size(f)
    total_rows <- total_rows + nrow(dt)
    total_bytes <- total_bytes + fsize

    filed_range <- range(dt$filed, na.rm = TRUE)
    if (filed_range[1] < min_filed) min_filed <- filed_range[1]
    if (filed_range[2] > max_filed) max_filed <- filed_range[2]

    concepts_present <- unique(dt$concept)
    n_fy <- sum(dt$period_type == "FY", na.rm = TRUE)
    n_q  <- sum(dt$period_type %in% c("Q1", "Q2", "Q3", "Q4"), na.rm = TRUE)

    ticker_stats[[tk]] <- list(
      ticker     = tk,
      n_rows     = nrow(dt),
      n_concepts = length(concepts_present),
      n_fy       = n_fy,
      n_q        = n_q,
      filed_min  = filed_range[1],
      filed_max  = filed_range[2],
      file_kb    = round(fsize / 1024, 1)
    )

    for (cn in concepts_present) {
      concept_ticker_count[cn] <- concept_ticker_count[cn] + 1L
    }
  }

  n_tickers <- length(ticker_stats)
  ticker_dt <- rbindlist(ticker_stats)

  # Concept coverage
  concept_dt <- data.table(
    concept    = names(concept_ticker_count),
    n_tickers  = unname(concept_ticker_count),
    pct        = round(unname(concept_ticker_count) / n_tickers * 100, 1)
  )
  setorder(concept_dt, -pct)

  # Summary output
  message(sprintf("\nTickers cached: %d", n_tickers))
  message(sprintf("Total rows: %s", format(total_rows, big.mark = ",")))
  message(sprintf("Total size: %.1f MB", total_bytes / 1024^2))
  message(sprintf("Filed date range: %s to %s", min_filed, max_filed))
  message(sprintf("Median rows/ticker: %.0f", median(ticker_dt$n_rows)))
  message(sprintf("Median concepts/ticker: %.0f", median(ticker_dt$n_concepts)))

  # Concept coverage table
  message("\n--- Concept Coverage ---")
  for (i in seq_len(nrow(concept_dt))) {
    row <- concept_dt[i]
    message(sprintf("  %-25s %3d/%d (%5.1f%%)",
                    row$concept, row$n_tickers, n_tickers, row$pct))
  }

  # Tickers with low coverage (< 10 concepts)
  low_cov <- ticker_dt[n_concepts < 10]
  if (nrow(low_cov) > 0) {
    setorder(low_cov, n_concepts)
    message(sprintf("\n--- Low-Coverage Tickers (%d with < 10 concepts) ---", nrow(low_cov)))
    for (i in seq_len(min(nrow(low_cov), 20))) {
      row <- low_cov[i]
      message(sprintf("  %-6s %2d concepts, %4d rows", row$ticker, row$n_concepts, row$n_rows))
    }
    if (nrow(low_cov) > 20) message(sprintf("  ... and %d more", nrow(low_cov) - 20))
  }

  # Check against master for gaps
  if (file.exists(master_path)) {
    master <- as.data.table(arrow::read_parquet(master_path))
    expected <- master[!is.na(cik)]
    expected <- expected[order(ticker, cik, -occurrence)]
    expected <- expected[!duplicated(expected[, .(ticker, cik)])]
    missing <- setdiff(expected$ticker, ticker_dt$ticker)
    if (length(missing) > 0) {
      message(sprintf("\n--- Missing Tickers (%d expected but not cached) ---", length(missing)))
      message("  ", paste(head(missing, 30), collapse = ", "))
      if (length(missing) > 30) message(sprintf("  ... and %d more", length(missing) - 30))
    } else {
      message("\nAll expected tickers are cached.")
    }
  }

  message("\n=== Validation Complete ===")

  invisible(list(
    summary = list(
      n_tickers   = n_tickers,
      total_rows  = total_rows,
      total_mb    = round(total_bytes / 1024^2, 1),
      filed_min   = min_filed,
      filed_max   = max_filed
    ),
    concept_coverage = concept_dt,
    ticker_stats     = ticker_dt
  ))
}


# =============================================================================
# 10. get_fundamentals()  --  Reader convenience function
# =============================================================================
#' Load cached fundamentals for a single ticker
#'
#' The cache stores one row per (concept, period, filing) so reporting
#' vintages survive re-fetches. By default this reader collapses vintages to
#' the latest view (one row per concept x period), which matches the
#' pre-vintage cache shape. Pass as_of for a point-in-time view, or
#' vintages = TRUE for the raw vintage table.
#'
#' @param ticker Character. Ticker symbol.
#' @param cik Character. 10-digit CIK (optional if only one match in cache).
#' @param cache_dir Character. Cache directory.
#' @param as_of Date or character. Collapse vintages to what was public on
#'   this date (rows filed after as_of are dropped). NULL = latest view.
#' @param vintages Logical. TRUE returns the raw vintage table (one row per
#'   concept x period x filing); as_of is ignored.
#' @return data.table or NULL if not cached.
get_fundamentals <- function(ticker, cik = NULL,
                             cache_dir = "cache/fundamentals",
                             as_of = NULL, vintages = FALSE) {

  dt <- NULL

  if (!is.null(cik)) {
    path <- file.path(cache_dir, sprintf("%s_%s.parquet", cik, ticker))
    if (!file.exists(path)) {
      # Shared-CIK alias: build_fundamentals dedups the fetch list by CIK,
      # so multi-listing filers cache under one roster ticker only
      # (0001437107_DISCA also serves DISCK and WBD; UA serves UAA; NWS
      # serves NWSA; FOX serves FOXA). Any file with the same CIK prefix
      # is the same filer.
      hits <- list.files(cache_dir,
                         pattern = sprintf("^%s_.*\\.parquet$", cik),
                         full.names = TRUE)
      if (length(hits) > 1) {
        warning(sprintf(
          "get_fundamentals: multiple cache files for CIK %s, using most recent",
          cik), call. = FALSE)
        hits <- hits[order(file.mtime(hits), decreasing = TRUE)]
      }
      if (length(hits) >= 1) path <- hits[1]
    }
    if (file.exists(path)) {
      dt <- as.data.table(arrow::read_parquet(path))
    }
  } else {
    # Search for matching file
    pattern <- sprintf("*_%s.parquet", ticker)
    files <- list.files(cache_dir, pattern = glob2rx(pattern), full.names = TRUE)

    if (length(files) > 1) {
      warning(sprintf("get_fundamentals: multiple files for %s, using most recent",
                      ticker), call. = FALSE)
      files <- files[order(file.mtime(files), decreasing = TRUE)]
    }
    if (length(files) >= 1) {
      dt <- as.data.table(arrow::read_parquet(files[1]))
    }
  }

  if (is.null(dt)) return(NULL)
  if (vintages) return(dt)
  pit_dedup(dt, as_of = as_of)
}


# =============================================================================
# 11. prototype_20_tickers()
# =============================================================================
#' Run the 20-ticker prototype for Session C
#'
#' Selects 20 representative tickers spanning sectors, market caps,
#' and edge cases. Fetches, caches, and runs coverage report.
#'
#' @param master_path Character. Path to constituent_master.parquet.
#' @param sector_path Character. Path to sector_industry.parquet.
#' @param cache_dir Character. Cache directory.
#' @param force_refresh Logical. Re-fetch even if cached.
#' @return List with $batch_result and $coverage.
prototype_20_tickers <- function(
    master_path = "cache/lookups/constituent_master.parquet",
    sector_path = "cache/lookups/sector_industry.parquet",
    cache_dir   = "cache/fundamentals",
    force_refresh = FALSE) {

  message("prototype_20_tickers: Session C prototype starting...")

  # Curated 20 tickers spanning sectors and edge cases:
  # Tech:        AAPL, MSFT, NVDA (large cap, different reporting patterns)
  # Financials:  JPM, BRK.B (banks lack COGS/inventory)
  # Healthcare:  JNJ, UNH
  # Consumer:    AMZN, WMT, PG
  # Industrial:  CAT, HON
  # Energy:      XOM, CVX
  # Utilities:   NEE
  # Real Estate: PLD
  # Comm Svc:    GOOGL, FB (listed as FB in constituent master; CIK 1326801)
  # Materials:   LIN
  # Cons Def:    KO
  prototype_tickers <- c(
    "AAPL", "MSFT", "NVDA",
    "JPM", "BRK.B",
    "JNJ", "UNH",
    "AMZN", "WMT", "PG",
    "CAT", "HON",
    "XOM", "CVX",
    "NEE",
    "PLD",
    "GOOGL", "FB",
    "LIN",
    "KO"
  )

  # Fetch
  batch_result <- build_fundamentals(
    master_path   = master_path,
    cache_dir     = cache_dir,
    tickers       = prototype_tickers,
    force_refresh = force_refresh
  )

  # Coverage report
  cov <- coverage_report(batch_result$results)

  message(sprintf("\nprototype_20_tickers: complete -- %d/%d tickers fetched",
                  length(batch_result$success), length(prototype_tickers)))

  list(batch_result = batch_result, coverage = cov)
}
