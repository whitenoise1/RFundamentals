# ============================================================================
# company_info.R  --  Company description blurb + SEC SIC label (Module 3b)
# ============================================================================
# Per-constituent company description for the analytics "Company Relevant
# Information" panel. Self-contained (does not touch the finviz sector scraper).
#   - description     : clean ~1-paragraph blurb from Wikipedia REST (matched by
#                       company name, with guards against wrong-entity / list /
#                       disambiguation pages; verified company-like).
#   - sic_description : SEC EDGAR submissions-API SIC industry label (regulatory
#                       tag; always present where EDGAR has the CIK) — also the
#                       fallback when Wikipedia yields no confident company page.
#
# Input : cache/lookups/constituent_master.parquet  (ticker, name, cik)
# Output: cache/lookups/company_info.parquet
#   ticker chr | name chr | cik chr | description chr | sic_description chr |
#   wiki_title chr | source chr ("wikipedia" | "sic" | "none")
#
# Dependencies: data.table, arrow, httr, jsonlite
# ============================================================================

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(httr); library(jsonlite)
})

.WIKI_UA  <- Sys.getenv("WIKI_UA", "RFundamentals/1.0 (research; contact@email.com)")
.EDGAR_UA <- Sys.getenv("EDGAR_UA", "BSTAR/1.0 contact@email.com")
.CI_RATE  <- as.numeric(Sys.getenv("CI_RATE_SEC", "0.12"))

if (!exists("%||%", mode = "function")) `%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

# Self-contained output validator (this module does not source the core stack).
.assert_output_ci <- function(obj, fn, checks) {
  for (nm in names(checks)) {
    ok <- tryCatch(isTRUE(checks[[nm]](obj)), error = function(e) FALSE)
    if (!ok) stop(sprintf("[%s] assertion failed: %s", fn, nm))
  }
  invisible(obj)
}

.ci_get_json <- function(url, ua) {
  for (a in 1:3) {
    r <- tryCatch(httr::GET(url, httr::add_headers(`User-Agent` = ua), httr::timeout(30)),
                  error = function(e) NULL)
    if (!is.null(r) && httr::status_code(r) == 200) {
      Sys.sleep(.CI_RATE)
      return(tryCatch(jsonlite::fromJSON(httr::content(r, as = "text", encoding = "UTF-8")),
                      error = function(e) NULL))
    }
    if (!is.null(r) && httr::status_code(r) == 404) return(NULL)
    Sys.sleep(2^a)
  }
  NULL
}

.pad_cik <- function(cik) formatC(as.integer(cik), width = 10, flag = "0")

# A page is a usable company page if it's a standard article whose Wikidata
# description / lead reads like a company (not a person, place, product, list).
.bad_title <- function(t) grepl("^(timeline|list|history|outline|index|comparison) of",
                                t, ignore.case = TRUE)
.is_company_like <- function(desc, extract) {
  s <- tolower(paste(desc %||% "", substr(extract %||% "", 1, 220)))
  grepl(paste0("compan|corporat|conglomerat|manufactur|retail|bank|insur|holding|firm|",
               "airline|automaker|enterprise|multinational|business|maker|producer|",
               "operator|brand|chain|group"), s)
}

#' Wikipedia REST blurb for a company name (with matching guards).
#' @return list(title, description) — both NA when no confident company page found.
.wiki_blurb <- function(name) {
  s <- .ci_get_json(sprintf(
    "https://en.wikipedia.org/w/api.php?action=query&format=json&list=search&srlimit=5&srsearch=%s",
    utils::URLencode(name, reserved = TRUE)), .WIKI_UA)
  titles <- tryCatch(s$query$search$title, error = function(e) NULL)
  if (is.null(titles) || !length(titles)) return(list(title = NA_character_, description = NA_character_))
  for (t in titles) {
    if (.bad_title(t)) next
    sm <- .ci_get_json(sprintf("https://en.wikipedia.org/api/rest_v1/page/summary/%s",
                               utils::URLencode(t, reserved = TRUE)), .WIKI_UA)
    if (is.null(sm)) next
    typ <- sm$type %||% "standard"; ex <- sm$extract %||% ""; d <- sm$description %||% ""
    if (identical(typ, "disambiguation") || nchar(ex) < 80) next
    if (.is_company_like(d, ex)) return(list(title = sm$title %||% t, description = ex))
  }
  list(title = NA_character_, description = NA_character_)
}

#' SEC EDGAR submissions SIC industry label for a CIK.
.sic_description <- function(cik) {
  j <- .ci_get_json(sprintf("https://data.sec.gov/submissions/CIK%s.json", .pad_cik(cik)), .EDGAR_UA)
  if (is.null(j)) return(NA_character_)
  (j$sicDescription %||% NA_character_)[1]
}

#' Build the per-constituent company_info lookup.
#' Resumable: skips tickers already present in an existing output parquet.
build_company_info <- function(master_path = "cache/lookups/constituent_master.parquet",
                               output_path  = "cache/lookups/company_info.parquet",
                               tickers = NULL, checkpoint_every = 25L) {
  stopifnot(file.exists(master_path))
  m <- as.data.table(arrow::read_parquet(master_path))
  m <- unique(m[!is.na(cik) & cik != "", .(ticker, name, cik)], by = "ticker")
  if (!is.null(tickers)) m <- m[ticker %in% toupper(tickers)]

  done <- if (file.exists(output_path)) as.data.table(arrow::read_parquet(output_path)) else NULL
  if (!is.null(done)) m <- m[!ticker %in% done$ticker]
  if (nrow(m) == 0) { message("[company_info] nothing to do (all cached)"); return(invisible(done)) }

  message(sprintf("[company_info] building %d tickers...", nrow(m)))
  rows <- if (is.null(done)) list() else list(done)
  batch <- list()
  flush <- function() {
    if (!length(batch)) return()
    rows[[length(rows) + 1L]] <<- rbindlist(batch, fill = TRUE); batch <<- list()
    arrow::write_parquet(rbindlist(rows, fill = TRUE), output_path)
  }
  for (i in seq_len(nrow(m))) {
    tk <- m$ticker[i]; nm <- m$name[i]; ck <- m$cik[i]
    wb  <- tryCatch(.wiki_blurb(nm),       error = function(e) list(title = NA, description = NA))
    sic <- tryCatch(.sic_description(ck),   error = function(e) NA_character_)
    desc <- wb$description
    src  <- if (!is.na(desc)) "wikipedia" else if (!is.na(sic)) "sic" else "none"
    if (is.na(desc) && !is.na(sic)) desc <- sic
    batch[[length(batch) + 1L]] <- data.table(ticker = tk, name = nm, cik = ck,
      description = desc %||% NA_character_, sic_description = sic %||% NA_character_,
      wiki_title = wb$title %||% NA_character_, source = src)
    if (i %% checkpoint_every == 0L) { flush(); message(sprintf("  ...%d/%d", i, nrow(m))) }
  }
  flush()
  out <- as.data.table(arrow::read_parquet(output_path))

  .assert_output_ci(out, "build_company_info", list(
    "is data.table"     = is.data.table,
    "has ticker col"    = function(x) "ticker" %in% names(x),
    "has description"   = function(x) "description" %in% names(x),
    "has sic col"       = function(x) "sic_description" %in% names(x),
    "has source col"    = function(x) "source" %in% names(x),
    "source values ok"  = function(x) all(x$source %in% c("wikipedia", "sic", "none"))
  ))

  message(sprintf("[company_info] DONE: %d tickers (%d wikipedia, %d sic-only, %d none)",
                  nrow(out), sum(out$source == "wikipedia"), sum(out$source == "sic"),
                  sum(out$source == "none")))
  invisible(out)
}
