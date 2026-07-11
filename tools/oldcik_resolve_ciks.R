# Old-CIK wave Tier 2: resolve CIKs for the NO_CIK census names via EDGAR
# company-name search, with per-CIK identity verification (postmortem rule
# 6: filing era + SIC vs the constituent's membership window and business).
#
# Inputs : cache/lookups/oldcik_scoping_census.parquet (148 NO_CIK rows)
#          data/sp500_constituents_.csv (name_when_added / name_when_removed)
# Output : cache/lookups/oldcik_cik_resolution.parquet
#          one row per census row: ticker, occ, search_name, cik, edgar_name,
#          sic, sic_desc, filings_first, filings_last, n_filings_in_window,
#          former_names, match_via, verdict (RESOLVED | AMBIGUOUS | NOT_FOUND)
#
# Resumable: rows already in the output parquet are skipped on rerun.
# Usage: Rscript tools/oldcik_resolve_ciks.R

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(jsonlite)
  library(httr); library(xml2)
})

.EDGAR_UA <- Sys.getenv("EDGAR_UA", "BSTAR/1.0 contact@email.com")
.RATE     <- as.numeric(Sys.getenv("EDGAR_RATE_SEC", "0.11"))
OUT_PATH  <- "cache/lookups/oldcik_cik_resolution.parquet"

.pad_cik <- function(cik) formatC(as.integer(cik), width = 10, flag = "0")

.http_get <- function(url, query = NULL, retries = 3L) {
  for (attempt in seq_len(retries)) {
    resp <- tryCatch(
      GET(url, query = query, add_headers(`User-Agent` = .EDGAR_UA),
          timeout(30)),
      error = function(e) NULL)
    if (!is.null(resp) && status_code(resp) == 200) {
      Sys.sleep(.RATE)
      return(content(resp, as = "text", encoding = "UTF-8"))
    }
    if (!is.null(resp) && status_code(resp) == 429) { Sys.sleep(2^attempt); next }
    if (attempt < retries) Sys.sleep(2^attempt)
  }
  NULL
}

# normalize a company name for matching: uppercase, strip punctuation and
# legal suffixes, collapse whitespace
.norm_name <- function(x) {
  x <- toupper(x)
  x <- gsub("/[A-Z ]*/?\\s*$", " ", x)  # EDGAR state suffixes: /KY/, /DE/, /NJ
  x <- gsub("[^A-Z0-9 ]", " ", x)
  x <- gsub("\\b(INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|COMPANIES|LTD|LIMITED|PLC|LP|LLC|HOLDINGS?|GROUP|THE|NEW|OLD|NV|SA|AG|ADR|CL|A|B)\\b",
            " ", x)
  trimws(gsub(" +", " ", x))
}

.name_tokens <- function(x) setdiff(strsplit(.norm_name(x), " ")[[1]], "")

# token-overlap score between a target name and an EDGAR conformed name
.name_score <- function(target, candidate) {
  tt <- .name_tokens(target); ct <- .name_tokens(candidate)
  if (!length(tt) || !length(ct)) return(0)
  length(intersect(tt, ct)) / length(union(tt, ct))
}

# raw-bytes GET: the atom feed declares ISO-8859-1; decoding it as UTF-8
# makes read_xml choke on any accented byte (foreign addresses in company
# blocks), so hand read_xml the raw bytes and let it honor the declaration
.http_get_raw <- function(url, query = NULL, retries = 3L) {
  for (attempt in seq_len(retries)) {
    resp <- tryCatch(
      GET(url, query = query, add_headers(`User-Agent` = .EDGAR_UA),
          timeout(30)),
      error = function(e) NULL)
    if (!is.null(resp) && status_code(resp) == 200) {
      Sys.sleep(.RATE)
      return(content(resp, as = "raw"))
    }
    if (!is.null(resp) && status_code(resp) == 429) { Sys.sleep(2^attempt); next }
    if (attempt < retries) Sys.sleep(2^attempt)
  }
  NULL
}

# -- browse-edgar company search: returns data.table(cik, name, sic, sic_desc)
search_browse_edgar <- function(company) {
  raw <- .http_get_raw("https://www.sec.gov/cgi-bin/browse-edgar",
                       query = list(action = "getcompany", company = company,
                                    type = "10-K", dateb = "", owner = "include",
                                    count = 40, output = "atom"))
  if (is.null(raw)) return(NULL)
  doc <- tryCatch(read_xml(raw), error = function(e) NULL)
  if (is.null(doc)) return(NULL)
  ns <- xml_ns_rename(xml_ns(doc), d1 = "a")

  # shape 2 FIRST: multi-match feeds carry <entry> blocks that each contain
  # a <company-info>, so probing for company-info first would silently
  # return only the alphabetically-first company. Entries carry cik + sic
  # but NO company name (EDGAR emits a broken title="ARRAY(0x..)"); the
  # caller must recover names via submissions JSON.
  # a third shape exists: an EXACT single-company match returns that
  # company's FILINGS as entries (accession-number blocks, no cik) plus a
  # top-level company-info -- so an empty entries parse must fall through
  entries <- xml_find_all(doc, ".//a:entry", ns)
  if (length(entries)) {
    out <- lapply(entries, function(e) {
      data.table(
        cik      = xml_text(xml_find_first(e, ".//a:cik", ns)),
        name     = NA_character_,
        sic      = xml_text(xml_find_first(e, ".//a:sic", ns)),
        sic_desc = NA_character_)
    })
    out <- rbindlist(out)[!is.na(cik) & nchar(cik) > 0]
    if (nrow(out)) return(out)
  }

  # shape 1: single match -> one top-level <company-info> block
  ci <- xml_find_first(doc, ".//a:company-info", ns)
  if (!inherits(ci, "xml_missing")) {
    return(data.table(
      cik      = xml_text(xml_find_first(ci, ".//a:cik", ns)),
      name     = xml_text(xml_find_first(ci, ".//a:conformed-name", ns)),
      sic      = xml_text(xml_find_first(ci, ".//a:assigned-sic", ns)),
      sic_desc = xml_text(xml_find_first(ci, ".//a:assigned-sic-desc", ns))))
  }
  NULL
}

# -- EDGAR full-text search fallback (2001+): display_names carry CIKs
search_fts <- function(company) {
  txt <- .http_get("https://efts.sec.gov/LATEST/search-index",
                   query = list(q = sprintf('"%s"', company), forms = "10-K"))
  if (is.null(txt)) return(NULL)
  j <- tryCatch(fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(j) || is.null(j$hits$hits)) return(NULL)
  rows <- lapply(j$hits$hits, function(h) {
    dn <- h$`_source`$display_names
    if (is.null(dn) || !length(dn)) return(NULL)
    m <- regmatches(dn[[1]], regexec("^(.*) \\(CIK (\\d{10})\\)", dn[[1]]))[[1]]
    if (length(m) < 3) return(NULL)
    data.table(cik = m[3], name = m[2], sic = NA_character_,
               sic_desc = NA_character_)
  })
  out <- rbindlist(Filter(Negate(is.null), rows))
  if (nrow(out)) unique(out, by = "cik") else NULL
}

# -- submissions JSON: identity evidence for one CIK
fetch_submissions <- function(cik) {
  txt <- .http_get(sprintf("https://data.sec.gov/submissions/CIK%s.json",
                           .pad_cik(cik)))
  if (is.null(txt)) return(NULL)
  tryCatch(fromJSON(txt, simplifyVector = TRUE), error = function(e) NULL)
}

# verify one candidate CIK against a census row; returns evidence list or NULL
verify_candidate <- function(cik, target_names, win_from, win_to) {
  sub <- fetch_submissions(cik)
  if (is.null(sub)) return(NULL)

  forms <- sub$filings$recent$form
  fdates <- as.Date(sub$filings$recent$filingDate)

  # recent caps at ~1000 filings; for active heavy filers that may not
  # reach back to the membership window. Pull older pages until it does.
  pad_from <- win_from - 550
  extra <- sub$filings$files
  if (!is.null(extra) && NROW(extra) &&
      length(fdates) && min(fdates, na.rm = TRUE) > pad_from) {
    fnames <- if (is.data.frame(extra)) extra$name else
      vapply(extra, `[[`, character(1), "name")
    for (fn in fnames) {
      pg <- .http_get(sprintf("https://data.sec.gov/submissions/%s", fn))
      if (is.null(pg)) next
      pj <- tryCatch(fromJSON(pg, simplifyVector = TRUE),
                     error = function(e) NULL)
      if (is.null(pj)) next
      forms  <- c(forms, pj$form)
      fdates <- c(fdates, as.Date(pj$filingDate))
      if (min(fdates, na.rm = TRUE) <= pad_from) break
    }
  }

  is_annual <- !is.null(forms) &
    grepl("^10-K|^10-Q", forms) & !grepl("/A$", forms)
  f10 <- fdates[is_annual]

  # dated name table: current name valid (last former 'to')..today
  nm_tab <- data.table(name = sub$name,
                       nfrom = as.Date("1990-01-01"), nto = Sys.Date())
  former <- ""
  if (!is.null(sub$formerNames) && length(sub$formerNames)) {
    fn <- as.data.table(sub$formerNames)
    fn[, nfrom := as.Date(substr(from, 1, 10))]
    fn[, nto   := as.Date(substr(to, 1, 10))]
    former <- paste(fn$name, collapse = " | ")
    nm_tab <- rbind(
      data.table(name = sub$name, nfrom = max(fn$nto, na.rm = TRUE),
                 nto = Sys.Date()),
      fn[, .(name, nfrom, nto)])
  }
  nm_tab <- nm_tab[!is.na(name) & nchar(name) > 0]

  # name evidence: best token-overlap of any target name vs current+former;
  # period_ok = the best-matching name was ACTUALLY IN USE during the
  # membership window (rejects entities that shed the matching name before
  # the window, e.g. CIK 813828 was 'VIACOM INC' only until 2005)
  nscore <- 0; period_ok <- FALSE
  if (nrow(nm_tab)) {
    nm_tab[, score := vapply(name, function(cn)
      max(vapply(target_names, .name_score, numeric(1), candidate = cn)),
      numeric(1))]
    nscore <- max(nm_tab$score)
    best_rows <- nm_tab[score >= nscore - 1e-9]
    period_ok <- any(!is.na(best_rows$nfrom) & !is.na(best_rows$nto) &
                     best_rows$nfrom <= (win_to + 100) &
                     best_rows$nto >= (win_from - 550))
  }

  # era evidence: 10-K/10-Q filings inside the (padded) membership window
  pad_to <- win_to + 100
  n_in_win <- sum(!is.na(f10) & f10 >= pad_from & f10 <= pad_to)

  list(
    cik           = .pad_cik(cik),
    edgar_name    = if (is.null(sub$name)) NA_character_ else sub$name,
    sic           = if (is.null(sub$sic)) NA_character_ else as.character(sub$sic),
    sic_desc      = if (is.null(sub$sicDescription)) NA_character_ else sub$sicDescription,
    tickers       = paste(unlist(sub$tickers), collapse = ","),
    former_names  = former,
    filings_first = if (length(f10)) min(f10, na.rm = TRUE) else as.Date(NA),
    filings_last  = if (length(f10)) max(f10, na.rm = TRUE) else as.Date(NA),
    n_in_window   = n_in_win,
    name_score    = nscore,
    period_ok     = period_ok
  )
}

# =============================================================================
# main
# =============================================================================
# library mode: source(this_file) with .RESOLVER_LIB_ONLY <- TRUE defined to
# get the search/verify functions without running the census loop
if (exists(".RESOLVER_LIB_ONLY") && isTRUE(.RESOLVER_LIB_ONLY)) {
  message("oldcik_resolve_ciks: library mode -- functions loaded, main skipped")
} else {

message("oldcik_resolve_ciks: loading census + roster")
cen <- as.data.table(read_parquet("cache/lookups/oldcik_scoping_census.parquet"))
t2  <- cen[category == "NO_CIK" & feasibility == "RECOVERABLE"]
ros <- fread("data/sp500_constituents_.csv", na.strings = c("NA", ""))
work <- merge(
  t2[, .(ticker, occ = occurrence, from = as.Date(from), to = as.Date(to),
         n_member_dates)],
  ros[, .(ticker, occ = occurrence, name_when_added, name_when_removed)],
  by = c("ticker", "occ"), all.x = TRUE)
setorder(work, ticker, occ)
message(sprintf("  %d census rows to resolve", nrow(work)))

done <- if (file.exists(OUT_PATH)) {
  as.data.table(read_parquet(OUT_PATH))
} else NULL
if (!is.null(done)) {
  message(sprintf("  resuming: %d rows already resolved", nrow(done)))
}

results <- if (is.null(done)) list() else list(done)

for (i in seq_len(nrow(work))) {
  tk  <- work$ticker[i]; occ <- work$occ[i]
  if (!is.null(done) && nrow(done[ticker == tk & occ_ == occ])) next

  names_try <- unique(na.omit(c(work$name_when_removed[i],
                                work$name_when_added[i])))
  names_try <- names_try[nchar(names_try) > 0]
  win_from <- work$from[i]; win_to <- work$to[i]

  message(sprintf("[%d/%d] %s (occ %d) '%s' window %s..%s",
                  i, nrow(work), tk, occ, names_try[1], win_from, win_to))

  # -- collect candidates: browse-edgar on each name, then FTS fallback
  cands <- NULL
  match_via <- NA_character_
  for (nm in names_try) {
    cands <- search_browse_edgar(nm)
    if (!is.null(cands) && nrow(cands)) { match_via <- "browse-edgar"; break }
    # progressively shorten: drop trailing token (legal suffixes etc.)
    toks <- strsplit(nm, " +")[[1]]
    while (is.null(cands) || !nrow(cands)) {
      if (length(toks) <= 1) break
      toks <- toks[-length(toks)]
      cands <- search_browse_edgar(paste(toks, collapse = " "))
    }
    if (!is.null(cands) && nrow(cands)) { match_via <- "browse-edgar-short"; break }
  }
  if (is.null(cands) || !nrow(cands)) {
    for (nm in names_try) {
      cands <- search_fts(nm)
      if (!is.null(cands) && nrow(cands)) { match_via <- "fts"; break }
    }
  }

  row_base <- data.table(
    ticker = tk, occ_ = occ, search_name = names_try[1],
    win_from = win_from, win_to = win_to,
    n_member_dates = work$n_member_dates[i])

  if (is.null(cands) || !nrow(cands)) {
    res <- cbind(row_base, data.table(
      cik = NA_character_, edgar_name = NA_character_, sic = NA_character_,
      sic_desc = NA_character_, tickers = NA_character_,
      former_names = NA_character_, filings_first = as.Date(NA),
      filings_last = as.Date(NA), n_in_window = 0L, name_score = 0,
      match_via = "none", n_candidates = 0L, verdict = "NOT_FOUND"))
    results[[length(results) + 1L]] <- res
    rbindlist(results, fill = TRUE) |> write_parquet(OUT_PATH)
    next
  }

  # -- pre-rank candidates by name score, verify top ones via submissions
  cands[, pre_score := vapply(name, function(cn)
    if (is.na(cn)) 0 else
      max(vapply(names_try, .name_score, numeric(1), candidate = cn)),
    numeric(1))]
  setorder(cands, -pre_score)
  top <- head(cands[pre_score > 0.2], 5)
  # multi-entry browse results carry no names (pre_score 0 across the
  # board): verify a wider slate via submissions, which supplies names
  if (!nrow(top)) top <- head(cands, 12)

  evid <- list()
  for (j in seq_len(nrow(top))) {
    ev <- verify_candidate(top$cik[j], names_try, win_from, win_to)
    if (!is.null(ev)) evid[[length(evid) + 1L]] <- ev
  }

  if (!length(evid)) {
    res <- cbind(row_base, data.table(
      cik = NA_character_, edgar_name = NA_character_, sic = NA_character_,
      sic_desc = NA_character_, tickers = NA_character_,
      former_names = NA_character_, filings_first = as.Date(NA),
      filings_last = as.Date(NA), n_in_window = 0L, name_score = 0,
      match_via = match_via, n_candidates = nrow(cands),
      verdict = "NOT_FOUND"))
    results[[length(results) + 1L]] <- res
    rbindlist(results, fill = TRUE) |> write_parquet(OUT_PATH)
    next
  }

  ed <- rbindlist(lapply(evid, as.data.table))
  # acceptance: filings in window AND decent name match AND the matching
  # name was in use during the window
  ed[, accept := n_in_window >= 2 & name_score >= 0.5 & period_ok]
  n_acc <- sum(ed$accept)

  pick <- if (n_acc >= 1) ed[accept == TRUE][order(-name_score, -n_in_window)][1]
          else ed[order(-period_ok, -name_score, -n_in_window)][1]
  verdict <- if (n_acc == 1) "RESOLVED"
             else if (n_acc > 1) "AMBIGUOUS"
             else "NOT_FOUND"

  res <- cbind(row_base, pick[, .(cik, edgar_name, sic, sic_desc, tickers,
                                  former_names, filings_first, filings_last,
                                  n_in_window, name_score, period_ok)],
               data.table(match_via = match_via, n_candidates = nrow(cands),
                          verdict = verdict))
  results[[length(results) + 1L]] <- res
  message(sprintf("    -> %s %s '%s' sic=%s filings %s..%s in_win=%d score=%.2f",
                  verdict, pick$cik, pick$edgar_name, pick$sic,
                  pick$filings_first, pick$filings_last,
                  pick$n_in_window, pick$name_score))

  rbindlist(results, fill = TRUE) |> write_parquet(OUT_PATH)
}

final <- rbindlist(results, fill = TRUE)
message("\n==== resolution summary ====")
print(final[, .N, by = verdict])
message(sprintf("wrote %s (%d rows)", OUT_PATH, nrow(final)))

}  # end library-mode guard
