# Old-CIK wave Tier 2: adversarial audit of the CIK resolution table
# (postmortem rule 6: verify identity per CIK before accepting).
#
# For every row of cache/lookups/oldcik_cik_resolution.parquet with a
# candidate CIK, re-fetch submissions/CIK.json and test:
#   A. name-period: the best-matching name (current or former) must have a
#      validity period overlapping the membership window. Catches entities
#      matched via a former name they had ALREADY SHED before the window
#      (e.g. CIK 813828 was 'VIACOM INC' only until 2005 -- in 2010-2019 it
#      was CBS Corp, so it cannot serve VIAB).
#   B. superset: matched-name tokens strictly contain extra tokens beyond
#      the target name -> subsidiary trap (AIRGAS CARBONIC vs AIRGAS).
#   C. era-tail: last in-scope 10-K/10-Q filed >= win_to - 400 days (the
#      entity filed until near its removal; removal usually = acquisition).
#   D. xbrl: companyfacts JSON exists and is non-trivial (HEAD-size check).
#
# Output: cache/lookups/oldcik_resolution_audit.parquet with per-check
# columns and audit_verdict (PASS | FLAG_<check> | NO_CIK).
# Usage: Rscript tools/oldcik_audit_resolution.R

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(jsonlite); library(httr)
})

.EDGAR_UA <- Sys.getenv("EDGAR_UA", "BSTAR/1.0 contact@email.com")
.RATE     <- as.numeric(Sys.getenv("EDGAR_RATE_SEC", "0.11"))
IN_PATH   <- "cache/lookups/oldcik_cik_resolution.parquet"
OUT_PATH  <- "cache/lookups/oldcik_resolution_audit.parquet"

.pad_cik <- function(cik) formatC(as.integer(cik), width = 10, flag = "0")

.http_get <- function(url, retries = 3L) {
  for (attempt in seq_len(retries)) {
    resp <- tryCatch(
      GET(url, add_headers(`User-Agent` = .EDGAR_UA), timeout(30)),
      error = function(e) NULL)
    if (!is.null(resp) && status_code(resp) == 200) {
      Sys.sleep(.RATE)
      return(content(resp, as = "text", encoding = "UTF-8"))
    }
    if (!is.null(resp) && status_code(resp) == 404) { Sys.sleep(.RATE); return(NA) }
    if (!is.null(resp) && status_code(resp) == 429) { Sys.sleep(2^attempt); next }
    if (attempt < retries) Sys.sleep(2^attempt)
  }
  NULL
}

.norm_name <- function(x) {
  x <- toupper(x)
  x <- gsub("/[A-Z ]*/?\\s*$", " ", x)  # EDGAR state suffixes: /KY/, /DE/, /NJ
  x <- gsub("[^A-Z0-9 ]", " ", x)
  x <- gsub("\\b(INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|COMPANIES|LTD|LIMITED|PLC|LP|LLC|HOLDINGS?|GROUP|THE|NEW|OLD|NV|SA|AG|ADR|CL|A|B)\\b",
            " ", x)
  trimws(gsub(" +", " ", x))
}
.name_tokens <- function(x) setdiff(strsplit(.norm_name(x), " ")[[1]], "")
.name_score <- function(target, candidate) {
  tt <- .name_tokens(target); ct <- .name_tokens(candidate)
  if (!length(tt) || !length(ct)) return(0)
  length(intersect(tt, ct)) / length(union(tt, ct))
}

audit_one <- function(cik, target_names, win_from, win_to) {
  txt <- .http_get(sprintf("https://data.sec.gov/submissions/CIK%s.json",
                           .pad_cik(cik)))
  if (is.null(txt) || identical(txt, NA)) {
    return(list(np_ok = NA, matched_name = NA_character_,
                matched_from = as.Date(NA), matched_to = as.Date(NA),
                superset_ok = NA, era_tail_ok = NA, xbrl_ok = NA,
                best_score = NA_real_))
  }
  sub <- fromJSON(txt, simplifyVector = TRUE)

  # name table: current name valid (last former 'to') .. today; formers dated
  nm_tab <- data.table(name = sub$name, from = as.Date(NA), to = as.Date(NA))
  if (!is.null(sub$formerNames) && length(sub$formerNames)) {
    fn <- as.data.table(sub$formerNames)
    fn[, from := as.Date(substr(from, 1, 10))]
    fn[, to   := as.Date(substr(to, 1, 10))]
    nm_tab <- rbind(data.table(name = sub$name,
                               from = max(fn$to, na.rm = TRUE),
                               to = Sys.Date()),
                    fn[, .(name, from, to)])
  } else {
    nm_tab[, `:=`(from = as.Date("1990-01-01"), to = Sys.Date())]
  }

  # best-matching name row vs any target name
  nm_tab[, score := vapply(name, function(cn)
    max(vapply(target_names, .name_score, numeric(1), candidate = cn)),
    numeric(1))]
  best <- nm_tab[order(-score)][1]

  # A. does the best name's validity overlap the membership window?
  np_ok <- isTRUE(best$score >= 0.5 &&
                  !is.na(best$from) && !is.na(best$to) &&
                  best$from <= win_to && best$to >= win_from)

  # B. superset trap: matched name has extra tokens beyond every target name
  extra <- vapply(target_names, function(tn)
    length(setdiff(.name_tokens(best$name), .name_tokens(tn))), integer(1))
  superset_ok <- any(extra == 0) || best$score >= 0.99

  # C. era tail: filed until near removal
  forms  <- sub$filings$recent$form
  fdates <- as.Date(sub$filings$recent$filingDate)
  f10 <- fdates[grepl("^10-K|^10-Q|^20-F", forms) & !grepl("/A$", forms)]
  era_tail_ok <- length(f10) > 0 &&
    max(f10, na.rm = TRUE) >= (win_to - 400)

  # D. XBRL companyfacts exists
  cf <- .http_get(sprintf(
    "https://data.sec.gov/api/xbrl/companyfacts/CIK%s.json", .pad_cik(cik)))
  xbrl_ok <- !is.null(cf) && !identical(cf, NA) && nchar(cf) > 5000

  list(np_ok = np_ok, matched_name = best$name,
       matched_from = best$from, matched_to = best$to,
       superset_ok = superset_ok, era_tail_ok = era_tail_ok,
       xbrl_ok = xbrl_ok, best_score = best$score)
}

# =============================================================================
res <- as.data.table(read_parquet(IN_PATH))
ros <- fread("data/sp500_constituents_.csv", na.strings = c("NA", ""))
res <- merge(res,
             ros[, .(ticker, occ_ = occurrence, name_when_added,
                     name_when_removed)],
             by = c("ticker", "occ_"), all.x = TRUE)

done <- if (file.exists(OUT_PATH)) as.data.table(read_parquet(OUT_PATH)) else NULL
out <- if (is.null(done)) list() else list(done)

for (i in seq_len(nrow(res))) {
  r <- res[i]
  if (!is.null(done) && nrow(done[ticker == r$ticker & occ_ == r$occ_])) next

  if (is.na(r$cik)) {
    row <- cbind(r[, .(ticker, occ_, search_name, cik, edgar_name, sic_desc,
                       win_from, win_to, n_member_dates, verdict)],
                 data.table(np_ok = NA, matched_name = NA_character_,
                            matched_from = as.Date(NA), matched_to = as.Date(NA),
                            superset_ok = NA, era_tail_ok = NA, xbrl_ok = NA,
                            best_score = NA_real_, audit_verdict = "NO_CIK"))
    out[[length(out) + 1L]] <- row
    rbindlist(out, fill = TRUE) |> write_parquet(OUT_PATH)
    next
  }

  targets <- unique(na.omit(c(r$name_when_removed, r$name_when_added)))
  a <- audit_one(r$cik, targets, r$win_from, r$win_to)

  flags <- c(
    if (!isTRUE(a$np_ok))       "name-period",
    if (!isTRUE(a$superset_ok)) "superset",
    if (!isTRUE(a$era_tail_ok)) "era-tail",
    if (!isTRUE(a$xbrl_ok))     "xbrl")
  av <- if (r$verdict != "RESOLVED") paste0("UNRESOLVED(", r$verdict, ")")
        else if (!length(flags)) "PASS"
        else paste0("FLAG:", paste(flags, collapse = "+"))

  row <- cbind(r[, .(ticker, occ_, search_name, cik, edgar_name, sic_desc,
                     win_from, win_to, n_member_dates, verdict)],
               as.data.table(a), data.table(audit_verdict = av))
  out[[length(out) + 1L]] <- row
  message(sprintf("[%d/%d] %-6s %s %-35s -> %s", i, nrow(res), r$ticker,
                  r$cik, substr(r$edgar_name, 1, 35), av))
  rbindlist(out, fill = TRUE) |> write_parquet(OUT_PATH)
}

final <- rbindlist(out, fill = TRUE)
message("\n==== audit summary ====")
print(final[, .N, by = audit_verdict][order(-N)])
message(sprintf("wrote %s (%d rows)", OUT_PATH, nrow(final)))
