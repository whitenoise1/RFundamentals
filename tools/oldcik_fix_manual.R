# Old-CIK wave Tier 2: manual-queue fixes for the resolver's leftovers.
# Uses curated EDGAR search names (inverted conformed names, legal names
# behind brands) but keeps identity verification MECHANICAL: every
# candidate passes verify_candidate (dated-name period + filing era) and
# the row is only upgraded when the evidence passes the same acceptance
# rule as the automated path.
#
# Also promotes AMBIGUOUS rows whose top pick already passed acceptance
# (multiple candidates passed; the top pick is the intended entity --
# eyeballed individually, see comments).
# Usage: Rscript tools/oldcik_fix_manual.R

.RESOLVER_LIB_ONLY <- TRUE
source("tools/oldcik_resolve_ciks.R")
suppressPackageStartupMessages({ library(data.table); library(arrow) })

OUT_PATH <- "cache/lookups/oldcik_cik_resolution.parquet"
res <- as.data.table(read_parquet(OUT_PATH))

# --- curated searches for the NOT_FOUND queue + the X mis-pick ----------
# ticker -> the EDGAR conformed-name search string that actually finds it
CURATED <- list(
  APOL = "Apollo Group",              # renamed Apollo Education Group 2013
  BCR  = "Bard C R",                  # inverted conformed name
  DO   = "Diamond Offshore",          # crowded 'Diamond *' namespace
  DWDP = "DowDuPont",                 # merger entity, renamed DuPont de Nemours
  ESV  = "Ensco",                     # Ensco plc -> Valaris plc, same CIK
  ETFC = "E Trade Financial",         # brand 'E-Trade' vs legal E TRADE FINANCIAL
  HNZ  = "Heinz H J",                 # inverted conformed name
  HOT  = "Starwood Hotel",            # Starwood Hotels & Resorts Worldwide
  JCP  = c("Penney J C", "J C Penney", "JC Penney"),  # holding vs operating
  SBNY = "Signature Bank",            # suspected FDIC-only filer (FRC class)
  TSS  = "Total System Services",     # brand TSYS vs legal name
  WCG  = "WellCare Health Plans",
  WYN  = "Wyndham Worldwide",         # 2006 Cendant spinoff, later TNL
  X    = "United States Steel",       # >12 'UNITED STATES *' filers broke cap
  LXK  = "Lexmark International"      # holding vs operating entity split
)

for (tk in names(CURATED)) {
  i <- which(res$ticker == tk & res$verdict != "RESOLVED")
  if (!length(i)) { message(sprintf("%s: already resolved, skip", tk)); next }
  i <- i[1]
  win_from <- res$win_from[i]; win_to <- res$win_to[i]
  targets <- unique(na.omit(c(CURATED[[tk]], res$search_name[i])))

  cands <- NULL
  for (q in CURATED[[tk]]) {
    more <- search_browse_edgar(q)
    if (!is.null(more) && nrow(more)) cands <- rbind(cands, more)
  }
  if (!is.null(cands)) cands <- unique(cands, by = "cik")
  if (is.null(cands) || !nrow(cands)) {
    message(sprintf("%s: curated search EMPTY", tk)); next
  }
  evid <- list()
  for (j in seq_len(min(nrow(cands), 30))) {
    ev <- verify_candidate(cands$cik[j], targets, win_from, win_to)
    if (!is.null(ev)) evid[[length(evid) + 1L]] <- ev
  }
  if (!length(evid)) { message(sprintf("%s: no verifiable candidates", tk)); next }
  ed <- rbindlist(lapply(evid, as.data.table))
  ed[, accept := n_in_window >= 2 & name_score >= 0.5 & period_ok]
  acc <- ed[accept == TRUE][order(-name_score, -n_in_window)]

  if (!nrow(acc)) {
    message(sprintf("%s: NO candidate passed (best: %s %s score=%.2f in_win=%d per=%s)",
                    tk, ed[order(-name_score)][1, cik],
                    ed[order(-name_score)][1, edgar_name],
                    ed[order(-name_score)][1, name_score],
                    ed[order(-name_score)][1, n_in_window],
                    ed[order(-name_score)][1, period_ok]))
    print(ed[, .(cik, edgar_name, name_score, n_in_window, period_ok,
                 filings_first, filings_last)])
    next
  }
  pick <- acc[1]
  set(res, i, "cik", pick$cik)
  set(res, i, "edgar_name", pick$edgar_name)
  set(res, i, "sic", pick$sic)
  set(res, i, "sic_desc", pick$sic_desc)
  set(res, i, "tickers", pick$tickers)
  set(res, i, "former_names", pick$former_names)
  set(res, i, "filings_first", pick$filings_first)
  set(res, i, "filings_last", pick$filings_last)
  set(res, i, "n_in_window", pick$n_in_window)
  set(res, i, "name_score", pick$name_score)
  set(res, i, "period_ok", pick$period_ok)
  set(res, i, "match_via", "manual-curated")
  set(res, i, "verdict", "RESOLVED")
  message(sprintf("%s: RESOLVED -> %s %s (score=%.2f in_win=%d, %d passed)",
                  tk, pick$cik, pick$edgar_name, pick$name_score,
                  pick$n_in_window, nrow(acc)))
}

# --- promote AMBIGUOUS rows whose top pick is the intended entity --------
# Eyeballed 2026-07-11: multiple same-family candidates passed acceptance
# (e.g. Allergan Inc vs Allergan plc -- window discriminates; the pick
# already ranked by score+era). X was fixed above; the rest are correct.
PROMOTE <- c("AGN", "ANDV", "ARG", "EVHC", "HRS", "LSI", "RTN", "SAI", "TWX")
for (tk in PROMOTE) {
  i <- which(res$ticker == tk & res$verdict == "AMBIGUOUS")
  if (length(i)) {
    set(res, i[1], "verdict", "RESOLVED")
    set(res, i[1], "match_via", paste0(res$match_via[i[1]], "+promoted"))
    message(sprintf("%s: AMBIGUOUS promoted (pick %s %s)", tk,
                    res$cik[i[1]], res$edgar_name[i[1]]))
  }
}

write_parquet(res, OUT_PATH)
message("\n==== after manual fixes ====")
print(res[, .N, by = verdict])
print(res[verdict != "RESOLVED", .(ticker, search_name, cik, edgar_name)])
