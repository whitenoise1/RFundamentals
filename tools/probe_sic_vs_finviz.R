# KNOWN_LIMITATIONS L1 bounding probe #1: SIC-vs-finviz sector
# disagreement across current constituents. The disagreement rate proxies
# how ambiguous/unstable sector assignment is for this universe -- runs
# BEFORE the single full rebuild (no rebuild required).
#
# Output: cache/lookups/sic_finviz_probe.parquet + console summary.
# Usage: Rscript tools/probe_sic_vs_finviz.R

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(httr); library(jsonlite)
})

.EDGAR_UA <- Sys.getenv("EDGAR_UA", "BSTAR/1.0 contact@email.com")
.RATE     <- as.numeric(Sys.getenv("EDGAR_RATE_SEC", "0.11"))
OUT <- "cache/lookups/sic_finviz_probe.parquet"

.pad_cik <- function(cik) formatC(as.integer(cik), width = 10, flag = "0")

# SIC major-group -> finviz-style sector. Coarse by design: the probe
# measures order-of-magnitude disagreement, not a perfect crosswalk.
sic_to_sector <- function(sic) {
  s <- suppressWarnings(as.integer(sic))
  if (is.na(s)) return(NA_character_)
  d2 <- s %/% 100
  # specific 4-digit ranges FIRST (a coarse major-group return above a
  # finer check makes the finer check unreachable)
  if (s >= 2830 & s < 2840) return("Healthcare")            # drugs
  if (s >= 3570 & s < 3580) return("Technology")            # computers
  if (s >= 3670 & s < 3680) return("Technology")            # semis
  if (s >= 3820 & s < 3830) return("Technology")            # instruments
  if (s >= 3840 & s < 3860) return("Healthcare")            # med devices
  if (s >= 5400 & s < 5500) return("Consumer Defensive")    # grocers
  if (s >= 5910 & s < 5920) return("Healthcare")            # drug stores
  if (s >= 7370 & s < 7380) return("Technology")            # software/IT
  if (s == 6798)            return("Real Estate")           # REITs
  if (s >= 6500 & s < 6600) return("Real Estate")
  # major groups
  if (s >= 100 & s < 1000)  return("Consumer Defensive")   # agriculture
  if (d2 %in% c(10, 12, 14)) return("Basic Materials")     # mining
  if (d2 == 13)             return("Energy")
  if (d2 %in% 15:17)        return("Industrials")           # construction
  if (d2 %in% c(20, 21))    return("Consumer Defensive")    # food, tobacco
  if (d2 %in% c(22, 23, 25, 31)) return("Consumer Cyclical")
  if (d2 %in% c(24, 26, 28, 32, 33)) return("Basic Materials")
  if (d2 == 27)             return("Communication Services") # publishing
  if (d2 %in% c(34, 35, 37)) return("Industrials")
  if (d2 == 36)             return("Technology")
  if (d2 == 38)             return("Healthcare")            # mostly medical
  if (d2 == 39)             return("Consumer Cyclical")
  if (d2 %in% c(40, 41, 42, 44, 45, 46, 47)) return("Industrials")
  if (d2 == 48)             return("Communication Services")
  if (d2 == 49)             return("Utilities")
  if (d2 %in% c(50, 51))    return("Industrials")           # wholesale
  if (d2 %in% 52:59)        return("Consumer Cyclical")     # retail
  if (d2 %in% 60:64)        return("Financial")
  if (d2 %in% c(65, 67))    return("Financial")
  if (d2 == 78 | d2 == 79)  return("Communication Services") # media/entertainment
  if (d2 == 80)             return("Healthcare")
  if (d2 == 82)             return("Consumer Defensive")    # education
  if (d2 %in% 70:77)        return("Consumer Cyclical")     # services
  if (d2 %in% 81:89)        return("Industrials")           # b2b services
  NA_character_
}

.http_get <- function(url) {
  for (a in 1:3) {
    r <- tryCatch(GET(url, add_headers(`User-Agent` = .EDGAR_UA), timeout(30)),
                  error = function(e) NULL)
    if (!is.null(r) && status_code(r) == 200) { Sys.sleep(.RATE); return(content(r, as = "text", encoding = "UTF-8")) }
    if (!is.null(r) && status_code(r) == 404) return(NA)
    Sys.sleep(2^a)
  }
  NULL
}

m <- as.data.table(read_parquet("cache/lookups/constituent_master.parquet"))
s <- as.data.table(read_parquet("cache/lookups/sector_industry.parquet"))
cur <- m[status == "ACTIVE" & !is.na(cik)]
cur <- merge(cur[, .(ticker, cik)], s[, .(ticker, finviz_sector = sector)],
             by = "ticker")
message(sprintf("probing %d current constituents", nrow(cur)))

done <- if (file.exists(OUT)) as.data.table(read_parquet(OUT)) else NULL
rows <- if (is.null(done)) list() else list(done)
for (i in seq_len(nrow(cur))) {
  tk <- cur$ticker[i]
  if (!is.null(done) && tk %in% done$ticker) next
  txt <- .http_get(sprintf("https://data.sec.gov/submissions/CIK%s.json",
                           .pad_cik(cur$cik[i])))
  sic <- NA_character_; sic_desc <- NA_character_
  if (!is.null(txt) && !identical(txt, NA)) {
    j <- tryCatch(fromJSON(txt, simplifyVector = TRUE), error = function(e) NULL)
    if (!is.null(j)) { sic <- as.character(j$sic); sic_desc <- j$sicDescription }
  }
  rows[[length(rows) + 1L]] <- data.table(
    ticker = tk, cik = cur$cik[i], sic = sic, sic_desc = sic_desc,
    sic_sector = sic_to_sector(sic), finviz_sector = cur$finviz_sector[i])
  if (i %% 50 == 0) {
    message(sprintf("  ...%d/%d", i, nrow(cur)))
    rbindlist(rows, fill = TRUE) |> write_parquet(OUT)
  }
}
final <- rbindlist(rows, fill = TRUE)
write_parquet(final, OUT)

ok <- final[!is.na(sic_sector) & !is.na(finviz_sector)]
agree <- ok[sic_sector == finviz_sector]
message(sprintf("\n==== L1 probe: SIC vs finviz sector ===="))
message(sprintf("mapped: %d/%d | agree: %d (%.1f%%) | disagree: %d (%.1f%%)",
                nrow(ok), nrow(final), nrow(agree),
                100 * nrow(agree) / nrow(ok),
                nrow(ok) - nrow(agree),
                100 * (1 - nrow(agree) / nrow(ok))))
print(ok[sic_sector != finviz_sector, .N,
         by = .(finviz_sector, sic_sector)][order(-N)][1:15])
