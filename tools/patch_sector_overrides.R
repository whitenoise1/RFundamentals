# Daily-update wave D2/A: apply the static sector override CSVs onto the
# live sector_industry.parquet WITHOUT a finviz re-scrape (a re-scrape of
# dead symbols risks poisoning from symbol reuse; the overrides win over
# any scrape anyway -- build_sector_industry applies the same helper on
# full rebuilds). SCD-aware and idempotent: only each ticker's CURRENT
# row (max valid_from) is corrected and new tickers are appended at the
# floor; accumulated dated history is never rewritten.
#
# Usage: Rscript tools/patch_sector_overrides.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })
source("R/sector_classifier.R")

path <- "cache/lookups/sector_industry.parquet"
stopifnot(file.exists(path))
sec <- as.data.table(read_parquet(path))
n0 <- nrow(sec)
has_scd <- "valid_from" %in% names(sec)

if (has_scd) {
  # split into current view (patched) + frozen history (untouched)
  sec[, valid_from := as.Date(valid_from)]
  setorder(sec, ticker, valid_from)
  cur_idx <- sec[, .I[.N], by = ticker]$V1
  current <- sec[cur_idx, .(ticker, sector, industry, source)]
  history <- sec[-cur_idx]
  cur_vf  <- sec[cur_idx, .(ticker, valid_from)]

  current <- apply_sector_overrides(current, add_missing = TRUE)
  current <- merge(current, cur_vf, by = "ticker", all.x = TRUE)
  # overrides are era-correct classifications: new tickers get the floor
  current[is.na(valid_from), valid_from := .SECTOR_VALID_FROM_FLOOR]
  sec <- rbind(history, current, use.names = TRUE)
  setorder(sec, ticker, valid_from)
  stopifnot(!anyDuplicated(sec[, .(ticker, valid_from)]))
} else {
  sec <- apply_sector_overrides(sec, add_missing = TRUE)
  stopifnot(!anyDuplicated(sec$ticker))
}

stopifnot(all(sec$sector %in% .FINVIZ_SECTORS))
write_parquet(sec, path)
message(sprintf("wrote %s: %d rows (was %d)", path, nrow(sec), n0))
