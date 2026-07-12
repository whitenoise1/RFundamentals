# Daily-update wave D2/A: apply the static sector override CSVs onto the
# live sector_industry.parquet WITHOUT a finviz re-scrape (a re-scrape of
# dead symbols risks poisoning from symbol reuse; the overrides win over
# any scrape anyway -- build_sector_industry step 4c has the same
# semantics for full rebuilds). Idempotent: re-running is a no-op.
#
# Usage: Rscript tools/patch_sector_overrides.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })
source("R/sector_classifier.R")

path <- "cache/lookups/sector_industry.parquet"
stopifnot(file.exists(path))
sec <- as.data.table(read_parquet(path))
n0 <- nrow(sec)

for (ov_csv in .SECTOR_OVERRIDE_CSVS) {
  if (!file.exists(ov_csv)) next
  ov <- fread(ov_csv)
  bad <- setdiff(ov$sector, .FINVIZ_SECTORS)
  if (length(bad)) stop(sprintf("%s: non-finviz sector strings: %s",
                                ov_csv, paste(bad, collapse = ", ")))
  n_new <- 0L; n_repl <- 0L
  for (i in seq_len(nrow(ov))) {
    tk <- ov$ticker[i]
    if (tk %in% sec$ticker) {
      row <- sec[ticker == tk]
      if (row$sector[1] != ov$sector[i] || row$industry[1] != ov$industry[i] ||
          row$source[1] != "override") {
        sec[ticker == tk, `:=`(sector = ov$sector[i], industry = ov$industry[i],
                               source = "override")]
        n_repl <- n_repl + 1L
      }
    } else {
      sec <- rbind(sec, data.table(ticker = tk, sector = ov$sector[i],
                                   industry = ov$industry[i],
                                   source = "override"), fill = TRUE)
      n_new <- n_new + 1L
    }
  }
  message(sprintf("%s: %d added, %d replaced", basename(ov_csv), n_new, n_repl))
}

stopifnot(!anyDuplicated(sec$ticker))
stopifnot(all(sec$sector %in% .FINVIZ_SECTORS))
write_parquet(sec, path)
message(sprintf("wrote %s: %d rows (was %d)", path, nrow(sec), n0))
