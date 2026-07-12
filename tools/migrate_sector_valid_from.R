# Daily-update wave D2/B: migrate cache/lookups/sector_industry.parquet
# to the SCD Type-2 schema by floor-stamping every existing row with
# valid_from = .SECTOR_VALID_FROM_FLOOR (best current estimate applied
# backward -- the documented KNOWN_LIMITATIONS L1 approximation).
# Value-neutral by construction: .sector_asof(dated, d) reproduces the
# pre-migration table for every date on the snapshot grid; the script
# verifies that before writing. Idempotent.
#
# Usage: Rscript tools/migrate_sector_valid_from.R

suppressPackageStartupMessages({ library(data.table); library(arrow) })
source("R/sector_classifier.R")

path <- "cache/lookups/sector_industry.parquet"
stopifnot(file.exists(path))
sec <- as.data.table(read_parquet(path))

if ("valid_from" %in% names(sec)) {
  message("already migrated: valid_from present, nothing to do")
  quit(status = 0)
}

dated <- copy(sec)
dated[, valid_from := .SECTOR_VALID_FROM_FLOOR]

# value-neutrality proof: the as-of view on the earliest and latest grid
# dates must reproduce the flat table exactly
for (d in c("2010-03-31", format(Sys.Date()))) {
  v <- .sector_asof(dated, d)[, .(ticker, sector, industry, source)]
  setorder(v, ticker)
  flat <- copy(sec)[, .(ticker, sector, industry, source)]
  setorder(flat, ticker)
  stopifnot(identical(v, flat))
}
message("value-neutrality verified: as-of view == flat table")

write_parquet(dated, path)
message(sprintf("migrated %s: %d rows floor-stamped %s",
                path, nrow(dated), .SECTOR_VALID_FROM_FLOOR))
