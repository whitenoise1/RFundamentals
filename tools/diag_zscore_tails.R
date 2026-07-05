# ============================================================================
# diag_zscore_tails.R
# ============================================================================
# Diagnostic for the task-#5 decision: do we need the [-3, 3] clip in the
# expanding-window z-score path?
#
# Builds 8 quarterly snapshots from the existing cache, computes expanding-
# window z-scores both with and without the clip, and tabulates:
#   - per-indicator |z| tail quantiles (unclipped vs clipped)
#   - pooled |z| distribution under both regimes
#   - tail symmetry
#   - fraction of values saturating the clip
#
# Input:  cache/snapshots_diag/, cache/fundamentals/, cache/prices/, cache/lookups/
# Output: stdout tables; cache/lookups/zscore_tails_<date>.parquet
#
# Snapshots are persisted to cache/snapshots_diag/ (not tempdir) so re-runs
# reuse existing raw parquets. Delete that directory to force a rebuild.
#
# Usage:  Rscript tools/diag_zscore_tails.R
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

source("R/constituent_master.R")
source("R/sector_classifier.R")
source("R/fundamental_fetcher.R")
source("R/indicator_compute.R")
source("R/pit_assembler.R")


# ---- Config ----
SNAPSHOT_DATES <- as.Date(c(
  "2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31",
  "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"
))
DIAG_DIR <- file.path("cache", "snapshots_diag")
OUT_PARQUET <- file.path("cache", "lookups",
                         sprintf("zscore_tails_%s.parquet", Sys.Date()))


# ---- Build or load snapshots ----
dir.create(DIAG_DIR, showWarnings = FALSE, recursive = TRUE)
master_dt <- as.data.table(
  arrow::read_parquet("cache/lookups/constituent_master.parquet"))
sector_dt <- as.data.table(
  arrow::read_parquet("cache/lookups/sector_industry.parquet"))

existing <- list.files(DIAG_DIR, pattern = "^pit_.*_raw\\.parquet$")
message(sprintf("diag_zscore_tails: %d raw snapshots already in %s",
                length(existing), DIAG_DIR))

raw_list <- list()
for (d in SNAPSHOT_DATES) {
  d <- as.Date(d)
  raw_path <- file.path(DIAG_DIR, sprintf("pit_%s_raw.parquet", d))
  if (file.exists(raw_path)) {
    raw_list[[as.character(d)]] <-
      as.data.table(arrow::read_parquet(raw_path))
    next
  }
  message(sprintf("  building snapshot %s", d))
  res <- tryCatch(
    assemble_snapshot(
      snapshot_date   = d,
      master_path     = "cache/lookups/constituent_master.parquet",
      sector_path     = "cache/lookups/sector_industry.parquet",
      fund_dir        = "cache/fundamentals",
      price_cache_dir = "cache/prices",
      output_dir      = DIAG_DIR,
      prefetch_prices = FALSE,
      zscore_mode     = "expanding_window",
      zscore_clip     = NULL,
      .master_dt      = master_dt,
      .sector_dt      = sector_dt
    ),
    error = function(e) { message("    ", e$message); NULL }
  )
  if (is.null(res)) next
  raw_list[[as.character(d)]] <- res$raw
}

if (length(raw_list) == 0) stop("no snapshots available; aborting")


# ---- Recompute expanding-window z-scores for both regimes ----
# Reuses the persisted raw snapshots. Fast -- no recomputation of indicators
# or re-reads from EDGAR. Runs in seconds.
compute_z <- function(clip) {
  out <- list()
  for (i in seq_along(SNAPSHOT_DATES)) {
    d <- as.Date(SNAPSHOT_DATES[i])
    key <- as.character(d)
    if (is.null(raw_list[[key]])) next
    prior <- raw_list[
      names(raw_list) %in% as.character(SNAPSHOT_DATES[seq_len(i - 1L)])]
    z <- zscore_expanding_window(
      raw_dt          = raw_list[[key]],
      history_dt_list = unname(prior),
      clip            = clip
    )
    out[[key]] <- z
  }
  out
}

message("\ncomputing expanding-window z (unclipped) ...")
zs_none <- compute_z(clip = NULL)
message("computing expanding-window z (clip = c(-3, 3)) ...")
zs_clip <- compute_z(clip = c(-3, 3))


# ---- Helpers ----
pool_col <- function(lst, col) {
  v <- unlist(lapply(lst, function(dt) {
    if (is.null(dt) || !(col %in% names(dt))) return(numeric(0))
    dt[[col]]
  }))
  v[!is.na(v)]
}

ind_names <- get_indicator_names()

per_indicator <- function(zlist, label) {
  rows <- list()
  for (nm in ind_names) {
    v <- pool_col(zlist, nm)
    if (length(v) == 0L) next
    av <- abs(v)
    rows[[nm]] <- data.table(
      regime    = label,
      indicator = nm,
      n         = length(v),
      p50       = as.numeric(quantile(av, 0.50)),
      p90       = as.numeric(quantile(av, 0.90)),
      p99       = as.numeric(quantile(av, 0.99)),
      p999      = as.numeric(quantile(av, 0.999)),
      max_abs   = max(av),
      n_at_3    = sum(av >= 3 - 1e-10),
      pct_at_3  = sum(av >= 3 - 1e-10) / length(v) * 100
    )
  }
  rbindlist(rows, fill = TRUE)
}


# ---- Per-indicator: side-by-side ----
summary_none <- per_indicator(zs_none, "unclipped")
summary_clip <- per_indicator(zs_clip, "clip[-3,3]")

# Wide side-by-side: show p999 and pct_at_3 before / after
wide <- merge(
  summary_none[, .(indicator, n_raw = n,
                   p999_none = p999, max_none = max_abs,
                   pct_bound_none = pct_at_3)],
  summary_clip[, .(indicator,
                   p999_clip = p999, max_clip = max_abs,
                   pct_bound_clip = pct_at_3)],
  by = "indicator"
)
setorder(wide, -p999_none)


# ---- Persist + print ----
if (!dir.exists(dirname(OUT_PARQUET))) {
  dir.create(dirname(OUT_PARQUET), recursive = TRUE, showWarnings = FALSE)
}
arrow::write_parquet(wide, OUT_PARQUET)
message(sprintf("\nwrote: %s", OUT_PARQUET))

message("\n=== per-indicator |z| tails: UNCLIPPED vs CLIPPED[-3,3] ===")
message("  pct_bound_clip = fraction of values saturating the clip bound.")
print(wide, nrows = 60, digits = 3)


# ---- Pooled distribution: unclipped vs clipped ----
pool_abs_none <- unlist(lapply(ind_names,
                               function(nm) pool_col(zs_none, nm)))
pool_abs_none <- abs(pool_abs_none)
pool_signed_none <- unlist(lapply(ind_names,
                                  function(nm) pool_col(zs_none, nm)))

pool_abs_clip <- unlist(lapply(ind_names,
                               function(nm) pool_col(zs_clip, nm)))
pool_abs_clip <- abs(pool_abs_clip)

message("\n=== pooled |z| distribution ===")
cmp <- data.table(
  stat = c("n",   "p50", "p90", "p99", "p99.9", "max",
           "pct_|z|>=3"),
  unclipped = c(
    length(pool_abs_none),
    as.numeric(quantile(pool_abs_none, 0.50)),
    as.numeric(quantile(pool_abs_none, 0.90)),
    as.numeric(quantile(pool_abs_none, 0.99)),
    as.numeric(quantile(pool_abs_none, 0.999)),
    max(pool_abs_none),
    sum(pool_abs_none >= 3) / length(pool_abs_none) * 100
  ),
  clip_m3_p3 = c(
    length(pool_abs_clip),
    as.numeric(quantile(pool_abs_clip, 0.50)),
    as.numeric(quantile(pool_abs_clip, 0.90)),
    as.numeric(quantile(pool_abs_clip, 0.99)),
    as.numeric(quantile(pool_abs_clip, 0.999)),
    max(pool_abs_clip),
    sum(pool_abs_clip >= 3 - 1e-10) / length(pool_abs_clip) * 100
  )
)
print(cmp, digits = 4)


# ---- Histogram of |z| bins for the clipped regime ----
# How are z values distributed across coarse bins under the clip?
bins <- c(-Inf, 0.5, 1, 1.5, 2, 2.5, 2.99, Inf)
lab_bins <- c("[0,0.5)", "[0.5,1)", "[1,1.5)", "[1.5,2)",
              "[2,2.5)", "[2.5,3)", "=3 (saturated)")
hist_none <- table(cut(pool_abs_none, breaks = bins, right = FALSE,
                        labels = lab_bins))
hist_clip <- table(cut(pool_abs_clip, breaks = bins, right = FALSE,
                        labels = lab_bins))
hist_dt <- data.table(
  bin        = lab_bins,
  unclipped  = as.integer(hist_none),
  unclipped_pct = as.integer(hist_none) / length(pool_abs_none) * 100,
  clipped    = as.integer(hist_clip),
  clipped_pct = as.integer(hist_clip) / length(pool_abs_clip) * 100
)

message("\n=== |z| histogram (abs) -- coverage by bin ===")
print(hist_dt, digits = 3)


# ---- Signed tail symmetry before clipping ----
message("\n=== signed tail counts (unclipped) ===")
n_high <- sum(pool_signed_none >  3, na.rm = TRUE)
n_low  <- sum(pool_signed_none < -3, na.rm = TRUE)
message(sprintf("  z >  3  : %d (%.3f%%)",
                n_high, n_high / length(pool_abs_none) * 100))
message(sprintf("  z < -3  : %d (%.3f%%)",
                n_low,  n_low  / length(pool_abs_none) * 100))
message(sprintf("  pos/neg : %.2f (1 = symmetric)",
                if (n_low == 0) NA else n_high / n_low))


# ---- Closing note ----
message("\n=== notes ===")
message("  Under clip = c(-3, 3):")
message(sprintf("    - %.2f%% of values are pinned at |z| = 3.",
                sum(pool_abs_clip >= 3 - 1e-10) / length(pool_abs_clip) * 100))
message(sprintf("    - p99.9 = %.2f (from %.2f unclipped).",
                as.numeric(quantile(pool_abs_clip, 0.999)),
                as.numeric(quantile(pool_abs_none, 0.999))))
message("  Clipping flattens the information-rich right tail of size /")
message("  valuation / growth indicators to a single value. Rank is")
message("  preserved up to the clip; values inside [-3, 3] are unchanged.")
