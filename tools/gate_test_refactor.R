# ============================================================================
# gate_test_refactor.R
# ============================================================================
# End-to-end gate test for this session's methodology refactor:
#   (1) C-3 NA-vs-zero split in .derive_quantities
#   (2) positive-denominator guards removed (sign preservation)
#   (3) cross-sectional [p2.5, p97.5] winsorization
#   (4) expanding-window median/MAD standardization
#   (5) default clip c(-5, 5)
#
# Reads the already-persisted diagnostic snapshots in cache/snapshots_diag/
# (no EDGAR fetches needed). Each assertion is labeled PASS/FAIL with a
# short message. Exit code = number of failed assertions.
#
# Usage:  Rscript tools/gate_test_refactor.R
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})
source("R/indicator_compute.R")
source("R/pit_assembler.R")

DIAG_DIR <- file.path("cache", "snapshots_diag")

.n_pass <- 0L
.n_fail <- 0L
gate <- function(label, cond, detail = "") {
  ok <- tryCatch(isTRUE(cond), error = function(e) FALSE)
  if (ok) {
    .n_pass <<- .n_pass + 1L
    message(sprintf("  PASS  %s", label))
  } else {
    .n_fail <<- .n_fail + 1L
    message(sprintf("  FAIL  %s  %s", label, detail))
  }
}


# ---- Load persisted snapshots ----
files_raw <- sort(list.files(DIAG_DIR, pattern = "^pit_.*_raw\\.parquet$",
                             full.names = TRUE))
files_zs  <- sort(list.files(DIAG_DIR, pattern = "^pit_.*_zscore\\.parquet$",
                             full.names = TRUE))
if (length(files_raw) < 4 || length(files_zs) < 4) {
  stop("gate_test_refactor: need >= 4 persisted diag snapshots in ",
       DIAG_DIR, "; run tools/diag_zscore_tails.R first")
}

raw_list <- lapply(files_raw, function(f) as.data.table(arrow::read_parquet(f)))
zs_list  <- lapply(files_zs,  function(f) as.data.table(arrow::read_parquet(f)))

raw_last <- raw_list[[length(raw_list)]]
zs_last  <- zs_list [[length(zs_list)]]

message(sprintf("\n[setup] loaded %d raw + %d zscore snapshots",
                length(raw_list), length(zs_list)))


# =============================================================================
# Gate 1 -- C-3 NA propagation on real data
# =============================================================================
message("\n=== Gate 1: C-3 NA propagation on real data ===")

# Some S&P 500 tickers will have at least one missing XBRL concept in the
# chain. Under C-3 we expect ebitda / fcf / net_debt / total_debt to be NA
# for those tickers rather than silently zero-filled.
pool_ebitda <- unlist(lapply(raw_list, function(dt) dt$enterprise_value))
n_ebitda_na <- sum(is.na(pool_ebitda))
gate("at least one ticker has NA enterprise_value (C-3 on net_debt cascade)",
     n_ebitda_na > 0,
     sprintf("(observed %d NAs across %d rows)",
             n_ebitda_na, length(pool_ebitda)))

# Before C-3, total_debt was always populated (NA->0). After C-3, total_debt
# can be NA when both LTD and STD are absent. Look for at least one case.
# We re-derive total_debt presence from the net_debt propagation path.
# (Raw snapshots don't store total_debt directly -- it feeds ev_ebitda etc.)
pool_ev <- unlist(lapply(raw_list, function(dt) dt$enterprise_value))
pool_ev_ebitda <- unlist(lapply(raw_list, function(dt) dt$ev_ebitda))
# If ev is NA, ev_ebitda should be NA (consistency check).
both_na <- is.na(pool_ev) & is.na(pool_ev_ebitda)
only_ev_na <- is.na(pool_ev) & !is.na(pool_ev_ebitda)
gate("ev NA -> ev_ebitda NA (cascade consistency)",
     sum(only_ev_na) == 0,
     sprintf("(%d inconsistent rows)", sum(only_ev_na)))


# =============================================================================
# Gate 2 -- Sign preservation (guard removal)
# =============================================================================
message("\n=== Gate 2: sign preservation (negative denominators) ===")

# Pool raw multiples across all snapshots and check at least one negative
# value exists for each sign-guard-removed indicator. Financial rows excluded
# to avoid incidental zeros for indicators that are financial-NA by design.
pool_signed <- function(col) {
  v <- unlist(lapply(raw_list, function(dt) {
    vals <- dt[[col]]
    if (is.null(vals)) return(numeric(0))
    vals[dt$sector %in% "Financial"] <- NA_real_
    vals
  }))
  v[!is.na(v)]
}

for (col in c("pb", "pfcf", "ev_ebitda", "roe", "roic",
              "debt_equity", "net_debt_ebitda", "payout_ratio")) {
  pv <- pool_signed(col)
  n_neg <- sum(pv < 0)
  gate(sprintf("%s has at least one negative value in the cross-section", col),
       n_neg > 0,
       sprintf("(observed %d negatives of %d values)", n_neg, length(pv)))
}


# =============================================================================
# Gate 3 -- Z-score bounds under default clip c(-5, 5)
# =============================================================================
# The persisted cache/snapshots_diag/*_zscore.parquet files were written by
# tools/diag_zscore_tails.R with clip = NULL on purpose (so the diagnostic
# could see the unclipped tails). For this gate we recompute z in memory
# with the current default clip c(-5, 5) and check the output is bounded.
message("\n=== Gate 3: z-score bounds under default clip c(-5, 5) ===")

ind_names <- get_indicator_names()

zs_default <- zscore_expanding_window(
  raw_dt          = raw_last,
  history_dt_list = head(raw_list, length(raw_list) - 1L)
)  # clip defaults to c(-5, 5)
all_z <- unlist(zs_default[, ..ind_names])
all_z <- all_z[!is.na(all_z)]

gate("all z-scores >= -5 (default clip)",
     all(all_z >= -5 - 1e-10),
     sprintf("(min = %.4f)", min(all_z)))

gate("all z-scores <= 5 (default clip)",
     all(all_z <=  5 + 1e-10),
     sprintf("(max = %.4f)", max(all_z)))

# At least one value must be non-extreme, otherwise everything's pinned
# (would indicate broken standardization).
mid_count <- sum(abs(all_z) < 1)
gate("at least 30% of z-scores lie inside |z| < 1 (sanity)",
     mid_count / length(all_z) > 0.30,
     sprintf("(fraction = %.3f)", mid_count / length(all_z)))

# Confirm that some values actually reach the clip bound (otherwise the
# clip is inactive and this gate is vacuously passing).
at_bound <- sum(abs(all_z) > 4.999)
gate("some values reach clip bound (clip is active, not trivially below)",
     at_bound > 0,
     sprintf("(count |z| > 4.999 = %d)", at_bound))


# =============================================================================
# Gate 4 -- Expanding-window effect is detectable
# =============================================================================
message("\n=== Gate 4: expanding-window vs per-snapshot differ ===")

# Compute per-snapshot z for the last snapshot, compare to expanding-window z.
# For a reasonably-stable indicator like gross_margin, the two should
# differ only modestly. For a heavy-tailed one like peg, they should differ
# meaningfully (pool median/MAD gets dragged by history).
raw_last_copy <- copy(raw_last)
zs_single <- zscore_cross_section(
  raw_last_copy[, setdiff(names(raw_last_copy), c("date", "industry")),
                with = FALSE],
  clip = c(-5, 5)
)
zs_expand <- zscore_expanding_window(raw_last, history_dt_list =
                                     head(raw_list, length(raw_list) - 1L),
                                     clip = c(-5, 5))

# Align on ticker
cmp <- merge(
  zs_single[, .(ticker, peg_single = peg, gm_single = gross_margin)],
  zs_expand[, .(ticker, peg_expand = peg, gm_expand = gross_margin)],
  by = "ticker"
)
abs_diff_peg <- mean(abs(cmp$peg_single - cmp$peg_expand), na.rm = TRUE)
abs_diff_gm  <- mean(abs(cmp$gm_single  - cmp$gm_expand),  na.rm = TRUE)

# Expanding-window should produce a different answer than per-snapshot
# (unless all history has the same distribution -- unlikely).
gate("expanding-window peg differs from per-snapshot peg (mean |diff| > 0.05)",
     !is.na(abs_diff_peg) && abs_diff_peg > 0.05,
     sprintf("(mean |diff| = %.4f)", abs_diff_peg))

# But the two standardizations should still be highly correlated -- same
# sign on the same firm, similar rank.
corr_peg <- cor(cmp$peg_single, cmp$peg_expand, use = "complete.obs")
gate("expanding-window peg correlates highly with per-snapshot peg (r > 0.9)",
     !is.na(corr_peg) && corr_peg > 0.90,
     sprintf("(r = %.3f)", corr_peg))

# Gross margin, a more stable indicator, should differ less dramatically.
corr_gm <- cor(cmp$gm_single, cmp$gm_expand, use = "complete.obs")
gate("expanding-window gross_margin stable (r > 0.95 with per-snapshot)",
     !is.na(corr_gm) && corr_gm > 0.95,
     sprintf("(r = %.3f)", corr_gm))


# =============================================================================
# Gate 5 -- No silent regression in success counts
# =============================================================================
message("\n=== Gate 5: snapshot-level success counts ===")

for (i in seq_along(raw_list)) {
  n <- nrow(raw_list[[i]])
  d <- if ("date" %in% names(raw_list[[i]])) {
    raw_list[[i]]$date[1]
  } else NA
  gate(sprintf("snapshot %s has >= 450 tickers (observed %d)", d, n),
       n >= 450)
}


# =============================================================================
# Summary
# =============================================================================
message(sprintf("\n=== gate_test_refactor: %d passed, %d failed ===",
                .n_pass, .n_fail))

if (.n_fail > 0) quit(status = .n_fail)
