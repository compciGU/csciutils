#' Format value labels for a single survey variable
#'
#' @param var_values A vector from a survey dataset
#' @param var_name Character. Variable name, used only for console messages.
#' @param wave_name Character. Study wave name, used only for console messages.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A named list with `var_label`, `label_formatted`, `code_formatted`.
#' @keywords internal
format_value_labels <- function(var_values, var_name, wave_name, verbose = TRUE) {

  # extract variable label (from Stata/haven metadata); use NA if absent
  var_label <- attr(var_values, "label")
  if (is.null(var_label)) var_label <- NA_character_

  # extract Stata value labels if present
  labels <- attr(var_values, "labels", exact = TRUE)

  # get unique observed values as strings, drop NAs, sort numerically
  labels_obs <- unique(as.character(unclass(var_values)))
  labels_obs <- labels_obs[!is.na(labels_obs)]
  labels_obs <- labels_obs[order(as.numeric(labels_obs))]

  # Case 1: no Stata labels — fall back to observed values --------------------
  if (is.null(labels) || all(is.na(labels))) {

    if (verbose)
      cat("NOTE: No Stata value labels found for variable:", var_name,
          "in dataset:", wave_name, "- using observed values instead.\n")

    label_formatted <- paste0("[", labels_obs, "] ", labels_obs)
    code_formatted  <- labels_obs

    # Case 2: Stata labels exist ------------------------------------------------
  } else {

    # sort Stata labels by their numeric code
    labels      <- labels[order(as.numeric(unname(labels)))]
    stata_codes <- as.character(unname(labels))

    # Case 2a: more observed values than Stata labels — merge both ------------
    if (length(unique(labels_obs)) > length(unique(stata_codes))) {

      if (verbose)
        cat("NOTE: More observed values than Stata labels for variable:", var_name,
            "in dataset:", wave_name, "- combining observed values with Stata labels.\n")

      # observed values not covered by Stata labels
      extra_obs  <- setdiff(labels_obs, stata_codes)
      obs_fmt    <- paste0("[", extra_obs, "] ", extra_obs)

      # strip leading numeric codes from Stata label text (e.g. "1. Yes" -> "Yes")
      label_text <- gsub("^\\s*[-+]?\\d+(?:\\.\\d+)?\\s*\\.\\s*", "", names(labels))
      stata_fmt  <- paste0("[", stata_codes, "] ", label_text)

      # combine and re-sort by numeric code
      label_formatted <- c(obs_fmt, stata_fmt)
      codes           <- as.numeric(sub("^\\[(-?\\d+)\\].*", "\\1", label_formatted))
      label_formatted <- label_formatted[order(codes)]
      code_formatted  <- c(labels_obs, setdiff(stata_codes, labels_obs))

      # Case 2b: Stata labels fully cover observed values — use labels as-is ----
    } else {

      if (verbose)
        cat("NOTE: Using Stata value labels for variable:", var_name,
            "in dataset:", wave_name, "- observed values fully covered by labels.\n")

      label_text      <- gsub("^\\s*[-+]?\\d+(?:\\.\\d+)?\\s*\\.\\s*", "", names(labels))
      label_formatted <- paste0("[", stata_codes, "] ", label_text)
      code_formatted  <- stata_codes
    }
  }

  list(
    var_label       = var_label,
    label_formatted = label_formatted,
    code_formatted  = code_formatted
  )
}
#' Count observed values aligned to a vector of value codes
#'
#' Tabulates `var_values` and aligns the counts to `code_formatted`, returning
#' zero for any code not observed in the data.
#'
#' @param var_values A survey variable vector.
#' @param code_formatted Character vector of value codes to align counts to.
#' @param label_formatted Character vector of formatted labels, the same length
#'   as `code_formatted`, used to build the output strings.
#'
#' @return Character vector of strings in the form `"[1] Yes: 42"`.
#' @keywords internal
count_obs <- function(var_values, code_formatted, label_formatted) {
  tab       <- table(as.character(unclass(var_values)), useNA = "ifany")
  n_obs_raw <- tab[match(code_formatted, names(tab))]
  n_obs_raw[is.na(n_obs_raw)] <- 0
  paste0(label_formatted, ": ", as.integer(n_obs_raw))
}

#' Strip leading numeric codes from Stata label text
#'
#' Removes patterns like `"1. "`, `"-2. "`, or `"3.5. "` from the start of
#' Stata value label strings, e.g. `"1. Yes"` becomes `"Yes"`.
#'
#' @param x Character vector of raw Stata label names.
#' @return Character vector with numeric prefixes removed.
#' @keywords internal
.strip_stata_prefix <- function(x) {
  gsub("^\\s*[-+]?\\d+(?:\\.\\d+)?\\s*\\.\\s*", "", x, perl = TRUE)
}

#' Merge observed values with Stata labels when observed values outnumber labels
#'
#' Combines extra observed values (not covered by Stata labels) with the
#' existing Stata-labelled values, then re-sorts everything by numeric code.
#'
#' @param labels_obs Character vector of unique observed values.
#' @param stata_codes Character vector of numeric codes from Stata labels.
#' @param labels Named numeric vector of Stata value labels (as from
#'   `attr(vec, "labels")`).
#'
#' @return Character vector of merged, sorted formatted labels.
#' @keywords internal
.merge_obs_stata <- function(labels_obs, stata_codes, labels) {
  extra_obs  <- setdiff(labels_obs, stata_codes)
  obs_fmt    <- paste0("[", extra_obs, "] ", extra_obs)

  label_text <- .strip_stata_prefix(names(labels))
  stata_fmt  <- paste0("[", stata_codes, "] ", label_text)

  combined <- c(obs_fmt, stata_fmt)
  codes    <- as.numeric(sub("^\\[(-?\\d+)\\].*", "\\1", combined))
  combined[order(codes)]
}
