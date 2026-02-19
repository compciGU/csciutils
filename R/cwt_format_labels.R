#' Format value labels for a single survey variable
#'
#' Handles three cases: no Stata labels exist (falls back to observed values),
#' more observed values than Stata labels (merges both), and Stata labels fully
#' covering observed values (uses labels as-is).
#'
#' @param var_values A vector from a survey dataset, possibly carrying
#'   Stata/haven attributes (`label`, `labels`).
#' @param var_name Character. Variable name, used only for console messages.
#' @param wave_name Character. Study wave name, used only for console messages.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A named list with three elements:
#'   \describe{
#'     \item{var_label}{Character scalar. Variable label from Stata metadata,
#'       or `NA` if absent.}
#'     \item{label_formatted}{Character vector of formatted value labels,
#'       e.g. `"[1] Yes"`.}
#'     \item{code_formatted}{Character vector of raw value codes aligned to
#'       `label_formatted`.}
#'   }
#' @keywords internal
format_value_labels <- function(var_values, var_name, wave_name, verbose = TRUE) {

  var_label <- attr(var_values, "label")
  if (is.null(var_label)) var_label <- NA_character_

  labels     <- attr(var_values, "labels", exact = TRUE)
  labels_obs <- unique(as.character(var_values))
  labels_obs <- labels_obs[!is.na(labels_obs)]
  labels_obs <- labels_obs[order(as.numeric(labels_obs))]

  if (is.null(labels) || all(is.na(labels))) {

    if (verbose)
      cat("NOTE: No Stata value labels found for variable:", var_name,
          "in dataset:", wave_name, "- using observed values instead.\n")

    label_formatted <- paste0("[", labels_obs, "] ", labels_obs)
    code_formatted  <- labels_obs

  } else {

    labels      <- labels[order(as.numeric(unname(labels)))]
    stata_codes <- as.character(unname(labels))

    if (length(unique(labels_obs)) > length(unique(stata_codes))) {

      if (verbose)
        cat("NOTE: More observed values than Stata labels for variable:", var_name,
            "in dataset:", wave_name, "- combining observed values with Stata labels.\n")

      label_formatted <- .merge_obs_stata(labels_obs, stata_codes, labels)
      code_formatted  <- c(labels_obs, setdiff(stata_codes, labels_obs))

    } else {

      if (verbose)
        cat("NOTE: Using Stata value labels for variable:", var_name,
            "in dataset:", wave_name, "- observed values fully covered by labels.\n")

      label_text      <- .strip_stata_prefix(names(labels))
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
  tab       <- table(var_values, useNA = "ifany")
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
