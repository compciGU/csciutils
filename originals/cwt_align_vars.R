#' Collapse technical variables to one row each with NA value columns
#'
#' Technical variables such as `t_weight` and `t_caseid` have one row per
#' respondent in the raw CWT, which is not meaningful. This function collapses
#' them to a single row per variable per wave and blanks out the value columns.
#'
#' @param cwt A CWT data frame as returned by [create_cwt()].
#'
#' @return The CWT with technical variables collapsed to one row each.
#'   Value columns (`value_n`, `value`, `value_code`) are set to `NA`
#'   for these rows.
#' @keywords internal
collapse_tech_vars <- function(cwt) {
  is_tech  <- grepl("t_weight|t_caseid", cwt$target_var)
  cwt_sub  <- cwt[!is_tech, ]
  cwt_tech <- cwt[is_tech, ]

  if (nrow(cwt_tech) == 0) return(cwt_sub)

  cwt_tech <- cwt_tech[!duplicated(cwt_tech[c("study_wave", "var_name")]), ]
  cwt_tech[, c("value_n", "value", "value_code")] <- NA

  rbind(cwt_sub, cwt_tech)
}

#' Check for variables in variable_map not found in the CWT
#'
#' Compares `src_var` entries in `variable_map` against `var_name` entries in
#' the CWT and prints a note for any that are missing. Useful for catching
#' typos in `src_var` names.
#'
#' @param cwt A CWT data frame as returned by [create_cwt()].
#' @param variable_map A variable mapping data frame with at least a `src_var`
#'   column.
#' @param verbose Logical. Print a message if missing variables are found?
#'   Default `TRUE`.
#'
#' @return Called for its side effect. Returns `NULL` invisibly.
#' @keywords internal
check_missing_vars <- function(cwt, variable_map, verbose = TRUE) {
  missing <- setdiff(
    tolower(variable_map$src_var),
    unique(tolower(cwt$var_name))
  )

  if (length(missing) > 0 && verbose) {
    cat(
      "NOTE: These variable_map variables were not found in the survey data:",
      paste(missing, collapse = ", "),
      "\nPlease double-check src_var names.\n"
    )
  }

  invisible(NULL)
}

#' Subset a survey list to waves present in an existing CWT
#'
#' Filters `my_survey_list` to retain only the waves whose `study_wave` values
#' appear in `cwt`. Warns if the CWT references waves absent from the survey.
#'
#' @param my_survey_list Named list of survey data frames.
#' @param cwt A CWT data frame with a `study_wave` column.
#' @param verbose Logical. Print a warning for missing waves? Default `TRUE`.
#'
#' @return A filtered named list of survey data frames.
#' @keywords internal
align_survey_to_cwt <- function(my_survey_list, cwt, verbose = TRUE) {
  waves_cwt    <- tolower(unique(cwt$study_wave))
  waves_survey <- tolower(names(my_survey_list))
  filtered     <- my_survey_list[waves_survey %in% waves_cwt]

  missing_waves <- setdiff(waves_cwt, waves_survey)
  if (length(missing_waves) > 0 && verbose) {
    warning(
      "These CWT waves are not present in my_survey_list: ",
      paste(missing_waves, collapse = ", "),
      call. = FALSE
    )
  }

  filtered
}

#' Identify variable_map rows whose variables are absent from the CWT
#'
#' When the CWT spans multiple files (`has_waves = TRUE`), matching is done on
#' both `src_var` and `study_wave` to avoid false positives where the same
#' variable name appears in different waves. Otherwise matching is on
#' `src_var` alone.
#'
#' @param cwt A CWT data frame with columns `var_name` and `study_wave`.
#' @param variable_map A variable mapping data frame with columns `src_var`
#'   and `study_wave`.
#' @param has_waves Logical. Whether the CWT spans multiple files or waves.
#'
#' @return A subset of `variable_map` containing only rows for variables not
#'   yet present in the CWT.
#' @keywords internal
find_missing_vars <- function(cwt, variable_map, has_waves) {
  if (has_waves) {

    variable_map$src_var    <- tolower(variable_map$src_var)
    variable_map$study_wave <- tolower(as.character(variable_map$study_wave))

    cwt_vars <- data.frame(
      src_var    = tolower(cwt$var_name),
      study_wave = tolower(as.character(cwt$study_wave)),
      stringsAsFactors = FALSE
    )

    dplyr::anti_join(variable_map, cwt_vars,
                     by = c("study_wave", "src_var"))

  } else {

    missing <- setdiff(
      tolower(variable_map$src_var),
      tolower(unique(cwt$var_name))
    )

    out <- variable_map[
      tolower(variable_map$src_var) %in% missing,
      ,
      drop = FALSE
    ]

    out[!is.na(out$src_var), ]
  }
}
