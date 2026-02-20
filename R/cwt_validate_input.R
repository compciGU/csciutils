#' Validate inputs to create_cwt
#'
#' Checks that `survey_data` is a named list and that `variable_map` contains
#' the required columns. Warns if any `study_wave` values in `variable_map`
#' have no corresponding entry in `survey_data`.
#'
#' @param survey_data A named list of survey data frames.
#' @param variable_map A data frame with columns `src_var`, `target_var`, and
#'   `study_wave`.
#'
#' @return `TRUE` invisibly. Called for its side effects (errors/warnings).
#' @keywords internal
validate_create_cwt <- function(survey_data, variable_map) {

  if (!is.list(survey_data) || is.null(names(survey_data))) {
    stop("`survey_data` must be a named list of data frames.", call. = FALSE)
  }

  required_cols <- c("src_var", "target_var", "study_wave")
  missing_cols  <- setdiff(required_cols, names(variable_map))
  if (length(missing_cols) > 0) {
    stop("`variable_map` is missing required columns: ",
         paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  unmatched_waves <- setdiff(
    unique(tolower(variable_map$study_wave)),
    tolower(names(survey_data))
  )
  if (length(unmatched_waves) > 0) {
    warning("These study_wave values in `variable_map` have no match in `survey_data`: ",
            paste(unmatched_waves, collapse = ", "), call. = FALSE)
  }

  invisible(TRUE)
}

#' Validate inputs to append_item
#'
#' Checks that `cwt` contains the required columns, delegates to
#' [validate_create_cwt()] for `survey_data` and `variable_map`, and ensures
#' the output directory exists (creating it if needed).
#'
#' @param cwt An existing CWT data frame.
#' @param survey_data A named list of survey data frames.
#' @param variable_map A variable mapping data frame.
#' @param output_dir Root output directory.
#' @param output_subdir Sub-directory relative to `output_dir`.
#'
#' @return `TRUE` invisibly. Called for its side effects (errors/warnings).
#' @keywords internal
validate_append_item <- function(cwt, survey_data, variable_map,
                                 output_dir, output_subdir) {

  required_cols <- c("var_name", "study_wave", "target_var")
  missing_cols  <- setdiff(required_cols, names(cwt))
  if (length(missing_cols) > 0) {
    stop("`cwt` is missing required columns: ",
         paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  validate_create_cwt(survey_data, variable_map)

  if (!dir.exists(output_dir)) {
    stop("`output_dir` does not exist: ", output_dir, call. = FALSE)
  }

  full_output_path <- file.path(output_dir, output_subdir)
  if (!dir.exists(full_output_path)) {
    message("Creating output directory: ", full_output_path)
    dir.create(full_output_path, recursive = TRUE)
  }

  invisible(TRUE)
}
