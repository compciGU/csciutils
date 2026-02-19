#' Append missing variables to an existing CWT
#'
#' Identifies variables present in `vars_df` but absent from `cwt`,
#' builds CWT rows for them, and appends them to the existing CWT.
#'
#' @param cwt Existing CWT data frame as returned by [create_cwt()].
#' @param survey Named list of survey data frames.
#' @param vars_df Variable mapping data frame with columns `src_var`,
#'   `target_var`, and `study_wave`.
#' @param save_file Logical. Save the result to an xlsx file? Default `TRUE`.
#' @param dir Root directory for the output file. Default `getwd()`.
#' @param out_dir Sub-directory relative to `dir`. Created if absent.
#' @param align_surveys_with_cwt Logical. Subset `survey` to waves found
#'   in `cwt`? Default `FALSE`.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return Updated CWT data frame.
#' @export
#' @examples
#' \dontrun{
#'   cwt_updated <- append_item(cwt, my_survey_list, new_vars_df)
#' }
append_item <- function(cwt, survey, vars_df, save_file = TRUE, dir = getwd(),
                        out_dir = "cwt_swd/cwts_appended",
                        align_surveys_with_cwt = FALSE,
                        verbose = TRUE) {

  validate_append_item(cwt, survey, vars_df, dir, out_dir)

  has_waves <- length(unique(cwt$file_name)) > 1

  if (align_surveys_with_cwt && has_waves) {
    survey <- align_survey_to_cwt(survey, cwt, verbose = verbose)
  }

  vars_missing_df <- find_missing_vars(cwt, vars_df, has_waves)

  if (nrow(vars_missing_df) == 0) {
    if (verbose) cat("No missing variables to add. Returning original cwt unchanged.\n")
    if (save_file) save_cwt(cwt, vars_df$study_wave[1], dir, out_dir)
    return(invisible(cwt))
  }

  if (verbose) cat("Found", nrow(vars_missing_df), "missing variable(s). Building CWT rows.\n")

  cwt_new  <- create_cwt(survey, vars_missing_df, verbose = verbose)
  cwt_bind <- dplyr::bind_rows(cwt, cwt_new)
  if (!has_waves) cwt_bind$wave <- NULL

  if (verbose) cat("Appended", nrow(cwt_new), "new row(s) to CWT.\n")
  if (save_file) save_cwt(cwt_bind, vars_missing_df$study_wave[1], dir, out_dir)

  invisible(cwt_bind)
}
