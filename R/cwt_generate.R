#' Generate or update a Crosswalk with Value Labels (CWT)
#'
#' Builds a Crosswalk with Value Labels (CWT) from survey data and a
#' variable mapping, or appends missing variables to an existing CWT.
#'
#' If `cwt` is `NULL`, a new CWT is constructed from scratch using
#' [build_cwt()]. If `cwt` is supplied, variables present in
#' `variable_map` but absent from `cwt` are identified and appended.
#'
#' Optionally saves the resulting CWT to an `.xlsx` file via [save_cwt()].
#'
#' @param cwt Optional existing CWT data frame. If `NULL`, a new CWT is built
#'   from `my_survey_list` and `variable_map`. Default `NULL`.
#' @param my_survey_list Named list of survey data frames, keyed by `study_wave`.
#' @param variable_map Data frame with columns `src_var`, `target_var`,
#'   and `study_wave`, defining the source-to-target variable mapping.
#' @param save_file Logical. Save the resulting CWT to file? Default `TRUE`.
#' @param dir Root output directory. Default `ROOT_DIR`.
#' @param out_dir Sub-directory relative to `dir` where the file is saved.
#'   Created if absent. Default `"cwt_swd/cwts_final"`.
#' @param file_suffix Character string appended to the file name before the
#'   extension. Default `"_cwt_finalized"`.
#' @param align_surveys_with_cwt Logical. If `TRUE`, restrict `my_survey_list`
#'   to waves already present in `cwt` before appending. Default `FALSE`.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A CWT data frame. Returned invisibly.
#'
#' @details
#' The function operates in two modes:
#'
#' \itemize{
#'   \item \strong{Build mode}: When `cwt = NULL`, a full CWT is created from
#'   scratch using all variables in `variable_map`.
#'   \item \strong{Append mode}: When `cwt` is provided, only variables not
#'   already present in `cwt` are constructed and appended.
#' }
#'
#' Uniqueness of `(study_wave, src_var)` mappings is enforced during validation.
#'
#'
#' @examples
#' \dontrun{
#' # Build from scratch
#' cwt <- generate_cwt(
#'   my_survey_list = lits,
#'   variable_map = lits_annotations_long
#' )
#'
#' # Append to existing CWT
#' cwt_updated <- generate_cwt(
#'   cwt = cwt,
#'   my_survey_list = lits,
#'   variable_map = new_mapping
#' )
#' }
#'  @export
generate_cwt <- function(cwt = NULL,
                         my_survey_list,
                         variable_map,
                         save_file = TRUE,
                         dir = Sys.getenv("ROOT_DIR"),
                         out_dir = "cwt_swd/cwts_final",
                         file_suffix = "_cwt_finalized",
                         align_surveys_with_cwt = FALSE,
                         verbose = TRUE) {

  if (is.null(cwt)) {

    if (verbose)
      cat("No existing cwt supplied. Building CWT from scratch.\n")

    cwt <- build_cwt(my_survey_list, variable_map, verbose = verbose)

    if (save_file)
      save_cwt(
        cwt = cwt,
        output_dir = dir,
        output_subdir = out_dir,
        file_prefix = variable_map$study_wave[1],
        file_suffix = file_suffix
      )

    return(invisible(cwt))
  }

  validate_append_item(cwt, my_survey_list, variable_map, dir, out_dir)

  has_waves <- length(unique(cwt$study_wave)) > 1

  if (align_surveys_with_cwt && has_waves) {
    my_survey_list <- align_survey_to_cwt(my_survey_list, cwt, verbose = verbose)
  }

  vars_missing_df <- find_missing_vars(cwt, variable_map, has_waves)

  if (nrow(vars_missing_df) == 0) {
    if (verbose)
      cat("No missing variables to add. Returning original cwt unchanged.\n")

    if (save_file)
      save_cwt(
        cwt = cwt,
        output_dir = dir,
        output_subdir = out_dir,
        file_prefix = variable_map$study_wave[1],
        file_suffix = file_suffix
      )

    return(invisible(cwt))
  }

  if (verbose)
    cat("Found", nrow(vars_missing_df),
        "missing variable(s). Building CWT rows.\n")

  cwt_new  <- build_cwt(my_survey_list, vars_missing_df, verbose = verbose)
  cwt_bind <- dplyr::bind_rows(cwt, cwt_new)

  if (!has_waves)
    cwt_bind$wave <- NULL

  if (verbose)
    cat("Appended", nrow(cwt_new), "new row(s) to CWT.\n")

  if (save_file)
    save_cwt(
      cwt = cwt_bind,
      output_dir = dir,
      output_subdir = out_dir,
      file_prefix = vars_missing_df$study_wave[1],
      file_suffix = file_suffix
    )

  invisible(cwt_bind)
}
