#' Save a CWT data frame to a file
#'
#' A general-purpose file writer for CWT outputs built on top of [write_file()].
#' The file name is built as `{file_prefix}{file_suffix}.xlsx`. Either
#' `file_prefix` or `study_wave` must be supplied so a name can be resolved.
#'
#' @param cwt A CWT data frame to save.
#' @param output_dir Root output directory. Default `getwd()`.
#' @param output_subdir Sub-directory relative to `output_dir`. Set to `NULL`
#'   to save directly in `output_dir`. Default `NULL`.
#' @param file_prefix Character. String prepended to the file name. Derived
#'   from `study_wave` if `NULL`. Default `NULL`.
#' @param file_suffix Character. String appended before the extension.
#'   Default `"_cwt_appended"`.
#' @param study_wave Character. A study wave string (e.g. `"lits_3"`) used
#'   to derive `file_prefix` when `file_prefix` is `NULL`. Default `NULL`.
#' @param overwrite Logical. Overwrite an existing file? Default `TRUE`.
#' @param msg Logical. Print a message after saving? Default `TRUE`.
#'
#' @return The full file path, invisibly.
#' @keywords internal
save_cwt <- function(cwt,
                     output_dir    = getwd(),
                     output_subdir = NULL,
                     file_prefix   = NULL,
                     file_suffix   = "_cwt_appended",
                     study_wave    = NULL,
                     overwrite     = TRUE,
                     msg           = TRUE) {

  # resolve file prefix -------------------------------------------------------
  if (is.null(file_prefix)) {
    if (is.null(study_wave)) {
      stop("Provide either `file_prefix` or `study_wave` to derive a file name.",
           call. = FALSE)
    }
    file_prefix <- sub("_.*$", "", tolower(study_wave))
  }

  # resolve output path -------------------------------------------------------
  output_path <- if (!is.null(output_subdir)) {
    file.path(output_dir, output_subdir)
  } else {
    output_dir
  }

  # build full file path ------------------------------------------------------
  file_name <- paste0(file_prefix, file_suffix, ".xlsx")
  full_path <- file.path(output_path, file_name)

  # delegate to write_file ----------------------------------------------------
  write_file(cwt, path = full_path, overwrite = overwrite, msg = msg)
}
