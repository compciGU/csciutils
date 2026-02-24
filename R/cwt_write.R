#' Save a CWT data frame to a file
#'
#' A general-purpose file writer for CWT outputs built on top of [write_file()].
#' The file name is built as `{file_prefix}{file_suffix}.xlsx`. If `file_prefix`
#' is `NULL`, it is derived from `cwt$study_wave[1]`.
#'
#' @param cwt A CWT data frame to save.
#' @param output_dir Root output directory. Default `ROOT_DIR`.
#' @param output_subdir Sub-directory relative to `output_dir`. Set to `NULL`
#'   to save directly in `output_dir`. Default `"cwt_swd/cwts_appended"`.
#' @param file_prefix Character. String prepended to the file name. Derived
#'   from `cwt$study_wave[1]` if `NULL`. Default `NULL`.
#' @param file_suffix Character. String appended before the extension.
#'   Default `"_cwt_appended"`.
#' @param overwrite Logical. Overwrite an existing file? Default `TRUE`.
#' @param msg Logical. Print a message after saving? Default `TRUE`.
#'
#' @return The full file path, invisibly.
#' @keywords internal
save_cwt <- function(cwt,
                     output_dir    = ROOT_DIR,
                     output_subdir = "cwt_swd/cwts_appended",
                     file_prefix   = NULL,
                     file_suffix   = "_cwt_appended",
                     overwrite     = TRUE,
                     msg           = TRUE) {

  # resolve file prefix -------------------------------------------------------
  if (is.null(file_prefix)) {
    if (is.null(cwt$study_wave) || length(cwt$study_wave) == 0 || is.na(cwt$study_wave[1])) {
      stop("`file_prefix` is NULL and `cwt$study_wave[1]` is missing; cannot derive a file name.",
           call. = FALSE)
    }
    file_prefix <- sub("_.*$", "", tolower(cwt$study_wave[1]))
  }

  # resolve output path -------------------------------------------------------
  if (!is.null(output_subdir)) {
    output_path <- file.path(output_dir, output_subdir)
  } else {
    output_path <- output_dir
  }

  # build full file path ------------------------------------------------------
  file_name <- paste0(file_prefix, file_suffix, ".xlsx")
  full_path <- file.path(output_path, file_name)

  # delegate to write_file ----------------------------------------------------
  write_file(cwt, path = full_path, overwrite = overwrite, msg = msg)
}
