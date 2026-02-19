#' Save a data frame to an xlsx file
#'
#' A general-purpose xlsx writer with flexible file naming. The file name is
#' built as `{file_prefix}{file_suffix}.xlsx`. Either `file_prefix` or
#' `study_wave` must be supplied so the name can be resolved.
#'
#' @param data A data frame to save.
#' @param output_dir Root output directory. Default `getwd()`.
#' @param output_subdir Sub-directory relative to `output_dir`. Set to `NULL`
#'   to save directly in `output_dir`. Created automatically if absent.
#' @param file_prefix Character. String prepended to the file name. Derived
#'   from `study_wave` if `NULL`. Default `NULL`.
#' @param file_suffix Character. String appended to the file name before the
#'   `.xlsx` extension. Default `"_cwt_appended"`.
#' @param study_wave Character. A study wave string (e.g. `"lits_3"`) used to
#'   derive `file_prefix` when `file_prefix` is `NULL`. Default `NULL`.
#' @param overwrite Logical. Overwrite an existing file? If `FALSE` and the
#'   file already exists, an error is raised. Default `TRUE`.
#'
#' @return The full file path, invisibly.
#' @keywords internal
save_to_xlsx <- function(data,
                         output_dir    = getwd(),
                         output_subdir = NULL,
                         file_prefix   = NULL,
                         file_suffix   = "_cwt_appended",
                         study_wave    = NULL,
                         overwrite     = TRUE) {

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

  if (!dir.exists(output_path)) {
    message("Creating output directory: ", output_path)
    dir.create(output_path, recursive = TRUE)
  }

  # build full file path and guard overwrites ---------------------------------
  file_name <- paste0(file_prefix, file_suffix, ".xlsx")
  full_path <- file.path(output_path, file_name)

  if (!overwrite && file.exists(full_path)) {
    stop("File already exists and `overwrite = FALSE`: ", full_path, call. = FALSE)
  }

  openxlsx::write.xlsx(data, file = full_path, row.names = FALSE)
  cat("Saved:", file_name, "->", full_path, "\n")
  invisible(full_path)
}
