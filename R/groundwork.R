#' Run the full CWT groundwork workflow for a project
#'
#' Orchestrates loading surveys, loading aligned CWTs, querying annotations,
#' creating/appending CWTs and (optionally) opening the output file.
#'
#' @param conn DBI connection
#' @param proj Project prefix to process
#' @param dir Root directory (defaults to Sys.getenv("ROOT_DIR"))
#' @param aligned_dir Relative path under `dir` to aligned CWT files
#' @param out_dir Relative path under `dir` for appended CWTs
#' @param file_suffix Suffix to append to output filenames
#' @param save_file Logical; whether to save appended CWT files
#' @param open_file Logical; if TRUE and a file is created, open via browseURL()
#' @param verbose Logical; print messages
#' @return The value returned by `create_cwt()`
#' @export
do_groundwork <- function(conn,
                          proj,
                          dir = Sys.getenv("ROOT_DIR"),
                          cwt_dir = file.path("cwts", "cwts_aligned"),
                          out_dir = file.path("cwts", "cwts_appended"),
                          file_suffix = "_cwt_appended",
                          save_file = TRUE,
                          open_file = FALSE,
                          verbose = TRUE) {

  # Load surveys
  if (verbose) message("Loading surveys for proj = '", proj, "' ...")
  my_survey_list <- get_survey_list(
    conn = conn,
    proj = proj,
    verbose = verbose
  )

  # Load CWTs
  path <- file.path(dir, cwt_dir)
  if (verbose) message("Loading CWTs from: ", path)

  cwt_list <- get_cwts(
    dir = path,
    proj = proj,
    to_global_env = FALSE,
    verbose = verbose
  )

  # If no CWTs found, build from scratch (single run)
  if (is.null(cwt_list) || length(cwt_list) == 0) {

    if (verbose) {
      message("No CWT found for '", proj, "' in directory ", path,
              ". Building from scratch.")
    }

    # Load annotations (long)
    if (verbose) message("Loading annotations (long format) for proj = '", proj, "' ...")
    variable_map <- get_annotations(
      conn = conn,
      proj = proj,
      format = "long",
      verbose = verbose
    )

    # Create / append CWT (from scratch)
    if (verbose) message("Creating/appending CWT ...")
    cwt <- get_cwt(
      cwt            = NULL,
      my_survey_list = my_survey_list,
      variable_map   = variable_map,
      save_file      = save_file,
      dir            = dir,
      out_dir        = out_dir,
      file_suffix    = file_suffix
    )

  } else {

    # Load annotations (long)
    if (verbose) message("Loading annotations (long format) for proj = '", proj, "' ...")
    variable_map <- get_annotations(
      conn = conn,
      proj = proj,
      format = "long",
      verbose = verbose
    )

    # Apply create_cwt to each CWT in the list
    if (verbose) message("Creating/appending CWT for ", length(cwt_list), " file(s) ...")
    cwts <- lapply(cwt_list, function(cwt) {
      get_cwt(
        cwt            = cwt,
        my_survey_list = my_survey_list,
        variable_map   = variable_map,
        save_file      = save_file,
        dir            = dir,
        out_dir        = out_dir,
        file_suffix    = file_suffix
      )
    })
  }

  # Optionally open output file (best-effort; assumes single proj-level filename)
  if (isTRUE(open_file) && isTRUE(save_file)) {
    out_file <- file.path(dir, out_dir, paste0(proj, file_suffix, ".xlsx"))

    if (file.exists(out_file)) {
      if (verbose) message("Opening: ", out_file)
      utils::browseURL(out_file)
    } else {
      if (verbose) message("Output file not found: ", out_file)
    }
  }

  cwts
}
