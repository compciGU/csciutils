#' Create a named list of survey datasets from the database
#'
#' Queries the `datasets` table, optionally filters to one or more projects via
#' the `proj` column, then reads each survey dataset via [read_survey_data()].
#' The result is returned as a named list keyed by `tag`. Optionally appends
#' `dataset_id` to each loaded dataset.
#'
#' @param conn A DBI connection.
#' @param proj Character vector or `NULL`. If supplied, only rows where
#'   `datasets$proj` matches one of these values are kept (case-insensitive).
#'   Examples: `"eb"`, `"ess"`, `c("eb","ess")`. Default `NULL`.
#' @param include_dataset_id Logical. Add a `dataset_id` column to each loaded
#'   dataset? Default `TRUE`.
#' @param use_original Logical. Passed to [read_survey_data()]. Default `TRUE`.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A named list of survey data frames, keyed by `tag`. Failed loads are
#'   stored as `NULL`.
#' @examples
#' \dontrun{
#' surveys <- create_survey_list(conn, proj = "eb")
#' }
#' @export
create_survey_list <- function(conn,
                               proj = NULL,
                               include_dataset_id = TRUE,
                               use_original = TRUE,
                               verbose = TRUE) {


  datasets <- DBI::dbGetQuery(conn, "SELECT * FROM datasets;")

  required_cols <- c("tag", "dataset_id")
  missing_cols  <- setdiff(required_cols, names(datasets))
  if (length(missing_cols) > 0) {
    stop("`datasets` is missing required columns: ",
         paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  if (!is.null(proj)) {

    if (!("proj" %in% names(datasets))) {
      stop("`proj` was supplied but column `proj` was not found in `datasets`.",
           call. = FALSE)
    }

    keep <- tolower(datasets$proj) %in% tolower(proj)
    datasets <- datasets[keep, , drop = FALSE]
  }

  tags        <- datasets$tag[!is.na(datasets$tag)]
  dataset_ids <- datasets$dataset_id[!is.na(datasets$dataset_id)]

  surveys <- list()

  for (i in seq_along(tags)) {

    tag <- tags[i]
    id  <- dataset_ids[i]

    tryCatch(
      {
        cat(sprintf("Reading dataset '%s' (dataset_id: %s)...\n", tag, id))

        df <- read_survey_data(conn = conn,
                               tag = tag,
                               use_original = use_original)

        if (include_dataset_id)
          df$dataset_id <- id

        surveys[[tag]] <- df

        cat(sprintf("Successfully loaded dataset '%s'.\n\n", tag))
      },
      error = function(e) {
        cat(sprintf("Failed to load dataset '%s' (dataset_id: %s).\n", tag, id))
        cat(sprintf("  Error: %s\n\n", conditionMessage(e)))
        surveys[[tag]] <- NULL
      }
    )
  }

  surveys
}





#' Read CWT tables from disk into a named list
#'
#' Reads CWT files from a directory and returns them as a named list keyed by
#' file name (without extension). Optionally filters the files to read based on
#' a project prefix found in the file name (e.g. `"evs"` in
#' `"evs_cwt_appended.csv"`). Supports `.xlsx`, `.csv`, and `.ods` files via
#' [read_file()].
#'
#' @param dir Root directory containing CWT files. Default resolved from
#'   `Sys.getenv("ROOT_DIR")` and `"cwt_swd/cwt_swd_aligned"`.
#' @param proj Character vector or `NULL`. If supplied, only files whose base
#'   name begins with one of these project prefixes followed by `"_"` are read
#'   (case-insensitive). Examples: `"evs"`, `"ess"`, `c("evs","ess")`.
#'   Default `NULL`.
#' @param pattern Character. File extension pattern to include. Default matches
#'   `.xlsx`, `.csv`, and `.ods`.
#' @param full_names Logical. Return full paths in the `names()` of the list?
#'   Default `FALSE`.
#' @param to_global_env Logical. Assign the list elements into `.GlobalEnv`?
#'   Default `FALSE`.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A named list of CWT tables.
#'
#' @examples
#' \dontrun{
#' cwts <- read_cwts(proj = "evs")
#' }
#' @export
read_cwts <- function(dir = file.path(Sys.getenv("ROOT_DIR"), "cwt_swd", "cwt_swd_aligned"),
                      proj = NULL,
                      pattern = "\\.(xlsx|csv|ods)$",
                      full_names = FALSE,
                      to_global_env = FALSE,
                      verbose = TRUE) {

  if (!dir.exists(dir)) {
    stop("CWT directory does not exist: ", dir, call. = FALSE)
  }

  cwt_files <- list.files(dir, pattern = pattern, full.names = TRUE)

  if (length(cwt_files) == 0) {
    if (verbose) cat("No CWT files found in:", dir, "\n")
    return(list())
  }

  if (!is.null(proj)) {

    base_names <- basename(cwt_files)
    keep <- rep(FALSE, length(base_names))

    for (p in proj) {
      keep <- keep | grepl(paste0("^", tolower(p), "_"),
                           tolower(base_names))
    }

    cwt_files <- cwt_files[keep]
  }

  cwts <- list()

  for (f in cwt_files) {

    f_base <- basename(f)
    f_name <- sub("\\.(xlsx|csv|ods)$", "", f_base, ignore.case = TRUE)

    tryCatch(
      {
        if (verbose) cat("Reading CWT file:", f_base, "\n")
        cwts[[f_name]] <- read_file(f)
      },
      error = function(e) {
        if (verbose) {
          cat("ERROR reading", f_base, ":\n")
          cat("  ", conditionMessage(e), "\n\n")
        }
        cwts[[f_name]] <- NULL
      }
    )
  }

  if (!full_names) {
    # keep list names as base file names without extension (current behavior)
  } else {
    names(cwts) <- file.path(dir, paste0(names(cwts)))
  }

  if (to_global_env)
    list2env(cwts, envir = .GlobalEnv)

  cwts
}
