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
get_survey_list <- function(conn,
                            # If you specify proj as the second argument, you can call the function by just writing get_survey_list(conn, "evs")
                            proj,
                            table = "datasets",
                            include_dataset_id = TRUE,
                            # We do not need this argument for the function, as it is part of read_survey_data
                            # use_original = TRUE,
                            verbose = TRUE) {

  # Let's go with query instead of sql as it is more descriptive of what the object is.
  # We can also filter for proj here and save the lines of code further down.

  #query <- paste0("SELECT * FROM ", table, ";")
  #datasets <- DBI::dbGetQuery(conn, query)

  #We can write this more compact with a stopifnot statement
  #required_cols <- c("tag", "dataset_id")
  #missing_cols  <- setdiff(required_cols, names(datasets))
  #if (length(missing_cols) > 0) {
  #  stop("`datasets` is missing required columns: ",
  #       paste(missing_cols, collapse = ", "), call. = FALSE)
  #}

  #This does not do what you want it to do. It checks if proj is a column in datasets. Instead of the column names in datasets you need to loop through the unique values for proj, check code below
  #if (!is.null(proj)) {
  #
  #  if (!("proj" %in% names(datasets))) {
  #    stop("`proj` was supplied but column `proj` was not found in `datasets`.",
  #         call. = FALSE)
  #  }

  # Do we need tolower? I think the general rule for lower case and the additional stopifnot for the proj value should be enough
  # To make sure that proj is not missing, we can add a NOT NULL constraint to the proj column in the database, i.e. adress the problem as early as possible.
  # That said and filtering for proj in query make these two lines redundant. 

  #datasets <- datasets[tolower(datasets$proj) %in% tolower(proj), , drop = FALSE]
  #datasets <- datasets[ !is.na(datasets$tag) & datasets$tag != "" & !is.na(datasets$dataset_id), , drop = FALSE]
  
  # Here is my suggestion:
  query <- paste0("SELECT * FROM ", table, " WHERE proj = '", proj, "';")
  datasets <- DBI::dbGetQuery(conn, query)
  
  required_cols <- c("tag", "dataset_id")
  stopifnot("Required columns missing from datasets table: 'tag' and/or 'dataset_id'" = 
              all(required_cols %in% names(datasets)))
  
  print(proj)
  stopifnot("Specified project does not exist in datasets table. Check the value for proj." = 
                proj %in% unique(datasets$proj))
  
  tags        <- datasets$tag
  dataset_ids <- datasets$dataset_id

  surveys <- list()

  for (i in seq_along(tags)) {

    tag <- tags[i]
    id  <- dataset_ids[i]

    tryCatch(
      {
        message(sprintf("Reading dataset '%s' (dataset_id: %s)...", tag, id))

        # Changed the value for use_original to TRUE as this is the default value in read_survey_data and it is what we want to use here, so no need to pass it as an argument to this function
        # Also, just using conn and tag is enough, no specification needed
        df <- read_survey_data(conn, tag, use_original = TRUE)

        if (include_dataset_id)
          df$dataset_id <- id

        surveys[[tag]] <- df

        message(sprintf("Successfully loaded dataset '%s'.\n", tag))
      },
      error = function(e) {
        message(sprintf("Failed to load dataset '%s' (dataset_id: %s).", tag, id))
        message(sprintf("  Error: %s\n", conditionMessage(e)))
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
get_cwts <- function(dir_cwts = file.path(Sys.getenv("ROOT_DIR"), "cwt_swd", "cwt_swd_aligned"),
                      proj = NULL,
                      pattern = "\\.(xlsx|csv|ods)$",
                      name_extension = FALSE,
                      to_global_env = FALSE,
                      verbose = TRUE) {

  if (!dir.exists(dir_cwts)) {
    stop("CWT directory does not exist: ", dir_cwts, call. = FALSE)
  }

  cwt_files <- list.files(dir_cwts, pattern = pattern, full.names = TRUE)

  if (length(cwt_files) == 0) {
    if (verbose) message("No CWT files found in:", dir_cwts, "\n")
    return(list())
  }

  if (!is.null(proj)) {

    base_names <- basename(cwt_files)
    pattern_proj <- paste0("^(", paste(tolower(proj), collapse = "|"), ")_")
    cwt_files <- cwt_files[grepl(pattern_proj, tolower(base_names))]
  }


  cwts <- list()

  for (f in cwt_files) {
    base_name <- basename(f)
    name_file <- sub("\\.(xlsx|csv|ods)$", "", base_name, ignore.case = TRUE) # clean file name to store in list

    tryCatch(
      {
        if (verbose) message(paste("Reading CWT file:", base_name))
        cwts[[name_file]] <- read_file(f)
      },
      error = function(e) {
        if (verbose) {
          message(paste("ERROR reading", base_name, ":"))
          message(paste("  ", conditionMessage(e), "\n"))
        }
        cwts[[f_name]] <- NULL
      }
    )
  }

  if (!name_extension) {
    # keep list names as base file names without extension (current behavior)
  } else {
    names(cwts) <- file.path(dir_cwts, paste0(names(cwts)))
  }

  if (to_global_env)
    list2env(cwts, envir = .GlobalEnv)

  cwts
}
