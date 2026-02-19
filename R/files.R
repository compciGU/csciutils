#' @export
read_file <- function(f, fn = NULL, msg = TRUE, ...) {
  if (missing(f) || length(f) != 1L) stop("Missing file")

  if (!file.exists(f)) {
    stop(sprintf("File not found: %s", f))
  }
  fa <- file.access(f, mode = 4)  # 0 = ok, -1 = not ok, maybe NA
  if (is.na(fa) || fa != 0) {
    stop(sprintf("Unable to read file: %s", f))
  }

  ## cache package availability
  dt_avail <- requireNamespace("data.table", quietly = TRUE)
  vroom_avail <- requireNamespace("vroom", quietly = TRUE)
  readxl_avail <- requireNamespace("readxl", quietly = TRUE)
  openxlsx_avail <- requireNamespace("openxlsx", quietly = TRUE)
  readstata13_avail <- requireNamespace("readstata13", quietly = TRUE)
  haven_avail <- requireNamespace("haven", quietly = TRUE)

  ## determine reader function
  if (is.null(fn)) {
    ext <- tolower(tools::file_ext(f))
    fn <- switch(ext,
      csv = {
        if (dt_avail) {
          function(path, ...) data.table::fread(path, data.table = FALSE, ...)
        } else if (vroom_avail) {
          function(path, ...) vroom::vroom(path, progress = FALSE, ...)
        } else {
          function(path, ...) utils::read.csv(path, stringsAsFactors = FALSE, ...)
        }
      },
      tsv = {
      if (dt_avail) {
        data.table::fwrite(x, file = path, sep = "\t", ...)
        } else {
        utils::write.table(x, file = path, sep = "\t", row.names = FALSE, quote = FALSE, ...)
       }
      },
      xlsx = {
        if (readxl_avail) {
          function(path, ...) readxl::read_excel(path, ...)
        } else if (openxlsx_avail) {
          function(path, ...) openxlsx::read.xlsx(path, ...)
        } else {
          stop("Reading .xlsx requires package 'readxl' or 'openxlsx' (install one or convert to CSV).")
        }
      },
      xls = {
        if (readxl_avail) {
          function(path, ...) readxl::read_excel(path, ...)
        } else {
          stop("Reading .xls requires package 'readxl' (install it or convert to CSV).")
        }
      },
      dta = {
        if (haven_avail) {
          function(path, ...) haven::read_dta(path, ...)
        } else if (readstata13_avail) {
          function(path, ...) readstata13::read.dta13(path, ...)
        } else {
        stop("Reading .dta requires package 'haven' or 'readstata13'.")
        }
      },
      sav = {
        if (haven_avail) {
          function(path, ...) haven::read_sav(path, ...)
        } else {
          stop("Reading .sav requires package 'haven'.")
        }
      },
      rds = readRDS,
      dput = dget,
      txt = readLines,
      rdata = function(path, env = new.env()) { load(path, envir = env); env },
      r = readLines,
      stop(sprintf("Unable to find function to open file: %s", f))
    )
  }

  if (isTRUE(msg)) message(sprintf("Reading file %s", f))

  if (!is.function(fn)) {
    ext <- tolower(tools::file_ext(f))
    stop(sprintf("No reader available for '%s' (extension: '%s'). Provide 'fn' or use a supported file.", f, ext))
  }

  tryCatch(
    fn(f, ...),
    error = function(e) {
      stop("Error reading file ", f, ": ", conditionMessage(e), call. = FALSE)
    }
  )
}

#' @export
write_file <- function(x, path, overwrite = FALSE, msg = TRUE, ...) {
  if (missing(path) || length(path) != 1L) stop("Missing path")

  dir <- dirname(path)
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE, showWarnings = FALSE)

  if (file.exists(path) && !isTRUE(overwrite)) {
    stop(sprintf("File already exists: %s (set overwrite = TRUE to overwrite)", path))
  }

  ext <- tolower(tools::file_ext(path))

  ## prefer fast writers when available
  dt_avail <- requireNamespace("data.table", quietly = TRUE)
  vroom_avail <- requireNamespace("vroom", quietly = TRUE)
  readxl_avail <- requireNamespace("readxl", quietly = TRUE)
  openxlsx_avail <- requireNamespace("openxlsx", quietly = TRUE)
  haven_avail <- requireNamespace("haven", quietly = TRUE)

  switch(ext,
    csv = {
      if (dt_avail) {
        data.table::fwrite(x, file = path, ...)
      } else if (vroom_avail) {
        ## vroom write uses vroom_write in some versions; fallback to write.csv
        if (is.data.frame(x)) {
          utils::write.csv(x, file = path, row.names = FALSE, ...)
        } else {
          utils::write.csv(as.data.frame(x), file = path, row.names = FALSE, ...)
        }
      } else {
        utils::write.csv(x, file = path, row.names = FALSE, ...)
      }
    },
    rds = {
      saveRDS(x, file = path, ...)
    },
    dput = {
      dput(x, file = path)
    },
    txt = {
      if (is.character(x)) {
        writeLines(x, con = path)
      } else if (is.vector(x)) {
        writeLines(as.character(x), con = path)
      } else {
        ## fallback: write table
        utils::write.table(x, file = path, quote = FALSE, row.names = FALSE, ...)
      }
    },
    rdata = {
      ## save the object as 'x' in the .RData file
      save(x, file = path, ...)
    },
    xlsx = {
      if (openxlsx_avail) {
        openxlsx::write.xlsx(x, file = path, ...)
      } else {
        stop("Writing .xlsx requires package 'openxlsx' (install it or choose another format).")
      }
    },
    sav = {
      if (haven_avail) {
        haven::write_sav(x, path, ...)
      } else {
        stop("Writing .sav requires package 'haven'.")
      }
    },
    dta = {
      if (haven_avail) {
        haven::write_dta(x, path, ...)
      } else {
        stop("Writing .dta requires package 'haven'.")
      }
    },
    stop(sprintf("No writer available for '.%s' files", ext))
  )

  if (isTRUE(msg)) message(sprintf("Wrote file %s", path))
  invisible(path)
}
#' @title Read Survey Data from Database Path
#'
#' @description Loads a survey dataset by reading the file path from the
#'   datasets table. The file path is constructed by combining the `ROOT_DIR`
#'   environment variable with the path from either the `original_path` or
#'   `path` column. If reading fails due to an encoding error, automatically
#'   retries with `"latin1"` encoding before giving up.
#'
#' @param conn A DBI database connection object.
#' @param dataset_tag Character. The tag of the dataset to read.
#' @param use_original Logical. If `TRUE` (default), uses the `original_path`
#'   column. If `FALSE`, uses the `path` column.
#' @param encoding Character or `NULL`. Encoding passed to the file reader
#'   (e.g. `"latin1"`). If `NULL` (default), the reader default is used and
#'   a `"latin1"` retry is attempted automatically on encoding errors.
#' @param msg Logical. If `TRUE`, prints progress messages. Default `TRUE`.
#' @param ... Additional arguments passed to `read_file`.
#'
#' @return The loaded dataset as a data frame.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- set_db_connection()
#'
#' # basic usage — latin1 retry happens automatically if needed
#' data <- read_survey_data(conn, dataset_tag = "lits_2")
#'
#' # force a specific encoding upfront
#' data <- read_survey_data(conn, dataset_tag = "lits_2", encoding = "latin1")
#'
#' DBI::dbDisconnect(conn)
#' }
read_survey_data <- function(conn,
                             dataset_tag,
                             use_original = TRUE,
                             encoding     = NULL,
                             msg          = TRUE,
                             ...) {

  # input checks
  if (missing(conn) || missing(dataset_tag)) {
    stop("Both `conn` and `dataset_tag` are required.", call. = FALSE)
  }

  ROOT_DIR <- Sys.getenv("ROOT_DIR")
  if (ROOT_DIR == "") {
    stop("`ROOT_DIR` environment variable is not set.", call. = FALSE)
  }

  # query database for file path
  path_column <- if (use_original) "original_path" else "path"
  query       <- paste0(
    "SELECT ", path_column, " FROM datasets WHERE tag = $1 LIMIT 1"
  )
  result <- DBI::dbGetQuery(conn, query, params = list(dataset_tag))

  if (nrow(result) == 0) {
    stop(sprintf("No dataset found with tag = '%s'.", dataset_tag), call. = FALSE)
  }

  relative_path <- result[[path_column]][1]
  if (is.na(relative_path) || relative_path == "") {
    stop(sprintf("The `%s` column is empty or NA for dataset_tag = '%s'.",
                 path_column, dataset_tag), call. = FALSE)
  }

  full_path <- file.path(ROOT_DIR, relative_path)

  # read file with automatic latin1 fallback
  .read_with_encoding_fallback(
    full_path   = full_path,
    dataset_tag = dataset_tag,
    encoding    = encoding,
    msg         = msg,
    ...
  )
}


#' Attempt to read a file, retrying with latin1 on encoding errors
#'
#' First attempts to read with the supplied `encoding` (or the reader default
#' if `NULL`). If the read fails with an encoding-related error, automatically
#' retries with `encoding = "latin1"`. Any other error is re-thrown immediately.
#'
#' @param full_path Character. Full file path to read.
#' @param dataset_tag Character. Used only for console messages.
#' @param encoding Character or `NULL`. Encoding for the first attempt.
#' @param msg Logical. Print progress messages?
#' @param ... Additional arguments passed to `read_file`.
#'
#' @return A data frame.
#' @keywords internal
.read_with_encoding_fallback <- function(full_path, dataset_tag, encoding, msg, ...) {

  is_encoding_error <- function(e) {
    grepl("encoding|invalid byte|convert string",
          conditionMessage(e), ignore.case = TRUE)
  }

  # first attempt
  first_result <- tryCatch(
    {
      if (!is.null(encoding)) {
        if (msg) cat("NOTE: Reading", dataset_tag, "with encoding =", encoding, "\n")
        read_file(full_path, encoding = encoding, msg = msg, ...)
      } else {
        read_file(full_path, msg = msg, ...)
      }
    },
    error = function(e) e
  )

  # success
  if (!inherits(first_result, "error")) return(first_result)

  # non-encoding error — re-throw immediately
  if (!is_encoding_error(first_result)) stop(first_result)

  # encoding error — retry with latin1
  if (msg) {
    cat("NOTE: Encoding error detected for", dataset_tag,
        "- retrying with encoding = 'latin1'.\n")
  }

  tryCatch(
    read_file(full_path, encoding = "latin1", msg = msg, ...),
    error = function(e) {
      stop(sprintf(
        "Failed to read '%s' with both default and latin1 encoding.\nLast error: %s",
        dataset_tag, conditionMessage(e)
      ), call. = FALSE)
    }
  )
}

#' @title Clean Variable Names in a Dataset
#'
#' @description Converts all variable names in a dataset to a clean format by:
#' - Converting to lowercase
#' - Replacing spaces and dots with underscores
#' - Removing special characters (keeping only letters, numbers, and underscores)
#'
#' @param data A data frame whose variable names should be cleaned
#'
#' @return A data frame with cleaned variable names
#'
#' @examples
#' \dontrun{
#' df <- data.frame("First Name" = 1:3, "Last.Name" = letters[1:3],
#'                  "Age (years)" = c(25, 30, 35))
#' clean_df <- clean_varnames(df)
#' names(clean_df)  # "first_name" "last_name" "age_years"
#' }
#'
#' @export
clean_varnames <- function(data) {
  if (!is.data.frame(data)) {
    stop("Input must be a data frame.")
  }

  # Get current names
  old_names <- names(data)

  # Clean names
  new_names <- old_names
  new_names <- tolower(new_names)                           # Convert to lowercase
  new_names <- gsub("[ .]", "_", new_names)                 # Replace spaces and dots with underscores
  new_names <- gsub("[^a-z0-9_]", "", new_names)            # Remove special characters (keep letters, numbers, underscores)
  new_names <- gsub("_+", "_", new_names)                   # Replace multiple underscores with single underscore
  new_names <- gsub("^_|_$", "", new_names)                 # Remove leading/trailing underscores

  # Handle empty names or duplicates
  if (any(new_names == "")) {
    empty_idx <- which(new_names == "")
    new_names[empty_idx] <- paste0("var_", empty_idx)
  }

  # Handle duplicate names
  if (any(duplicated(new_names))) {
    new_names <- make.unique(new_names, sep = "_")
  }

  # Assign cleaned names
  names(data) <- new_names

  return(data)
}


#' @export
remove_all_labels <- function(data) {
  attr(data, "variable.labels") <- NULL

  data[] <- lapply(data, function(x) {
    attr(x, "label") <- NULL
    attr(x, "labels") <- NULL
    attr(x, "format.spss") <- NULL
    attr(x, "display_width") <- NULL
    class(x) <- setdiff(class(x), c("haven_labelled", "labelled"))
    x
  })

  return(data)
}
