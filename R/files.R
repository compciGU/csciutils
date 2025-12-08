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
        if (readstata13_avail) {
          function(path, ...) readstata13::read.dta13(path, ...)
        } else {
          stop("Reading .dta requires package 'readstata13'.")
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
#' @description Loads a survey dataset by reading the file path from the datasets table.
#' The file path is constructed by combining the ROOT_DIR environment variable with 
#' the path from either the original_path or path column.
#'
#' @param conn A DBI database connection object
#' @param dataset_tag The tag of the dataset to read
#' @param use_original Logical. If TRUE (default), uses the original_path column. 
#'   If FALSE, uses the path column.
#' @param msg Logical. If TRUE, prints a message when reading the file.
#' @param ... Additional arguments passed to read_file
#'
#' @return The loaded dataset (typically a data frame)
#'
#' @examples
#' \dontrun{
#' conn <- set_db_connection()
#' data <- read_survey_data(conn, dataset_tag = "survey2024")
#' data_processed <- read_survey_data(conn, dataset_tag = "survey2024", use_original = FALSE)
#' DBI::dbDisconnect(conn)
#' }
#'
#' @export
read_survey_data <- function(conn, dataset_tag, use_original = TRUE, msg = TRUE, ...) {
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required but not installed.")
  }
  
  if (missing(conn) || missing(dataset_tag)) {
    stop("Both 'conn' and 'dataset_tag' are required.")
  }
  
  # Get ROOT_DIR from environment
  root_dir <- Sys.getenv("ROOT_DIR")
  if (root_dir == "") {
    stop("ROOT_DIR environment variable is not set.")
  }
  
  # Determine which column to use
  path_column <- if (use_original) "original_path" else "path"
  
  # Query the datasets table
  query <- sprintf("SELECT %s FROM datasets WHERE dataset_tag = $1", path_column)
  result <- DBI::dbGetQuery(conn, query, params = list(dataset_tag))
  
  if (nrow(result) == 0) {
    stop(sprintf("No dataset found with dataset_tag = '%s'", dataset_tag))
  }
  
  # Get the relative path
  relative_path <- result[[path_column]][1]
  
  if (is.na(relative_path) || relative_path == "") {
    stop(sprintf("The %s column is empty or NA for dataset_tag = '%s'", path_column, dataset_tag))
  }
  
  # Construct the full path
  full_path <- file.path(root_dir, relative_path)
  
  # Use read_file to load the data
  read_file(full_path, msg = msg, ...)
}