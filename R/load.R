
# Load Surveys as List object ----

load_survey_list <- function(conn, proj) {

  TABLE <- "datasets"

  query <- paste0("SELECT * FROM ", TABLE, " WHERE proj = '", proj, "';")
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

        df <- read_survey_data(conn, tag, use_original = TRUE)

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


# Load CWTs ----

# Should we add a "status" argument to decide which cwt to load? I.e., aligned,
# appeneded or recoded?

# If status is original or aligned, there is only one cwt file,
# so function needs to be assigned to an object that is then available in the global env

# If status is appended or recoding, then there are several cwts that are automatically
# loaded in the global envr

load_cwt <- function(proj, status = "appended") {

  #proj = "ases"
  #status = "appended"

  print(proj)

  EXT = "\\.(xlsx|csv|ods)$"

  if(status == "appended") {

    CWT_DIR = file.path(Sys.getenv("ROOT_DIR"), "cwts", "appended", proj)

  } else if (status == "aligned") {

    CWT_DIR = file.path(Sys.getenv("ROOT_DIR"), "cwts", "aligned")

  } else if (status == "recoded") {

    CWT_DIR = file.path(Sys.getenv("ROOT_DIR"), "cwts", "recoding", proj)

  } else if (status == original) {

    CWT_DIR = file.path(Sys.getenv("ROOT_DIR"), "cwts", "original")

  }

  stopifnot("CWT directory does not exist" = dir.exists(CWT_DIR))

  if(status == "aligned") {

    cwt_file <- list.files(CWT_DIR, pattern = paste0("^", proj, ".*", EXT))
    if (length(cwt_file) == 0) {
      stop(sprintf("No CWT files found for this project: %s", proj), call. = FALSE)
    }
    message(paste("Reading CWT file:", cwt_file))
    cwt <- read_file(paste0(CWT_DIR, "/", cwt_file))

  } else if (status == "original") {

    cwt_file <- list.files(CWT_DIR, pattern = paste0("^", proj, "_cwt", EXT))
    if (length(cwt_file) == 0) {
      stop(sprintf("No CWT files found for this project: %s", proj), call. = FALSE)
    }

    message(paste("Reading CWT file:", cwt_file))
    cwt <- read_file(paste0(CWT_DIR, "/", cwt_file))

  } else if (status == "appended" || status == "recoding") {

    cwt_file <- list.files(CWT_DIR)

    if (length(cwt_file) == 0) {
    stop(sprintf("No CWT files found for this project: %s", proj), call. = FALSE)
    }

    for (f in cwt_file){

      file_name <- gsub(EXT, "", f)

      file <- read_file(paste0(CWT_DIR, "/", f))

      assign(paste0(proj, "_", file_name), file, envir = .GlobalEnv)

    }
  }

}



# Load Annotations from DB ----

load_annotations <- function(conn, proj, reshape = FALSE) {


  ANN_TABLE = paste0(proj, "_src_annotations")
  ID_COLS = c("dataset_id", "study_wave")
  SPLIT_SEPARATORS = "\\s*[;,]\\s*"

  query <- paste0("SELECT * FROM ", ANN_TABLE, ";")
  ann <- DBI::dbGetQuery(conn, query)


  missing_cols <- setdiff(ID_COLS, names(ann))
  if (length(missing_cols) > 0) {
    stop("Annotation table is missing required id column(s): ",
         paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  if (reshape == FALSE) {
    message(paste("Loaded", nrow(ann), "row(s) from", ANN_TABLE, "in wide format\n"))
    return(ann)

  } else {

    # pivot to long (base R)
    id_df <- ann[ID_COLS]
    value_cols <- setdiff(names(ann), ID_COLS)

    long_list <- lapply(value_cols, function(col) {
      out <- id_df
      out$target_var <- col
      out$src_var <- ann[[col]]
      out
    })

    ann_long <- do.call(rbind, long_list)

    # split multiple mappings into rows
    ann_long$src_var <- trimws(as.character(ann_long$src_var))
    keep <- !is.na(ann_long$src_var) & ann_long$src_var != ""
    ann_long <- ann_long[keep, , drop = FALSE]


    if (nrow(ann_long) > 0) {

      parts <- strsplit(ann_long$src_var, SPLIT_SEPARATORS, perl = TRUE)
      n_parts <- lengths(parts)

      ann_long_expanded <- ann_long[rep(seq_len(nrow(ann_long)), n_parts), , drop = FALSE]
      ann_long_expanded$src_var <- trimws(unlist(parts, use.names = FALSE))

      keep2 <- !is.na(ann_long_expanded$src_var) &
        ann_long_expanded$src_var != ""

      ann_long <- ann_long_expanded[keep2, , drop = FALSE]
    }

    rownames(ann_long) <- NULL

    message(paste("Loaded", nrow(ann_long), "annotation mapping row(s) from",
                  ANN_TABLE, "in long format"))


    ann_long

  }

}




# Get Variable lookup ----


get_vars_lookup <- function(conn,
                            proj,
                            include_dataset_id = TRUE) {

  # Constants
  PROJ <-  match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                                       "intune", "issp", "lits", "nbb", "neb", "wvs"), several.ok = TRUE)


  # Load survey here
  my_survey_list <- get_survey_list(conn = conn, proj = PROJ)

  # Validations
  #stopifnot


  if (!is.list(my_survey_list) || is.null(names(my_survey_list))) {
    stop("`my_survey_list` must be a named list of data frames.", call. = FALSE)
  }

  lookup_list <- list()

  for (i in seq_along(my_survey_list)) {

    survey_tag <- names(my_survey_list)[i]
    data       <- my_survey_list[[i]]

    if (is.null(data)) {
      if (verbose) cat("Skipping NULL survey:", survey_tag, "\n")
      next
    }

    var_names  <- names(data)

    var_labels <- vapply(var_names, function(x){
      label <- attr(data[[x]], "label")
      if (is.null(label)) {
        ""
      } else if (length(label) > 1) {
        stop(paste0("Column `", x, "` has more than one label."), call. = FALSE)
      } else {
        as.character(label)[1]
      }
    },
    character(1)
    )


    if (include_dataset_id) {

      if (!("dataset_id" %in% names(data))) {
        stop("`include_dataset_id = TRUE` but `dataset_id` column not found in survey: ",
             survey_tag, call. = FALSE)
      }

      dataset_id <- data$dataset_id[1]

      survey_lookup <- data.frame(
        dataset_id = dataset_id,
        study_wave = survey_tag,
        var_name   = var_names,
        var_label  = var_labels,
        stringsAsFactors = FALSE
      )

    } else {

      survey_lookup <- data.frame(
        study_wave = survey_tag,
        var_name   = var_names,
        var_label  = var_labels,
        stringsAsFactors = FALSE
      )
    }

    lookup_list[[i]] <- survey_lookup
  }

  all_vars_lookup_table <- do.call(rbind, lookup_list)
  rownames(all_vars_lookup_table) <- NULL

  all_vars_lookup_table
}



