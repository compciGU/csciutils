
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

load_var_validations <- function(
    conn,
    proj            = NULL,
    my_survey_list  = NULL,
    include_validations = c("dataset_id", "t_annotations")
) {
  # Validate
  VALID_PROJS <- c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                   "intune", "issp", "lits", "nbb", "neb", "wvs")

  stopifnot(
    "Either `proj` or `my_survey_list` must be specified."  = !is.null(proj) || !is.null(my_survey_list),
    "`my_survey_list` must be a named list of data frames." = is.null(my_survey_list) || (is.list(my_survey_list) && !is.null(names(my_survey_list)))
  )

  if (!is.null(proj)) {
    proj <- match.arg(proj, choices = VALID_PROJS)
  }

  if (!is.null(my_survey_list)) {
    proj <- sub("_.*", "", names(my_survey_list)[1])
  } else {
    stop("Loading survey data from `proj` alone is not yet implemented.", call. = FALSE)
  }

  VALIDATORS <- match.arg(include_validations,
                          choices = c("dataset_id", "t_annotations"),
                          several.ok = TRUE)

  use_dataset_id  <- "dataset_id"    %in% VALIDATORS
  use_annotations <- "t_annotations" %in% VALIDATORS

  # Load annotations
  if (use_annotations) {
    annotations <- load_annotations(conn = conn, proj = proj, reshape = TRUE)
    char_cols <- vapply(annotations, is.character, logical(1))
    annotations[char_cols] <- lapply(annotations[char_cols], tolower)
  }

  # Build one lookup table per survey
  lookup_list <- vector("list", length(my_survey_list))

  for (i in seq_along(my_survey_list)) {
    survey_tag <- names(my_survey_list)[i]
    data       <- my_survey_list[[i]]

    if (is.null(data)) next

    if (!is.data.frame(data)) {
      stop(paste0("Survey `", survey_tag, "` is not a data frame."), call. = FALSE)
    }
    if (use_dataset_id && !"dataset_id" %in% names(data)) {
      stop(paste0("`dataset_id` column not found in survey `", survey_tag, "`."), call. = FALSE)
    }

    # Extract variable labels
    var_labels <- vapply(names(data), function(col) {
      label <- attr(data[[col]], "label")
      if (is.null(label)) return("")
      if (length(label) > 1) {
        stop(paste0("Column `", col, "` in survey `", survey_tag, "` has more than one label."), call. = FALSE)
      }
      as.character(label)
    }, character(1))

    # Build core lookup table
    lookup <- data.frame(
      study_wave = survey_tag,
      var_name   = names(data),
      var_label  = var_labels,
      stringsAsFactors = FALSE
    )

    if (use_dataset_id) {
      lookup <- cbind(dataset_id = data$dataset_id[1], lookup)
    }

    # Filter & join target annotations
    if (use_annotations) {
      wave_ann <- annotations[annotations$study_wave %in% survey_tag, , drop = FALSE]
      if (nrow(wave_ann) > 0) {
        lookup   <- lookup[lookup$var_name %in% wave_ann$src_var, , drop = FALSE]
        idx      <- match(lookup$var_name, wave_ann$src_var)
        lookup$target_var <- wave_ann$target_var[idx]
      }
    }

    lookup_list[[i]] <- lookup
  }

  # Combine
  result <- do.call(rbind, lookup_list)
  rownames(result) <- NULL
  result
}



