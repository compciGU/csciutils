# User level CWT functions ----

## Bind CWTS if stored seperatly in file system ----

bind_cwts <- function(proj, status, indicator) {

  #proj <- "ases"
  #indicator <- c("swd", "tech", "controls")
  #status <- "appended"

  if(status == "appended") {

    CWT_DIR = file.path(Sys.getenv("ROOT_DIR"), "cwts", "appended", proj)

  } else if (status == "recoded") {

    CWT_DIR = file.path(Sys.getenv("ROOT_DIR"), "cwts", "recoding", proj)

  }

  EXT <- c(".xlsx", ".odt", ".csv")

  ll <- list()

  for(i in indicator) {

    #i <- "swd"

    file_candidates <- file.path(CWT_DIR, paste0(i, EXT))
    cwt_file <- file_candidates[file.exists(file_candidates)][1]

    if (is.na(cwt_file))
      stop(sprintf("No file found for %s with extensions %s", i,
                   paste(EXT, collapse = ", ")), call. = FALSE)

    df <- read_file(cwt_file)

    ll[[i]] <- df

  }

  combined_cwt <- do.call(rbind, ll)
  rownames(combined_cwt) <- NULL

  return(combined_cwt)

}







## CWT write function ----

write_cwt <- function(cwt, proj = NULL, split = FALSE, ...) {    # if you want to filter for only swd then you can idicate this in the split

  # Constants
  SPLIT_GROUPS <- list(
    controls = c("t_age", "t_cntry", "t_female", "t_lrscale", "t_polint", "t_ageedu",
                 "t_educ", "t_isced", "t_yrsedu", "t_educ2", "t_educ3", "t_year",
                 "t_yob", "t_ageedu_year", "t_ageedu2", "t_educ_cs", "t_satlife"),
    swd     = c("t_satdem", "t_satdem_eu", "t_satpolitics", "t_satpolsys",
                "t_pridedem", "t_satdemdev"),
    tech    = c("t_sampdesign", "t_surveymode", "t_resprate", "t_langinterview",
                "t_caseid", "t_weight_pst", "t_wave", "t_weight", "t_weight_des",
                "t_weight_despst"),
    trust   = c("trust_army", "trust_gov", "trust_jus", "trust_parl", "trust_police",
                "trust_polpart", "trust_press", "trust_rel", "trust_tv", "trust_army2",
                "trust_parl2", "trust_rel2", "trust_police2", "trust_polpart2",
                "trust_media", "trust_jus2", "trust_publofficials", "trust_civserv")
  )

  # split argument:
  #   FALSE         -> no split
  #   TRUE          -> split on all groups
  #   character vector -> split on specified groups only
  SPLIT <- if (isFALSE(split)) {
    FALSE
  } else if (isTRUE(split)) {
    names(SPLIT_GROUPS)
  } else {
    match.arg(split, choices = names(SPLIT_GROUPS), several.ok = TRUE)
  }

  PROJ <- if (!is.null(proj)) {
    match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses",
                                "eb", "eqls", "ess", "evs",
                                "intune", "issp", "lits", "nbb",
                                "neb", "wvs"))
  } else {
    sw <- cwt$study_wave[1]
    if (is.null(sw) || is.na(sw)) stop(
      "`proj` is NULL and `cwt$study_wave[1]` is missing; cannot derive a file name.",
      call. = FALSE
    )
    tolower(sub("_.*$", "", sw))
  }



  ROOT_DIR <- Sys.getenv("ROOT_DIR")
  DIR_APPENDED <- file.path(ROOT_DIR, "cwts", "appended", PROJ)
  DIR_RECODED  <- file.path(ROOT_DIR, "cwts", "recoded",  PROJ)

  # Validate inputs
  stopifnot("'cwt' must be a data.frame" = is.data.frame(cwt))
  stopifnot("'ROOT_DIR' environment variable is not set" =  ROOT_DIR != "")
  stopifnot("'appended' output dir does not exist" = dir.exists(DIR_APPENDED))
  stopifnot("'recoded' output dir does not exist"  = dir.exists(DIR_RECODED))




  # Build (split) data list and file paths
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

  if (!isFALSE(SPLIT)) {
    # split them in groups
    f_list <- setNames(vector("list", length(SPLIT)), SPLIT)
    for(f in SPLIT){
      f_list[[f]] <- cwt[cwt$target_var %in% SPLIT_GROUPS[[f]], ]
    }
    paths_appended  <- file.path(DIR_APPENDED, paste0(SPLIT, ".xlsx"))
    paths_recoded   <- file.path(DIR_RECODED,  paste0(SPLIT, "_", timestamp, ".xlsx"))
  } else {
    f_list          <- list(full_cwt = cwt)
    paths_appended  <- file.path(DIR_APPENDED, "full_cwt.xlsx")
    paths_recoded   <- file.path(DIR_RECODED,  paste0("full_cwt_", timestamp, ".xlsx"))
  }



  # Write files
  lapply(seq_along(f_list), function(i){
    write_file(f_list[[i]], path = paths_appended[i], ...)
    write_file(f_list[[i]], path = paths_recoded[i],  ...)
    })


  invisible(NULL)
}




## Append CWT`s source variable annotations to existing CWT ----


append_cwt <- function(conn,
                       proj) {

  # Constants
  #CONN <-  conn #we could remove the conn argument if we have ian object conn in the global environment
  PROJ <-  match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                                       "intune", "issp", "lits", "nbb", "neb", "wvs"), several.ok = TRUE)

   # Load objects
  cwt <- load_cwt(proj = PROJ, status = "aligned")   # Load cwts
  annotations <- load_annotations(conn = conn, proj = PROJ, reshape = T) # Load annotations

  # Validations
  stopifnot("'annotations' must be a data.frame" = is.data.frame(annotations))
  validate_existing_cwt(cwt)                                                                         # custom helper function to validate CWT input

  # Append annotations to existing CWT
  has_waves <- length(unique(cwt$study_wave)) > 1

  # if (align_surveys_with_cwt) {
  #   if (has_waves) {
  #     my_survey_list <- align_survey_to_cwt(my_survey_list, cwt)                                     # custom helper function to align study waves to waves existing in supplied cwt - probably not needed as I don`t want to load the surveys twice - in the append and build cwt functions
  #   } else {
  #     message("NOTE: `align_surveys_with_cwt = TRUE` was set but CWT has only one wave — alignment skipped.")
  #   }
  # }

  # Check for missing vars in CWT and append
  missing_ann <- find_missing_vars(cwt, annotations, has_waves)                                      # custom helper function to identify missing vars in cet that are in the annotations

  if (nrow(missing_ann) == 0) {
    message("No missing variable annotations to add. Returning original CWT unchanged.")
    return(invisible(cwt))
  }

  message("Found ", nrow(missing_ann), " missing variable annotation(s). Building CWT rows.")

  cwt_new <- build_cwt(
    conn = conn,
    proj = PROJ,
    annotations  = missing_ann
  )

  cwt_bind <- dplyr::bind_rows(cwt, cwt_new)

  message("Appended ", nrow(cwt_new), " new row(s) to CWT.")

  invisible(cwt_bind)
}


## Build CWTs from scratch ----


build_cwt <- function(conn, proj, annotations = NULL) {

  # Constants
  MISSING_CODES <- c("-999", "-99", "999", "99")
  PROJ <-  match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                                       "intune", "issp", "lits", "nbb", "neb", "wvs"), several.ok = TRUE)

                                        # custom helper function to validate annotation input

  # Load objects
  my_survey_list <- load_survey_list(conn = conn, proj = PROJ) # Load survey list
  if (is.null(annotations)) annotations <- load_annotations(conn = conn, proj = PROJ, reshape = TRUE) # Load annotations only if non supplied: I need this condition to append only missing annotations in the cwt_append function otherwise I would always re-build from scratch and the append

  ## Remove missing codes in source annotations
  src_chr           <- trimws(as.character(annotations$src_var))
  annotations_clean <- annotations[!(src_chr %in% trimws(as.character(MISSING_CODES))), , drop = FALSE]

  # Validations
  validate_create_cwt(my_survey_list, annotations_clean)

  # Build the CWTs based on supplied variable annotations - I keep the loop but reallocate the list now for faster execution
  survey_names <- names(my_survey_list)
  labels_list <- vector("list", length(survey_names))
  names(labels_list) <- survey_names

  for (i in seq_along(survey_names)) {
    n <- survey_names[i]

    message("Survey: ", n)

    dataset <- my_survey_list[[n]]
    study_wave <- n

    vars <- annotations_clean$src_var[
      tolower(annotations_clean$study_wave) == tolower(study_wave)
    ]

    dataset_filtered <- dataset[
      , tolower(names(dataset)) %in% tolower(vars),
      drop = FALSE
    ]

    var_names <- names(dataset_filtered)
    survey_result <- vector("list", length(var_names))

    for (j in seq_along(var_names)) {
      v <- var_names[j]

      target_var <- annotations_clean$target_var[
        tolower(annotations_clean$src_var) == tolower(v) &
          tolower(annotations_clean$study_wave) == tolower(study_wave)
      ]
      target_var <- target_var[!is.na(target_var)]

      vec <- dataset_filtered[[v]]

      fmt <- format_value_labels(vec, v, study_wave)

      n_obs_fmt <- format_number_obs(
        vec,
        fmt$code_formatted,
        fmt$label_formatted
      )

      survey_result[[j]] <- data.frame(
        study_wave   = study_wave,
        var_name     = v,
        var_label    = fmt$var_label,
        value_n      = n_obs_fmt,
        value        = fmt$label_formatted,
        value_code   = fmt$code_formatted,
        target_var   = target_var,
        target_value = "",
        stringsAsFactors = FALSE
      )
    }

    labels_list[[i]] <- survey_result
  }

  labels_list <- unlist(labels_list, recursive = FALSE)

  cwt <- dplyr::bind_rows(labels_list)
  cwt <- collapse_tech_vars(cwt)                                                                # helper function to collapse technical variables to only one row in the cwt

  # check for annotations that did not appear in the dataset
  missing <- setdiff(
    tolower(annotations_clean$src_var),
    unique(tolower(cwt$var_name))
  )

  if (length(missing) > 0) {
    message(paste(
      "NOTE: These source variable annotations were not found in the survey data:",
      paste(missing, collapse = ", "),
      "-> Please double-check src_var names.")
    )
  }

  cwt
}


# CWT Helper functions ----

## Validate Helper functions ----


# see in build_or_append_cwt
validate_existing_cwt <- function(cwt) {

  # Constants
  REQUIRED_COLS <- c("var_name", "study_wave", "target_var")

  # Validations
  missing_cols <- setdiff(REQUIRED_COLS, names(cwt))

  if (length(missing_cols) > 0) {
    stop(
      "`cwt` is missing required columns: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}


validate_create_cwt <- function(my_survey_list, annotations) {

  # Constants
  REQUIRED_COLS <- c("src_var", "target_var", "study_wave")

  # Validations
  stopifnot("`my_survey_list` must be a named list." = is.list(my_survey_list))
  stopifnot("`my_survey_list` must have names."      = !is.null(names(my_survey_list)))
  stopifnot("`annotations` must be a data.frame."    = is.data.frame(annotations))

  missing_cols <- setdiff(REQUIRED_COLS, names(annotations))

  if (length(missing_cols) > 0) {
    stop(
      "`annotations` is missing required columns: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  duplicates <- duplicated(annotations[c("study_wave", "src_var")]) |
    duplicated(annotations[c("study_wave", "src_var")], fromLast = TRUE)

  if (any(duplicates)) {
    dup_rows <- annotations[duplicates, c("study_wave", "src_var", "target_var")]

    stop(
      paste(
        "The (study_wave, src_var, target_var) mapping in `annotations` must be unique.",
        "Duplicate rows found:",
        paste(apply(dup_rows, 1, paste, collapse = " | "), collapse = "\n"),
        sep = "\n"
      ),
      call. = FALSE
    )
  }

  unmatched_waves <- setdiff(
    unique(tolower(annotations$study_wave)),
    tolower(names(my_survey_list))
  )

  if (length(unmatched_waves) > 0) {
    warning(
      "These study_wave values in `annotations` have no match in `my_survey_list`: ",
      paste(unmatched_waves, collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

## Align helper functions ----

# align_survey_to_cwt <- function(my_survey_list, cwt) {
#   waves_cwt    <- tolower(unique(cwt$study_wave))
#   waves_survey <- tolower(names(my_survey_list))
#   filtered     <- my_survey_list[waves_survey %in% waves_cwt]
#
#   missing_waves <- setdiff(waves_cwt, waves_survey)
#   if (length(missing_waves) > 0) {
#     warning(
#       "These CWT waves are not present in my_survey_list: ",
#       paste(missing_waves, collapse = ", "),
#       call. = FALSE
#     )
#   }
#
#   filtered
# }

find_missing_vars <- function(cwt, annotations, has_waves) {

  ann            <- annotations
  ann$src_var    <- tolower(ann$src_var)
  ann$study_wave <- tolower(as.character(ann$study_wave))

  if (has_waves) {

    cwt_vars <- data.frame(
      src_var    = tolower(cwt$var_name),
      study_wave = tolower(as.character(cwt$study_wave)),
      stringsAsFactors = FALSE
    )

    dplyr::anti_join(
      ann,
      cwt_vars,
      by = c("study_wave", "src_var")
    )

  } else {

    missing <- setdiff(
      tolower(ann$src_var),
      tolower(unique(cwt$var_name))
    )

    out <- ann[tolower(ann$src_var) %in% missing, , drop = FALSE]

    out[!is.na(out$src_var), ]
  }
}


collapse_tech_vars <- function(cwt) {

  TECH_VARS <- c("t_weight", "t_caseid", "t_weight_des", "t_weight_despst",  "t_sampdesign")

  is_tech  <- cwt$target_var %in% TECH_VARS
  cwt_sub  <- cwt[!is_tech, ]
  cwt_tech <- cwt[is_tech, ]

  if (nrow(cwt_tech) == 0) return(cwt_sub)

  cwt_tech <- cwt_tech[!duplicated(cwt_tech[c("study_wave", "var_name")]), ]
  cwt_tech[, c("value_n", "value", "value_code")] <- NA

  rbind(cwt_sub, cwt_tech)
}

## Dormat helper functions ----

format_value_labels <- function(var_values, var_name, wave_name, verbose = TRUE) {

  # extract variable label (from Stata/haven metadata); use NA if absent
  var_label <- attr(var_values, "label")
  if (is.null(var_label)) var_label <- NA_character_

  # extract Stata value labels if present
  labels <- attr(var_values, "labels", exact = TRUE)

  # get unique observed values as strings, drop NAs, sort numerically
  labels_obs <- unique(as.character(unclass(var_values)))
  labels_obs <- labels_obs[!is.na(labels_obs)]
  labels_obs <- labels_obs[order(as.numeric(labels_obs))]

  # Case 1: no Stata labels — fall back to observed values
  if (is.null(labels) || all(is.na(labels))) {

      message("NOTE: No Stata value labels found for variable: ", var_name,
              " in dataset: ", wave_name, " - using observed values instead.")

    label_formatted <- paste0("[", labels_obs, "] ", labels_obs)
    code_formatted  <- labels_obs

  } else {

    # sort Stata labels by their numeric code
    labels      <- labels[order(as.numeric(unname(labels)))]
    stata_codes <- as.character(unname(labels))

    # Case 2a: more observed values than Stata labels — merge both
    if (length(unique(labels_obs)) > length(unique(stata_codes))) {

        message("NOTE: More observed values than Stata labels for variable: ", var_name,
                " in dataset: ", wave_name,
                " - combining observed values with Stata labels.")

      extra_obs <- setdiff(labels_obs, stata_codes)
      obs_fmt   <- paste0("[", extra_obs, "] ", extra_obs)

      label_text <- .strip_stata_prefix(names(labels))
      stata_fmt  <- paste0("[", stata_codes, "] ", label_text)

      combined <- c(obs_fmt, stata_fmt)
      codes    <- as.numeric(sub("^\\[(-?\\d+)\\].*", "\\1", combined))
      label_formatted <- combined[order(codes)]

      code_formatted <- c(labels_obs, setdiff(stata_codes, labels_obs))

    } else {

      # Case 2b: Stata labels fully cover observed values — use labels as-is
        message("NOTE: Using Stata value labels for variable: ", var_name,
                " in dataset: ", wave_name,
                " - observed values fully covered by labels.")

      label_text      <- .strip_stata_prefix(names(labels))
      label_formatted <- paste0("[", stata_codes, "] ", label_text)
      code_formatted  <- stata_codes
    }
  }

  list(
    var_label       = var_label,
    label_formatted = label_formatted,
    code_formatted  = code_formatted
  )
}

format_number_obs <- function(var_values, code_formatted, label_formatted) {
  tab       <- table(as.character(unclass(var_values)), useNA = "ifany")
  n_obs_raw <- tab[match(code_formatted, names(tab))]
  n_obs_raw[is.na(n_obs_raw)] <- 0
  paste0(label_formatted, ": ", as.integer(n_obs_raw))
}

.strip_stata_prefix <- function(x) {                                           # internal helper function of format_value_labels
  gsub("^\\s*[-+]?\\d+(?:\\.\\d+)?\\s*\\.\\s*", "", x, perl = TRUE)
}



