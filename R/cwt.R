# User-facing functions ----


<<<<<<< HEAD
#' Combine crosswalk tables stored as separate files
#'
#' @description
=======
#' Bind crosswalk tables stored as separate files
#'
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#' Read and row-bind crosswalk table (CWT) files for one or more indicators
#' from a project's \code{appended} or \code{recoded} directory.
#'
#' @param proj A single project code, such as \code{"ess"} or \code{"eb"}.
#' @param status Which CWT version to load. Must be one of
#'   \code{"appended"} or \code{"recoded"}.
#' @param indicator A character vector of indicator groups to load, such as
#'   \code{c("swd", "controls")}.
#'
<<<<<<< HEAD
#' @returns A data frame formed by row-binding the requested CWT files which - Columns are not modified.
=======
#' @returns A data frame formed by row-binding the requested CWT files.
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#'
#' @details
#' For each indicator, the function looks for a file named
#' \code{<indicator>.<ext>} and reads the first match in the order
#' \code{.xlsx}, \code{.odt}, \code{.csv}.
#'
#' The \code{ROOT_DIR} environment variable must point to the project root.
#'
#' @references
#' Kołczyńska, M. (2022). Combining multiple survey sources: A reproducible
#' workflow and toolbox for survey data harmonization. \emph{Methodological
#' Innovations}, 15(1), 62--72. \doi{10.1177/20597991221077923}
#'
#' @seealso [write_cwt()], [build_cwt()], [append_cwt()]
#' @family cwt
#'
#' @examplesIf Sys.getenv("ROOT_DIR") != ""
#' bind_cwts(
#'   proj = "ess",
#'   status = "appended",
#'   indicator = c("swd", "controls")
#' )
#' @export
bind_cwts <- function(proj, status, indicator) {

  if (status == "appended") {

    CWT_DIR <- file.path(Sys.getenv("ROOT_DIR"), "cwts", "appended", proj)

  } else if (status == "recoded") {

    CWT_DIR <- file.path(Sys.getenv("ROOT_DIR"), "cwts", "recoded", proj)

  }

  EXT <- c(".xlsx", ".odt", ".csv")

  ll <- list()

  for (i in indicator) {

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




<<<<<<< HEAD
#' Save a crosswalk table to a disk
#'
#' @description
#' Write a crosswalk table (CWT) to the project's \code{appended} directory as
#' the appended snapshot and to the \code{recoded} directory as a timestamped
#' file to code target values.
=======
#' Write a crosswalk table to disk
#'
#' Write a crosswalk table (CWT) to the project's \code{appended} directory as
#' the current version and to the \code{recoded} directory as a timestamped
#' snapshot.
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#'
#' @param cwt A data frame containing the crosswalk table.
#' @param proj A project code. If `NULL`, the function derives it from
#'   \code{cwt$study_wave[1]}.
<<<<<<< HEAD
#' @param split Controls whether to split the CWT in controls, SwD, trust, tech varibale subsets before writing.
=======
#' @param split Controls whether to split the CWT before writing.
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#' @param ... Additional arguments passed to [csciutils::write_file()].
#'
#' @returns `NULL`, invisibly.
#'
#' @section Split options:
#' \itemize{
#'   \item \code{FALSE}: write the full CWT as \code{full_cwt.xlsx}.
#'   \item \code{TRUE}: split into all predefined groups.
#'   \item A character vector: split into the selected groups only.
#' }
#'
#' @section Directory layout:
#' Both output directories must exist before calling \code{write_cwt()}:
#'
#' \preformatted{
#' <ROOT_DIR>/cwts/appended/<proj>/
#' <ROOT_DIR>/cwts/recoded/<proj>/
#' }
#'
#' @section Thematic groups:
#' When \code{split} is not \code{FALSE}, rows are assigned by matching
#' \code{target_var} against predefined groups:
#'
#' \itemize{
#'   \item \code{"controls"}: socio-demographic variables.
#'   \item \code{"swd"}: satisfaction with democracy variables.
#'   \item \code{"tech"}: technical and design variables.
#'   \item \code{"trust"}: trust in institutions variables.
#' }
#'
#' @section File naming:
#' Appended files are written as \code{<group>.xlsx}. Recoded files append a
#' \code{YYYY_MM_DD} timestamp.
#'
#' @references
#' Kołczyńska, M. (2022). Combining multiple survey sources: A reproducible
#' workflow and toolbox for survey data harmonization. \emph{Methodological
#' Innovations}, 15(1), 62--72. \doi{10.1177/20597991221077923}
#'
#' @seealso [build_cwt()], [append_cwt()], [bind_cwts()]
#' @family cwt
#'
#' @examplesIf Sys.getenv("ROOT_DIR") != ""
#' write_cwt(cwt, proj = "ess")
#' write_cwt(cwt, proj = "ess", split = TRUE)
#' write_cwt(cwt, proj = "ess", split = c("swd", "controls"))
#' @export
write_cwt <- function(cwt, proj = NULL, split = FALSE, ...) {

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

  ROOT_DIR     <- Sys.getenv("ROOT_DIR")
  DIR_APPENDED <- file.path(ROOT_DIR, "cwts", "appended", PROJ)
  DIR_RECODED  <- file.path(ROOT_DIR, "cwts", "recoded",  PROJ)

  stopifnot("'cwt' must be a data.frame"              = is.data.frame(cwt))
  stopifnot("'ROOT_DIR' environment variable not set" = ROOT_DIR != "")
  stopifnot("'appended' output dir does not exist"    = dir.exists(DIR_APPENDED))
  stopifnot("'recoded' output dir does not exist"     = dir.exists(DIR_RECODED))

  ts <- timestamp()

  if (!isFALSE(SPLIT)) {
    f_list <- setNames(vector("list", length(SPLIT)), SPLIT)
    for (f in SPLIT) {
      f_list[[f]] <- cwt[cwt$target_var %in% SPLIT_GROUPS[[f]], ]
    }
    paths_appended <- file.path(DIR_APPENDED, paste0(SPLIT, ".xlsx"))
    paths_recoded  <- file.path(DIR_RECODED,  paste0(SPLIT, "_", ts, ".xlsx"))
  } else {
    f_list         <- list(full_cwt = cwt)
    paths_appended <- file.path(DIR_APPENDED, "full_cwt.xlsx")
    paths_recoded  <- file.path(DIR_RECODED,  paste0("full_cwt_", ts, ".xlsx"))
  }

  lapply(seq_along(f_list), function(i) {
    csciutils::write_file(f_list[[i]], path = paths_appended[i], ...)
    csciutils::write_file(f_list[[i]], path = paths_recoded[i],  ...)
  })

  invisible(NULL)
}




<<<<<<< HEAD
#' Bind new variable annotations to an existing crosswalk table
#'
#' @description
#' Compare source-variable annotations in the database with an existing aligned
#' CWT and append rows only for variables not yet documented.
#'
#'@inheritParams build_cwt
#'
#' @returns The updated CWT data frame appended with multiple rows per variable (one row per variable value) not yet included in the original CWT, returned invisibly. Takes columns from [build_cwt()] which are not modified. If no variables are missing,
=======
#' Append new variable annotations to an existing crosswalk table
#'
#' Compare source-variable annotations in the database with an existing aligned
#' CWT and append rows only for variables not yet documented.
#'
#' @param conn A database connection passed to [load_annotations()] and
#'   [build_cwt()].
#' @param proj One or more survey project codes. Must be one or more of: `"ases"`, `"cceb"`, `"cdcee"`, `"cses"`,
#'   `"eb"`, `"eqls"`, `"ess"`, `"evs"`, `"intune"`, `"issp"`, `"lits"`,
#'   `"nbb"`, `"neb"`, `"wvs"`.
#'
#' @returns The updated CWT, returned invisibly. If no variables are missing,
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#'   the original CWT is returned unchanged.
#'
#' @details
#' Missing variables are identified by comparing the annotation table with the
#' existing CWT on \code{study_wave} and \code{src_var}. The missing rows are
#' then built with [build_cwt()] and appended.
#'
#' The input CWT must contain at least \code{var_name}, \code{study_wave}, and
#' \code{target_var}.
#'
#' @references
#' Kołczyńska, M. (2022). Combining multiple survey sources: A reproducible
#' workflow and toolbox for survey data harmonization. \emph{Methodological
#' Innovations}, 15(1), 62--72. \doi{10.1177/20597991221077923}
#'
#' @seealso [build_cwt()], [write_cwt()], [find_missing_vars()]
#' @family cwt
#'
#' @examplesIf Sys.getenv("ROOT_DIR") != ""
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' cwt_updated <- append_cwt(conn = con, proj = "ess")
#' @export
append_cwt <- function(conn, proj) {

  PROJ <- match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                                      "intune", "issp", "lits", "nbb", "neb", "wvs"), several.ok = TRUE)

  cwt         <- load_cwt(proj = PROJ, status = "aligned")
  annotations <- load_annotations(conn = conn, proj = PROJ, reshape = TRUE)

  stopifnot("'annotations' must be a data.frame" = is.data.frame(annotations))
  validate_existing_cwt(cwt)

  has_waves   <- length(unique(cwt$study_wave)) > 1
  missing_ann <- find_missing_vars(cwt, annotations, has_waves)

  if (nrow(missing_ann) == 0) {
    message("No missing variable annotations to add. Returning original CWT unchanged.")
    return(invisible(cwt))
  }

  message("Found ", nrow(missing_ann), " missing variable annotation(s). Building CWT rows.")

  cwt_new  <- build_cwt(conn = conn, proj = PROJ, annotations = missing_ann)
  cwt_bind <- dplyr::bind_rows(cwt, cwt_new)

  message("Appended ", nrow(cwt_new), " new row(s) to CWT.")

  invisible(cwt_bind)
}




<<<<<<< HEAD
#' Create a crosswalk table from source survey data
#'
#' @description
#' Build a crosswalk table (CWT) from scratch by extracting variable labels, value labels,
#' and observed frequencies for annotated source variables from stata files.
#'
#' @param conn A database connection passed to inline functions used to load survey data, exisitn CWTs or annotations.
#' @param proj One or more survey project codes. Must be one or more of: `"ases"`, `"cceb"`, `"cdcee"`, `"cses"`,
#'   `"eb"`, `"eqls"`, `"ess"`, `"evs"`, `"intune"`, `"issp"`, `"lits"`,
#'   `"nbb"`, `"neb"`, `"wvs"`.
#' @param annotations A data frame with at least \code{src_var},
#'   \code{target_var}, and \code{study_wave}. If `NULL` (the default), annotations are
=======
#' Build a crosswalk table from source survey data
#'
#' Build a crosswalk table (CWT) by extracting variable labels, value labels,
#' and observed frequencies for annotated source variables.
#'
#' @param conn A database connection used to load survey data and annotations.
#' @param proj One or more survey project codes.
#' @param annotations A data frame with at least \code{src_var},
#'   \code{target_var}, and \code{study_wave}. If `NULL`, annotations are
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#'   loaded from the database.
#'
#' @returns A data frame with one row per unique source value, variable, and
#'   survey wave.
#'
#' \describe{
<<<<<<< HEAD
#'   \item{study_wave}{Survey wave identifier (e.g. eb_37.2 or issp_2009).}
=======
#'   \item{study_wave}{Survey wave identifier.}
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
#'   \item{var_name}{Source variable name.}
#'   \item{var_label}{Source variable label.}
#'   \item{value_n}{Formatted value label with observed count.}
#'   \item{value}{Formatted source value label, for example
#'     \code{"[1] Yes"}.}
#'   \item{value_code}{Source value code stored as character.}
#'   \item{target_var}{Harmonized target variable name.}
#'   \item{target_value}{Target value code, to be filled in during recoding.}
#' }
#'
#' Technical variables such as \code{t_weight} and \code{t_caseid} are reduced
#' to one row per wave, with value-level columns set to `NA`.
#'
#' @details
#' The function implements the variable-selection and values-crosswalk steps of
#' the workflow described in Kołczyńska (2022). For each annotated variable, it
#' extracts source labels with [format_value_labels()] and appends observed
#' counts with [format_number_obs()].
#'
#' Before processing, common missing-value codes (\code{"-999"},
#' \code{"-99"}, \code{"999"}, \code{"99"}) are removed from the annotation
#' table.
#'
#' @references
#' Kołczyńska, M. (2022). Combining multiple survey sources: A reproducible
#' workflow and toolbox for survey data harmonization. \emph{Methodological
#' Innovations}, 15(1), 62--72. \doi{10.1177/20597991221077923}
#'
#' @seealso [append_cwt()], [write_cwt()], [format_value_labels()]
#' @family cwt
#'
#' @examplesIf Sys.getenv("ROOT_DIR") != ""
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' cwt <- build_cwt(conn = con, proj = "ess")
#'
#' ann <- load_annotations(con, proj = "ess", reshape = TRUE)
#' cwt_trust <- build_cwt(
#'   conn = con,
#'   proj = "ess",
#'   annotations = ann[ann$target_var == "trust_parl", ]
#' )
#' @export
build_cwt <- function(conn, proj, annotations = NULL) {

  MISSING_CODES <- c("-999", "-99", "999", "99")
  PROJ <- match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                                      "intune", "issp", "lits", "nbb", "neb", "wvs"), several.ok = TRUE)

  my_survey_list <- load_survey_list(conn = conn, proj = PROJ)
  if (is.null(annotations)) annotations <- load_annotations(conn = conn, proj = PROJ, reshape = TRUE)

  src_chr           <- trimws(as.character(annotations$src_var))
  annotations_clean <- annotations[!(src_chr %in% trimws(as.character(MISSING_CODES))), , drop = FALSE]

  validate_create_cwt(my_survey_list, annotations_clean)

  survey_names <- names(my_survey_list)
  labels_list  <- vector("list", length(survey_names))
  names(labels_list) <- survey_names

  for (i in seq_along(survey_names)) {
    n <- survey_names[i]

    message("Survey: ", n)

    dataset    <- my_survey_list[[n]]
    study_wave <- n

    vars <- annotations_clean$src_var[
      tolower(annotations_clean$study_wave) == tolower(study_wave)
    ]

    dataset_filtered <- dataset[
      , tolower(names(dataset)) %in% tolower(vars),
      drop = FALSE
    ]

    var_names     <- names(dataset_filtered)
    survey_result <- vector("list", length(var_names))

    for (j in seq_along(var_names)) {
      v <- var_names[j]

      target_var <- annotations_clean$target_var[
        tolower(annotations_clean$src_var)    == tolower(v) &
          tolower(annotations_clean$study_wave) == tolower(study_wave)
      ]
      target_var <- target_var[!is.na(target_var)]

      vec       <- dataset_filtered[[v]]
      fmt       <- format_value_labels(vec, v, study_wave)
      n_obs_fmt <- format_number_obs(vec, fmt$code_formatted, fmt$label_formatted)

      survey_result[[j]] <- data.frame(
        study_wave   = toupper(study_wave),  # change if toupper or tolower
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
  cwt <- collapse_tech_vars(cwt)

  missing <- setdiff(
    tolower(annotations_clean$src_var),
    unique(tolower(cwt$var_name))
  )

  if (length(missing) > 0) {
    message(paste(
      "NOTE: These source variable annotations were not found in the survey data:",
      paste(missing, collapse = ", "),
      "-> Please double-check src_var names."
    ))
  }

  cwt
}


# Internal helper functions ----


#' Validate an existing crosswalk table
#'
#' Checks that a CWT data frame contains the minimum columns required by
#' [append_cwt()]. Stops with an informative error if any are missing.
#'
#' @param cwt A data frame to validate. Must contain `var_name`, `study_wave`,
#'   and `target_var`.
#'
#' @returns Invisibly returns `TRUE` if validation passes.
#'
#' @seealso [validate_create_cwt()] for the equivalent check applied to
#'   [build_cwt()] inputs; [append_cwt()] where this function is called.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
validate_existing_cwt <- function(cwt) {

  REQUIRED_COLS <- c("var_name", "study_wave", "target_var")
  missing_cols  <- setdiff(REQUIRED_COLS, names(cwt))

  if (length(missing_cols) > 0) {
    stop(
      "`cwt` is missing required columns: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  invisible(TRUE)
}


#' Validate inputs for crosswalk table creation
#'
#' Checks that the survey list and annotation data frame supplied to
#' [build_cwt()] satisfy all structural requirements before processing begins.
#' Stops on missing columns or duplicate `study_wave` x `src_var` keys;
#' warns about annotation waves with no matching survey.
#'
#' @param my_survey_list A named list of survey data frames, one element per
#'   study wave.
#' @param annotations A data frame with at least the columns `src_var`,
#'   `target_var`, and `study_wave`. Each `study_wave` x `src_var`
#'   combination must be unique.
#'
#' @returns Invisibly returns `TRUE` if all checks pass.
#'
#' @seealso [validate_existing_cwt()] for the equivalent check applied to
#'   [append_cwt()] inputs; [build_cwt()] where this function is called.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
validate_create_cwt <- function(my_survey_list, annotations) {

  REQUIRED_COLS <- c("src_var", "target_var", "study_wave")

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
        "The (study_wave, src_var) mapping in `annotations` must be unique.",
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




#' Find source variables missing from an existing crosswalk table
#'
#' Anti-joins the annotation table against an existing CWT to identify
#' variables that are annotated but not yet documented. Called internally by
#' [append_cwt()] to determine which rows to build.
#'
#' @param cwt A data frame representing the existing CWT. Must contain
#'   `var_name` and, when `has_waves = TRUE`, `study_wave`.
#' @param annotations A data frame of source-to-target variable annotations
#'   with at least the columns `src_var` and `study_wave`.
#' @param has_waves Logical. When `TRUE`, matching is performed jointly on
#'   `study_wave` x `src_var`. When `FALSE`, matching uses `src_var` only.
#'
#' @returns A data frame with the same columns as `annotations`, containing
#'   only the rows for variables absent from the CWT. Returns a zero-row data
#'   frame when all annotated variables are already present.
#'
#' @seealso [append_cwt()] where this function is called;
#'   [validate_existing_cwt()] for prior structural validation of `cwt`.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
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

    dplyr::anti_join(ann, cwt_vars, by = c("study_wave", "src_var"))

  } else {

    missing <- setdiff(tolower(ann$src_var), tolower(unique(cwt$var_name)))
    out     <- ann[tolower(ann$src_var) %in% missing, , drop = FALSE]
    out[!is.na(out$src_var), ]
  }
}



#' Collapse technical variables to one row per wave
#'
#' Deduplicates CWT rows for predefined technical variables (e.g. `t_weight`,
#' `t_caseid`) to one row per `study_wave` x `var_name` and sets their
#' value-level columns to `NA`. This keeps the CWT compact for variables
#' whose individual value codes carry no substantive meaning.
#'
#' @param cwt A data frame as produced by [build_cwt()].
#'
#' @returns A data frame with the same columns as `cwt`. Technical-variable
#'   rows are deduplicated with `value_n`, `value`, and `value_code` set to
#'   `NA`; all other rows are returned unchanged.
#'
#' @seealso [build_cwt()] where this function is called.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
collapse_tech_vars <- function(cwt) {

  TECH_VARS <- c("t_weight", "t_caseid", "t_weight_des", "t_weight_despst", "t_sampdesign")

  is_tech  <- cwt$target_var %in% TECH_VARS
  cwt_sub  <- cwt[!is_tech, ]
  cwt_tech <- cwt[is_tech, ]

  if (nrow(cwt_tech) == 0) return(cwt_sub)

  cwt_tech <- cwt_tech[!duplicated(cwt_tech[c("study_wave", "var_name")]), ]
  cwt_tech[, c("value_n", "value", "value_code")] <- NA

  rbind(cwt_sub, cwt_tech)
}


#' Extract and format variable and value labels
#'
#' Pulls the variable label and value labels from a haven/Stata-labelled
#' vector, falling back gracefully when metadata is absent or incomplete, and
#' formats everything into the `"[code] label"` convention used throughout
#' the CWT.
#'
#' @param var_values A vector, typically a haven-labelled column from a Stata
#'   or SPSS file.
#' @param var_name A string giving the variable name; used in diagnostic
#'   messages only.
#' @param wave_name A string giving the study-wave identifier; used in
#'   diagnostic messages only.
#' @param verbose Logical. When `TRUE` (the default), emits a `message()` for
#'   each of the three label-handling cases below.
#'
#' @returns A named list with three elements:
#'   * `var_label`: A length-1 string with the variable label, or `NA`.
#'   * `label_formatted`: A character vector of `"[code] label"` strings,
#'     sorted by numeric code.
#'   * `code_formatted`: A character vector of raw value codes, parallel to
#'     `label_formatted`.
#'
#' @details
#' Three cases are handled in order:
#'
#' 1. **No Stata labels**: falls back to unique observed values, formatted as
#'    `"[value] value"`.
#' 2. **Observed values exceed Stata labels**: merges both; extra observed
#'    values are appended as self-labelled entries.
#' 3. **Stata labels fully cover observed values**: Stata labels are used
#'    as-is after stripping numeric prefixes via [.strip_stata_prefix()].
#'
#' @seealso [format_number_obs()] to append frequency counts to the formatted
#'   labels; [build_cwt()] where both are called together.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
format_value_labels <- function(var_values, var_name, wave_name, verbose = TRUE) {

  var_label <- attr(var_values, "label")
  if (is.null(var_label)) var_label <- NA_character_

  labels <- attr(var_values, "labels", exact = TRUE)

  labels_obs <- unique(as.character(unclass(var_values)))
  labels_obs <- labels_obs[!is.na(labels_obs)]
  labels_obs <- labels_obs[order(as.numeric(labels_obs))]

  if (is.null(labels) || all(is.na(labels))) {

    message("NOTE: No Stata value labels found for variable: ", var_name,
            " in dataset: ", wave_name, " - using observed values instead.")

    label_formatted <- paste0("[", labels_obs, "] ", labels_obs)
    code_formatted  <- labels_obs

  } else {

    labels      <- labels[order(as.numeric(unname(labels)))]
    stata_codes <- as.character(unname(labels))

    if (length(unique(labels_obs)) > length(unique(stata_codes))) {

      message("NOTE: More observed values than Stata labels for variable: ", var_name,
              " in dataset: ", wave_name,
              " - combining observed values with Stata labels.")

      extra_obs <- setdiff(labels_obs, stata_codes)
      obs_fmt   <- paste0("[", extra_obs, "] ", extra_obs)

      label_text <- .strip_stata_prefix(names(labels))
      stata_fmt  <- paste0("[", stata_codes, "] ", label_text)

      combined        <- c(obs_fmt, stata_fmt)
      codes           <- as.numeric(sub("^\\[(-?\\d+)\\].*", "\\1", combined))
      label_formatted <- combined[order(codes)]
      code_formatted  <- c(labels_obs, setdiff(stata_codes, labels_obs))

    } else {

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


#' Append observed frequencies to formatted value labels
#'
#' Counts how often each value code appears in the data and appends the count
#' to its formatted label string, producing entries of the form
#' `"[code] label: N"`. Codes absent from the data receive a count of 0.
#'
#' @param var_values A vector of observed values for a single variable.
#' @param code_formatted A character vector of value codes, as returned by
#'   [format_value_labels()].
#' @param label_formatted A character vector of `"[code] label"` strings,
#'   parallel to `code_formatted`.
#'
#' @returns A character vector the same length as `code_formatted`, with each
#'   element in the form `"[code] label: N"`.
#'
#' @seealso [format_value_labels()] which produces the inputs to this
#'   function; [build_cwt()] where both are called together.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
format_number_obs <- function(var_values, code_formatted, label_formatted) {
  tab       <- table(as.character(unclass(var_values)), useNA = "ifany")
  n_obs_raw <- tab[match(code_formatted, names(tab))]
  n_obs_raw[is.na(n_obs_raw)] <- 0
  paste0(label_formatted, ": ", as.integer(n_obs_raw))
}


#' Generate a locale-safe UTC timestamp string
#'
#' Returns the current date (or a supplied date-time) formatted as
#' `"YYYY_MM_DD"` under the `"C"` locale and UTC timezone. Used by
#' [write_cwt()] to produce reproducible, locale-independent file names for
#' recoded CWT snapshots.
#'
#' @param time A `POSIXct` value. Defaults to `Sys.time()`.
#'
#' @returns A single string of the form `"YYYY_MM_DD"`.
#'
#' @details
#' Locale and timezone overrides are applied with [withr::local_locale()] and
#' [withr::local_timezone()], so the caller's session settings are never
#' modified.
#'
#' @seealso [write_cwt()] where the timestamp is embedded in recoded file
#'   names.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
timestamp <- function(time = Sys.time()) {
  withr::local_locale(c("LC_TIME" = "C"))
  withr::local_timezone("UTC")
<<<<<<< HEAD
  format(time, "%Y-%m-%d")
=======
  format(time, "%Y_%m_%d")
>>>>>>> 376e5acb60c3b7a51b88123a29d82dcd19222017
}


#' Strip leading numeric prefixes from Stata value label strings
#'
#' Removes numeric prefixes (e.g. `"1. "`, `"2, "`, `"-1: "`) that Stata
#' sometimes prepends to value label names, leaving only the human-readable
#' text.
#'
#' @param x A character vector of raw Stata label names.
#'
#' @returns A character vector the same length as `x`, with leading numeric
#'   prefixes removed.
#'
#' @seealso [format_value_labels()] where this function is called.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
.strip_stata_prefix <- function(x) {
  gsub("^\\s*[-+]?\\d+(?:\\.\\d+)?\\s*(?:\\.\\s*|,\\s*|:\\s*)", "", x, perl = TRUE)
}

