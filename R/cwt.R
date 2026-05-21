# User-facing functions ----



#' Combine crosswalk tables stored as separate files
#'
#' @description
#' Read and row-bind crosswalk table (CWT) files for one or more indicators
#' from a project's \code{appended} or \code{recoded} directory.
#'
#' @param proj A single project code, such as \code{"ess"} or \code{"eb"}.
#' @param status Which CWT version to load. Must be one of
#'   \code{"appended"} or \code{"recoded"}.
#' @param indicator A character vector of indicator groups to load, such as
#'   \code{c("swd", "controls")}.
#' @param timestamp Uses the current `sysdate()` timestamp as default. Otherwise supply timestamp for date of CWT creation.
#'
#' @returns A data frame formed by row-binding the requested CWT files which - Columns are not modified.
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
#' @seealso
#'  * [write_cwt()] saves a CWT to the directory.
#'  * [build_cwt()] creates a CWT from scratch.
#'  * [append_cwt()] binds missing variables to an existing CWT.
#'
#' @family cwt
#'
#' @examplesIf Sys.getenv("ROOT_DIR") != ""
#' bind_cwts(
#'   proj = "ess",
#'   status = "appended",
#'   indicator = c("swd", "controls")
#' )
#' @export
bind_cwts <- function(proj, status, indicator, timestamp = timestamp()) {

  TIMESTAMP <- "2026-03-31" #timestamp

  if (status == "appended") {

    CWT_DIR <- file.path(Sys.getenv("ROOT_DIR"), "cwts", "appended", proj)

  } else if (status == "recoded") {

    CWT_DIR <- file.path(Sys.getenv("ROOT_DIR"), "cwts", "recoded", proj)

  }

  EXT <- c(".xlsx", ".odt", ".csv")

  ll <- list()

  for (i in indicator) {

    file_candidates <- file.path(CWT_DIR, paste0(TIMESTAMP,"_" ,i, EXT))
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





#' Save a crosswalk table to a disk
#'
#' @description
#' Write a crosswalk table (CWT) to the project's \code{appended} directory as
#' the appended snapshot and to the \code{recoded} directory as a timestamped
#' file to code target values.
#'
#' @param cwt A data frame containing the crosswalk table.
#' @param proj A project code. If `NULL`, the function derives it from
#'   \code{cwt$study_wave[1]}.
#' @param split Controls whether to split the CWT in controls, SwD, trust, tech varibale subsets before writing.
#' @param split_pattern Supplies a custom split pattern that overwirtes the default one.
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
#' @seealso
#'  * [bind_cwt()] binds CWT files together.
#'  * [build_cwt()] creates a CWT from scratch.
#'  * [append_cwt()] binds missing variables to an existing CWT.
#'
#' @family cwt
#'
#' @examplesIf Sys.getenv("ROOT_DIR") != ""
#' write_cwt(cwt, proj = "ess")
#' write_cwt(cwt, proj = "ess", split = TRUE)
#' write_cwt(cwt, proj = "ess", split = c("swd", "controls"))
#' @export
write_cwt <- function(cwt, proj = NULL, split = FALSE, split_pattern = NULL, ...) {

  # Regex patterns used to build split files from `target_var`
  SPLIT_PATTERNS <- list(
    controls = c(
      "^t_age$",
      "^t_ageedu",
      "^t_cntry$",
      "^t_educ",
      "^t_female$",
      "^t_isced",
      "^t_lrscale$",
      "^t_polint$",
      "^t_satlife$",
      "^t_yob$",
      "^t_yrsedu",
      "^t_vote"
    ),
    swd = c(
      "^t_pridedem$",
      "^t_satdem",
      "^t_satpolitics$",
      "^t_satpolsys$"
    ),
    tech = c(
      "^t_year",
      "^t_caseid$",
      "^t_langinterview$",
      "^t_resprate$",
      "^t_sampdesign$",
      "^t_surveymode$",
      "^t_wave$",
      "^t_weight"
    ),
    trust = c(
      "^trust_"
    )
  )

  split_patterns <- if (is.null(split_pattern)) {
    SPLIT_PATTERNS
  } else {
    split_pattern
  }

  split <- if (isFALSE(split)) {
    FALSE
  } else if (isTRUE(split)) {
    names(split_patterns)
  } else {
    match.arg(split, choices = names(split_patterns), several.ok = TRUE)
  }

  proj <- if (!is.null(proj)) {
    match.arg(
      proj,
      choices = c(
        "ases", "cceb", "cdcee", "cses",
        "eb", "eqls", "ess", "evs",
        "intune", "issp", "lits", "nbb",
        "neb", "wvs"
      )
    )
  } else {
    sw <- cwt$study_wave[1]
    if (is.null(sw) || is.na(sw)) {
      stop(
        "`proj` is NULL and `cwt$study_wave[1]` is missing; cannot derive a file name.",
        call. = FALSE
      )
    }
    tolower(sub("_.*$", "", sw))
  }

  root_dir     <- Sys.getenv("ROOT_DIR")
  dir_appended <- file.path(root_dir, "cwts", "appended", proj)
  dir_recoded  <- file.path(root_dir, "cwts", "recoded", proj)

  stopifnot("'cwt' must be a data.frame"              = is.data.frame(cwt))
  stopifnot("'target_var' column missing from `cwt`"  = "target_var" %in% names(cwt))
  stopifnot("'ROOT_DIR' environment variable not set" = root_dir != "")
  stopifnot("'appended' output dir does not exist"    = dir.exists(dir_appended))
  stopifnot("'recoded' output dir does not exist"     = dir.exists(dir_recoded))

  ts <- timestamp()

  if (!isFALSE(split)) {
    f_list <- lapply(split, function(group) {
      patterns <- split_patterns[[group]]

      if (length(patterns) == 0L) {
        return(cwt[0, , drop = FALSE])
      }

      keep <- Reduce(`|`, lapply(patterns, grepl, x = cwt$target_var))
      cwt[keep, , drop = FALSE]
    })
    names(f_list) <- split

    paths_appended <- file.path(dir_appended, paste0(split, ".xlsx"))
    paths_recoded  <- file.path(dir_recoded, paste0(ts, "_", split, ".xlsx"))
  } else {
    f_list         <- list(full_cwt = cwt)
    paths_appended <- file.path(dir_appended, "full_cwt.xlsx")
    paths_recoded  <- file.path(dir_recoded, paste0(ts, "_full_cwt.xlsx"))
  }

  lapply(seq_along(f_list), function(i) {
    csciutils::write_file(f_list[[i]], path = paths_appended[i], ...)
    csciutils::write_file(f_list[[i]], path = paths_recoded[i], ...)
  })

  invisible(NULL)
}




#' Bind new variable annotations to an existing crosswalk table
#'
#' @description
#' Compare source-variable annotations in the database with an existing aligned
#' CWT and append rows only for variables not yet documented.
#'
#'@inheritParams build_cwt
#'
#' @returns The updated CWT data frame appended with multiple rows per variable (one row per variable value) not yet included in the original CWT, returned invisibly. Takes columns from [build_cwt()] which are not modified. If no variables are missing,
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
#' @seealso
#'  * [write_cwt()] saves a CWT to the directory.
#'  * [build_cwt()] creates a CWT from scratch.
#'  * [bind_cwt()] binds CWT files in directory together.
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

  cwt$value_code     <- as.character(cwt$value_code)
  cwt_new$value_code <- as.character(cwt_new$value_code)

  cwt_bind <- dplyr::bind_rows(cwt, cwt_new)

  message("Appended ", nrow(cwt_new), " new row(s) to CWT.")

  invisible(cwt_bind)
}





#' Build a crosswalk table from survey data and annotations
#'
#' @description
#' Builds a crosswalk table (CWT) from a .stata file. The function
#' loads the survey data, matches source variables from the annotation table to
#' variables in the survey data, extracts value labels and observation counts,
#' and returns a standardized CWT ready for recoding.
#'
#' @param conn A database connection. This is passed to helper functions such as
#'   [load_survey_list()] and [load_annotations()].
#'
#' @param proj One or more survey project codes. Must be one or more of:
#'   `"ases"`, `"cceb"`, `"cdcee"`, `"cses"`, `"eb"`, `"eqls"`, `"ess"`,
#'   `"evs"`, `"intune"`, `"issp"`, `"lits"`, `"nbb"`, `"neb"`, `"wvs"`.
#'
#' @param annotations Optional annotation data frame. If `NULL`, annotations are
#'   loaded automatically using [load_annotations()] with `reshape = TRUE`.
#'   The annotation table must contain at least `src_var`, `target_var`, and
#'   `study_wave`.
#'
#' @return A data frame containing the generated CWT with the following columns:
#' \describe{
#'   \item{study_wave}{Survey wave or dataset name.}
#'   \item{var_name}{Source variable name in the original survey data.}
#'   \item{var_label}{Variable label, where available.}
#'   \item{value_n}{Number of observations for each value.}
#'   \item{value}{Value label.}
#'   \item{value_code}{Original value code.}
#'   \item{target_var}{Target variable name from the annotation table.}
#'   \item{target_value}{Empty target-value column to be completed during recoding.}
#' }
#'
#' @details
#' The function first loads the relevant survey datasets using
#' [load_survey_list()]. It then loads or receives an annotation table and removes
#' common missing-value codes from `src_var`: `"-999"`, `"-99"`, `"999"`, and
#' `"99"`.
#'
#' For each survey wave, the function keeps only variables listed in the
#' annotation table, extracts labels and counts, and combines the results into a
#' single CWT. Technical variables are collapsed using [collapse_tech_vars()].
#'
#' If source variables appear in the annotation table but are not found in the
#' survey data, the function returns the CWT and raises a warning listing the
#' missing variables.
#'
#' @seealso
#' [load_survey_list()], [load_annotations()], [write_cwt()], [bind_cwts()],
#' [append_cwt()]
#'
#' @family cwt
#'
#' @examples
#' \dontrun{
#' conn <- set_db_connection()
#'
#' ess_cwt <- build_cwt(conn, "ess")
#' dplyr::glimpse(ess_cwt)
#'
#' multi_cwt <- build_cwt(conn, c("ess", "evs"))
#' dplyr::glimpse(multi_cwt)
#' }
#'
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
        study_wave   = tolower(study_wave),  # change tolower
        var_name     = tolower(v),           # change tolower
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
    warning(
      sprintf(
        "%d source variable annotation(s) not found in survey data: %s\nDouble-check src_var names in your annotation table.",
        length(missing),
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  cwt
}


#' Check whether all variables are present in the recoded CWT
#'
#' @description
#' Validates that every variable from the aligned CWT and the annotation sheet
#' is present in the final CWT to be recoded. Two checks are performed:
#'
#' \itemize{
#'   \item All `var_name` + `study_wave` combinations from the aligned CWT are
#'     found in the recoded CWT.
#'   \item All `src_var` + `study_wave` combinations from the annotation sheet
#'     (excluding common missing-value codes) are found in the recoded CWT.
#' }
#'
#' @param conn A database connection passed to inline functions used to load
#'   existing CWTs or annotations.
#' @param proj One or more survey project codes. Must be one or more of:
#'   `"ases"`, `"cceb"`, `"cdcee"`, `"cses"`, `"eb"`, `"eqls"`, `"ess"`,
#'   `"evs"`, `"intune"`, `"issp"`, `"lits"`, `"nbb"`, `"neb"`, `"wvs"`.
#'
#' @return Invisibly returns a named list with two deduplicated data frames,
#'   each containing `target_var` and `study_wave`:
#' \describe{
#'   \item{aligned_missing_rows}{Variables from the aligned CWT missing in the recoded CWT.}
#'   \item{ann_missing_rows}{Variables from the annotation sheet missing in the recoded CWT.}
#' }
#'
#' @details.
#' The recoded CWT is loaded via [bind_cwts()] with
#' \code{indicator = c("controls", "swd", "tech", "trust")}.
#'
#' @family cwt
#'
#' @keywords internal
check_cwt_recoded <- function(conn, proj) {

  PROJ <- match.arg(proj, choices = c("ases", "cceb", "cdcee", "cses", "eb", "eqls", "ess", "evs",
                                      "intune", "issp", "lits", "nbb", "neb", "wvs"), several.ok = TRUE)
  EXT <- c(".xlsx", ".odt", ".csv")
  ALIGNED_DIR <- file.path(Sys.getenv("ROOT_DIR"), "cwts", "aligned")
  MISSING_CODES <- c("-999", "-99", "999", "99")

  # Create flag if aligned CWT is in dir
  file_candidates <- file.path(ALIGNED_DIR, paste0(PROJ, EXT))
  aligned_f <- list.files(ALIGNED_DIR)
  cwt_there <- any(file.exists(file_candidates))

 # Load files
  cwt_aligned <- if(cwt_there) load_cwt(proj = PROJ, status = "aligned")
  annotations <- load_annotations(conn = conn, proj = PROJ, reshape = TRUE)
  cwt_bind    <- bind_cwts(proj = PROJ, status = "recoded", indicator = c("controls", "swd", "tech", "trust"))

  # Build composite keys
  cwt_bind$key_bind       <- paste0(cwt_bind$var_name, "_", cwt_bind$study_wave)
  cwt_aligned$key_aligned <- if (cwt_there) paste0(cwt_aligned$var_name, "_", cwt_aligned$study_wave)

  # All annotations in cwt_bind
  src_chr   <- trimws(as.character(annotations$src_var))
  ann_clean <- annotations[!(src_chr %in% MISSING_CODES), , drop = FALSE]
  ann_clean$key_ann <- tolower(paste0(ann_clean$src_var, "_", ann_clean$study_wave))

  # Find differences
  diff_aligned <- if (cwt_there) setdiff(cwt_aligned$key_aligned, cwt_bind$key_bind)
  diff_ann     <- setdiff(unique(ann_clean$key_ann), cwt_bind$key_bind)


  # Subset missing rows
  ann_missing_rows <- unique(ann_clean[ann_clean$key_ann %in% diff_ann, c("target_var", "study_wave")])
  aligned_missing_rows <- if (!is.null(cwt_aligned)) unique(cwt_aligned[cwt_aligned$key_aligned %in% diff_aligned, c("target_var", "study_wave")])

  # Report
  if (length(diff_aligned) == 0 && length(diff_ann) == 0) {

    message("No missing variables in finalised CWT - ready to start recoding!")

  } else {

    if (length(diff_ann) > 0) {
      warning(sprintf(
        "[%s] Cannot find target variable '%s' (wave: %s) from the annotation sheet in the final CWT to recode.",
        paste(PROJ, collapse = ", "), ann_missing_rows$target_var, ann_missing_rows$study_wave
      ))
    }

    if (cwt_there && length(diff_aligned) > 0) {
      warning(sprintf(
        "[%s] Cannot find target variable '%s' (wave: %s) from the aligned CWT in the final CWT to recode.",
        paste(PROJ, collapse = ", "), aligned_missing_rows$target_var, aligned_missing_rows$study_wave
      ))
    }

  }

  invisible(list(
    aligned_missing_rows = if (cwt_there) aligned_missing_rows else character(0),
    ann_missing_rows     = ann_missing_rows
  ))
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

    key_cwt <- paste0(cwt_vars$src_var, "_", cwt_vars$study_wave)
    key_ann <- paste0(ann$src_var,      "_", ann$study_wave)

    ann[!key_ann %in% key_cwt, , drop = FALSE]

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
  var_label <- as.character(var_label)[1]

  labels <- attr(var_values, "labels", exact = TRUE)

  # observed values as code strings, preserving tagged NA separately
  obs_raw   <- unique(unclass(var_values))
  obs_codes <- unique(.handle_tagged_values(obs_raw))

  # flags for observed missingness
  has_plain_na_obs  <- any(is.na(obs_codes))
  obs_nonmissing    <- obs_codes[!is.na(obs_codes)]
  obs_nonmissing    <- unique(obs_nonmissing[.order_mixed_codes(obs_nonmissing)])

  if (is.null(labels) || length(labels) == 0) {

    if (verbose) {
      message(
        "NOTE: No Stata value labels found for variable: ", var_name,
        " in dataset: ", wave_name, " - using observed values instead."
      )
    }

    code_formatted <- c(obs_nonmissing, if (has_plain_na_obs) NA_character_)
    label_formatted <- ifelse(
      is.na(code_formatted),
      "[NA] NA",
      paste0("[", code_formatted, "] ", code_formatted)
    )

  } else {

    label_text  <- as.character(.strip_stata_prefix(names(labels)))
    stata_codes <- .handle_tagged_values(unname(labels))

    ord <- .order_mixed_codes(stata_codes)
    stata_codes <- stata_codes[ord]
    label_text  <- label_text[ord]

    stata_has_plain_na <- any(is.na(stata_codes))
    stata_regular_codes  <- as.character(stata_codes[!is.na(stata_codes)])
    stata_regular_labels <- as.character(label_text[!is.na(stata_codes)])
    stata_missing_labels <- as.character(label_text[is.na(stata_codes)])

    # extra observed non-missing codes not present in labels
    extra_obs <- setdiff(obs_nonmissing, stata_regular_codes)

    regular_df <- data.frame(
      code  = as.character(c(stata_regular_codes, extra_obs)),
      label = as.character(c(stata_regular_labels, extra_obs)),
      stringsAsFactors = FALSE
    )

    regular_df <- regular_df[!duplicated(regular_df$code), , drop = FALSE]
    regular_df <- regular_df[.order_mixed_codes(regular_df$code), , drop = FALSE]

    missing_df <- data.frame(
      code  = rep(NA_character_, length(stata_missing_labels)),
      label = stata_missing_labels,
      stringsAsFactors = FALSE
    )

    # add plain observed NA if it exists but is not already represented in labels
    if (has_plain_na_obs && !stata_has_plain_na) {
      missing_df <- rbind(
        missing_df,
        data.frame(
          code = NA_character_,
          label = "NA",
          stringsAsFactors = FALSE
        )
      )
    }

    combined_df <- rbind(regular_df, missing_df)

    code_formatted <- as.character(combined_df$code)
    label_formatted <- ifelse(
      is.na(code_formatted),
      paste0("[NA] ", combined_df$label),
      paste0("[", code_formatted, "] ", combined_df$label)
    )

    if (verbose) {
      if (length(extra_obs) > 0 || (has_plain_na_obs && !stata_has_plain_na)) {
        message(
          "NOTE: Observed values exceed Stata labels for variable: ", var_name,
          " in dataset: ", wave_name,
          " - appending unlabelled observed values."
        )
      } else {
        message(
          "NOTE: Using Stata value labels for variable: ", var_name,
          " in dataset: ", wave_name,
          " - observed values fully covered by labels."
        )
      }
    }
  }

  label_formatted <- as.character(label_formatted)
  code_formatted  <- as.character(code_formatted)

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

  observed_codes <- .handle_tagged_values(unclass(var_values))
  tab <- table(observed_codes, useNA = "ifany")

  n_obs_raw <- vapply(code_formatted, function(code) {
    if (is.na(code)) {
      sum(is.na(observed_codes))
    } else {
      val <- tab[match(code, names(tab))]
      if (length(val) == 0 || is.na(val)) 0L else as.integer(val)
    }
  }, integer(1))

  paste0(label_formatted, ": ", n_obs_raw)
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
  format(time, "%Y-%m-%d")

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


#' Order a vector of mixed numeric and string codes
#'
#' Sorts a vector that contains both numeric-looking codes (e.g. `"1"`, `"2"`)
#' and non-numeric strings (e.g. `"non-applicable"`, `"non-response"`) by placing numeric codes
#' first in numeric order, followed by strings in alphabetical order, with
#' `NA`s last. Coercion warnings from [as.numeric()] are suppressed
#' intentionally — `NA` production for non-numeric strings is expected
#' behaviour, not an error.
#'
#' @param x A vector of value codes, typically a mix of numeric strings and
#'   character labels as found in CWT `value_code` columns.
#'
#' @returns An integer vector of indices suitable for use with `[` or
#'   [order()], ordering elements as: numeric codes (ascending) → string
#'   codes (alphabetical) → `NA`s.
#'
#' @seealso [format_value_labels()] where this ordering is applied.
#'
#' @family cwt-internal
#' @keywords internal
#' @noRd
.order_mixed_codes <- function(x) {
  x_chr <- as.character(x)
  x_num <- suppressWarnings(as.numeric(x_chr))
  order(is.na(x_num), x_num, x_chr)
}

#' @family cwt-internal
#' @keywords internal
#' @noRd
.handle_tagged_values <- function(x) {

  out <- as.character(x)


  out[is.na(x)] <- NA_character_

  # tagged NA only exists for double vectors
  if (typeof(x) == "double") {
    tagged <- haven::is_tagged_na(x)
    if (any(tagged)) {
      out[tagged] <- paste0("NA(", haven::na_tag(x[tagged]), ")")
    }
  }

  out
}

