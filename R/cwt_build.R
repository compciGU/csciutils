#' Build a Crosswalk with Value Labels (CWT)
#'
#' Constructs a Crosswalk with Value Labels (CWT) from a list of survey datasets
#' and a source-to-target variable mapping. For each `study_wave` in
#' `my_survey_list`, the function selects the mapped variables, extracts value
#' labels (or falls back to observed values when labels are missing), counts
#' observed values, and returns a long CWT with one row per value per variable
#' per wave.
#'
#' After construction, technical variables (e.g. `t_weight`, `t_caseid`) are
#' collapsed to a single row per wave and missing mapped variables are reported.
#'
#' @param my_survey_list A named list of survey data frames, keyed by `study_wave`.
#' @param variable_map A data frame with columns `src_var`, `target_var`, and
#'   `study_wave`, defining the source-to-target variable mapping.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A CWT data frame with one row per value per variable per wave.
#'   Includes `study_wave`, `var_name`, `var_label`, `value_n`, `value`,
#'   `value_code`, `target_var`, and `target_value`.
#'
#' @examples
#' \dontrun{
#' cwt <- build_cwt(my_survey_list, variable_map)
#' }
#' @export
build_cwt <- function(my_survey_list, variable_map, verbose = TRUE) {

  validate_create_cwt(my_survey_list, variable_map)

  labels_list <- list()
  index       <- 1

  for (n in names(my_survey_list)) {
    cat("Survey:", n, "\n")

    dataset    <- my_survey_list[[n]]
    study_wave <- n
    vars <- variable_map$src_var[
      tolower(variable_map$study_wave) == tolower(study_wave)
    ]

    dataset_filtered <- dataset[
      ,
      tolower(names(dataset)) %in% tolower(vars),
      drop = FALSE
    ]

    for (v in names(dataset_filtered)) {

      target_var <- variable_map$target_var[
        tolower(variable_map$src_var)    == tolower(v) &
          tolower(variable_map$study_wave) == tolower(study_wave)
      ]
      target_var <- target_var[!is.na(target_var)]

      vec <- dataset_filtered[[v]]

      fmt <- format_value_labels(vec, v, study_wave)      # utils_labels

      n_obs_fmt <- count_obs(
        vec,
        fmt$code_formatted,
        fmt$label_formatted
      )

      labels_list[[index]] <- data.frame(
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

      index <- index + 1
    }
  }

  cwt <- dplyr::bind_rows(labels_list)
  cwt <- collapse_tech_vars(cwt)
  check_missing_vars(cwt, variable_map)

  cwt
}
