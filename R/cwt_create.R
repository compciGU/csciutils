#' Create a Codebook with Value Labels (CWT)
#'
#' @param survey_list A named list of survey data frames, keyed by study_wave
#' @param vars_df A data frame with columns \code{src_var}, \code{target_var},
#'   and \code{study_wave}
#' @return A CWT data frame with one row per value per variable per wave
#' @export
create_cwt <- function(survey_list, vars_df) {

  labels_list <- list()
  index       <- 1

  for (n in names(survey_list)) {
    cat("Survey:", n, "\n")

    dataset    <- survey_list[[n]]
    study_wave <- n
    vars       <- vars_df$src_var[tolower(vars_df$study_wave) == study_wave]

    dataset_filtered <- dataset[, tolower(names(dataset)) %in% tolower(vars), drop = FALSE]

    for (v in names(dataset_filtered)) {

      target_var <- vars_df$target_var[
        toupper(vars_df$src_var)    == toupper(v) &
          toupper(vars_df$study_wave) == toupper(study_wave)
      ]
      target_var <- target_var[!is.na(target_var)]

      vec <- dataset_filtered[[v]]
      fmt <- format_value_labels(vec, v, study_wave)      # utils_labels
      n_obs_fmt <- count_obs(vec,                         # utils_labels
                             fmt$code_formatted,
                             fmt$label_formatted)

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
  cwt <- collapse_tech_vars(cwt)                          # utils_vars
  check_missing_vars(cwt, vars_df)                        # utils_vars
  cwt
}
