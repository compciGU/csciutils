#' Create a variable label lookup table from a survey list
#'
#' Builds a long lookup table with one row per variable per survey in a survey
#' list (as returned by [create_survey_list()]). Variable labels are read from
#' the `"label"` attribute (e.g. haven/Stata metadata). If a label is missing,
#' an empty string is used.
#'
#' Optionally includes `dataset_id` (if present in each survey data frame).
#'
#' @param my_survey_list A named list of survey data frames (survey list),
#'   typically returned by [create_survey_list()].
#' @param include_dataset_id Logical. Include a `dataset_id` column in the
#'   output? If `TRUE`, expects each survey to contain a `dataset_id` column.
#'   Default `TRUE`.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A data frame with columns `survey_tag`, `var_name`, `var_label`, and
#'   optionally `dataset_id`.
#'
#' @examples
#' \dontrun{
#' surveys <- create_survey_list(conn, proj = "ess", include_dataset_id = TRUE)
#' vars_lu <- create_vars_lookup(surveys, include_dataset_id = TRUE)
#' }
#' @export
create_vars_lookup <- function(my_survey_list,
                               include_dataset_id = TRUE,
                               verbose = TRUE) {

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

    var_labels <- character(length(var_names))
    for (j in seq_along(var_names)) {
      label <- attr(data[[j]], "label")
      if (is.null(label)) {
        var_labels[j] <- ""
      } else {
        var_labels[j] <- as.character(label)[1]
      }
    }

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

    lookup_list[[length(lookup_list) + 1]] <- survey_lookup
  }

  all_vars_lookup_table <- do.call(rbind, lookup_list)
  rownames(all_vars_lookup_table) <- NULL

  all_vars_lookup_table
}







#' Read and reshape a variable annotation sheet from the database
#'
#' Queries an annotation table from the database and reshapes it to long format.
#' The returned data frame contains one row per `(dataset_id, study_wave,
#' target_var, src_var)` mapping. Cells containing multiple source variables are
#' automatically split into multiple rows when separated by common delimiters
#' (e.g. `";"` or `","`).
#'
#' @param conn A DBI connection.
#' @param table Character. Annotation table name in the database
#'   (e.g. `"cses_src_annotations"`).
#' @param id_cols Character vector. Columns to keep as identifiers (not pivoted).
#'   Default `c("dataset_id", "study_wave")`.
#' @param split_separators Character. Regular expression of separators used to
#'   detect and split multiple values in a cell. Default splits on `;` or `,`.
#' @param verbose Logical. Print progress messages? Default `TRUE`.
#'
#' @return A long annotation data frame with columns `dataset_id`, `study_wave`,
#'   `target_var`, and `src_var`.
#'
#' @examples
#' \dontrun{
#' cses_annotations_long <- query_annotations(
#'   conn,
#'   table = "cses_src_annotations"
#' )
#' }
#' @export
query_annotations <- function(conn,
                              table,
                              id_cols = c("dataset_id", "study_wave"),
                              split_separators = "\\s*[;,]\\s*",
                              format = c("long", "wide"),
                              verbose = TRUE) {

  format <- match.arg(format)

  sql <- paste0("SELECT * FROM ", table, ";")
  ann <- DBI::dbGetQuery(conn, sql)

  missing_cols <- setdiff(id_cols, names(ann))
  if (length(missing_cols) > 0) {
    stop("Annotation table is missing required id column(s): ",
         paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  if (format == "wide") {
    if (verbose)
      cat("Loaded", nrow(ann), "row(s) from", table, "in wide format\n")
    return(ann)
  }

  value_cols <- setdiff(names(ann), id_cols)
  if (length(value_cols) == 0) {
    stop("No columns left to pivot after removing id_cols.", call. = FALSE)
  }

  # pivot to long (base R) ----------------------------------------------------
  id_df <- ann[id_cols]
  long_list <- vector("list", length(value_cols))
  idx <- 1

  for (col in value_cols) {

    out <- id_df
    out$target_var <- col
    out$src_var    <- ann[[col]]

    long_list[[idx]] <- out
    idx <- idx + 1
  }

  ann_long <- do.call(rbind, long_list)

  # split multiple mappings into rows -----------------------------------------
  ann_long$src_var <- trimws(as.character(ann_long$src_var))
  keep <- !is.na(ann_long$src_var) & ann_long$src_var != ""
  ann_long <- ann_long[keep, , drop = FALSE]

  if (nrow(ann_long) > 0) {

    parts <- strsplit(ann_long$src_var, split_separators, perl = TRUE)
    n_parts <- lengths(parts)

    ann_long_expanded <- ann_long[rep(seq_len(nrow(ann_long)), n_parts), , drop = FALSE]
    ann_long_expanded$src_var <- trimws(unlist(parts, use.names = FALSE))

    keep2 <- !is.na(ann_long_expanded$src_var) &
      ann_long_expanded$src_var != ""

    ann_long <- ann_long_expanded[keep2, , drop = FALSE]
  }

  rownames(ann_long) <- NULL

  if (verbose)
    cat("Loaded", nrow(ann_long), "annotation mapping row(s) from",
        table, "in long format\n")

  ann_long
}
