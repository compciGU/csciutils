#' @export
read_db_credentials <- function(file = "~/.db_credentials_cci") {
  if (!file.exists(path.expand(file))) {
    stop("Credentials file not found: ", file)
  }
  
  lines <- readLines(path.expand(file))
  creds <- list()
  
  for (line in lines) {
    if (grepl("=", line)) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      key <- trimws(parts[1])
      value <- trimws(parts[2])
      creds[[key]] <- value
    }
  }
  
  return(creds)
}

#' @export
set_db_connection <- function(creds_file = "~/.db_credentials_cci") {
  creds <- read_db_credentials(creds_file)
  DBI::dbConnect(RPostgres::Postgres(), host = creds$host, port = as.integer(creds$port), dbname = creds$dbname, user = creds$user, password = creds$password)
}


#' @title Join Questions with Scales
#'
#' @description Retrieves all questions and their associated scales from the database.
#' Uses a LEFT JOIN to include questions that don't have scales.
#'
#' @param conn A DBI database connection object
#' @param inner_join Logical. If TRUE, uses INNER JOIN to return only questions 
#'   with scales. If FALSE (default), uses LEFT JOIN to return all questions.
#'
#' @return A data frame with combined question and scale information
#'
#' @examples
#' \dontrun{
#' conn <- connect_db()
#' questions_scales <- add_scales_to_questions(conn)
#' DBI::dbDisconnect(conn)
#' }
#' #' @export
add_scales_to_questions <- function(conn, inner_join = FALSE) {
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required but not installed.")
  }
  
  join_type <- if (inner_join) "INNER JOIN" else "LEFT JOIN"
  
  query <- sprintf("
    SELECT q.*, s.*
    FROM questions q
    %s scales s ON q.scale_id = s.scale_id AND q.dataset_id = s.dataset_id
    ORDER BY q.dataset_id, q.question_id
  ", join_type)
  
  DBI::dbGetQuery(conn, query)
}