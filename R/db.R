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