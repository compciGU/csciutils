# csciutils
Package: csciutils
Type: Package
Title: Tools for Creating and Managing Cross-Walk Tables (CWTs)
Version: 0.0.1
Authors@R: c(
    person("Melina", "Liethmann", email = "melina@example.com", role = c("aut", "cre")),
    person("Maximilian", "Hornung", email = "maximilian.hornung@gu.se", role = "aut")
)
Description: Utilities to load survey data, read aligned CWTs, query annotation mappings,
    validate mappings and create/apply appended CWTs. This package provides a workflow
    wrapper to orchestrate those steps.
License: MIT + file LICENSE
Encoding: UTF-8
LazyData: true
Imports:
    DBI,
    utils
Suggests:
    testthat,
    roxygen2
Roxygen: list(markdown = TRUE)
RoxygenNote: 7.2.2
