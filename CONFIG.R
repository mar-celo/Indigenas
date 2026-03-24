virtualenv_create("~/venvs/r-reticulate")
virtualenv_install(
  "C:/venvs/r-reticulate",
  packages = "databricks-connect==16.4.*"
)
usethis::edit_r_environ()


use_virtualenv("C:/venvs/r-reticulate", required = TRUE)

reticulate::virtualenv_list()


library(reticulate)

virtualenv_create(
  envname = "r-reticulate-py312",
  python = "/opt/homebrew/bin/python3.12"
)


library(reticulate)

py_discover_config()
virtualenv_list()

py_config()

library(reticulate)

virtualenv_create(
  envname = "r-databricks-py312",
  python = "/opt/homebrew/bin/python3.12"
)

use_virtualenv("r-databricks-py312", required = TRUE)

py_config()
