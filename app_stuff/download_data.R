library(glue)
library(dplyr)
library(lubridate)
library(dotenv)
library(bigrquery)
library(DBI)

setwd('/root/app_stuff')
dotenv::load_dot_env()

   # Authentication for BQ
bigrquery::bq_auth(path = Sys.getenv('auth_path'), use_oob = TRUE)


# Connect to DB
bq_con <- DBI::dbConnect(bigquery(),
                         project = Sys.getenv('project_id'),
                         dataset = Sys.getenv('dataset_id'))
sql <- glue('select * from arxiv_paper_repository.arxiv_paper_repository where date(submitted) between \'{floor_date(Sys.Date() - 90, \'month\')}\' AND \'{Sys.Date()}\' AND title <> \'\' ')

full_results <- bigrquery::bq_table_download(bigrquery::bq_project_query(x = Sys.getenv('project_id'), query = sql), max_results = Inf)

