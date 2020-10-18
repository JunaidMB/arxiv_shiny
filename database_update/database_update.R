library(bigrquery)
library(DBI)
library(aRxiv)
library(lubridate)
library(dplyr)
library(glue)
library(dotenv)
library(tidyverse)
library(stringi)
library(stringr)

# Update script

# Authentication for BQ
bigrquery::bq_auth(path = Sys.getenv('auth_path'))


# Connect to DB
bq_con <- DBI::dbConnect(bigquery(),
                         project = Sys.getenv('project_id'),
                         dataset = Sys.getenv('dataset_id'))

# Pull down DB table


# Obtain latest days papers for select categories
categories <- c('stat.AP', 'stat.CO', 'stat.ML', 'stat.ME', 'stat.TH','math.OC', 'math.PR', 'math.ST', 'math.CO', 'cs.AI', 'cs.GT', 'cs.CV')
select_choices <- aRxiv::arxiv_cats$abbreviation
names(select_choices) <- aRxiv::arxiv_cats$description
choices <- select_choices[select_choices %in% categories]

# Set and format dates for the arxiv API - Order of SQL categories should be identical to order in choices list!!
sql <- "select
max(case when categories like '%stat.AP%' then date(submitted) else date('1990-01-01') end) as stat_AP,
max(case when categories like '%stat.CO%' then date(submitted) else date('1990-01-01') end) as stat_CO,
max(case when categories like '%stat.ML%' then date(submitted) else date('1990-01-01') end) as stat_ML,
max(case when categories like '%stat.ME%' then date(submitted) else date('1990-01-01') end) as stat_ME,
max(case when categories like '%stat.TH%' then date(submitted) else date('1990-01-01') end) as stat_TH,
max(case when categories like '%cs.AI%' then date(submitted) else date('1990-01-01') end) as cs_AI,
max(case when categories like '%cs.GT%' then date(submitted) else date('1990-01-01') end) as cs_GT,
max(case when categories like '%cs.CV%' then date(submitted) else date('1990-01-01') end) as cs_cv,
max(case when categories like '%math.CO%' then date(submitted) else date('1990-01-01') end) as math_CO,
max(case when categories like '%math.OC%' then date(submitted) else date('1990-01-01') end) as math_OC,
max(case when categories like '%math.PR%' then date(submitted) else date('1990-01-01') end) as math_PR,
max(case when categories like '%math.ST%' then date(submitted) else date('1990-01-01') end) as math_ST
from
arxiv_paper_repository.arxiv_paper_repository"

db_state <- bigrquery::bq_table_download(bigrquery::bq_project_query(x = Sys.getenv('project_id'), query = sql), max_results = Inf)
transposed_db_state <- t(db_state)


# Update table for all categories
today_results <- data.frame()

for (i in seq_along(choices)) {
  
  last_db_day <- lubridate::as_date(transposed_db_state[[i]]) + days(1)
  today <- Sys.Date()
  
  formatted_last_db_day <- stringr::str_c(stringr::str_c(gsub("-", "", last_db_day) , "*"))
  formatted_today <- stringr::str_c(stringr::str_c(gsub("-", "", Sys.Date()) , "*"))
  
  
  results_today <- try(as_tibble(aRxiv::arxiv_search(query = glue("cat: ({choices[[i]]}) AND submittedDate: [{formatted_last_db_day} TO {formatted_today}]")
                                                     , limit = 10000
                                                     , sort_by = c("submitted")
                                                     , ascending = FALSE
                                                     , batchsize = 10000)), silent = TRUE)
  
  today_results <- dplyr::union_all(today_results, results_today)
  
  cat(i, names(choices)[i], "\n")
  
}

# Format date
today_results <- today_results %>% 
  dplyr::mutate(submitted = lubridate::as_datetime(submitted),
                updated = lubridate::as_datetime(updated)) %>% 
  distinct()

# Write the table back to the database
DBI::dbWriteTable(conn = bq_con,
                  name = 'arxiv_paper_repository.arxiv_paper_repository',
                  value = today_results,
                  as_bq_fields(today_results),
                  overwrite = FALSE,
                  append = TRUE)

# Close connection
DBI::dbDisconnect(conn = bq_con)

