library(jsonlite)
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
library(plumber)
library(future)
plan(multiprocess)

update_bq_table <- function() {
  
  # Get environmental variables
  dotenv::load_dot_env()
  
  # Authentication for BQ
  bigrquery::bq_auth(path = Sys.getenv('auth_path'))
  
  
  # Connect to DB
  bq_con <- DBI::dbConnect(bigquery(),
                           project = Sys.getenv('project_id'),
                           dataset = Sys.getenv('dataset_id'))
  
  # Pull down DB table
  
  
  # Obtain latest days papers for select categories
  #categories <- c('stat.AP', 'stat.CO', 'stat.ML', 'stat.ME', 'stat.TH','math.OC', 'math.PR', 'math.ST', 'math.CO', 'cs.AI', 'cs.GT', 'cs.CV')
  select_choices <- aRxiv::arxiv_cats$abbreviation
  names(select_choices) <- aRxiv::arxiv_cats$description
  choices <- select_choices
  #choices <- select_choices[select_choices %in% categories]
  
  # Set and format dates for the arxiv API - Order of SQL categories should be identical to order in choices list!!
  sql <- "select
count(*),
max(case when categories like '%stat.AP%' then date(submitted) else date('1990-01-01') end) as stat_AP,
max(case when categories like '%stat.CO%' then date(submitted) else date('1990-01-01') end) as stat_CO,
max(case when categories like '%stat.ML%' then date(submitted) else date('1990-01-01') end) as stat_ML,
max(case when categories like '%stat.ME%' then date(submitted) else date('1990-01-01') end) as stat_ME,
max(case when categories like '%stat.TH%' then date(submitted) else date('1990-01-01') end) as stat_TH,
max(case when categories like '%q-bio.BM%' then date(submitted) else date('1990-01-01') end) as q_bio_BM,
max(case when categories like '%q-bio.CB%' then date(submitted) else date('1990-01-01') end) as q_bio_CB,
max(case when categories like '%q-bio.GN%' then date(submitted) else date('1990-01-01') end) as q_bio_GN,
max(case when categories like '%q-bio.MN%' then date(submitted) else date('1990-01-01') end) as q_bio_MN,
max(case when categories like '%q-bio.NC%' then date(submitted) else date('1990-01-01') end) as q_bio_NC,
max(case when categories like '%q-bio.OT%' then date(submitted) else date('1990-01-01') end) as q_bio_OT,
max(case when categories like '%q-bio.PE%' then date(submitted) else date('1990-01-01') end) as q_bio_PE,
max(case when categories like '%q-bio.QM%' then date(submitted) else date('1990-01-01') end) as q_bio_QM,
max(case when categories like '%q-bio.SC%' then date(submitted) else date('1990-01-01') end) as q_bio_SC,
max(case when categories like '%q-bio.TO%' then date(submitted) else date('1990-01-01') end) as q_bio_TO,
max(case when categories like '%cs.AR%' then date(submitted) else date('1990-01-01') end) as cs_AR,
max(case when categories like '%cs.AI%' then date(submitted) else date('1990-01-01') end) as cs_AI,
max(case when categories like '%cs.CL%' then date(submitted) else date('1990-01-01') end) as cs_CL,
max(case when categories like '%cs.CC%' then date(submitted) else date('1990-01-01') end) as cs_CC,
max(case when categories like '%cs.CE%' then date(submitted) else date('1990-01-01') end) as cs_CE,
max(case when categories like '%cs.CG%' then date(submitted) else date('1990-01-01') end) as cs_CG,
max(case when categories like '%cs.GT%' then date(submitted) else date('1990-01-01') end) as cs_GT,
max(case when categories like '%cs.CV%' then date(submitted) else date('1990-01-01') end) as cs_CV,
max(case when categories like '%cs.CY%' then date(submitted) else date('1990-01-01') end) as cs_CY,
max(case when categories like '%cs.CR%' then date(submitted) else date('1990-01-01') end) as cs_CR,
max(case when categories like '%cs.DS%' then date(submitted) else date('1990-01-01') end) as cs_DS,
max(case when categories like '%cs.DB%' then date(submitted) else date('1990-01-01') end) as cs_DB,
max(case when categories like '%cs.DL%' then date(submitted) else date('1990-01-01') end) as cs_DL,
max(case when categories like '%cs.DM%' then date(submitted) else date('1990-01-01') end) as cs_DM,
max(case when categories like '%cs.DC%' then date(submitted) else date('1990-01-01') end) as cs_DC,
max(case when categories like '%cs.GL%' then date(submitted) else date('1990-01-01') end) as cs_GL,
max(case when categories like '%cs.GR%' then date(submitted) else date('1990-01-01') end) as cs_GR,
max(case when categories like '%cs.HC%' then date(submitted) else date('1990-01-01') end) as cs_HC,
max(case when categories like '%cs.IR%' then date(submitted) else date('1990-01-01') end) as cs_IR,
max(case when categories like '%cs.IT%' then date(submitted) else date('1990-01-01') end) as cs_IT,
max(case when categories like '%cs.LG%' then date(submitted) else date('1990-01-01') end) as cs_LG,
max(case when categories like '%cs.LO%' then date(submitted) else date('1990-01-01') end) as cs_LO,
max(case when categories like '%cs.MS%' then date(submitted) else date('1990-01-01') end) as cs_MS,
max(case when categories like '%cs.MA%' then date(submitted) else date('1990-01-01') end) as cs_MA,
max(case when categories like '%cs.MM%' then date(submitted) else date('1990-01-01') end) as cs_MM,
max(case when categories like '%cs.NI%' then date(submitted) else date('1990-01-01') end) as cs_NI,
max(case when categories like '%cs.NE%' then date(submitted) else date('1990-01-01') end) as cs_NE,
max(case when categories like '%cs.NA%' then date(submitted) else date('1990-01-01') end) as cs_NA,
max(case when categories like '%cs.OS%' then date(submitted) else date('1990-01-01') end) as cs_OS,
max(case when categories like '%cs.OH%' then date(submitted) else date('1990-01-01') end) as cs_OH,
max(case when categories like '%cs.PF%' then date(submitted) else date('1990-01-01') end) as cs_PF,
max(case when categories like '%cs.PL%' then date(submitted) else date('1990-01-01') end) as cs_PL,
max(case when categories like '%cs.RO%' then date(submitted) else date('1990-01-01') end) as cs_RO,
max(case when categories like '%cs.SE%' then date(submitted) else date('1990-01-01') end) as cs_SE,
max(case when categories like '%cs.SD%' then date(submitted) else date('1990-01-01') end) as cs_SD,
max(case when categories like '%cs.SC%' then date(submitted) else date('1990-01-01') end) as cs_SC,
max(case when categories like '%nlin.AO%' then date(submitted) else date('1990-01-01') end) as nlin_AO,
max(case when categories like '%nlin.CG%' then date(submitted) else date('1990-01-01') end) as nlin_CG,
max(case when categories like '%nlin.CD%' then date(submitted) else date('1990-01-01') end) as nlin_CD,
max(case when categories like '%nlin.SI%' then date(submitted) else date('1990-01-01') end) as nlin_SI,
max(case when categories like '%nlin.PS%' then date(submitted) else date('1990-01-01') end) as nlin_PS,
max(case when categories like '%math.AG%' then date(submitted) else date('1990-01-01') end) as math_AG,
max(case when categories like '%math.AT%' then date(submitted) else date('1990-01-01') end) as math_AT,
max(case when categories like '%math.AP%' then date(submitted) else date('1990-01-01') end) as math_AP,
max(case when categories like '%math.CT%' then date(submitted) else date('1990-01-01') end) as math_CT,
max(case when categories like '%math.CA%' then date(submitted) else date('1990-01-01') end) as math_CA,
max(case when categories like '%math.CO%' then date(submitted) else date('1990-01-01') end) as math_CO,
max(case when categories like '%math.AC%' then date(submitted) else date('1990-01-01') end) as math_AC,
max(case when categories like '%math.CV%' then date(submitted) else date('1990-01-01') end) as math_CV,
max(case when categories like '%math.DG%' then date(submitted) else date('1990-01-01') end) as math_DG,
max(case when categories like '%math.DS%' then date(submitted) else date('1990-01-01') end) as math_DS,
max(case when categories like '%math.FA%' then date(submitted) else date('1990-01-01') end) as math_FA,
max(case when categories like '%math.GM%' then date(submitted) else date('1990-01-01') end) as math_GM,
max(case when categories like '%math.GN%' then date(submitted) else date('1990-01-01') end) as math_GN,
max(case when categories like '%math.GT%' then date(submitted) else date('1990-01-01') end) as math_GT,
max(case when categories like '%math.GR%' then date(submitted) else date('1990-01-01') end) as math_GR,
max(case when categories like '%math.HO%' then date(submitted) else date('1990-01-01') end) as math_HO,
max(case when categories like '%math.IT%' then date(submitted) else date('1990-01-01') end) as math_IT,
max(case when categories like '%math.KT%' then date(submitted) else date('1990-01-01') end) as math_KT,
max(case when categories like '%math.LO%' then date(submitted) else date('1990-01-01') end) as math_LO,
max(case when categories like '%math.MP%' then date(submitted) else date('1990-01-01') end) as math_MP,
max(case when categories like '%math.MG%' then date(submitted) else date('1990-01-01') end) as math_MG,
max(case when categories like '%math.NT%' then date(submitted) else date('1990-01-01') end) as math_NT,
max(case when categories like '%math.NA%' then date(submitted) else date('1990-01-01') end) as math_NA,
max(case when categories like '%math.OA%' then date(submitted) else date('1990-01-01') end) as math_OA,
max(case when categories like '%math.OC%' then date(submitted) else date('1990-01-01') end) as math_OC,
max(case when categories like '%math.PR%' then date(submitted) else date('1990-01-01') end) as math_PR,
max(case when categories like '%math.QA%' then date(submitted) else date('1990-01-01') end) as math_QA,
max(case when categories like '%math.RT%' then date(submitted) else date('1990-01-01') end) as math_RT,
max(case when categories like '%math.RA%' then date(submitted) else date('1990-01-01') end) as math_RA,
max(case when categories like '%math.SP%' then date(submitted) else date('1990-01-01') end) as math_SP,
max(case when categories like '%math.ST%' then date(submitted) else date('1990-01-01') end) as math_ST,
max(case when categories like '%math.SG%' then date(submitted) else date('1990-01-01') end) as math_SG,
max(case when categories like '%astro-ph%' then date(submitted) else date('1990-01-01') end) as astro_ph,
max(case when categories like '%cond-mat.dis-nn%' then date(submitted) else date('1990-01-01') end) as cond_mat_dis_nn,
max(case when categories like '%cond-mat.mes-hall%' then date(submitted) else date('1990-01-01') end) as cond_mat_mes_hall,
max(case when categories like '%cond-mat.mtrl-sci%' then date(submitted) else date('1990-01-01') end) as cond_mat_mtrl_sci,
max(case when categories like '%cond-mat.other%' then date(submitted) else date('1990-01-01') end) as cond_mat_other,
max(case when categories like '%cond-mat.soft%' then date(submitted) else date('1990-01-01') end) as cond_mat_soft,
max(case when categories like '%cond-mat.stat-mech%' then date(submitted) else date('1990-01-01') end) as cond_mat_stat_mech,
max(case when categories like '%cond-mat.str-el%' then date(submitted) else date('1990-01-01') end) as cond_mat_str_el,
max(case when categories like '%cond-mat.supr-con%' then date(submitted) else date('1990-01-01') end) as cond_mat_supr_con,
max(case when categories like '%gr-qc%' then date(submitted) else date('1990-01-01') end) as gr_qc,
max(case when categories like '%hep-ex%' then date(submitted) else date('1990-01-01') end) as hep_ex,
max(case when categories like '%hep-lat%' then date(submitted) else date('1990-01-01') end) as hep_lat,
max(case when categories like '%hep-ph%' then date(submitted) else date('1990-01-01') end) as hep_ph,
max(case when categories like '%hep-th%' then date(submitted) else date('1990-01-01') end) as hep_th,
max(case when categories like '%math-ph%' then date(submitted) else date('1990-01-01') end) as math_ph,
max(case when categories like '%nucl-ex%' then date(submitted) else date('1990-01-01') end) as nucl_ex,
max(case when categories like '%nucl-th%' then date(submitted) else date('1990-01-01') end) as nucl_th,
max(case when categories like '%physics.acc-ph%' then date(submitted) else date('1990-01-01') end) as physics_acc_ph,
max(case when categories like '%physics.ao-ph%' then date(submitted) else date('1990-01-01') end) as physics_ao_ph,
max(case when categories like '%physics.atom-ph%' then date(submitted) else date('1990-01-01') end) as physics_atom_ph,
max(case when categories like '%physics.atm-clus%' then date(submitted) else date('1990-01-01') end) as physics_atm_clus,
max(case when categories like '%physics.bio-ph%' then date(submitted) else date('1990-01-01') end) as physics_bio_ph,
max(case when categories like '%physics.chem-ph%' then date(submitted) else date('1990-01-01') end) as physics_chem_ph,
max(case when categories like '%physics.class-ph%' then date(submitted) else date('1990-01-01') end) as physics_class_ph,
max(case when categories like '%physics.comp-ph%' then date(submitted) else date('1990-01-01') end) as physics_comp_ph,
max(case when categories like '%physics.data-an%' then date(submitted) else date('1990-01-01') end) as physics_data_an,
max(case when categories like '%physics.flu-dyn%' then date(submitted) else date('1990-01-01') end) as physics_flu_dyn,
max(case when categories like '%physics.gen-ph%' then date(submitted) else date('1990-01-01') end) as physics_gen_ph,
max(case when categories like '%physics.geo-ph%' then date(submitted) else date('1990-01-01') end) as physics_geo_ph,
max(case when categories like '%physics.hist-ph%' then date(submitted) else date('1990-01-01') end) as physics_hist_ph,
max(case when categories like '%physics.ins-det%' then date(submitted) else date('1990-01-01') end) as physics_ins_det,
max(case when categories like '%physics.med-ph%' then date(submitted) else date('1990-01-01') end) as physics_med_ph,
max(case when categories like '%physics.optics%' then date(submitted) else date('1990-01-01') end) as physics_optics,
max(case when categories like '%physics.ed-ph%' then date(submitted) else date('1990-01-01') end) as physics_ed_ph,
max(case when categories like '%physics.soc-ph%' then date(submitted) else date('1990-01-01') end) as physics_soc_ph,
max(case when categories like '%physics.plasm-ph%' then date(submitted) else date('1990-01-01') end) as physics_plasm_ph,
max(case when categories like '%physics.pop-ph%' then date(submitted) else date('1990-01-01') end) as physics_pop_ph,
max(case when categories like '%physics.space-ph%' then date(submitted) else date('1990-01-01') end) as physics_space_ph,
max(case when categories like '%quant-ph%' then date(submitted) else date('1990-01-01') end) as quant_ph
from
arxiv_paper_repository.arxiv_paper_repository
"
  
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

}

#* Check token - Request body should contain a token to authenticate HTTP request
#* @filter checkAuth
function(req, res){
  
  request <- jsonlite::fromJSON(req$postBody)
  
  if(request$token != Sys.getenv('plumber_token_auth')) {
    
    return(list(message = "token is incorrect"))
    
  } else {
    plumber::forward()
  }
}

#* Update arxiv repository table in BigQuery -- Batch Deployment
#* @post /update
function(){
# Hard coded operations ----
#   # Authentication for BQ
#   bigrquery::bq_auth(path = Sys.getenv('auth_path'))
#   
#   
#   # Connect to DB
#   bq_con <- DBI::dbConnect(bigquery(),
#                            project = Sys.getenv('project_id'),
#                            dataset = Sys.getenv('dataset_id'))
#   
#   # Pull down DB table
#   
#   
#   # Obtain latest days papers for select categories
#   categories <- c('stat.AP', 'stat.CO', 'stat.ML', 'stat.ME', 'stat.TH','math.OC', 'math.PR', 'math.ST', 'math.CO', 'cs.AI', 'cs.GT', 'cs.CV')
#   select_choices <- aRxiv::arxiv_cats$abbreviation
#   names(select_choices) <- aRxiv::arxiv_cats$description
#   choices <- select_choices[select_choices %in% categories]
#   
#   # Set and format dates for the arxiv API - Order of SQL categories should be identical to order in choices list!!
#   sql <- "select
# max(case when categories like '%stat.AP%' then date(submitted) else date('1990-01-01') end) as stat_AP,
# max(case when categories like '%stat.CO%' then date(submitted) else date('1990-01-01') end) as stat_CO,
# max(case when categories like '%stat.ML%' then date(submitted) else date('1990-01-01') end) as stat_ML,
# max(case when categories like '%stat.ME%' then date(submitted) else date('1990-01-01') end) as stat_ME,
# max(case when categories like '%stat.TH%' then date(submitted) else date('1990-01-01') end) as stat_TH,
# max(case when categories like '%cs.AI%' then date(submitted) else date('1990-01-01') end) as cs_AI,
# max(case when categories like '%cs.GT%' then date(submitted) else date('1990-01-01') end) as cs_GT,
# max(case when categories like '%cs.CV%' then date(submitted) else date('1990-01-01') end) as cs_cv,
# max(case when categories like '%math.CO%' then date(submitted) else date('1990-01-01') end) as math_CO,
# max(case when categories like '%math.OC%' then date(submitted) else date('1990-01-01') end) as math_OC,
# max(case when categories like '%math.PR%' then date(submitted) else date('1990-01-01') end) as math_PR,
# max(case when categories like '%math.ST%' then date(submitted) else date('1990-01-01') end) as math_ST
# from
# arxiv_paper_repository.arxiv_paper_repository"
#   
#   db_state <- bigrquery::bq_table_download(bigrquery::bq_project_query(x = Sys.getenv('project_id'), query = sql), max_results = Inf)
#   transposed_db_state <- t(db_state)
#   
#   
#   # Update table for all categories
#   today_results <- data.frame()
#   
#   for (i in seq_along(choices)) {
#     
#     last_db_day <- lubridate::as_date(transposed_db_state[[i]]) + days(1)
#     today <- Sys.Date()
#     
#     formatted_last_db_day <- stringr::str_c(stringr::str_c(gsub("-", "", last_db_day) , "*"))
#     formatted_today <- stringr::str_c(stringr::str_c(gsub("-", "", Sys.Date()) , "*"))
#     
#     
#     results_today <- try(as_tibble(aRxiv::arxiv_search(query = glue("cat: ({choices[[i]]}) AND submittedDate: [{formatted_last_db_day} TO {formatted_today}]")
#                                                        , limit = 10000
#                                                        , sort_by = c("submitted")
#                                                        , ascending = FALSE
#                                                        , batchsize = 10000)), silent = TRUE)
#     
#     today_results <- dplyr::union_all(today_results, results_today)
#     
#     cat(i, names(choices)[i], "\n")
#     
#   }
#   
#   # Format date
#   today_results <- today_results %>% 
#     dplyr::mutate(submitted = lubridate::as_datetime(submitted),
#                   updated = lubridate::as_datetime(updated)) %>% 
#     distinct()
#   
#   # Write the table back to the database
#   DBI::dbWriteTable(conn = bq_con,
#                     name = 'arxiv_paper_repository.arxiv_paper_repository',
#                     value = today_results,
#                     as_bq_fields(today_results),
#                     overwrite = FALSE,
#                     append = TRUE)
#   
#   # Close connection
#   DBI::dbDisconnect(conn = bq_con)
  
# Implement function ----
  future(update_bq_table())
  
  return(list(message = "Update Successful"))
}