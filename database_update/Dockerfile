FROM rocker/verse:4.0.2

RUN R -e "install.packages(c('jsonlite', 'bigrquery', 'DBI' ,'aRxiv', 'lubridate', 'dplyr', 'glue', 'dotenv', 'tidyverse', 'stringi', 'stringr', 'plumber'), repos = 'http://cran.us.r-project.org')"


COPY plumber.R plumber.R
COPY .env .env
COPY arxiv-shiny-20448991f220.json arxiv-shiny-20448991f220.json

EXPOSE 8000

ENTRYPOINT ["R", "-e", "library(plumber); library(bigrquery); library(jsonlite); library(DBI); library(aRxiv); library(dotenv); plumb('plumber.R')$run(port=8000, host='0.0.0.0')"]

