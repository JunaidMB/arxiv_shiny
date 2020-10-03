# Install R version 3.5.1
FROM r-base:4.0.2

# system libraries of general use - I don't know if these are right ????
RUN apt-get update && apt-get install -y \
    default-jdk \
    libbz2-dev \
    zlib1g-dev \
    gfortran \
    liblzma-dev \
    libpcre3-dev \
    libreadline-dev \
    xorg-dev \
    sudo \  
    pandoc \
    pandoc-citeproc \
    libcurl4-gnutls-dev \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    libxml2-dev


RUN R -e "install.packages(c('aRxiv', 'shiny', 'glue' ,'stringr', 'stringi', 'gmailr', 'ggplot2', 'tableHTML', 'dplyr', 'lubridate', 'cleanNLP', 'dotenv', 'bigrquery', 'DBI'), repos = 'http://cran.us.r-project.org')"


# copy the app to the image
RUN mkdir /root/app_stuff
COPY app_stuff /root/app_stuff
COPY app_stuff/.env /root/app_stuff/.env
COPY app_stuff/arxiv-shiny-20448991f220.json /root/app_stuff/arxiv-shiny-20448991f220.json

COPY Rprofile.site /usr/lib/R/etc/

EXPOSE 3838

CMD ["R", "-e", "library(dotenv); library(cleanNLP); cnlp_init_udpipe(); shiny::runApp('/root/app_stuff', host='0.0.0.0', port=3838)"]