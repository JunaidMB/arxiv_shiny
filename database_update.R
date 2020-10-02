# Update script

# Connect to DB

# Pull down DB table


# Obtain latest days papers for all categories
select_choices <- aRxiv::arxiv_cats$abbreviation
names(select_choices) <- aRxiv::arxiv_cats$description

last_db_day <- lubridate::as_date(max(papers$submitted))
today <- Sys.Date()

formatted_last_db_day <- str_c(str_c(gsub("-", "", last_db_day) , "*"))
formatted_today <- str_c(str_c(gsub("-", "", Sys.Date()) , "*"))

# Update table for all categories
today_results <- data.frame()

for (i in seq_along(select_choices)) {
  
  results_today <- as_tibble(arxiv_search(query = glue("cat: ({select_choices[[i]]}) AND submittedDate: [{formatted_last_db_day} TO {formatted_today}]")
                                          , limit = 10000
                                          , sort_by = c("submitted")
                                          , ascending = FALSE
                                          , batchsize = 10000))
  
  papers <- dplyr::union(today_results, results_today)
  
  cat(i, names(select_choices)[i], "\n")
  
}

# Update the full table of results (Only retain last 3 months)
papers <- union(papers, today_results)

# Write the table back to the database
