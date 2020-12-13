library(aRxiv)
library(glue)
library(dplyr)
library(stringi)
library(stringr)
library(lubridate)
library(ggplot2)
library(shiny)
library(dotenv)
library(cleanNLP)
library(bigrquery)
library(DBI)


# Define selection categories for the UI
select_choices <- aRxiv::arxiv_cats$abbreviation
names(select_choices) <- aRxiv::arxiv_cats$description
choices <- select_choices

ui <- function(req) { fluidPage(
   title = "Arxiv Aggregator - Reader's Digest",
   
   titlePanel("Arxiv Reader's Digest"),
   sidebarLayout(
      sidebarPanel(
         
         selectizeInput(
            inputId = 'subject_select',
            label = 'Select the Subjects from Menu',
            choices = choices,
            selected = "stat.ML",
            multiple = TRUE),
         
         
         dateRangeInput('dateRange',
                        label = 'Date Range',
                        start = lubridate::today(tzone = "GMT") - 3,
                        end = lubridate::today(tzone = "GMT") - 2,
                        min = floor_date(lubridate::today(tzone = "GMT") - 90, 'month'),
                        max = lubridate::today(tzone = "GMT")
         ),
         
         # textInput(inputId = 'search_string',
         #           label = 'Optional: Search Titles',
         #           value = "",
         #           placeholder = "Markov Chains"),
         
         
         br(),
         
         actionButton(inputId = "arxiv.get.results", label = "Run"),
         downloadButton(outputId = "downloadData", label = "Download"),
         
         
         
         
      ),
      mainPanel(
         
         strong("Please note: Submission dates may not line up exactly with the arXiv website."),
         
         br(),
         br(),
         br(),
         
         dataTableOutput('table')
         
      )
   )
) }

server <- function(input, output) {
   
   # Define a helper function
   # process_search_string <- function(search_string) {
   #    if (stri_count(search_string, regex="\\S+") == 1) {
   #       
   #       return(search_string)
   #    } else if (stri_count(search_string, regex="\\S+") > 1) {
   #       
   #       df <- data.frame(id = 1,
   #                        text = search_string)
   #       
   #       nlp_annotated_df <- cnlp_annotate(df)$token
   #       
   #       # Pull out nouns and symbols only 
   #       
   #       keywords <- nlp_annotated_df %>% 
   #          filter(upos %in% c("NOUN", "PROPN", "SYM")) %>% 
   #          pull(token)
   #       
   #       parsed_string <- paste(keywords, collapse =  "|")
   #       
   #       return(parsed_string) } else {
   #          
   #          return("")
   #       }
   # }
   
   # Reactive functions
   build_table <- eventReactive(input$arxiv.get.results, {
      # Input parameters
      
      ## Title
      parsed_string <- process_search_string(search_string = input$search_string)
      
      ## Select Categories to search
      selected_cats <- c(input$subject_select)
      
      ## Select date range
      date_range <- input$dateRange
      
      withProgress(message = 'Fetching Results', value = 0.5, {
         
         
         # Pull data from database - full_results is initially downloaded from the DB
         
         full_results <- full_results %>%
            arrange(submitted) %>%
            filter(id != "")
         
         if (nchar(parsed_string) == 0) {
            # Filter the dataframe based on input
            full_results <- full_results %>%
               filter((str_detect(string = categories, pattern = paste(selected_cats, collapse = "|"), negate = FALSE)) &
                         between(as_date(submitted), as_date(date_range[1]), as_date(date_range[2]) ))
         } else {
            
            # Filter the dataframe based on input
            full_results <- full_results %>%
               filter((str_detect(string = categories, pattern = paste(selected_cats, collapse = "|"), negate = FALSE)) &
                         between(as_date(submitted), as_date(date_range[1]), as_date(date_range[2]) ) &
                         str_detect(string = title, pattern = parsed_string, negate = FALSE))
         }
         
         
      })
      
      ## Restrict to only a few columns
      full_results 
   })
   
   # App rendered table
   app_build_table <- eventReactive(input$arxiv.get.results, { 
      build_table() %>% 
         mutate(submitted = str_sub(submitted, end = 10),
                authors = str_replace_all(authors, "[|]", " & "),
                link_pdf = paste0("<a href='", link_pdf, "'target='_blank'>", link_pdf, "</a>")) %>%
         select(Title = title, Submission_Date = submitted, Authors = authors, PDF_Link = link_pdf, Primary_Category = primary_category) %>%
         distinct()
         })
   
   # Download button table
   
   dl_build_table <- eventReactive(input$arxiv.get.results, {
      build_table() %>% 
         mutate(submitted = str_sub(submitted, end = 10),
                authors = str_replace_all(authors, "[|]", " & ")) %>%
         select(Title = title, Submission_Date = submitted, Authors = authors, PDF_Link = link_pdf, Primary_Category = primary_category) %>%
         distinct()
   })
   
   
   # Output table
   output$table <- renderDataTable(expr = app_build_table(), escape = FALSE)
   
   # Download Button
   output$downloadData <- downloadHandler(
      filename = function() {
         date_range <- input$dateRange
         paste("arxiv_export_", date_range[1], "-", date_range[2], ".csv",  sep = "")
      },
      content = function(file) {
         write.csv(dl_build_table(), file)
      })
   
}

shinyApp(ui = ui, server = server)