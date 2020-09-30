library(aRxiv)
library(glue)
library(dplyr)
library(stringi)
library(stringr)
library(lubridate)
library(ggplot2)
library(shiny)
library(cleanNLP)

ui <- fluidPage(
   title = 'Arxiv Aggregator - A Web App over the Arxiv API',
   
   titlePanel("A Web App over the Arxiv API"),
   sidebarLayout(
      sidebarPanel(
         
         selectizeInput(
            inputId = 'subject_select',
            label = 'Select the Subjects from Menu',
            choices = c("Statistics - Applications" = "stat.AP",
                        "Statistics - Computation" = "stat.CO",
                        "Statistics - Machine Learning" = "stat.ML",
                        "Statistics - Theory" = "math.ST",
                        "Mathematics - Probability" = "math.PR",
                        "Mathematics - Statistics" = "math.ST",
                        "Mathematics - Optimization and Control" = "math.OC",
                        "Mathematics - Functional Analysis" = "math.FA",
                        "Mathematics - Combinatorics" = "math.CO",
                        "Computer Science - Discrete Mathematics" = "cs.DM",
                        "Computer Science - Artificial Intelligence" = "cs.AI",
                        "Computer Science - Computer Vision and Pattern Recognition" = "cs.CV"),
            selected = "stat.ML",
            multiple = TRUE),
         
         dateRangeInput('dateRange',
                        label = 'Date Range',
                        start = Sys.Date() - 3,
                        end = Sys.Date() - 2,
                        min = floor_date(Sys.Date() - 90, 'month'),
                        max = Sys.Date()
         ),
         
         textInput(inputId = 'search_string',
                   label = 'Optional: Search Titles',
                   value = "",
                   placeholder = "Markov Chains"),
         
         numericInput(inputId = "resultlimits",
                      label = "How many results would you like to return? \n Note: You may see fewer results than the limit",
                      value = 100,
                      min = 1,
                      max = 1000,
                      step = 1),
         
         h5("Wait 3 seconds before sending a new request"),
         
         br(),
         
         actionButton(inputId = "arxiv.get.results", label = "Run"),
         
         
         
         
      ),
      mainPanel(
         
         strong("Please note: Submission dates may not line up exactly with the arXiv website."),
         
         br(),
         br(),
         br(),
         
         dataTableOutput('table')
         
      )
   )
)

server <- function(input, output) {
   
   # Define a helper function
   process_search_string <- function(search_string) {
      
      if (search_string > 0) {
         
         df <- data.frame(id = 1,
                          text = search_string)
         
         nlp_annotated_df <- cnlp_annotate(df)$token
         
         # Pull out nouns and symbols only 
         
         keywords <- nlp_annotated_df %>% 
            filter(upos %in% c("NOUN", "PROPN", "SYM")) %>% 
            pull(token)
         
         parsed_string <- paste(keywords, collapse =  " | ")
         
         return(parsed_string) } else {
            
            return("")
         }
      
   }
   
   observeEvent(input$arxiv.get.results, {
      
      # Input parameters
      ## Title
      parsed_string <- process_search_string(search_string = input$search_string)
      title <- if_else(nchar(parsed_string) > 0, glue('ti:({parsed_string})'), "") 
      
      ## Select Categories to search
      selected_cats <- c(input$subject_select)
      categories <- paste(selected_cats, collapse = " | ")
      cats <- glue('cat:({categories})')
      
      ## Select date range
      date_range <- input$dateRange
      date1 <- as_date(input$dateRange[[1]])
      date2 <- as_date(input$dateRange[[2]])
      formatted_date1 <- str_c(gsub("-", "", date_range[[1]]) , "*")
      formatted_date2 <- str_c(gsub("-", "", date_range[[2]]) , "*")
      date_string <- if_else(date2 - date1 > 0, glue('submittedDate:["{formatted_date1}" TO "{formatted_date2}"]'), glue('submittedDate:({formatted_date1})'))
      
      # Construct query to feed to arxiv search
      query_elements <- stri_remove_empty(c(title, cats), FALSE)
      partial_query_pasted <- paste(query_elements, collapse = ' AND ')
      query <- glue('{partial_query_pasted} AND {date_string}')
      
      
      ## Limit
      limit <- c(input$resultlimits)
      
      withProgress(message = 'Fetching Results', value = 0.5, {
         
         
         full_results <- as_tibble(arxiv_search(query = query
                                                , limit = limit
                                                , sort_by = c("submitted")
                                                , ascending = FALSE
                                                , batchsize = limit))
         
      })
      
      ## Restrict to only a few columns
      results <- full_results %>% 
         mutate(submitted = str_sub(submitted, end = 10),
                authors = str_replace_all(authors, "[|]", " & "),
                link_pdf = paste0("<a href='", link_pdf, "'target='_blank'>", link_pdf, "</a>")) %>% 
         select(Title = title, Submission_Date = submitted, Authors = authors, PDF_Link = link_pdf, Primary_Category = primary_category)
      
      # Output table
      output$table <- renderDataTable(
         expr = results, escape = FALSE)
      
      
   }) 
   
   
}

shinyApp(ui = ui, server = server)