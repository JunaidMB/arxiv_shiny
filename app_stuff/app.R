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

# Define selection categories for the UI
categories <- c('stat.AP', 'stat.CO', 'stat.ML', 'stat.ME', 'stat.TH','math.OC', 'math.PR', 'math.ST', 'math.CO', 'cs.AI', 'cs.GT', 'cs.CV')
select_choices <- aRxiv::arxiv_cats$abbreviation
names(select_choices) <- aRxiv::arxiv_cats$description
choices <- select_choices[select_choices %in% categories]

ui <- fluidPage(
   title = 'Arxiv Aggregator - A Web App over the Arxiv API',
   
   titlePanel("A Web App over the Arxiv API"),
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
                        start = Sys.Date() - 3,
                        end = Sys.Date() - 2,
                        min = floor_date(Sys.Date() - 90, 'month'),
                        max = Sys.Date()
         ),
         
         
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


   # Authentication for BQ
   bigrquery::bq_auth(path = Sys.getenv('auth_path'))
   
   
   # Connect to DB
   bq_con <- DBI::dbConnect(bigquery(),
                            project = Sys.getenv('project_id'),
                            dataset = Sys.getenv('dataset_id'))
   
   observeEvent(input$arxiv.get.results, {
      
      # Input parameters
      
      ## Select Categories to search
      selected_cats <- c(input$subject_select)

      ## Select date range
      date_range <- input$dateRange
      
      withProgress(message = 'Fetching Results', value = 0.5, {
         
         
         # Pull data from database
         sql <- glue("select * from arxiv_paper_repo.arxiv_paper_repo where date(submitted) between '{as_date(date_range[1])}' AND '{as_date(date_range[2])}' AND title <> ''")
         
         full_results <- bigrquery::bq_table_download(bigrquery::bq_project_query(x = Sys.getenv('project_id'), query = sql), max_results = Inf)
         
         full_results <- full_results %>% 
            arrange(submitted) %>% 
            filter(id != "")
         
         # Filter the dataframe based on input
         full_results <- full_results %>% 
            filter((str_detect(string = categories, pattern = paste(selected_cats, collapse = "|"), negate = FALSE)) & 
                      between(as_date(submitted), as_date(date_range[1]), as_date(date_range[2]) )  )
         
      })
      
      ## Restrict to only a few columns
      results <- full_results %>% 
         mutate(submitted = str_sub(submitted, end = 10),
                authors = str_replace_all(authors, "[|]", " & "),
                link_pdf = paste0("<a href='", link_pdf, "'target='_blank'>", link_pdf, "</a>")) %>% 
         select(Title = title, Submission_Date = submitted, Authors = authors, PDF_Link = link_pdf, Primary_Category = primary_category) %>% 
         distinct()
      
      # Output table
      output$table <- renderDataTable(
         expr = results, escape = FALSE)
      
      
   }) 
   
   
}

shinyApp(ui = ui, server = server)