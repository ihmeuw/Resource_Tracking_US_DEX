## ==================================================
## Author(s): Sawyer Crosby, Azalea Thomson
## Date: Jan 31, 2025
## Purpose: Shiny app to visualize and vet modeled outputs 
## ==================================================

## ======================= SETUP ============================
rm(list = ls())

## loading libraries
library(arrow)
library(tools)
library(data.table)
library(readxl)
library(tidyr); library(ggplot2); library(dplyr)
library(RColorBrewer)
library(shiny)
library(shinythemes)
library(plotly)
library(patchwork)
library(systemfonts)
library(textshaping)
library(grid)
library(gridExtra)
library(scales)
library(treemapify)
library(ragg)
library(scattermore)
library(geofacet)
library(treemap)

'%ni%' <- Negate('%in%')

## making sure we use the data.table versions of these
melt <- data.table::melt
dcast <- data.table::dcast
options(scipen = 999)

## setting directories for data 
data_dir <- "FILEPATH/pipeline_output/"
model_dir <- "FILEPATH/shiny/"
convergence_path <- "FILEPATH/model_version_#/"
final_dir <- "FILEPATH/04_final/" 

## this line is necessary if running locally, but breaks if publishing the app
setwd(dirname(rstudioapi::getSourceEditorContext()$path))

## ======================= Input choices ============================
## shapefiles
load("location_shape.RData") ## from current working directory
load("state_shape.RData") ## from current working directory
## choices
choice_run <- rev(list.files(model_dir, pattern = "run_"))
load("choice_acause.RData") ## from current working directory
load("choice_location_name.RData") ## from current working directory
choice_county <- choice_location_name[choice_location_name %like% " - "]
load("county_states.RData") ## from current working directory
load("tree_key.RData")
source('other_choices.R',local=T)

## ======================= Plot sizes/colors ============================
source('colors_and_sizes.R',local=T)
## ======================= Functions ============================
source('data_functions.R',local=T)
## ========================= shiny UI ==========================
ui <- suppressWarnings(navbarPage(
  theme = shinytheme("simplex"),
  title = "DEX 3.0 Quick Shiny",
  sidebarLayout(
    sidebarPanel(
      width = 2,
      selectizeInput(inputId = "run_id", label = "Run ID", choices = choice_run),
      selectizeInput(inputId = "model", label = "Model", choices = c()),
      checkboxInput(inputId = "include_race", label = "By race", value = FALSE), 
      checkboxInput(inputId = "start2010", label = "Remove years < 2010", value = TRUE),
      radioButtons(inputId = "showoutlier", label = "Outliers", choices = c('show','remove')),
      selectizeInput(inputId = "acause", label = "Cause", choices = choice_acause),
      selectizeInput(inputId = "toc", label = "Type of care", choices = choice_toc),
      selectizeInput(inputId = "metric", label = "Metric", choices = choice_metric),
      selectizeInput(inputId = "geo", label = "Geography", choices = choice_geo),
      selectizeInput(inputId = "payer", label = "Payer", choices = choice_payer),
      selectizeInput(inputId = "pri", label = "Primary payer", choices = choice_pri_payer),
      selectizeInput(inputId = "sex", label = "Sex", choices = choice_sex_id),
      selectizeInput(inputId = "age", label = "Age", choices = choice_age),
      selectizeInput(inputId = "year", label = "Year", choices = choice_year),
      selectizeInput(inputId = "race", label = "Race/ethnicity", choices = choice_race)
    ),
    mainPanel(
      tabsetPanel(
        tabPanel(
          "Histogram",
          br(),
          checkboxInput(inputId = "hist_log", label = "Log space", value = FALSE),
          hr(),
          fluidRow(
            column(1, actionButton(inputId = "hist_go", label = "Load plot")),
            column(2, downloadButton(outputId = "hist_download", label = "Download plot"))
          ),
          br(),
          plotOutput(outputId = "hist")
        ),
        tabPanel(
          "Map",
          br(),
          selectizeInput(inputId = "map_states", label = "State subset (leave blank to select all)", choices = choice_state, multiple = TRUE, selected = NULL),
          hr(),
          conditionalPanel(
            "!(typeof input.map_states !== 'undefined' && input.map_states.length > 0) & (input.geo == 'county')", ## static
            fluidRow(
              column(1, actionButton(inputId = "map_go", label = "Load plot")),
              column(2, downloadButton(outputId = "map_download", label = "Download plot"))
            ),
            h4("* Map is not interactive for county data unless a 'state subset' is specified"),
            br(),
            plotOutput(outputId = "map")
          ),
          conditionalPanel(
            "(typeof input.map_states !== 'undefined' && input.map_states.length > 0) | (input.geo != 'county')", ## interactive
            fluidRow(
              column(1, actionButton(inputId = "map_go_i", label = "Load plot")),
              column(2, downloadButton(outputId = "map_download_i", label = "Download plot"))
            ),
            br(),
            plotlyOutput(outputId = "map_i")
          )
        ),
        tabPanel(
          "Scatter",
          br(),
          fluidRow(
            column(2, checkboxInput(inputId = "scatter_drop", label = "Drop outliers (97.5%)", value = FALSE)),
            column(4, radioButtons(inputId = "scatter_shape", label = "Axes:", choices = c("Default", "Log space", "Square"), inline = TRUE)), 
          ),
          hr(),
          conditionalPanel(
            "(input.geo == 'county') | ((input.geo == 'state') & (input.age == 'all' | input.year == 'all'))", ## static
            fluidRow(
              column(1, actionButton(inputId = "scatter_go", label = "Load plot")),
              column(2, downloadButton(outputId = "scatter_download", label = "Download plot"))
            ),
            h4("* Scatter is not interactive for (1) county data or (2) state data when age or year == 'all'"),
            br(),
            plotOutput(outputId = "scatter")
          ),
          conditionalPanel(
            "(input.geo == 'national') | ((input.geo == 'state') & (input.age != 'all' & input.year != 'all'))", ## interactive
            fluidRow(
              column(1, actionButton(inputId = "scatter_go_i", label = "Load plot")),
              column(2, downloadButton(outputId = "scatter_download_i", label = "Download plot"))
            ),
            br(),
            plotlyOutput(outputId = "scatter_i")
          )
        ),
        tabPanel(
          "Compare model versions",
          br(),
          fluidRow(
            column(2, selectizeInput(inputId = "compare_run_id", label = "Compare run", choices = choice_run)),
            column(2, selectizeInput(inputId = "compare_model", label = "Compare model", choices = c())),
            column(2, radioButtons(inputId = "plot_value", label = "Plot value:", choices = c("mean", "ui"), inline = TRUE)),
            column(2, radioButtons(inputId = "color_value", label = "Plot color value:", choices = c("age", "year"), inline = TRUE)),
            column(2, checkboxInput(inputId = "facet_by_loc", label = "Facet by location", value = FALSE)),
            column(4, radioButtons(inputId = "comparemv_shape", label = "Axes:", choices = c("Default", "Log space", "Square"), inline = TRUE)),
          ),
          hr(),
          conditionalPanel(
            "(input.geo == 'county') | ((input.geo == 'state') & (input.age == 'all' | input.year == 'all'))", ## static
            fluidRow(
              column(1, actionButton(inputId = "compare_mv_go", label = "Load plot")),
              column(2, downloadButton(outputId = "compare_mv_download", label = "Download plot"))
            ),
            br(),
            plotOutput(outputId = "compare_mv")
          ),
          conditionalPanel(
            "(input.geo == 'national') | ((input.geo == 'state') & (input.age != 'all' & input.year != 'all'))", ## interactive
            fluidRow(
              column(1, actionButton(inputId = "compare_mv_go_i", label = "Load plot")),
              column(2, downloadButton(outputId = "compare_mv_download_i", label = "Download plot"))
            ),
            br(),
            plotlyOutput(outputId = "compare_mv_i")
          )
        ),
        tabPanel(
          "Popout",
          br(),
          fluidRow(
            column(2, selectizeInput(inputId = "popout_loc", label = "Location popout", choices = c())),
            column(2, checkboxInput(inputId = "popout_cis", label = "Show CI", value = FALSE)), 
            conditionalPanel(
              "input.geo == 'county'", 
              column(4, radioButtons(inputId = "popout_type", label = "Include...", choices = c("Counties in state", "All counties"), inline = TRUE))
            )
          ),
          hr(),
          fluidRow(
            column(1, actionButton(inputId = "popout_go", label = "Load plot")),
            column(2, downloadButton(outputId = "popout_download", label = "Download plot"))
          ),
          br(),
          plotlyOutput(outputId = "popout")
        ),
        tabPanel(
          "Age-time",
          br(),
          selectizeInput(inputId = "agetime_loc", label = "Location subset (leave blank to select all)", choices = c(), multiple = TRUE, selected = NULL, width = "100%"),
          conditionalPanel(
            "input.geo == 'county'",
            fluidRow(
              column(2, selectizeInput(inputId = "agetime_state", label = "Add all counties for...", choices = choice_state, multiple = TRUE, selected = NULL, options = list(maxItems = 1))),
              column(1, actionButton(inputId = "agetime_loc_clear", label = "Clear locations"))
            )
          ),
          fluidRow(
            column(2, radioButtons(inputId = "agetime_scale", label = "Axes", choices = c("Fixed" = "fixed", "Free Y axis" = "free_y"), inline = TRUE)),
            column(2, radioButtons(inputId = "facet_value", label = "Facet variable", choices = c("location","acause","year"), inline = TRUE)),
            column(4, checkboxGroupInput(inputId = "agetime_cis", label = "Show:", choices = c("Raw data", "Raw data CI", "Model mean", "Model CI"), selected = c("Raw data", "Model mean"), inline = TRUE)),
            column(2, radioButtons(inputId = "arrange_states", label = "Arrange state facets", choices = c("Alphabetical","Adjacent"), inline = TRUE))
          ),
          hr(),
          fluidRow(
            column(1, actionButton(inputId = "agetime_go", label = "Load plot")),
            column(2, downloadButton(outputId = "agetime_download", label = "Download plot"))
          ),
          conditionalPanel(
            "!(typeof input.agetime_loc !== 'undefined' && input.agetime_loc.length > 0) & (input.geo == 'county')", ## static
            h4("* The all-county plot will not display: load plot (~5min wait time) then download and open in browser")
          ),
          br(),
          plotOutput(outputId = "agetime")
        ), 
        tabPanel(
          "Coeff variation",
          br(),
          selectizeInput(inputId = "cvscatter_loc", label = "Location subset (leave blank to select all)", choices = c(), multiple = TRUE, selected = NULL, width = "100%"),
          hr(),
          fluidRow(
            column(1, actionButton(inputId = "cvscatter_go", label = "Load plot")),
          ),
          br(),
          plotlyOutput(outputId = "cvscatter")
        ), 
        tabPanel(
          "Tree map (modeled)",
          br(),
          fluidRow(
            column(1, actionButton(inputId = "model_tree_go", label = "Load plot"))
          ),
          br(),
          plotOutput(outputId = "model_tree")
        ), 
        tabPanel(
          "Tree map (final)",
          br(),
          selectizeInput(inputId = "tree_toc", label = "Type of care", choices = choice_toc, multiple = F, selected = NULL, width = "100%"),
          hr(),
          fluidRow(
            column(2, selectizeInput(inputId = "scaled_version", label = "Scaled version", choices = c())),
            column(1, actionButton(inputId = "tree_go", label = "Load plot")),
          ),
          br(),
          plotOutput(outputId = "tree")
        ), 
        tabPanel(
          "Convergence",
          br(),
          fluidRow(
            column(2, selectizeInput(inputId = "conv_acause", label = "Cause", choices = c("All", choice_acause), selected = "All", multiple = TRUE)),
            column(2, selectizeInput(inputId = "conv_toc", label = "TOC", choices = c("All", choice_toc), selected = "All", multiple = TRUE)),
            column(2, selectizeInput(inputId = "conv_metric", label = "Metric", choices = c("All", choice_metric), selected = "All", multiple = TRUE)),
            column(2, selectizeInput(inputId = "conv_geo", label = "Geography", choices = c("All", choice_geo), selected = "All", multiple = TRUE))
          ), 
          fluidRow(
            column(2, selectizeInput(inputId = "conv_payer", label = "Payer", choices = c("All", choice_payer, "Utilization" = "na"), selected = "All", multiple = TRUE)),
            column(2, selectizeInput(inputId = "conv_pri", label = "Primary payer", choices = c("All", choice_pri_payer), selected = "All", multiple = TRUE)),
            column(2, selectizeInput(inputId = "conv_sex", label = "Sex", choices = c("All", choice_sex_id), selected = "All", multiple = TRUE))
          ),
          fluidRow(
            column(2, actionButton(inputId = "conv_set", label = "Set to sidebar values?")),
            column(2, actionButton(inputId = "conv_set_all", label = "Set to all?")), 
            column(2, radioButtons(inputId = "conv_table", label = "Table type", choices = c("Binary", "Detail"), inline = TRUE))
          ),
          hr(),
          fluidRow(
            column(2, actionButton(inputId = "convergence_go", label = "Load table")),
            column(4, radioButtons(inputId = "conv_yesno", label = "Show `convergence`=...", choices = c("Yes and no", "Yes", "No"), inline = TRUE)) 
          ),
          br(),
          br(),
          br(),
          DT::dataTableOutput(outputId = "convergence")
        )
      )
    )
  )
))

## ====================== shiny server =============================
## =================================================================
server <- function(input, output, session) {
  
  ## ----------------------- dynamically updating inputs --------------------------
  observe({
    
    ## year choices
    if(input$start2010){
      updateSelectizeInput(
        session,
        inputId = "year",
        choices = c("all", 2022:2010)
      )
    }else{
      updateSelectizeInput(
        session,
        inputId = "year",
        choices = choice_year
      )
    }
    
    ## model choices
    models <- gsub("model=", "", rev(list.files(paste0(model_dir, input$run_id, "/data/"))))
    updateSelectizeInput(
      session,
      inputId = "model",
      choices = models, 
      selected = input$model
    )
    
    ## scaled version (final results) choices
    svs <- gsub("scaled_version_", "", rev(list.files(final_dir))) %>% as.numeric()
    svs <- svs[!(svs<39 | is.na(svs))]
    updateSelectizeInput(
      session,
      inputId = "scaled_version",
      choices = svs, 
      selected = input$scaled_version
    )
    updateSelectizeInput(
      session,
      inputId = "tree_toc",
      choices = choice_toc,
      selected = isolate(input$tree_toc)
    )
    ## compare model choices
    compare_models <- gsub("model=", "", rev(list.files(paste0(model_dir, input$compare_run_id, "/data/"))))
    updateSelectizeInput(
      session,
      inputId = "compare_model",
      choices = compare_models,
      selected = input$compare_model
    )
    
    if(input$metric %like% "spend"){
      updateSelectizeInput(
        session,
        inputId = "pri",
        choices = choice_pri_payer[!choice_pri_payer %like% "_mc"], 
        selected = isolate(input$pri)
      )
    }else{
      updateSelectizeInput(
        session,
        inputId = "pri",
        choices = choice_pri_payer, 
        selected = isolate(input$pri)
      )
    }
    
    ## give option of all payers for coeff var scatter
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if (length(input$cvscatter_loc)>0) c(choice_payer,'all')
    )
    updateSelectizeInput(
      session,
      inputId = "pri_payer",
      choices = if (length(input$cvscatter_loc)>0) c(choice_pri_payer,'all')
    )
    ## correct payer for util
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if(input$metric %in% c("encounters_per_person", "days_per_encounter")) c("Utilization" = "na") #else choice_payer
    )
    ## valid pri-payer/payer combos for spend
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if(input$metric %like% "spend" & input$pri == "mdcr") c("MDCR" = "mdcr", "OOP" = "oop") #else choice_payer
    )
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if(input$metric %like% "spend" & input$pri == "mdcd") c("MDCD" = "mdcd", "OOP" = "oop") #else choice_payer
    )
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if(input$metric %like% "spend" & input$pri == "mdcr_mdcd") c("MDCD" = "mdcd", "OOP" = "oop", "MDCR" = "mdcr") #else choice_payer
    )
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if(input$metric %like% "spend" & input$pri %in% c("mdcr_priv","priv")) c("Private" = "priv", "OOP" = "oop") #else choice_payer
    )
    updateSelectizeInput(
      session,
      inputId = "payer",
      choices = if(input$metric %like% "spend" & input$pri == "oop") c("OOP" = "oop") #else choice_payer
    )
    
    ## set location subset options
    updateSelectizeInput(
      session,
      inputId = "popout_loc",
      choices = if(input$geo == "state") choice_state else choice_county,
      selected = isolate(input$popout_loc)
    )
    updateSelectizeInput(
      session,
      inputId = "agetime_loc",
      choices = if(input$geo == "state") choice_state else if (input$geo == "county") choice_county else c("(NA)tional"),
      selected = isolate(input$agetime_loc)
    )
    if(input$geo == "county" & length(input$agetime_state) > 0){
      updateSelectizeInput(
        session,
        inputId = "agetime_loc",
        selected = choice_county[choice_county %like% paste0(input$agetime_state, " -")]
      )
    }
    updateSelectizeInput(
      session,
      inputId = "cvscatter_loc",
      choices = choice_state,
      selected = isolate(input$cvscatter_loc)
    )
  })
  
  
  
  ## clear locs
  observeEvent(input$agetime_loc_clear, {
    updateSelectizeInput(
      session,
      inputId = "agetime_loc",
      selected = NA
    )
    updateSelectizeInput(
      session,
      inputId = "agetime_state",
      selected = NA
    )
  })
  
  ## set convergenge selections to sidebar selections
  observeEvent(input$conv_set, {
    updateSelectizeInput(session, inputId = "conv_acause", selected = input$acause)
    updateSelectizeInput(session, inputId = "conv_toc", selected = input$toc)
    updateSelectizeInput(session, inputId = "conv_metric", selected = input$metric)
    updateSelectizeInput(session, inputId = "conv_geo", selected = input$geo)
    updateSelectizeInput(session, inputId = "conv_payer", selected = input$payer)
    updateSelectizeInput(session, inputId = "conv_pri", selected = input$pri)
    updateSelectizeInput(session, inputId = "conv_sex", selected = input$sex)
  })
  
  ## set convergenge selections to sidebar selections
  observeEvent(input$conv_set_all, {
    updateSelectizeInput(session, inputId = "conv_acause", selected = "All")
    updateSelectizeInput(session, inputId = "conv_toc", selected = "All")
    updateSelectizeInput(session, inputId = "conv_metric", selected = "All")
    updateSelectizeInput(session, inputId = "conv_geo", selected = "All")
    updateSelectizeInput(session, inputId = "conv_payer", selected = "All")
    updateSelectizeInput(session, inputId = "conv_pri", selected = "All")
    updateSelectizeInput(session, inputId = "conv_sex", selected = "All")
  })
  

  ## ----------------------- reactive datasets --------------------------
  DATA <- reactive(
    tryCatch(
      expr = {
        DATA <- read_data(x = F, input)
        return(DATA)
      },
      error = function(e){
        DATA <- data.table(
          dataset = character(), location = character(), location_name = character(), age_group_years_start = numeric(), 
          year_id = numeric(), raw_val = numeric(), se = numeric()
        )
        return(DATA)
      }
    )
  )
  
  ALL_CAUSE_DATA <- reactive(
    tryCatch(
      expr = {
        ALL_CAUSE_DATA <- read_all_cause_data(input)
        return(ALL_CAUSE_DATA)
      },
      error = function(e){
        DATA <- data.table(
          dataset = character(), location = character(), location_name = character(), age_group_years_start = numeric(), 
          year_id = numeric(), raw_val = numeric(), se = numeric()
        )
        return(ALL_CAUSE_DATA)
      }
    )
  )
  
  MODEL <- reactive(
    tryCatch(
      expr = {
        MODEL <- read_model(x = F, input, compare = F)
        return(MODEL)
      },
      error = function(e){
        MODEL <- data.table(
          location = character(), location_name = character(), age_group_years_start = numeric(), 
          year_id = numeric(), mean = numeric(), lower = numeric(), upper = numeric(), median = numeric()
        )
        return(MODEL)
      }
    )
  )
  
  COMPARE_MODEL <- reactive(
    tryCatch(
      expr = {
        COMPARE_MODEL <- read_model(x = F, input, compare = T)
        return(COMPARE_MODEL)
      },
      error = function(e){
        COMPARE_MODEL <- data.table(
          location = character(), location_name = character(), age_group_years_start = numeric(), 
          year_id = numeric(), mean = numeric(), lower = numeric(), upper = numeric(), median = numeric()
        )
        return(COMPARE_MODEL)
      }
    )
  )
  
  ALL_CAUSE_MODEL <- reactive(
    tryCatch(
      expr = {
        ALL_CAUSE_MODEL <- read_all_cause_model(input, compare = F)
        return(ALL_CAUSE_MODEL )
      },
      error = function(e){
        ALL_CAUSE_MODEL  <- data.table(
          location = character(), location_name = character(), age_group_years_start = numeric(), 
          year_id = numeric(), mean = numeric(), lower = numeric(), upper = numeric(), median = numeric()
        )
        return(ALL_CAUSE_MODEL)
      }
    )
  )
  
  CV <- reactive({
    tryCatch(
      expr = {
        CV <- get_coef_var(input)
        return(CV)
      },
      error = function(e){
        CV <- data.table()
        return(CV)
      }
    )
  })
  
  FINAL <- reactive({
    tryCatch(
      expr = {
        FINAL <- read_final(input)
        return(FINAL)
      },
      error = function(e){
        FINAL <- data.table()
        return(FINAL)
      }
    )
  })
  
  MODEL_TREE <- reactive({
    tryCatch(
      expr = {
        MODEL_TREE <- read_model_for_tree(input)
        return(MODEL_TREE)
      },
      error = function(e){
        MODEL_TREE <- data.table()
        return(MODEL_TREE)
      }
    )
  })
  
  CONV <- reactive({
    tryCatch(
      expr = {
        CONV <- read_convergence(input)
      return(CONV)
    },
    error = function(e){
      CONV <- data.table()
      return(CONV)
    }
    )
  })
  
  
  ## ---------------------- histogram ---------------------------
  ## (static)
  observeEvent(input$hist_go, {
    # debug(myhist) ## another way to debug (uncomment this, run the app, click load, then step through)
    output$hist <- renderPlot(
      myhist(input, DATA(), MODEL()),
      width = plot_sizes$hist$px[1],
      height = plot_sizes$hist$px[2]
    )
  })
  
  ## ----------------------- map --------------------------
  ## (static)
  observeEvent(input$map_go,{
    # debug(mymap)
    output$map <- renderPlot(
      mymap(input, DATA(), MODEL()),
      width = plot_sizes$map$px[1],
      height = plot_sizes$map$px[2]
    )
  })
  ## (interactive)
  observeEvent(input$map_go_i,{
    # debug(mymap)
    output$map_i <- renderPlotly(
      mymap(input, DATA(), MODEL(), interactive = TRUE)
    )
  })
  
  ## ------------------------ scatter -------------------------
  ## (static)
  observeEvent(input$scatter_go, {
    # debug(myscatter)
    output$scatter <- renderPlot(
      myscatter(input, DATA(), MODEL()),
      width = plot_sizes$scatter$px[1],
      height = plot_sizes$scatter$px[2]
    )
  })
  ## (interactive)
  observeEvent(input$scatter_go_i, {
    # debug(myscatter)
    output$scatter_i <- renderPlotly(
      myscatter(input, DATA(), MODEL(), interactive = TRUE)
    )
  })
  
  ## ------------------------ compare model versions scatter -------------------------
  ## (static)
  observeEvent(input$compare_mv_go, {
    WH <- get_dims(input, units = "px")
    ## if all counties, don't set width/height
    if(isolate(input$geo) == "county" | isolate(input$facet_by_loc) == F ){
      output$compare_mv <- renderPlot(
        mymvcompare(input, MODEL(), COMPARE_MODEL()),
        width = plot_sizes$scatter$px[1],
        height = plot_sizes$scatter$px[2]
      )
    }else{
      output$compare_mv <- renderPlot(
        mymvcompare(input, MODEL(), COMPARE_MODEL()),
        width = WH$W,
        height = WH$H
      )
    }
  })
  ## (interactive)
  observeEvent(input$compare_mv_go_i, {
    # debug(mymvcompare)
    output$compare_mv_i <- renderPlotly(
      mymvcompare(input, MODEL(), COMPARE_MODEL(), interactive = TRUE)
    )
  })
  ## ------------------------ popout -------------------------
  ## (static)
  observeEvent(input$popout_go, {
    # debug(mypopout)
    output$popout <- renderPlotly(
      mypopout(input, MODEL())
    )
  })
  
  ## ----------------------- age / time --------------------------
  ## (static)
  observeEvent(input$agetime_go, {
    WH <- get_dims(input, units = "px")
    
    ## location facet
    if ( isolate(input$facet_value)!= 'acause'){
      ## if all counties, don't set width/height
      if(isolate(input$geo) == "county" & length(isolate(input$agetime_loc)) == 0){
        output$agetime <- renderPlot(
          myagetime(input, DATA(), MODEL())
        )
      }else{
        output$agetime <- renderPlot(
          myagetime(input, DATA(), MODEL()),
          width = WH$W,
          height = WH$H
        )
      }
    }else{ ## cause facet
      
      output$agetime <- renderPlot(
        myagetime(input, ALL_CAUSE_DATA(), ALL_CAUSE_MODEL()),
        width = WH$W,
        height = WH$H
      )
      
    }
  })
  
  ## ------------------------ scatter state coef variation -------------------------
  ## (static)
  observeEvent(input$cvscatter_go, {
    # WH <- get_dims(input, units = "px")
    output$cvscatter <- renderPlotly(
      mycvscatter(input, CV())
      # width = plot_sizes$scatter$px[1],
      # height = plot_sizes$scatter$px[2]
    )

  })
  ## ------------------------ model data tree map -------------------------
  ## (static)
  observeEvent(input$model_tree_go, {

    output$model_tree <- renderPlot(
      mytree(input, MODEL_TREE())
    )
    
  })
  
  ## ------------------------ final data tree map -------------------------
  ## (static)
  observeEvent(input$tree_go, {
    
    output$tree <- renderPlot(
      mytree(input, FINAL())
    )
    
  })
  ## ----------------------- convergence --------------------------
  observeEvent(input$convergence_go, {
    output$convergence <- DT::renderDataTable({
      withProgress(message = 'Loading convergence', min = 0, max = 100, value = 0, {
        if(isolate(input$model) == "") stop("Please select a model")
        if(
          length(isolate(input$conv_acause)) == 0
          | length(isolate(input$conv_toc)) == 0
          | length(isolate(input$conv_metric)) == 0
          | length(isolate(input$conv_geo)) == 0
          | length(isolate(input$conv_payer)) == 0
          | length(isolate(input$conv_pri)) == 0
          | length(isolate(input$conv_sex)) == 0
        ){
          stop("Please make a selection above")
        }
        setProgress(50, "Reading data")
        tmp <- isolate(copy(CONV()))
        if(nrow(tmp) == 0){
          stop("No convergence data here")
        }
        if(input$conv_yesno == "Yes"){
          tmp <- tmp[convergence == "Yes"]
        }else if(input$conv_yesno == "No"){
          tmp <- tmp[convergence == "No"]
        }
        
        DT::datatable(
          tmp,
          options = list("pageLength" = 100)
        )
      })
    })
  })
  
  ## ----------------------- download handlers --------------------------
  ## Downloading is a two step process. In this section, all the plots we
  ## "downlaoded" before are actually downloaded as files from the browser. 
  ## This is the second step.
  output$hist_download <- downloadHandler(
    filename = function() "histogram.pdf",
    content = function(file) file.copy("histogram.pdf", file, overwrite=TRUE)
  )
  output$map_download <- downloadHandler(
    filename = function() "map.pdf",
    content = function(file) file.copy("map.pdf", file, overwrite=TRUE)
  )
  output$map_download_i <- downloadHandler( ## duplicate to handle interactive download
    filename = function() "map.pdf",
    content = function(file) file.copy("map.pdf", file, overwrite=TRUE)
  )
  output$scatter_download <- downloadHandler(
    filename = function() "scatter.pdf",
    content = function(file) file.copy("scatter.pdf", file, overwrite=TRUE)
  )
  output$scatter_download_i <- downloadHandler(  ## duplicate to handle interactive download
    filename = function() "scatter.pdf",
    content = function(file) file.copy("scatter.pdf", file, overwrite=TRUE)
  )
  output$compare_mv_download <- downloadHandler(
    filename = function() "compare_mv.pdf",
    content = function(file) file.copy("compare_mv.pdf", file, overwrite=TRUE)
  )
  output$compare_mv_download_i <- downloadHandler(  ## duplicate to handle interactive download
    filename = function() "compare_mv.pdf",
    content = function(file) file.copy("compare_mv.pdf", file, overwrite=TRUE)
  )
  output$popout_download <- downloadHandler(
    filename = function() "popout.pdf",
    content = function(file) file.copy("popout.pdf", file, overwrite=TRUE)
  )
  output$agetime_download <- downloadHandler(
    filename = function() "agetime.pdf",
    content = function(file) file.copy("agetime.pdf", file, overwrite=TRUE)
  )
}

## running shiny app
## ==================================================
shinyApp(ui = ui, server = server)

