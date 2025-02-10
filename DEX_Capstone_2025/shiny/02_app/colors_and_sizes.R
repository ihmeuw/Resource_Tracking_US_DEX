## colors
dataset_colors <- c(
  "MDCR" = "#0B84A5", 
  "MDCD" = "#E3170A", 
  "MSCAN" = "#F6C85F", 
  "KYTHERA" = "#679436", 
  "SIDS" = "purple", 
  "SEDD" = "purple", 
  "NIS" = "purple", 
  "NEDS" = "purple", 
  "MEPS" = "black"
)

## plot sizes these are listed as c(width,height)
plot_sizes <- list(
  hist = list(px = c(600, 800), inch = c(8,10)), 
  map = list(px = c(900, 1200), inch = c(10,10)),
  scatter = list(px = c(800, 600), inch = c(8,6)),
  popout = list(px = c(800, 600), inch = c(8,6)),
  agetime = list(
    national = list(px = c(500, 500), inch = c(8, 8)),
    state = list(px = c(1500, 300), inch = c(20, 4)), ## per row, per col
    county = list(px = c(1500, 300), inch = c(20, 4)) ## per row, per col
  ),
  compare_mv = list(
    national = list(px = c(500, 500), inch = c(8, 8)),
    state = list(px = c(1500, 300), inch = c(20, 4)), ## per row, per col
    county = list(px = c(1500, 300), inch = c(20, 4)) ## per row, per col
  )
)

## function to get dimensions of facet plot given how many locations we're facetting on
get_dims <- function(input, units){
  if(units == "inch"){
    scalar <- 2
  }else{
    scalar <- 200
  }
  if (isolate(input$facet_value) == 'acause'){
    plot_sizes <- list(
      agetime = list(
        national = list(px = c(1500, 300), inch = c(20, 4)),
        state = list(px = c(1500, 300), inch = c(20, 4)) ## per row, per col
      ))
    N_locs <- length(choice_acause)
  }else if (isolate(input$facet_value) == 'year'){
    N_locs <- length(choice_year)
  }else{
    N_locs <- length(isolate(input$agetime_loc))
  }
  N_rows <- ceiling(N_locs/5)
  max_N <- if(isolate(input$geo) == "national") 1 else if(isolate(input$geo) == "state") length(choice_state) else length(choice_county) 
  max_rows <- ceiling(max_N/5)
  if(N_locs == 0){ ## all states
    W <- scalar+plot_sizes$agetime[[isolate(input$geo)]][[units]][1]
    H <- scalar+plot_sizes$agetime[[isolate(input$geo)]][[units]][2]*max_rows
  }else if(N_locs > 5){ ## at least 1 full row
    W <- scalar+plot_sizes$agetime[[isolate(input$geo)]][[units]][1]
    H <- scalar+plot_sizes$agetime[[isolate(input$geo)]][[units]][2]*N_rows
  }else{ ## less than 1 row
    W <- scalar+(N_locs/5)*plot_sizes$agetime[[isolate(input$geo)]][[units]][1]
    H <- scalar+plot_sizes$agetime[[isolate(input$geo)]][[units]][2]
  }
  return(list("W" = W, "H" = H))
}

## function to add label to plot
get_label <- function(input){
  label <- paste0(
    "run_id=", isolate(input$run_id),
    ", model=", isolate(input$model),
    ", acause=", isolate(input$acause),
    ", toc=", isolate(input$toc),
    ", metric=", isolate(input$metric),
    ", geo=", isolate(input$geo),
    ", payer=", isolate(input$payer),
    ", pri=", isolate(input$pri),
    ", sex=", isolate(input$sex),
    ", age=", isolate(input$age),
    ", year=", isolate(input$year)
  )
  label <- paste0(strwrap(label, 50), collapse = "\n")
  return(label)
}
