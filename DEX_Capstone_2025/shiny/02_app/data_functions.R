myround <- function(x) ifelse(x > 10, round(x), signif(x,2))

read_data <- function(x, input){
  
  select_cols <- c('dataset', 'location', 'location_name', 'age_group_years_start', 'year_id', 'raw_val', 'se')
  
  if (input$include_race == T){
    select_cols <- c(select_cols,'race_cd')
  }
  
  
  if (x == F){
    x <- input$acause
  }
  
  fp <- paste0(data_dir,input$run_id,"/data") 
  if (isolate(input$include_race) == T){
    fp <- gsub('data','data_race',fp)
  }
  

  fp <- paste0(
    fp,
    "/metric=", input$metric,
    "/geo=", input$geo,
    "/toc=", input$toc,
    "/pri_payer=", input$pri,
    "/payer=", input$payer,'/'
  )
  present <- dir.exists(fp)
  

  if(present == T){
    DATA <- arrow::open_dataset(fp)
    if( length(input$agetime_loc) >0 & isolate(input$facet_value) == 'acause') DATA <- DATA %>% filter(location == input$agetime_loc)
    if(input$age != "all") DATA <- DATA %>% filter(age_group_years_start == as.numeric(input$age))
    if(input$year != "all") DATA <- DATA %>% filter(year_id == as.numeric(input$year))
    if (as.numeric(gsub('run_','',input$run_id)) >= 49){
      DATA <- DATA %>% filter(acause == x & sex_id == as.numeric(input$sex))
    }
    if(input$year == "all" & input$start2010 == TRUE) DATA <- DATA %>% filter(year_id >= 2010)
    if (input$include_race == T & input$race != 'ALL') DATA <- DATA %>% filter(race_cd == input$race)
    DATA <- DATA %>%
      select(all_of(select_cols)) %>%
      collect() %>%
      data.table()
  }else{
    DATA <- data.table(
      dataset = character(), location = character(), location_name = character(), age_group_years_start = numeric(),
      year_id = numeric(), raw_val = numeric(), se = numeric()
    )
  }

  DATA$acause <- x
  return(DATA)
}

read_all_cause_data <- function(input){

  DATA <- rbindlist(lapply(choice_acause, read_data, input),fill = T)

}

read_model <- function(x, input, compare = F){
  
  select_cols <- c('location', 'location_name', 'age_group_years_start', 'year_id', 'mean', 'lower', 'upper', 'median', 'outlier', 'dataset', 'raw_val')
  
  if (input$include_race == T){
    select_cols <- c(select_cols,'race_cd')
  }
  
  if (compare == T){
    read_mod <- input$compare_model
    read_run <- input$compare_run_id
  }else{
    read_mod <- input$model
    read_run <- input$run_id
  }
  
  if (x == F){
    x <- input$acause
  }
  fp <- paste0(
    model_dir, 
    read_run, "/data",
    "/model=", read_mod,
    "/acause=", x,
    "/toc=", input$toc,
    "/metric=", input$metric,
    "/geo=", input$geo,
    "/payer=", input$payer,
    "/pri_pay_", input$pri,
    "_sex", input$sex, "-0.parquet"
  )
  if(file.exists(fp)){
    MODEL <- arrow::open_dataset(fp) 
    if( length(input$agetime_loc) >0 & isolate(input$facet_value) == 'acause') MODEL <- MODEL %>% filter(location == input$agetime_loc)
    if(input$age != "all") MODEL <- MODEL %>% filter(age_group_years_start == as.numeric(input$age))
    if(input$year != "all") MODEL <- MODEL %>% filter(year_id == as.numeric(input$year))
    if(input$year == "all" & input$start2010 == TRUE) MODEL <- MODEL %>% filter(year_id >= 2010)
    if (input$include_race == T & input$race != 'ALL') MODEL <- MODEL %>% filter(race_cd == input$race)
    MODEL <- MODEL %>%
      select(all_of(select_cols)) %>%
      collect() %>%
      data.table()
  }else{
    MODEL <- data.table(
      location = character(), location_name = character(), age_group_years_start = numeric(), 
      year_id = numeric(), mean = numeric(), lower = numeric(), upper = numeric(), median = numeric()
    )
  }
  MODEL$acause <- x
  return(MODEL)
}

read_all_cause_model <- function(input, compare = F){
  MODEL <- rbindlist(lapply(choice_acause, read_model, input),fill = T)
}

read_convergence <- function(input){
  model <- gsub("DEX_LSAE_model_v_", "", input$model)
  fp <- gsub("#", model, convergence_path)
  if(input$conv_table == "Detail"){
    fp <- paste0(fp, "parquet_convergence_detail")
  }else{
    fp <- paste0(fp, "parquet_convergence")
  }
  
  if(file.exists(fp)){
    CONV <- arrow::open_dataset(fp) %>%
      filter(
        acause %in% input$conv_acause
        & toc %in% input$conv_toc
        & metric %in% input$conv_metric
        & geo %in% input$conv_geo
        & payer %in% input$conv_payer
        & pri_payer %in% input$conv_pri
        & sex_id %in% input$conv_sex)%>% collect() %>% data.table()
    
  }else{
    CONV <- data.table()
  }
  return(CONV)
}

read_model_for_tree <- function(input){
  
  select_cols <- c('pri_payer', 'payer', 'location', 'age_group_years_start', 'year_id', 'sex_id', 'acause', 'mean')
  
  if (input$include_race == T){
    select_cols <- c(select_cols,'race_cd')
  }
  
  fp <- paste0(
    model_dir, 
    input$run_id, "/data",
    "/model=", input$model
  )
  if(file.exists(fp)){
    MODEL_AGG <- arrow::open_dataset(fp)
    MODEL_SP <- MODEL_AGG %>%
      filter(toc == input$toc & 
             geo == input$geo &
             metric == 'spend_per_encounter') %>%
      select(all_of(select_cols)) %>%
      rename(spend = mean)
    
    if (input$include_race == T){
      MODEL_VOL <- MODEL_AGG %>%
        filter(toc == input$toc & 
                 geo == input$geo &
                 metric == 'encounters_per_person') %>%
        select(all_of(select_cols)) %>%
        mutate(payer = NULL) %>%
        rename(vol = mean) %>%
        left_join(MODEL_SP) %>%
        mutate(tot_spend = vol * spend) %>%
        filter(!is.na(tot_spend))%>%
        group_by(race_cd,acause)%>%
        summarise(mean_spend = sum(tot_spend, na.rm = TRUE)) %>%
        collect() %>% as.data.table()
    }else{
      MODEL_VOL <- MODEL_AGG %>%
        filter(toc == input$toc & 
                 geo == input$geo &
                 metric == 'encounters_per_person') %>%
        select(all_of(select_cols)) %>%
        mutate(payer = NULL) %>%
        rename(vol = mean) %>%
        left_join(MODEL_SP) %>%
        mutate(tot_spend = vol * spend) %>%
        filter(!is.na(tot_spend))%>%
        group_by(acause)%>%
        summarise(mean_spend = sum(tot_spend, na.rm = TRUE)) %>%
        collect() %>% as.data.table()
    }

    MODEL_TREE <- merge(MODEL_VOL, tree_key, by ='acause')
    MODEL_TREE <- MODEL_TREE[!(is.na(mean_spend))]
    MODEL_TREE <- MODEL_TREE[!(mean_spend==0)]
    MODEL_TREE[ ,tot_spend_yr := sum(mean_spend)]

    MODEL_TREE$pct_sp_yr<-MODEL_TREE$mean_spend/MODEL_TREE$tot_spend_yr
    #Can play around with the threshold, want to maximize cause names displayed as words
    MODEL_TREE$cause_name1<-ifelse(MODEL_TREE$pct_sp_yr>0.015, MODEL_TREE$cause_name,MODEL_TREE$number)
    setnames(MODEL_TREE,'mean_spend','spend')
    
    if (input$include_race == T & input$race != 'ALL') MODEL_TREE <- MODEL_TREE %>% filter(race_cd == input$race)
  }else{
    MODEL_TREE <- data.table()
  }
  
  return(MODEL_TREE)
    
}

read_final <- function(input){

  fp <- paste0(
    final_dir,'scaled_version_',
    input$scaled_version, "/collapsed/data/geo=state/",
    "/toc=", input$tree_toc
  )

  if(file.exists(fp)){
    FINAL <- arrow::open_dataset(fp)
    if (input$include_race == T & input$race != 'ALL') FINAL <- FINAL %>% filter(race_cd == input$race)
    FINAL <- FINAL %>%
      select(mean_spend, acause) %>%
      group_by(acause) %>%
      summarise(spend = sum(mean_spend, na.rm = TRUE)) %>%
      collect() %>%
      data.table()
    FINAL$spend <- as.numeric(FINAL$spend)
    FINAL<- FINAL[!is.na(acause)]
    FINAL <- FINAL[!(is.na(spend))]
    FINAL <- FINAL[!(spend==0)]
    FINAL <- merge(FINAL, tree_key, by ='acause')
    FINAL[ ,tot_spend_yr := sum(spend)]
    FINAL$pct_sp_yr<-FINAL$spend/FINAL$tot_spend_yr
    #Can play around with the threshold, want to maximize cause names displayed as words
    FINAL$cause_name1<-ifelse(FINAL$pct_sp_yr>0.015, FINAL$cause_name,FINAL$number)
    
  }else{
    FINAL <- data.table()
  }

  return(FINAL)
}

get_coef_var <- function(input){
  
  if (input$geo != 'county'){
    print('This plot is only for county data')
  }
  fp <- paste0(
    model_dir,
    input$run_id, "/data/",
    "model=", input$model,
    "/acause=", input$acause,
    "/toc=", input$toc,
    "/metric=", input$metric,
    "/geo=", input$geo
  )
  if(dir.exists(fp)){
    CV <- arrow::open_dataset(fp)
    if(input$payer != "all") CV <- CV %>% filter(payer == input$payer)
    if(input$pri != "all") CV <- CV %>% filter(pri_payer == input$pri)
    if(input$age != "all") CV <- CV %>% filter(age_group_years_start == as.numeric(input$age))
    if(input$year != "all") CV <- CV %>% filter(year_id == as.numeric(input$year))
    
    CV <- CV %>% 
      filter(outlier==0) %>%
      left_join(county_states, by = 'location')%>%
      group_by(state) %>%
      mutate(modeled_mean = mean)%>%
      summarise(mean_raw = mean(raw_val,na.rm = T),
                sd_raw = sd(raw_val,na.rm = T),
                n_obs = n(),
                mean_mod = mean(modeled_mean,na.rm = T),
                sd_mod = sd(modeled_mean,na.rm = T)) %>%
      mutate(cv_raw = sd_raw/mean_raw,
             cv_mod = sd_mod/mean_mod) %>%
      arrange(desc(cv_raw)) %>%as.data.table()
    return(CV)
  }else{
    CV <- data.table()
  }
  
  return(CV)
}

mytree <- function(input, final_data){
  
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    setProgress(33, "Reading data")
    tree_data <- isolate(copy(final_data))

    if(nrow(tree_data) == 0 ) stop("No data here")
    tree_data[,cause_name1:=gsub("'", "", cause_name1)]
    tree_data[,cause_name1:=gsub(",", "", cause_name1)]
    setProgress(66, "Plotting")
    if (input$include_race == T){
      title <- paste0("Spending by cause: ", input$race)
    }else{
      title <- paste0("Spending by cause")
    }
    
    p <- treemap(tree_data,
                 index=c('cause_name_lvl1','cause_name1'),#
                 vSize='spend',
                 vColor='cause_name_lvl1',
                 title=paste0("Spending by cause"),
                 position.legend = 'right') 
    
  })
  
}

myhist <- function(input, raw_data, model_data){

  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    if(isolate(input$model) == "") stop("Please select a model")
    setProgress(33, "Reading data")
    tmp_d <- isolate(copy(raw_data))
    tmp_m <- isolate(copy(model_data))
    if(nrow(tmp_d) == 0 & nrow(tmp_m) == 0) stop("No model data or raw data here")
    
    tmp_d[,"data_type" := "Raw data"]
    tmp_d[,c("lower", "upper") := NA]
    setnames(tmp_d, "raw_val", "mean")
    tmp_m[,"data_type" := "Model mean"]
    tmp <- rbind(tmp_d, tmp_m, use.names = T, fill = T)
    
    setProgress(66, "Plotting")
    ## calculate mean, min, max
    lines <- tmp[,.(middle = mean(mean, na.rm = T), bottom = min(mean, na.rm = T), top = max(mean, na.rm = T)), by = data_type]
    
    ## make plots
    p <- tmp %>%
      ggplot() +
      geom_histogram(aes(x = mean)) +
      xlab("") +
      ylab("") +
      ggtitle("Model mean") +
      theme_bw(base_size = 20) +
      geom_vline(aes(xintercept = middle), data = lines, color = "red4") +
      geom_vline(aes(xintercept = bottom), data = lines, color = "red4") +
      geom_vline(aes(xintercept = top), data = lines, color = "red4") +
      facet_grid(data_type~., scales = "free_y")
    
    if(isolate(input$hist_log)){
      p <- p + scale_x_continuous(trans = "log", labels = myround)
    }
    
    ## save plot (to prep for download handler)
    ggsave(
      filename = "histogram.pdf", 
      width = plot_sizes$hist$inch[1], height = plot_sizes$hist$inch[2], units = "in",
      plot = p + labs(subtitle = get_label(reactiveValuesToList(input))) + theme(plot.subtitle = element_text(size = 12))
    )
    
    ## show plot
    p
  })
}

mymap <- function(input, raw_data, model_data, interactive = FALSE){
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {

    if(isolate(input$model) == "") stop("Please select a model")
    if(isolate(input$geo) == "national") stop("National map not allowed")
    if(isolate(input$age) == "all" | isolate(input$year) == "all") stop("Pleace specify both age and year")
    
    setProgress(33, "Reading data")
    tmp_d <- isolate(copy(raw_data))
    tmp_m <- isolate(copy(model_data))
    if(nrow(tmp_d) == 0 & nrow(tmp_m) == 0) stop("No model data or raw data here")
    if(nrow(tmp_d) > 0){
      tmp_d <- tmp_d[,.(mean = mean(raw_val, na.rm = T), dataset = paste0(unique(dataset), collapse = ", ")), by = .(location, location_name, age_group_years_start, year_id)]
      tmp_d[,"data_type" := "Raw data"]
      tmp_d[,c("lower", "upper") := NA]
    }
    if(nrow(tmp_m) > 0){
      tmp_m[,"data_type" := "Model mean"]
    }
    tmp <- rbind(tmp_d, tmp_m, use.names = T, fill = T)
    
    ## filter to states if applicable
    if(length(isolate(input$map_states)) > 0){
      if(isolate(input$geo) == "state"){
        tmp <- tmp[location %in% isolate(input$map_states)]  
      }else{
        tmp[,state := gsub(" -.+", "", location_name)]
        tmp <- tmp[state %in% isolate(input$map_states)]  
      }
    }
    
    setProgress(66, "Plotting")
    ## make map data
    if(isolate(input$geo) == "county"){
      tmp[,mcnty := as.integer(location)]
      map_data <- merge(tmp, location_shape, by="mcnty", allow.cartesian = TRUE)
    }else if(isolate(input$geo) == "state"){
      tmp[,id := location]
      map_data <- merge(tmp, state_shape, by="id", allow.cartesian = TRUE)
    }
    
    ## make plot
    p <- ggplot(map_data) +
      geom_polygon(aes(
        x = long,
        y = lat,
        group = group,
        fill = mean,
        text = paste0(location_name, "\nDataset(s): ", dataset, "\nVal: ", myround(mean), "\nLCI: ", myround(lower),"\nUCI: ", myround(upper))
      )) +
      geom_path(data=state_shape, aes(x=long, y=lat, group=group), colour="black", size=0.09) +
      scale_fill_gradientn(colours = rev(brewer.pal(5, "Spectral"))) +
      scale_x_continuous("", breaks=NULL, expand=c(0,0)) +
      scale_y_continuous("", breaks=NULL, expand=c(0,0)) +
      coord_fixed(ratio=1) +
      theme_bw(base_size = 20) +
      labs(fill = "") +
      theme(
        legend.position = "left",
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        plot.subtitle = element_text(size = 10),
        axis.text = element_blank(),
        axis.ticks = element_blank(),
        axis.line = element_line(colour = "black")
      ) +
      facet_grid(data_type~.)
    
    ## save plot (to prep for download handler)
    ggsave(
      filename = "map.pdf", 
      width = plot_sizes$map$inch[1], height = plot_sizes$map$inch[2], units = "in",
      plot = p + labs(subtitle = get_label(reactiveValuesToList(input))) + theme(plot.subtitle = element_text(size = 12))
    )
    
    ## show plot
    if(interactive){
      ggplotly(p + theme(legend.position = "none"), tooltip = "text", width = plot_sizes$map$px[1], height = plot_sizes$map$px[2])
    }else{
      p
    }
    
  })
}

myscatter <- function(input, raw_data, model_data, interactive = FALSE){
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    if(isolate(input$model) == "") stop("Please select a model")
    if(isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$geo) == "county"){
      stop("Pleace specify age and/or year for the county-level scatter")
    }
    
    setProgress(33, "Reading data")
    tmp_d <- isolate(copy(raw_data))
    tmp_m <- isolate(copy(model_data))
    if(nrow(tmp_d) == 0 & nrow(tmp_m) == 0) stop("No model data or raw data here")
    if(nrow(tmp_d) == 0) stop("No raw data here")
    if(nrow(tmp_m) == 0) stop("No model data here")
    tmp <- merge(tmp_d, tmp_m, by = intersect(names(tmp_d), names(tmp_m)))
    
    if(isolate(input$scatter_drop)){
      tmp <- tmp[raw_val < quantile(raw_val, .975, na.rm = T)]
    }
    
    setProgress(66, "Plotting")
    ## plot
    if(interactive){
      p <- tmp %>%
        ggplot() +
        geom_point(aes(
          x = raw_val,
          y = mean,
          color = dataset,
          text = paste0(
            "\nDataset: ", dataset,
            "\nLocation: ", location_name,
            "\nAge: ", age_group_years_start,
            "\nYear: ", year_id,
            "\nData val: ", myround(raw_val),
            "\nModel val: ", myround(mean),
            "\nAbs diff: ", myround(abs(raw_val - mean)))),
          alpha = .8, size = 2, pch = c(19)) ## "pch" speeds things up a bit
    }else{
      p <- tmp %>%
        ggplot() +
        geom_point(aes( ## MUCH faster for MANY overlapping points, but doesn't work with plotly
          x = raw_val,
          y = mean,
          color = dataset),
          alpha = .8)
    }
    
    p <- p +
      scale_color_manual(values = dataset_colors, limits = force) +
      geom_abline(intercept = 0, slope = 1) +
      xlab("Raw data") +
      ylab("Model mean") +
      theme_bw(base_size = 20) +
      theme(legend.position = "right")
    
    
    if(isolate(input$scatter_shape) == "Square"){
      t <- max(max(tmp$raw_val, na.rm = T), max(tmp$mean, na.rm = T))
      p<-p+coord_equal(xlim=c(0,t),ylim=c(0,t))
    }else if(isolate(input$scatter_shape) == "Log space"){
      p <- p +
        scale_x_continuous(trans = "log", labels = myround) +
        scale_y_continuous(trans = "log", labels = myround)
    }
    
    ## save plot (to prep for download handler)
    ggsave(
      filename = "scatter.pdf", 
      width = plot_sizes$scatter$inch[1], height = plot_sizes$scatter$inch[2], units = "in",
      plot = p + labs(subtitle = get_label(reactiveValuesToList(input))) + theme(plot.subtitle = element_text(size = 12))
    )
    
    ## show plot
    if(interactive){
      ggplotly(p, tooltip = "text", width = plot_sizes$scatter$px[1], height = plot_sizes$scatter$px[2])
    }else{
      p
    }
    
  })
}

mypopout <- function(input, model_data){
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    
    if(isolate(input$model) == "") stop("Please select a model")
    if((isolate(input$age) == "all" & isolate(input$year) == "all") | (isolate(input$age) != "all" & isolate(input$year) != "all")){
      stop("Please specify age or year (not both)")
    }
    if(isolate(input$popout_loc) == "") stop("Please specify a popout location")
    
    setProgress(33, "Reading data")
    tmp <- isolate(copy(model_data))
    if(nrow(tmp) == 0) stop("No model data here")
    
    setProgress(66, "Plotting")
    ## higlight popout location
    if(isolate(input$geo) == "state"){
      tmp_pop <- tmp[location == isolate(input$popout_loc)]
      tmp_other <- tmp[location != isolate(input$popout_loc)]  
    }else{
      tmp_pop <- tmp[location_name == isolate(input$popout_loc)]
      tmp_other <- tmp[location_name != isolate(input$popout_loc)]
      if(isolate(input$popout_type) == "Counties in state"){
        tmp_other <- tmp_other[location_name %like% gsub(" -.+", "", isolate(input$popout_loc))]
      }
    }
    
    ## make plot
    p <- tmp %>%
      ggplot() +
      geom_line(
        data = tmp_other,
        aes(
          x = if(isolate(input$age) == "all") age_group_years_start else year_id,
          y = mean,
          group = location,
          text = if(isolate(input$age) == "all") paste0("\nLocation: ", location_name, "\nAge: ", age_group_years_start, "\nModel mean: ", mean) else paste0("\nLocation: ", location_name, "\nYear: ", year_id, "\nModel mean: ", myround(mean))
        ),
        color = "grey50", alpha = .5) +
      geom_line(
        data = tmp_pop,
        aes(
          x = if(isolate(input$age) == "all") age_group_years_start else year_id,
          y = mean,
          group = location,
          text = if(isolate(input$age) == "all") paste0("\nLocation: ", location_name, "\nAge: ", age_group_years_start, "\nModel mean: ", mean) else paste0("\nLocation: ", location_name, "\nYear: ", year_id, "\nModel mean: ", myround(mean))
        ), 
        color = "cornflowerblue", size = 1.5) +
      xlab(if(isolate(input$age) == "all") "Age" else "Year") +
      ylab("Model mean") +
      theme_bw(base_size = 20)
    
    if(isolate(input$popout_cis)){
      p <- p + 
        geom_ribbon(
          data = tmp_pop,
          aes(
            x = if(isolate(input$age) == "all") age_group_years_start else year_id,
            ymin = lower,
            ymax = upper,
            group = location,
            text = if(isolate(input$age) == "all") paste0("\nLocation: ", location_name, "\nAge: ", age_group_years_start, "\nModel mean: ", mean) else paste0("\nLocation: ", location_name, "\nYear: ", year_id, "\nModel mean: ", myround(mean))
          ), 
          alpha = .3, fill = "cornflowerblue")
    }
    
    ## save plot (to prep for download handler)
    ggsave(
      filename = paste0("popout.pdf"),
      width = plot_sizes$popout$inch[1], height = plot_sizes$popout$inch[2], units = "in", 
      plot = p + labs(subtitle = paste0(get_label(reactiveValuesToList(input)), ", popout=", isolate(input$popout_loc))) + theme(plot.subtitle = element_text(size = 12))
    )
    
    ## show plot
    ggplotly(p, tooltip = "text", width = plot_sizes$popout$px[1], height = plot_sizes$popout$px[2])
    
  })
}

myagetime <- function(input, raw_data, model_data){
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    if(isolate(input$model) == "") stop("Please select a model")
    
    if ( isolate(input$include_race) == T & isolate(input$geo) == 'county' ){
      stop("No county race models available")
    }
    if ( isolate(input$include_race) == T & input$year == "all" & input$race == 'ALL' & input$age == 'all'){
      stop("Cannot plot all races with all years and all ages, please select a non 'all' year, age, or race")
    }
    if ( isolate(input$facet_value) == 'age' & isolate(input$geo) == 'county' ){
      stop("Faceting on age not currently allowed for county models")
    } 
    if ( isolate(input$facet_value) == 'acause' & isolate(input$geo) == 'county' ){
      stop("Faceting on acause not currently allowed for county models")
    } 
    if ( isolate(input$geo) == 'state' & (isolate(input$facet_value) == 'acause' | isolate(input$facet_value)== 'year') & length(isolate(input$agetime_loc)) != 1 ){
      stop("Please specify a single location for faceting on something other than location")
    } 
    
    if ( isolate(input$arrange_states) == 'Adjacent' & isolate(input$geo) != 'state' ){
      stop('Please select alphabetical faceting for any geo besides state')
    }
    
    setProgress(33, "Reading data")
    tmp_d <- isolate(copy(raw_data))
    tmp_m <- isolate(copy(model_data))
    if(nrow(tmp_d) == 0 & nrow(tmp_m) == 0) stop("No model data or raw data here")
    if(nrow(tmp_m) == 0 & ("Model mean" %in% isolate(input$agetime_cis) | "Model CI" %in% isolate(input$agetime_cis))) stop("No model data here")
    if(nrow(tmp_d) == 0 & "Raw data" %in% isolate(input$agetime_cis)) stop("No raw data here")
    
    keep_cols <- c('dataset', 'location', 'location_name', 'age_group_years_start', 'year_id', 'outlier','raw_val','acause')
    if (input$include_race == T){
      keep_cols <- c(keep_cols,'race_cd')
    }
    
    if (nrow(tmp_m)>0){
      if(input$showoutlier == 'show'){
        tmp_outlier_d <- isolate(copy(model_data[!is.na(outlier),..keep_cols]))
        tmp_d <- merge(tmp_d,tmp_outlier_d, all=T)
        tmp_m <- tmp_m[,`:=`(dataset = NULL,outlier=NULL,raw_val=NULL)]
        tmp_d$outlier <- as.factor(tmp_d$outlier)
      }
      
      if (input$showoutlier == 'remove'){
        tmp_outlier_d <- isolate(copy(model_data[!is.na(outlier),..keep_cols]))
        tmp_d <- merge(tmp_d,tmp_outlier_d, all=T)
        tmp_m <- tmp_m[,`:=`(dataset = NULL,outlier=NULL,raw_val=NULL)]
        tmp_d <- tmp_d[outlier!= 1]
      }
    }else{
      tmp_d[,outlier:=0]
      tmp_d$outlier <- as.factor(tmp_d$outlier)
      if (input$include_race == T){
        tmp_d <- tmp_d[race_cd != 'UNK']
      }

    }

    
    ## filter to location if applicable
    if(length(isolate(input$agetime_loc)) > 0){
      if(isolate(input$geo) == "state"){
        tmp_d <- tmp_d[location %in% isolate(input$agetime_loc)]  
        tmp_m <- tmp_m[location %in% isolate(input$agetime_loc)]  
      }else if(isolate(input$geo) == "county"){
        mcnties <- county_states[shiny_cnty_name %in% isolate(input$agetime_loc)]$location
        tmp_d <- tmp_d[location %in% mcnties]  
        tmp_m <- tmp_m[location %in% mcnties]  
      }
    }
    
    setProgress(66, "Plotting")    

    tmp_d[,age_group_years_start := as.numeric(age_group_years_start)]
    tmp_m[,age_group_years_start := as.numeric(age_group_years_start)]
    
    if (isolate(input$arrange_states) == 'Adjacent' & isolate(input$facet_value) == 'location' & isolate(input$geo) == 'state'){
      us_state_grid2 <- as.data.table(us_state_grid2)
      state_facet_order <- rbind(us_state_grid2[name == 'Hawaii'], us_state_grid2[order(col,row)][!(name == 'Hawaii')])
      tmp_d$location_name <- factor(tmp_d$location_name, levels=state_facet_order$name)
      tmp_m$location_name <- factor(tmp_m$location_name, levels=state_facet_order$name)
    }

    ## make plot
    if (isolate(input$facet_value) == 'location'){
      p <- ggplot() +
          facet_wrap(~location_name, ncol = 5, scales = isolate(input$agetime_scale)) +
          xlab(if(isolate(input$age) == "all") "Age" else "Year")+
          labs(shape = 'Outlier')
    }else if (isolate(input$facet_value) == 'year'){
      p <- ggplot() +
        facet_wrap(~year_id, ncol = 5, scales = isolate(input$agetime_scale)) +
        xlab("Year") +
        labs(shape = 'Outlier')
    }else{
      p <- ggplot() +
        facet_wrap(~acause, ncol = 5, scales = isolate(input$agetime_scale)) +
        xlab(if(isolate(input$age) == "all") "Age" else "Year") +
        labs(shape = 'Outlier')

    }
    

    p <- p + 
      labs(color = if(isolate(input$include_race) == T & isolate(input$race) == 'ALL') "Race" 
           else if(isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$facet_value) != 'year') "Year" 
           else "Dataset")+
      theme_bw(base_size = 20) +
      theme(legend.position = "top")
    
    ## add limites
    if(isolate(input$age) == "all"){
      p <- p + scale_x_continuous(limits = c(0, 85), breaks = c(0,10,20,30,40,50,60,70,80))
    }else if(isolate(input$year) == "all" & isolate(input$start2010) == FALSE & isolate(input$age) != "all"){
      p <- p + scale_x_continuous(limits = c(2000, 2022), breaks = c(2000, 2005, 2010, 2015, 2020, 2022))
    }else{
      p <- p + scale_x_continuous(limits = c(2010, 2022), breaks = c(2010, 2015, 2020,2022))
    }
    
    
    if("Model mean" %in% isolate(input$agetime_cis)){
      p <- p + 
        geom_line(
          data = tmp_m,
          aes(
            x = if(isolate(input$age) == "all") age_group_years_start else year_id,
            y = mean,
            color = if(isolate(input$include_race) == T & isolate(input$race) == 'ALL') as.factor(race_cd) 
            else if (isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$facet_value) != "year")  as.factor(year_id) ))
    }
    
    if("Model CI" %in% isolate(input$agetime_cis)){
      p <- p + 
        geom_ribbon(
          data = tmp_m,
          aes(
            x = if(isolate(input$age) == "all") age_group_years_start else year_id,
            ymin = lower,
            ymax = upper,
            fill = if(isolate(input$include_race == T) & isolate(input$race) == 'ALL') as.factor(race_cd)
            else if (isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$facet_value) != "year")  as.factor(year_id) ),
          alpha = .3)+
        guides(fill = FALSE) 
    }

    
    if("Raw data" %in% isolate(input$agetime_cis)){
      if(nrow(tmp_d) > 0){
        p <- p + geom_point(
          data = tmp_d,
          size = 3,
          aes(
            x = if(isolate(input$age) == "all") age_group_years_start else year_id,
            y = raw_val,
            color = if(isolate(input$include_race) == T & isolate(input$race) == 'ALL') as.factor(race_cd) 
            else if (isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$facet_value) != "year")  as.factor(year_id) 
            else as.factor(dataset),
            shape = if(input$showoutlier == 'show') outlier
          )) + 
          {if ( (isolate(input$include_race) == F & isolate(input$age) == "all" & isolate(input$year) != "all") | 
                (isolate(input$include_race) == F & isolate(input$age) != "all" & isolate(input$year) == "all") |
                isolate(input$facet_value) == 'year') scale_color_manual(values = dataset_colors, limits = force) }
        
      }
    }
    
    
    if("Raw data CI" %in% isolate(input$agetime_cis)){
      if(nrow(tmp_d) > 0){
        tmp_d[,l := raw_val - 1.96*se]
        tmp_d[l < 0, l := 0]
        tmp_d[,u := raw_val + 1.96*se]
        p <- p + 
          geom_linerange(
            data = tmp_d,
            aes(
              x = if(isolate(input$age) == "all") age_group_years_start else year_id,
              y = raw_val,
              ymin = l,
              ymax = u,
              color = if(isolate(input$include_race) == T & isolate(input$race) == 'ALL') as.factor(race_cd) 
              else if (isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$facet_value) != "year")  as.factor(year_id) 
              else as.factor(dataset),
              shape = if(input$showoutlier == 'show') outlier
            ))
      }
    }
    
    ## save plot (to prep for download handler)
    WH <- get_dims(isolate(input), units = "inch")
    ggsave(
      filename = "agetime.pdf",
      width = WH$W,
      height = WH$H,
      units = "in",
      plot = p + labs(subtitle = get_label(reactiveValuesToList(input))) + theme(plot.subtitle = element_text(size = 12)),
      limitsize = FALSE
    )
    
    ## show plot if we want, otherwise just return blank plot to save time
    if(isolate(input$geo) == "county" & length(isolate(input$agetime_loc)) == 0){
      ## dev.off()
      plot.new()
    }else{
      p
    }
    
  })
}

mymvcompare <- function(input, model_data, model_data2, interactive = FALSE){
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    if(isolate(input$model) == "") stop("Please select a model")
    if(isolate(input$compare_run_id) == "") stop("Please select a comparison run id")
    if(isolate(input$compare_model) == "") stop("Please select a comparison model")
    if(isolate(input$age) == "all" & isolate(input$year) == "all" & isolate(input$geo) == "county"){
      stop("Pleace specify age and/or year for the county-level scatter")
    }
    if( isolate(input$facet_by_loc) == T & interactive == T) stop("Facetting with plotly not allowed currently.")
    
    
    setProgress(33, "Reading data")
    tmp_m <- isolate(copy(model_data))
    tmp_m2 <- isolate(copy(model_data2))
    if(nrow(tmp_m) == 0 & nrow(tmp_m2) == 0) stop("No model data or comparison model data here")
    if(nrow(tmp_m) == 0) stop("No model data here")
    if(nrow(tmp_m2) == 0) stop("No comparison model data here")
    
    if (isolate(input$plot_value) == 'mean'){
      tmp_m[, plot_val:= mean]
      tmp_m2[, plot_val_compare:= mean]
      lab_value <- 'mean estimate'
    }
    if (isolate(input$plot_value) == 'ui'){
      tmp_m <- tmp_m[, plot_val:= upper-lower]
      tmp_m2 <- tmp_m2[, plot_val_compare:= upper-lower]
      lab_value <- 'UI difference'
    }
    
    keep_cols <- c('location', 'location_name', 'age_group_years_start', 'year_id', 'dataset', 'plot_val')
    if (input$include_race == T){
      keep_cols <- c(keep_cols,'race_cd')
    }
    keep_cols2 <- gsub('plot_val','plot_val_compare',keep_cols)
    
    tmp_m <- tmp_m[,..keep_cols]
    tmp_m2 <- tmp_m2[,..keep_cols2]
    
    tmp <- merge(tmp_m, tmp_m2, by = intersect(names(tmp_m), names(tmp_m2)))

    if (isolate(input$color_value) == 'age'){
      color_val <- 'age_group_years_start'
    }else{
      color_val <- 'year_id'
    }
    
    setProgress(66, "Plotting")
    ## plot
    if(interactive){
      p <- tmp %>%
        ggplot() +
        geom_point(aes(
          x = plot_val,
          y = plot_val_compare,
          color = as.factor(get(color_val)), 
          text = paste0(
            "\nDataset: ", dataset,
            "\nLocation: ", location_name,
            "\nAge: ", age_group_years_start,
            "\nYear: ", year_id,
            "\nModel val: ", myround(plot_val),
            "\nCompare model val: ", myround(plot_val_compare),
            "\nAbs diff: ", myround(abs(plot_val - plot_val_compare)))),
          alpha = .8, size = 2, pch = c(19)) ## "pch" speeds things up a bit
    }else{
      p <- tmp %>%
        ggplot() +
        geom_point(
          size =3,
          aes( ## MUCH faster for MANY overlapping points, but doesn't work with plotly
            x = plot_val,
            y = plot_val_compare,
            color = as.factor(get(color_val)) ),
          alpha = .8) 
    }
    
    p <- p +
      geom_abline(intercept = 0, slope = 1) +
      xlab(paste0("Model ",lab_value)) +
      ylab(paste0("Compare model ",lab_value)) +
      labs(color = color_val)+
      theme_bw(base_size = 20) +
      theme(legend.position = "top")
    
    if (isolate(input$facet_by_loc) ){
      p <- p + facet_wrap(~location_name, ncol = 5)
    }
    
    
    if(isolate(input$comparemv_shape) == "Square"){
      t <- max(max(tmp$plot_val, na.rm = T), max(tmp$plot_val_compare, na.rm = T))
      p<-p+coord_equal(xlim=c(0,t),ylim=c(0,t))
    }else if(isolate(input$comparemv_shape) == "Log space"){
      p <- p +
        scale_x_continuous(trans = "log", labels = myround) +
        scale_y_continuous(trans = "log", labels = myround)
    }
    
    ## save plot (to prep for download handler)
    WH <- get_dims(isolate(input), units = "inch")
    ggsave(
      filename = "compare_mv.pdf", 
      width = WH$W,
      height = WH$H,
      units = "in",
      plot = p + labs(subtitle = get_label(reactiveValuesToList(input))) + theme(plot.subtitle = element_text(size = 12)),
      limitsize = FALSE
    )
    
    ## show plot
    if(interactive){
      ggplotly(p, tooltip = "text", width = plot_sizes$compare_mv$px[1], height = plot_sizes$compare_mv$px[2])
    }else{
      p
    }
    
  })
}

mycvscatter <- function(input, cv_data, interactive = FALSE){
  
  withProgress(message = 'Making plot', min = 0, max = 100, value = 0, {
    
    if(isolate(input$model) == "") stop("Please select a model")
    if(isolate(input$geo) != 'county') stop('Please select geo = county')
    setProgress(33, "Reading data")
    tmp <- isolate(copy(cv_data))
    if(nrow(tmp) == 0) stop("No data here")
    setProgress(66, "Plotting")
    ## plot
    p <- tmp %>%
      ggplot() +
      geom_point(aes(
        x = cv_raw,
        y = cv_mod,
        text = paste0(
          "\nLocation: ", state,
          "\nData mean: ", myround(mean_raw),
          "\nModel mean: ", myround(mean_mod),
          "\n#Obs: ", n_obs,
          "\ncv_mod/cv_raw: ", myround(cv_mod/cv_raw))),
        alpha = .8, size = 2, pch = c(19))+
      geom_abline(intercept = 0, slope = 1) +
      xlab("Raw data coeff var") +
      ylab("Model mean coeff var") +
      theme_bw(base_size = 20) +
      theme(legend.position = "right")+
      labs(title = paste0('State variation in data and modeled means: ',input$metric, ', ', input$toc,', ', input$year))
    
    ggplotly(p, tooltip = "text", width = plot_sizes$popout$px[1], height = plot_sizes$popout$px[2])
    
  })
  
}