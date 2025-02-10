# -------------------------------------------------------
#  FIT TMB MODELS (using templates)
#
#   Authors: Haley Lescinsky & Azalea Thomson
#   Referenced code written by: Laura Dwyer-Lindgren   
#
# -------------------------------------------------------

suppressPackageStartupMessages(library(lbd.loader, lib.loc = "FILEPATH"))
suppressPackageStartupMessages(library(lsae, lib.loc = lbd.loader::pkg_loc("lsae")))
suppressPackageStartupMessages(library(fishmethods, lib.loc = "FILEPATH"))

## Load libraries from singularity image
load_pkg <- c('terra', 'sf', 'arrow', 'RColorBrewer','splines', 'stringr', 'data.table', 'dplyr','TMB','optimx','matrixStats', 'ggplot2', 'Matrix', 'actuar')
suppressPackageStartupMessages(invisible(lapply(load_pkg, library, character.only = TRUE)))

'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')

t_start <- Sys.time()
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile()) 
setwd(here)
source(paste0(here, "/functions.R"))

set.seed(98121)


# ## Get settings ------------------------------------------------------------------------------------
t0 <- Sys.time()



if(interactive()){
  # ARGUMENTS FOR RUNNING INTERACTIVELY
  
  days_metrics <- F
  max_model_year <- 2019
  include_race <- F
  extrapolate_ages <- T
  if (include_race == T){
    full_age_span <- T 
  }else{
    full_age_span <- F 
  }
  run_id <- '81'
  model_type <- 'no_loc'
  geog <- "national"
  cause <- "diabetes_typ2"
  care <- "HH"
  met <-  "encounters_per_person"
  pri_pay <- "mdcr"
  pay <- 'na'
  sex <- "1"
  
  ## These args don't often change but double check if you need to change them!
  list2env(pull_interactive_args(run_id, met), globalenv())
  if (include_race == T){
    param_map <- fread(paste0("/FILEPATH/run_",run_id,"/params_for_model_race.csv"))
    param_template <- param_map[acause == cause & toc == care & metric == met & pri_payer == pri_pay & payer == pay & sex_id == sex & geo == geog][1]
  }else{
    param_map <- fread(paste0("/FILEPATH/run_",run_id,"/params_for_model.csv"))
    param_template <- param_map[acause == cause & toc == care & metric == met & pri_payer == pri_pay & payer == pay & sex_id == sex & geo == geog]
  }

  if (met %like% 'spend'){
    mad_cutoff <- 4
  }else{
    mad_cutoff <- 5
  }
  
  model_version <- paste0('test_',model_type)
  outdir <- paste0("/FILEPATH/", Sys.getenv('USER'), "_", model_type, "_run", run_id, "/")
  if(!dir.exists(outdir)){dir.create(outdir)}
  if(!dir.exists(paste0(outdir,"/diagnostics/"))){dir.create(paste0(outdir,"/diagnostics/"), recursive = T)}
  if(!dir.exists(paste0(outdir,"/convergence/"))){dir.create(paste0(outdir, "/convergence/"))}
  
  
}else{
  # PULLING IN ARGUMENTS FROM LAUNCHER 
  
  args <- commandArgs(trailingOnly = TRUE)
  message(args)
  outdir <- args[1]
  job_path <- args[2]
  run_id <- args[3] 
  MEMSET <- args[4]
  
  param_template <- fread(job_path)[mem_set == MEMSET & task_id == Sys.getenv("SLURM_ARRAY_TASK_ID")]
  setcolorder(param_template, c("acause", "toc", "metric", "pri_payer", "payer", "sex_id", "geo"))

  
  list2env(pull_parallelized_args(param_template), globalenv())
  
  if (use_profiled_params == T){
    start_val <- param_template$starting_intercept
    if (is.na(start_val)){
      start_val <- c()
    }
  }
  
  print(paste0('Outdir: ', outdir))
  
  model_version <- gsub("/", "", gsub("^.*model_version_", "", outdir))
}



##----------------------------------------------------------------------
## Set conditional arguments
##----------------------------------------------------------------------  

## If modeling days metrics we want to imitate how the other metrics are treated
## so reset the metrics here and we will set them back at the end
if (days_metrics == T){
  orig_met <- met
  if (met == 'spend_per_day'){ met <- 'spend_per_encounter'}
  if (met == 'days_per_encounter'){ met <- 'spend_per_encounter'} 
  message('Set orig metric ',orig_met,' to be ',met)
}else{
  orig_met <- met
}
param_template$metric <- met
param_template$orig_metric <- orig_met

## Set outlier group variables - geo dependent
if (geog == 'national'){
  outlier_group_vars <- c('year_id', 'age_group_years_start')
}else{
  outlier_group_vars <- c('dataset', 'age_group_years_start')
}
if (model_type == 'intercept'){
  outlier_group_vars <- c('dataset')
}

## Possible covariates; just ICD9 now
poss_covars <- 'icd9'

## Get the TMB matching script
match_script_dict <- get_match_script_dict(include_race = include_race)
orig_matching_script <- match_script_dict[(geo %like% geog | geo %like% 'any') & mod_type == model_type & match_script != 'glm']$match_script

## Whether to save plots (only if interactive)
save_plots <- ifelse(interactive(), T, F)
note <- paste0("", drop_outliers, "_drop_outliers") # plots will have _wFE if FE included

## Set starting intercept; lower better for encounters
if (met == 'encounters_per_person'){
  max_starting_intercept <- 1
  distribution <- 'poisson'
}

if (met == 'spend_per_encounter'){
  max_starting_intercept <- 6
  distribution <- 'gaussian'
}

## Keep track of params
param_template <- param_template[,.(acause,toc,metric,orig_metric,pri_payer,payer,sex_id,geo)]
params <- paste0(param_template, collapse = "_")

print(paste0('params:',params))
message(paste0('params:',params))

##----------------------------------------------------------------------
## Format data for TMB
##----------------------------------------------------------------------  
cat("\n\n***** Format data & load adjacency matricies \n"); flush.console()


a <- 0
print(a)
print(outlier_group_vars)
quant_upper <- 1 - a
quant_lower <- 0 + a


# Data - pulls data and returns it with merged covariates, int, icd9, a65
list2env(prep_data(run_id, param_template,sim_data = sim_data, orig_matching_script, include_race), globalenv())
nrow(data)

if (include_race == T){
  data <- data[!(race_cd %in% c('OTH','MULT','UNK'))]
  if ( any(unique(data$race_cd) %ni% c('BLCK','WHT','API','AIAN','HISP')) ){
    stop('You have incorrect race/ethnicity values in your data')
  }
}

if (nrow(data)>0){
  
  ## Drop specific outliers
  data <- manual_outlier(data, param_template)
  
  ## Returns 2 plots of raw data with uncertainty if spend metric, and data with outlier column based on MAD
  raw_data <- outlier_data(data, param_template, mad_cutoff, quant_upper, quant_lower, outlier_group_vars, set_se_floor = set_se_floor, normalize_mad = normalize_mad)
  data <- raw_data[[1]]
  raw_data_plot1 <- raw_data[[2]]
  raw_data_plot2 <- raw_data[[3]]
  

  
  if (nrow(data[outlier==0])>0){
    # Index data (location_index, year_index, age_index, race_index)
    list2env(index_data(data, param_template, run_id, include_race), globalenv())
    
    # Adjacency Matricies for the LCAR variables - returns 3 matrices: graph_a, graph_j, and graph_t
    list2env(prep_matricies(data, geog), globalenv())
    # Data counts  - returns 3 counts: num_a, num_j, and num_t
    list2env(count_variables(graph_a,graph_j,graph_t), globalenv()) 
    
    if (include_race == T){
      data <- data[order(age_index,location_index,year_index,race_index)]
    }else{
      data <- data[order(age_index,location_index,year_index)] 
    }
    
    if (extrapolate_ages == T ){
      other_metric_ages <- pull_other_metric_ages(run_id, param_template, include_race, full_age_span, data)
      additional_ages <- setdiff(other_metric_ages, c(min(ages):max(ages)))
      if (length(additional_ages) == 0){
        additional_ages <- NA
      }
    }else{
      additional_ages <- NA
    }

  }
  
  ## Returns matching script, updated param_template, and interactions for model (or writes out param template if insufficient data)
  list2env(assess_match_script(data, orig_matching_script, geog, years, ages, param_template, model_type), globalenv())
  
  if( is.null(restriction) ){
    ##----------------------------------------------------------------------
    ## Assign covariates and TMB variables
    ##----------------------------------------------------------------------
    
    covars <- c('int')
    # if intercept model, remove other covars
    if (model_type == 'intercept'){
      poss_covars <- c()
    }

    print(paste0("using covariates: ", paste0(covars, collapse = ",")))
    
    if (param_template$metric == 'encounters_per_person'){ ##poisson
      ii <- which(data$n_people > 0)
      if(drop_outliers == T){
        ii <- intersect(ii, which(data$outlier != 1))
      }
      Y <- data$n_encounters[ii]
      N <- data$n_people[ii]
    }else{ ##gaussian
      ii <- which(round(data$se)>0)
      if(drop_outliers == T){
        ii <- intersect(ii, which(data$outlier != 1))
      }
      Y <- data$raw_val[ii]
      N <- data$n_encounters[ii]
    }
    
    
    data[, source_index:= as.integer(factor(dataset)) - 1L]
    gold_standard <- unique(data[reference_dataset == 1]$source_index)
    num_d <- length(unique(data$source_index))
    
    num_r <- length(unique(data[outlier==0]$race_index))
    
    if (matching_script != 'glm'){ 
      ##----------------------------------------------------------------------
      ## Initialize data
      ##----------------------------------------------------------------------
      ## Get the names of the random effects (used in prediction)
      list2env(parse_re_names(matching_script), globalenv())
      if (num_res!=0){
        for (i in 1:num_res){
          if ( !(NA %in% re_name_list[[i]]) ){
            assign(paste0('re',i,'_dim'),re_name_list[[i]])
          }
        }
      }
  
      
      initialize_tmb <- function(covars, starting_intercept){
        
        
        tmb_data <- list(
          link = distribution,
          Y = Y,
          N = N,
          SE = data$se[ii],
          X = as.matrix(data[ii, covars, with=F]),
          A = data$age_index[ii],
          J = data$location_index[ii],
          T = data$year_index[ii],
          D = data$source_index[ii],
          R = data$race_index[ii],
          graph_a = graph_a,
          graph_j = graph_j,
          graph_t = graph_t,
          num_d = num_d,
          num_r = num_r,
          num_j = num_j,
          rho_t_alpha = rho_t_alpha,
          rho_t_beta = rho_t_beta,
          rho_a_alpha = rho_a_alpha,
          rho_a_beta = rho_a_beta,
          rho_j_alpha = rho_j_alpha,
          rho_j_beta = rho_j_beta,
          sigma_alpha = sigma_alpha, 
          sigma_beta = sigma_beta)
       
        
        # remove any values that are NULL
        any_null <- sum(sapply(tmb_data, is.null))
        if(any_null > 0 ){
          tmb_data <- tmb_data[-which(sapply(tmb_data, is.null))]
        }

        
        ## Set parameters for TMB with starting values-------------------------------------------------------------------------
        cat("\n\n***** Set parameters\n"); flush.console()
        
        tmb_par <- list(
          B = c(starting_intercept, rep(0, length(covars)-1)) # intercept & covariate effects
        )
        
        re_prior_list <- parse_re_priors(matching_script, re_name_list)
        
        re_list <- parse_res(matching_script)
        
        tmb_par <- append(tmb_par, re_prior_list)
        tmb_par <- append(tmb_par, re_list)
        
        message("Done initializing data")
        return(list(tmb_data,tmb_par))
      }
  
      
      ##----------------------------------------------------------------------
      ## Fit model
      ##----------------------------------------------------------------------
      intercepts <- get_intercepts(use_profiled_params, met)
      
      ## 1. Loop through intercepts
      for(starting_intercept in intercepts){
        
        t0 <- Sys.time()
        tmb_inputs <- initialize_tmb(covars, starting_intercept)
        tmb_data <- tmb_inputs[[1]]
        tmb_par <- tmb_inputs[[2]]
        model_fitted <- fit_model(covars, data, tmb_data, tmb_par, starting_intercept, include_race)
        
        
        if(convergence_status == 1){
          print(paste0(starting_intercept, " didn't converge; trying next"))
          next
          
        }else{
          break
        }
      }
      
      ## 2. Loop through removing FEs
      if(convergence_status == 1){
        message("Model did not converge up to starting value ", starting_intercept,", now removing FEs 1 by 1")
        message(covars)
        poss_covars <- covars[!(covars == 'int')]
        if(length(poss_covars)>0){
          for (i in poss_covars){
            message('Removing covariate ',i, ' and trying again')
            covars <- covars[!covars ==i]
            
            tmb_inputs <- initialize_tmb(covars, starting_intercept)
            tmb_data <- tmb_inputs[[1]]
            tmb_par <- tmb_inputs[[2]]
            model_fitted <- fit_model(covars, data, tmb_data, tmb_par, starting_intercept, include_race)
            
            if(convergence_status == 1){
              print(paste0("Covars: ",covars," didn't converge; trying next"))
              next
              
            }else{
              break
            }
          }
          
        }
      }
    }else{

      intercepts <- 0
      message('Using glm')
      if (param_template$n_zeros == nrow(data[outlier==0])){
        message('Only zeros')
        intercept <- 0
        convergence_status <- 0
        convergence_note <- 'Zeros data only'
        int <- rnorm(n=num_draws,mean= intercept, sd=0)
      }else{
        glm_fit <- fit_glm_model(data, distribution,num_draws)
        convergence_status <- glm_fit[[1]]  
        convergence_note <- glm_fit[[2]]  
        int <- glm_fit[[3]]  
      }

    }

    
    ## 3. If an intercept model and TMB didn't converge, fit a glm
    if ( param_template$model_type == 'intercept' & convergence_status == 1 ){
      message('TMB intercept model did not converge/did not run; running a glm')
      glm_fit <- fit_glm_model(data, distribution,num_draws)
      convergence_status <- glm_fit[[1]]  
      convergence_note <- glm_fit[[2]]  
      int <- glm_fit[[3]]  
      matching_script <- 'glm'
      param_template[, match_script:='glm']
    }
    ##----------------------------------------------------------------------
    ## Predictions
    ##----------------------------------------------------------------------
    if(convergence_status != 1){
      
      if ( matching_script!= 'glm' ){
        
        fe_report <- summary(model_fitted$sdrep,select="fixed") %>% as.data.table()
        fe_report <- fe_report[1:length(covars)][,low:=Estimate-(1.96*`Std. Error`)][,hi:=Estimate+(1.96*`Std. Error`)]
        fe_report[,signif:=ifelse(0>low & 0<hi,'not signif','signif')]
        fe_report[,Estimate:=paste0(round(Estimate, digits = 2),' (',signif,')')]
        
        
        ## --------------------
        ## Generate draws
        ## --------------------
        
        
        cat("\n\n***** forming predictions \n"); flush.console()
        
        draws <- do_prediction()
        
        draws <- icd_shift_draws(draws, data)
        
        draws <- adjust_zeros(draws,data, include_race,geog)
        
        draws <- extrapolate_draws_for_age(draws, index_key, additional_ages)
        
      }else{
        
        draws <- generate_glm_draws(int)
        
        draws <- icd_shift_draws(draws, data)
        
        draws <- adjust_zeros(draws,data,include_race,geog)
        
        draws <- extrapolate_draws_for_age(draws, index_key, additional_ages)
        
      }
      
      if (is.null(draws)| bad_draws != ''){
        pdf(paste0(outdir, "/diagnostics/model_fit_", params, "_", note, "_noconvergence.pdf"), height = 8, width = 11)
        print(raw_data_plot1)
        print(raw_data_plot2)
        dev.off()
        message("Model did not converge")
        
        param_template_w_convergence <- copy(param_template)
        param_template_w_convergence[, metric:=orig_met][,orig_metric:=NULL]
        param_template_w_convergence[, `:=` (convergence = 1,
                                             convergence_note = paste0('bad draws - ', bad_draws),
                                             starting_intercept = NA,
                                             first_int_tried = intercepts[1],
                                             percent_final_draws_fixed = NA,
                                             rho_t = NA,
                                             rho_a = NA)]
        write_dataset(param_template_w_convergence,
                      existing_data_behavior = c("overwrite"),
                      path = paste0(outdir, "/convergence/"),
                      basename_template = paste0("acause_",params, "_part{i}.parquet"))
        message('Extreme draws')
        
      }else{
        
        # if draws are okay, keep going
        
        if ( matching_script!= 'glm' ){
          x <- model_fitted$opt$par[grepl("logit_rho.*t$", names(model_fitted$opt$par))]
          rho_t <- exp(x)/(1+exp(x))
          x <- model_fitted$opt$par[grepl("logit_rho.*a$", names(model_fitted$opt$par))]
          rho_a <- exp(x)/(1+exp(x))
        }else{
          rho_t <- NA
          rho_a <- NA
          starting_intercept <- 0
          intercepts <- 0
          percent_final_draws_fixed <- 0
          fe_report <- NA
        }
        
        ## --------------------
        ## Visualize model fit
        ## --------------------
        draws[, ui:=(upper - lower) / mean]
        draws[, mask:= ifelse(ui > .5, .5, 0)]
        draws[, mask:= ifelse(ui > 1, 1, mask)]
        draws[, mask:= ifelse(ui > 1.6, 1.6, mask)]
        
        param_template_w_convergence <- copy(param_template)
        param_template_w_convergence[, metric:=orig_met][,orig_metric:=NULL]
        param_template_w_convergence[, `:=` (convergence = convergence_status,
                                             convergence_note = convergence_note,
                                             starting_intercept = starting_intercept,
                                             first_int_tried = intercepts[1],
                                             percent_final_draws_fixed = percent_final_draws_fixed,
                                             mean_est = mean(draws$mean),
                                             rho_t = rho_t,
                                             rho_a = rho_a,
                                             mask_0 = nrow(draws[mask==0])/nrow(draws),
                                             mask_50 = nrow(draws[mask==.5])/nrow(draws),
                                             mask_100plus = nrow(draws[mask>=1])/nrow(draws) )]
        write_dataset(param_template_w_convergence,
                      existing_data_behavior = c("overwrite"),
                      path = paste0(outdir, "/convergence/"),
                      basename_template = paste0("acause_",params, "_part{i}.parquet"))
        
        
        if (!dir.exists(paste0(outdir, "/diagnostics/"))){
          dir.create(paste0(outdir, "/diagnostics/"))
        }
        
        
        ## If running interactively, look at plots
        if( save_plots == T ){
          library(gridExtra)
          library(grid)
          plot_list <- visualize_fit(data, draws, ages, years, outdir, param_template, note, fe_report, drop_outliers)
          
          
          pdf(paste0(outdir, "/diagnostics/model_fit_", params, "_", note, ".pdf"), height = 8, width = 11)
          print(raw_data_plot1)
          print(raw_data_plot2)

          if(model_type!= 'intercept' & model_type != 'no_loc' ){
            print(plot_state_res(location_re_effects))
          }
          for ( i in plot_list){
            print(i)
          }
          grid.newpage()
          grid.arrange(tableGrob(param_template_w_convergence[,1:10], theme = ttheme_minimal(base_size = 5)),
                       tableGrob(param_template_w_convergence[,11:20],theme = ttheme_minimal(base_size = 5)),
                       tableGrob(param_template_w_convergence[,21:length(names(param_template))],theme = ttheme_minimal(base_size = 5)) ,nrow=3)
          dev.off()
          

          print(Sys.time() - t_start)
        }
        
        ## --------------------
        ## Save data
        ## --------------------
        
        if(!interactive()){
          
          param_template[,metric:=orig_metric][,orig_metric:=NULL]
          
          prep_shiny_data(data, draws, ages, years, param_template, run_id, model_version)
          
          save_draws(geog, draws, param_template, index_key, outdir)
        }
        
      }
      
      
    }else{ 
      
      
      pdf(paste0(outdir, "/diagnostics/model_fit_", params, "_", note, "_noconvergence.pdf"), height = 8, width = 11)
      print(raw_data_plot1)
      print(raw_data_plot2)
      dev.off()
      
      param_template_w_convergence <- copy(param_template)
      param_template_w_convergence[, metric:=orig_met][,orig_metric:=NULL]
      param_template_w_convergence[, `:=` (convergence = 1,
                                           convergence_note = convergence_note,
                                           starting_intercept = NA,
                                           first_int_tried = NA,
                                           percent_final_draws_fixed = NA,
                                           rho_t = NA,
                                           rho_a = NA)]
      
      write_dataset(param_template_w_convergence,
                    existing_data_behavior = c("overwrite"),
                    path = paste0(outdir, "/convergence/"),
                    basename_template = paste0("acause_",params, "_part{i}.parquet"))
      
      message("Final model did not converge, exiting..")
      print("Final model did not converge, exiting..")
  
    }
  
  }
}else{
  message('No data')
  param_template_w_convergence <- copy(param_template)
  param_template_w_convergence[, metric:=orig_met][,orig_metric:=NULL]
  param_template_w_convergence[, `:=` (convergence = 1,
                                       convergence_note = 'no_data',
                                       restriction = 'no_data',
                                       starting_intercept = NA,
                                       rho_t = NA,
                                       rho_a = NA)]
}


print(paste0('Done'))

print(Sys.time() - t0)
