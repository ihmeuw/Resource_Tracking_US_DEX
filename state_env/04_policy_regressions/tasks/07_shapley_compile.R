rm(list = ls())
pacman::p_load(tidyverse, data.table)

dir <- "FILEPATH"

draws <- rbindlist(lapply(Sys.glob(paste0(dir,"shapley_draws/*")), function(f){
  load(f)
  return(decomp)
}))

fwrite(draws,"FILEPATH/shapley_decomp_draws.csv")

decomp <- draws[,.(contribution = mean(value), lower = quantile(value, 0.025), upper = quantile(value, 0.975)), by="factor"]
fwrite(decomp,"FILEPATH/shapley_decomp.csv")
