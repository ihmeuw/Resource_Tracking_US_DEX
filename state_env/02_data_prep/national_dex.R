#############################################
# Get national DEX for age standardization
#############################################

# Setup ---------------
rm(list = ls())
pacman::p_load(data.table, haven, tidyverse, sparklyr)
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

# Setup for spark configuration 
conf <- list()
conf$`spark.driver.extraJavaOptions` <- '-Duser.timezone=GMT'
conf$`spark.executor.extraJavaOptions` <- '-Duser.timezone=GMT'
conf$`spark.sql.session.timeZone` <- 'UTC'
# Set based on threads available from qsub
conf$`sparklyr.cores.local` <- 38
conf$`sparklyr.shell.executor-memory` <- "10G"
conf$`sparklyr.shell.driver-memory` <- "50G"
conf$`spark.yarn.executor.memoryOverhead` <- "50G"
conf$`spark.driver.maxResultSize` <- "25G"
conf$`spark.serializer` <- "org.apache.spark.serializer.KryoSerializer"
conf$`spark.kryoserializer.buffer.max`= "2000MB"
conf$`spark.driver.extraJavaOptions` = "-XX:+UseG1GC"
# conf$`spark.sql.parquet.mergeSchema` <- 'TRUE'
sc <- spark_connect(master="local", config=conf)

age_groups <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
age_groups <- age_groups[!age_group_id %in% c(2,3,4,31,32,235)]
age_groups <- rbind(age_groups, 
                    data.table(age_group_id = c(28,160), age_group_years_start = c(0,85), age_group_years_end = c(1,125)), 
                    fill = TRUE)

# Format data -----------------------
ndex <- spark_read_parquet(sc, "FILEPATH", memory = FALSE)

ndex <- ndex %>% collect() %>% setDT()
ndex_backup <- copy(ndex)
ndex <- melt(ndex, id.vars = c("age","sex","year"), variable.name = "draw")
ndex[, draw := as.character(draw)]
ndex[, dep_var := str_remove_all(draw, "[:digit:]")][, draw := as.integer(str_remove_all(draw, "\\D"))]
ndex[, value := 1000000*value]

ndex[dep_var == "expend_scaled", dep_var := "agg"][dep_var == "expend_pub_scaled", dep_var := "pub"]
ndex[dep_var == "expend_pri_scaled", dep_var := "priv"][dep_var == "expend_oop_scaled", dep_var := "oop"]

# ndex <- dcast(ndex, age + sex + year + draw ~ dep_var, value.var = "value")
setnames(ndex, c("year","sex","value"), c("year_id","sex_id","ndex_tot_as"))

# Standardize columns and merge popualtion ------------------
population <- get_population(age_group_id = c(unique(age_groups$age_group_id),31,32,235), 
                             location_id = 102, 
                             year_id = unique(ndex$year_id), 
                             sex_id = c(1,2), 
                             gbd_round_id = 7,
                             decomp_step = "iterative",
                             status = 'best') 
population[age_group_id %in% c(31,32,235), `:=`(population = sum(population), age_group_id = 160), by=c("location_id","year_id")]
population <- unique(population)

ndex <- merge(ndex, age_groups[,.(age_group_id, age_group_years_start)], by.x = "age", by.y = "age_group_years_start")
ndex <- merge(ndex, population[,.(sex_id, age_group_id, year_id, population)], by=c("age_group_id","sex_id","year_id"))

# Currency conversion -----------------------------------
ndex[, yr := year_id][, year_id := 2016]
ndex <- dex_currency_conversion(ndex,c("ndex_tot_as"))
ndex[, year_id := yr][, yr := NULL]

# Create output with age/sex -----------------------------
# Year-age-sex output
ndex_as <- copy(ndex)
ndex_as[, ndex_pc_as := ndex_tot_as/population][, n_pop_as := population][, population := NULL]

# Create year specific age-aggregated output
ndex <- ndex[,.(ndex_tot = sum(ndex_tot_as), n_pop = sum(population)), by=c("year_id","draw","dep_var")]
ndex[, ndex_pc := ndex_tot/n_pop]

fwrite(ndex_as, "FILEPATH/ndex_by_age.csv")
fwrite(ndex, "FILEPATH/ndex_by_year.csv")
