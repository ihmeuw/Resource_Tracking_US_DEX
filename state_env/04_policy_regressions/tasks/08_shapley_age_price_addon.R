# Make a data set with the Shapley decomp amounts, *by draws*
# USERNAME
# 11-3

library(openxlsx)
library(data.table)
library(parallel)

# Step 1: Regress unadjusted health spending on age/sex adjusted spending to get variation attributable to age-sex

data <- fread("FILEPATH/agg_standardized.csv")

draws_agesex <- rbindlist(mclapply(1:1000, function(i){
  simple_lm <- lm(spending_pc ~ spending_pc_age_standardized, data = data[draw == i])
  X <- 1 - summary(simple_lm)$r.squared
  return(data.table(factor = "age_sex", scaled_value_age = X, draw = i))
}))

# Step 1.5: Regress age/sex adjusted spending on price standardized spending to get price attribution
draws_prices <- rbindlist(mclapply(1:1000, function(i){
  simple_lm <- lm(spending_pc_age_standardized ~ spending_pc_standardized, data = data[draw == i])
  X <- 1 - summary(simple_lm)$r.squared
  return(data.table(factor = "rpp_cpi", scaled_value_price = X, draw = i))
}))

# Step 2: Read in Shapley decomp output
decomp <- fread("FILEPATH/shapley_decomp_draws.csv")
others <- decomp[,.(factor = "other", value = 1 - sum(value)), by = "draw"]
decomp <- rbind(decomp, others)

# Step 3: Scale so they all add to 1
decomp <- merge(decomp, draws_agesex[,.(scaled_value_age, draw)], by = "draw")
decomp <- merge(decomp, draws_prices[,.(scaled_value_price, draw)], by = "draw")
decomp[, scaled_value := (1-scaled_value_age-scaled_value_price)*value]
decomp[, scaled_value_age := NULL][, scaled_value_price := NULL]

setnames(draws_agesex,"scaled_value_age","scaled_value")
setnames(draws_prices,"scaled_value_price","scaled_value")

decomp <- rbind(decomp, draws_agesex, fill = T)
decomp <- rbind(decomp, draws_prices, fill = T)
if(sum(decomp[draw==1]$scaled_value)!=1){
  stop("Values don't sum to 1")
}

# Step 4: combine categories
behave <- decomp[factor %in% c("PA_mets", "cig_pc_15"), .(factor = "behavioral", value = sum(value), scaled_value = sum(scaled_value)), by = "draw"]
decomp_adj <- rbind(decomp[! factor %in% c("PA_mets", "cig_pc_15")], behave)

# Step 5: aggregate across factors
decomp_agg <- decomp_adj[,.(scaled_value = mean(scaled_value), 
                            scaled_value_lower = quantile(scaled_value, 0.025), 
                            scaled_value_upper = quantile(scaled_value, 0.975)), by = c("factor")]

# Step 6: Clean up and save for donut and table
decomp_agg[, names := c("Population density", "Lag distributed income per person", "Year", "Unexplained", "Age/sex", "Regional price parity", "Behavioral health (smoking and physical activity)")]

write.csv(decomp_agg, "FILEPATH/decomp_adjustment_donut.csv", row.names = F)

make_UI <- function(mean, lower, upper, decimal_points=1){
  mean <- mean*100
  lower <- lower*100
  upper <- upper*100
  val <- paste0( round(mean, decimal_points), "% (", round(lower, decimal_points), "-", round(upper, decimal_points), ")")
  return(val)
}

decomp_agg[, table_val := make_UI(scaled_value, scaled_value_lower, scaled_value_upper), by = "factor"]
decomp_agg <- decomp_agg[order(names), .(names, table_val)]
colnames(decomp_agg) <- c("Factor", "Percent variation explained (95% UI)")

write.xlsx(decomp_agg, "FILEPATH/decomp_variation.xlsx")

