# Wrapper for the main currency conversion function, with two main purposes:
# 1. standardize parameters across dex formatting, etc. (i.e. use 2020 USD, etc.)
# 2. allow for multiple columns to be converted at the same time
# Assumes you've got a year_id column already
dex_currency_conversion <- function(df, cols){
  source("FILEPATH/currency_conversion.R")
  setDT(df)
  df[, index := .I]
  static_cols <- setdiff(colnames(df), cols)
  df_static <- select(df, static_cols)
  df_converted <- lapply(cols, function(col){
    sub_df <- select(copy(df), col, year_id, index)
    sub_df[, year := year_id][, iso3 := "USA"]
    sub_df <- currency_conversion(data = sub_df, col.loc = "iso3", col.value = col,
                              currency = "usd", col.currency.year = "year", base.year = 2020)
    sub_df[, iso3 := NULL]
    return(sub_df)
  }) %>% reduce(left_join, by = c("index","year_id"))
  df_converted <- merge(df_static, df_converted, by=c("index","year_id"))
  df_converted[, index := NULL]
  return(df_converted)
}
