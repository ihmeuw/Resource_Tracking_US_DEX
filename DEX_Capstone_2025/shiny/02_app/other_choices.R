## choices
choice_race <- c(
  "All races" = "ALL",
  "Non-Hispanic, Black" = "BLCK",
  "Non-Hispanic, White" = "WHT",
  "Non-Hispanic American Indian or Alaska Native" = "AIAN",
  "Hispanic, Any race" = "HISP",
  "Non-Hispanic, Asian, Pacific Islander" = "API"
)

choice_toc <- c(
  "Inpatient" = "IP",
  "Emergency" = "ED",
  "Ambulatory" = "AM", 
  "Home health" = "HH", 
  "Pharma" = "RX",
  "Dental" = "DV",
  "Skilled nursing" = "NF"
)
choice_metric <- c(
  "Spend per encounter" = "spend_per_encounter",
  "Spend per day" = "spend_per_day",
  "Encounters per person" = "encounters_per_person",
  "Days per encounter" = "days_per_encounter"
)
choice_geo <- c("state", "county", "national")

choice_payer <- c(
  "MDCR" = "mdcr", 
  "MDCD" = "mdcd",
  "Private" = "priv", 
  "OOP" = "oop"
)
choice_pri_payer <- c(
  "MDCR" = "mdcr", 
  "MDCR (MC)" = "mdcr_mc", 
  "MDCD" = "mdcd", 
  "MDCD (MC)" = "mdcd_mc", 
  "Private" = "priv", 
  "OOP" = "oop",
  "MDCR (MC) MDCD" = "mdcr_mc_mdcd",
  "MDCR MDCD" = "mdcr_mdcd",
  "MDCR PRIV" = "mdcr_priv"
)
choice_sex_id <- c(1,2)
choice_year <- c("all", 2022:1999)
choice_age <- c("all", c(0, 1, 1:17*5))


choice_state <- c(
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID",
  "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
  "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
  "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
)
