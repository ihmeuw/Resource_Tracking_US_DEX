## Recoding Carrier claim values to TOC for our data
# This script shouldn't need to be run often as long as type of care definitions aren't changing.
# Variables of interest are coded as lists with our toc as values, so that encoding is easy to read.
# Once lists are created, they're combined into data.tables for merging.

library(data.table)

# NCH_CLM_TYPE_CD - type of claim that was submitted
nch_clm_type_cd <- list(
  "10" = "HH",
  "20" = "NF",
  "30" = "NF",
  "40" = "OP",
  "50" = "HOSPICE",
  "60" = "IP",
  "61" = "IP",
  "71" = "OTH",
  "72" = "OTH",
  "81" = "OTH",
  "82" = "OTH"
)
clm_type_dt <- data.table(cbind(nch_clm_type_cd), keep.rownames = TRUE)
colnames(clm_type_dt) <- c("nch_clm_type_cd", "toc")
fwrite(clm_type_dt, "FILEPATH/nch_clm_type_cd_dictionary.csv")

# LINE_PLACE_OF_SRVC_CD
line_cms_place_srvc_cd <- list(
  "01" = "RX", # Pharmacy **
  "02" = "AM", # Telehealth Provided Other than in Patient’s Home
  "03" = "AM", # School
  "04" = "AM", # Homeless Shelter
  "05" = "AM", # Indian Health Service Free-standing Facility
  "06" = "AM", # Indian Health Service Provider-based Facility
  "07" = "AM", # Tribal 638 Free-standing Facility
  "08" = "AM", # Tribal 638 Provider-based Facility
  "09" = "OTH", # Prison/Correctional Facility
  "10" = "AM", # Telehealth Provided in Patient’s Home
  "11" = "AM", # Office
  "12" = "HH", # Home
  "13" = "NF", # Assisted Living Facility
  "14" = "NF", # Group Home *
  "15" = "AM", # Mobile Unit
  "16" = "OTH", # Temporary Lodging
  "17" = "AM", # Walk-in Retail Health Clinic
  "18" = "AM", # Place of Employment- Worksite
  "19" = "AM", # Off Campus-Outpatient Hospital
  "20" = "AM", # Urgent Care Facility
  "21" = "IP", # Inpatient Hospital
  "22" = "AM", # On Campus-Outpatient Hospital
  "23" = "ED", # Emergency Room – Hospital
  "24" = "AM", # Ambulatory Surgical Center
  "25" = "AM", # Birthing Center
  "26" = "OTH", # Military Treatment Facility
  "27" = "OTH", # Unassigned
  "28" = "OTH", # Unassigned
  "29" = "OTH", # Unassigned
  "30" = "OTH", # Unassigned
  "31" = "NF", # Skilled Nursing Facility
  "32" = "NF", # Nursing Facility
  "33" = "NF", # Custodial Care Facility
  "34" = "NF", # Hospice
  "35" = "OTH", # Unassigned
  "36" = "OTH", # Unassigned
  "37" = "OTH", # Unassigned
  "38" = "OTH", # Unassigned
  "39" = "OTH", # Unassigned
  "40" = "OTH", # Unassigned
  "41" = "OTH", # Ambulance - Land
  "42" = "OTH", # Ambulance – Air or Water
  "43" = "OTH", # Unassigned
  "44" = "OTH", # Unassigned
  "45" = "OTH", # Unassigned
  "46" = "OTH", # Unassigned
  "47" = "OTH", # Unassigned
  "48" = "OTH", # Unassigned
  "49" = "AM", # Independent Clinic
  "50" = "AM", # Federally Qualified Health Center
  "51" = "IP", # Inpatient Psychiatric Facility
  "52" = "AM", # Psychiatric Facility-Partial Hospitalization
  "53" = "AM", # Community Mental Health Center
  "54" = "NF", # Intermediate Care Facility/ Individuals with Intellectual Disabilities
  "55" = "NF", # Residential Substance Abuse Treatment Facility
  "56" = "IP", # Psychiatric Residential Treatment Center
  "57" = "AM", # Non-residential Substance Abuse Treatment Facility
  "58" = "AM", # Non-residential Opioid Treatment Facility
  "59" = "OTH", # Unassigned
  "60" = "AM", # Mass Immunization Center
  "61" = "NF", # Comprehensive Inpatient Rehabilitation Facility
  "62" = "AM", # Comprehensive Outpatient Rehabilitation Facility
  "63" = "OTH", # Unassigned
  "64" = "OTH", # Unassigned
  "65" = "AM", # End-Stage Renal Disease Treatment Facility
  "66" = "OTH", # Unassigned
  "67" = "OTH", # Unassigned
  "68" = "OTH", # Unassigned
  "69" = "OTH", # Unassigned
  "70" = "OTH", # Unassigned
  "71" = "AM", # Public Health Clinic
  "72" = "AM", # Rural Health Clinic
  "73" = "OTH", # Unassigned
  "74" = "OTH", # Unassigned
  "75" = "OTH", # Unassigned
  "76" = "OTH", # Unassigned
  "77" = "OTH", # Unassigned
  "78" = "OTH", # Unassigned
  "79" = "OTH", # Unassigned
  "80" = "OTH", # Unassigned
  "81" = "AM", # Independent Laboratory
  "82" = "OTH", # Unassigned
  "83" = "OTH", # Unassigned
  "84" = "OTH", # Unassigned
  "85" = "OTH", # Unassigned
  "86" = "OTH", # Unassigned
  "87" = "OTH", # Unassigned
  "88" = "OTH", # Unassigned
  "89" = "OTH", # Unassigned
  "90" = "OTH", # Unassigned
  "91" = "OTH", # Unassigned
  "92" = "OTH", # Unassigned
  "93" = "OTH", # Unassigned
  "94" = "OTH", # Unassigned
  "95" = "OTH", # Unassigned
  "96" = "OTH", # Unassigned
  "97" = "OTH", # Unassigned
  "98" = "OTH", # Unassigned
  "99" = "OTH" # Other Place of Service
)
place_codes_dt <- data.table(cbind(line_cms_place_srvc_cd), keep.rownames = TRUE)
colnames(place_codes_dt) <- c("line_cms_place_srvc_cd", "toc")
fwrite(place_codes_dt, "FILEPATH/line_cms_place_srvc_cd_dictionary.csv")


# LINE_PLACE_OF_SRVC_NAME - place of service code 
line_cms_place_srvc_name <- list(
  "01" = "Pharmacy **",
  "02" = "Telehealth Provided Other than in Patient’s Home",
  "03" = "School",
  "04" = "Homeless Shelter",
  "05" = "Indian Health Service Free-standing Facility",
  "06" = "Indian Health Service Provider-based Facility",
  "07" = "Tribal 638 Free-standing Facility",
  "08" = "Tribal 638 Provider-based Facility",
  "09" = "Prison/Correctional Facility",
  "10" = "Telehealth Provided in Patient’s Home",
  "11" = "Office",
  "12" = "Home",
  "13" = "Assisted Living Facility",
  "14" = "Group Home *",
  "15" = "Mobile Unit",
  "16" = "Temporary Lodging",
  "17" = "Walk-in Retail Health Clinic",
  "18" = "Place of Employment- Worksite",
  "19" = "Off Campus-Outpatient Hospital",
  "20" = "Urgent Care Facility",
  "21" = "Inpatient Hospital",
  "22" = "On Campus-Outpatient Hospital",
  "23" = "Emergency Room – Hospital",
  "24" = "Ambulatory Surgical Center",
  "25" = "Birthing Center",
  "26" = "Military Treatment Facility",
  "27" = "Unassigned",
  "28" = "Unassigned",
  "29" = "Unassigned",
  "30" = "Unassigned",
  "31" = "Skilled Nursing Facility",
  "32" = "Nursing Facility",
  "33" = "Custodial Care Facility",
  "34" = "Hospice",
  "35" = "Unassigned",
  "36" = "Unassigned",
  "37" = "Unassigned",
  "38" = "Unassigned",
  "39" = "Unassigned",
  "40" = "Unassigned",
  "41" = "Ambulance - Land",
  "42" = "Ambulance – Air or Water",
  "43" = "Unassigned",
  "44" = "Unassigned",
  "45" = "Unassigned",
  "46" = "Unassigned",
  "47" = "Unassigned",
  "48" = "Unassigned",
  "49" = "Independent Clinic",
  "50" = "Federally Qualified Health Center",
  "51" = "Inpatient Psychiatric Facility",
  "52" = "Psychiatric Facility-Partial Hospitalization",
  "53" = "Community Mental Health Center",
  "54" = "Intermediate Care Facility/ Individuals with Intellectual Disabilities",
  "55" = "Residential Substance Abuse Treatment Facility",
  "56" = "Psychiatric Residential Treatment Center",
  "57" = "Non-residential Substance Abuse Treatment Facility",
  "58" = "Non-residential Opioid Treatment Facility",
  "59" = "Unassigned",
  "60" = "Mass Immunization Center",
  "61" = "Comprehensive Inpatient Rehabilitation Facility",
  "62" = "Comprehensive Outpatient Rehabilitation Facility",
  "63" = "Unassigned",
  "64" = "Unassigned",
  "65" = "End-Stage Renal Disease Treatment Facility",
  "66" = "Unassigned",
  "67" = "Unassigned",
  "68" = "Unassigned",
  "69" = "Unassigned",
  "70" = "Unassigned",
  "71" = "Public Health Clinic",
  "72" = "Rural Health Clinic",
  "73" = "Unassigned",
  "74" = "Unassigned",
  "75" = "Unassigned",
  "76" = "Unassigned",
  "77" = "Unassigned",
  "78" = "Unassigned",
  "79" = "Unassigned",
  "80" = "Unassigned",
  "81" = "Independent Laboratory",
  "82" = "Unassigned",
  "83" = "Unassigned",
  "84" = "Unassigned",
  "85" = "Unassigned",
  "86" = "Unassigned",
  "87" = "Unassigned",
  "88" = "Unassigned",
  "89" = "Unassigned",
  "90" = "Unassigned",
  "91" = "Unassigned",
  "92" = "Unassigned",
  "93" = "Unassigned",
  "94" = "Unassigned",
  "95" = "Unassigned",
  "96" = "Unassigned",
  "97" = "Unassigned",
  "98" = "Unassigned",
  "99" = "Other Place of Service"
)
place_names_dt <- data.table(cbind(line_cms_place_srvc_name), keep.rownames = TRUE)
colnames(place_names_dt) <- c("line_cms_place_srvc_cd", "line_cms_place_srvc_name")
fwrite(place_names_dt, "FILEPATH/line_cms_place_srvc_name_dictionary.csv")
