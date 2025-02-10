library(data.table)

BLG_PRVDR_TYPE_CD <- list(
'01' = 'P', #Physician 
'02' = 'P', #Speech Language Pathologist 
'03' = 'F', #Oral Surgery (Dentist only) 
'04' = 'F', #Cardiac Rehabilitation and Intensive Cardiac Rehabilitation 
'05' = 'P', # Anesthesiology Assistant 
'06' = 'F', #Chiropractic 
'07' = 'F', #Optometry 
'08' = 'P', #Certified Nurse Midwife 
'09' = 'P', #Certified Registered Nurse Anesthetist (CRNA) 
'10' = 'F', #Mammography Center 
'11' = 'F', #Independent Diagnostic Testing Facility (IDTF) 
'12' = 'F', #Podiatry 
'13' = 'F', #Ambulatory Surgical Center 
'14' = 'P', #Nurse Practitioner 
'15' = 'F', #Medical Supply Company with Orthotist 
'16' = 'F', #Medical Supply Company with Prosthetist 
'17' = 'F', #Medical Supply Company with Orthotist-Prosthetist 
'18' = 'F', #Other Medical Supply Company 
'19' = 'P', #Individual Certified Orthotist
'20' = 'P', #Individual Certified Prosthetist 
'21' = 'P', #Individual Certified Prosthetist-Orthotist 
'22' = 'F', #Medical Supply Company with Pharmacist
'23' = 'F', #Ambulance Service Provider 
'24' = 'F', #Public Health or Welfare Agency 
'25' = 'F', #Voluntary Health or Charitable Agency 
'26' = 'P', #Psychologist, Clinical 
'27' = 'F', #Portable X-Ray Supplier 
'28' = 'P', #Audiologist 
'29' = 'P', #Physical Therapist in Private Practice 
'30' = 'P', #Occupational Therapist in Private Practice 
'31' = 'F', #Clinical Laboratory 
'32' = 'F', #Clinic or Group Practice 
'33' = 'P', #Registered Dietitian or Nutrition Professional 
'34' = 'F', #Mass Immunizer Roster Biller 
'35' = 'F', #Radiation Therapy Center 
'36' = 'F', #Slide Preparation Facility 
'37' = 'P', #Licensed Clinical Social Worker 
'38' = 'P', #Certified Clinical Nurse Specialist 
'39' = 'F', #Advance Diagnostic Imaging 
'40' = 'P', #Optician 
'41' = 'P', #Physician Assistant 
'42' = 'F', #Hospital-General 
'43' = 'F', #Skilled Nursing Facility 
'44' = 'F', #Intermediate Care Nursing Facility 
'45' = 'F', #Other Nursing Facility 
'46' = 'F', #Home Health Agency 
'47' = 'F', #Pharmacy 
'48' = 'F', #Medical Supply Company with Respiratory Therapist
'49' = 'F', #Department Store 
'50' = 'F', #Grocery Store 
'51' = 'F', #Indian Health Service Facility 
'52' = 'F', #Oxygen supplier 
'53' = 'P', #Pedorthic personnel 
'54' = 'F', #Medical supply company with pedorthic personnel 
'55' = 'F', #Rehabilitation Agency 
'56' = 'P', #Ocularist 
'57' = 'F' #All Other 
)


BLG_PRVDR_TYPE_CD_dt <- data.table(cbind(BLG_PRVDR_TYPE_CD), keep.rownames = TRUE)

colnames(BLG_PRVDR_TYPE_CD_dt) <- c("BLG_PRVDR_TYPE_CD", "fac_prof_ind")


