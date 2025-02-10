import numpy as np

#MDCR Race dictionary: 0=unknown, 1=white, 2=black, 3=other, 4=asian, 5 = hispanic, 6=north american native
race_dict_mdcr = {
    0 : "UNK",
    1 : "WHT",
    2 : "BLCK",
    3 : "OTH",
    4 : "API",
    5 : "HISP",
    6 : "AIAN"
}

#BLG_PRVDR_TYPE_CD
taf_ot_type_cd_dict={
    '01' : ["AM","01 = Physician"],
    '02' : ["AM","02 = Speech Language Pathologist"],
    '03' : ["DV","03 = Oral Surgery (Dentist only)"],
    '04' : ["AM","04 = Cardiac Rehabilitation and Intensive Cardiac Rehabilitation"],
    '05' : ["AM","05 = Anesthesiology Assistant"],
    '06' : ["AM","06 = Chiropractic"],
    '07' : ["AM","07 = Optometry"],
    '08' : ["AM","08 = Certified Nurse Midwife"],
    '09' : ["AM","09 = Certified Registered Nurse Anesthetist (CRNA)"],
    '10' : ["AM","10 = Mammography Center"],
    '11' : ["AM","11 = Independent Diagnostic Testing Facility (IDTF)"],
    '12' : ["AM","12 = Podiatry"],
    '13' : ["AM","13 = Ambulatory Surgical Center"],
    '14' : ["AM","14 = Nurse Practitioner"],
    '15' : ["OTH","15 = Medical Supply Company with Orthotist"],
    '16' : ["OTH","16 = Medical Supply Company with Prosthetist"],
    '17' : ["OTH","17 = Medical Supply Company with Orthotist-Prosthetist"],
    '18' : ["OTH","18 = Other Medical Supply Company"],
    '19' : ["AM","19 = Individual Certified Orthotis"],
    '20' : ["AM","20 = Individual Certified Prosthetist"],
    '21' : ["AM","21 = Individual Certified Prosthetist-Orthotist"],
    '22' : ["OTH","22 = Medical Supply Company with Pharmacis"],
    '23' : ["OTH","23 = Ambulance Service Provider"],
    '24' : ["OTH","24 = Public Health or Welfare Agency"],
    '25' : ["AM","25 = Voluntary Health or Charitable Agency"],
    '26' : ["AM","26 = Psychologist, Clinical"],
    '27' : ["AM","27 = Portable X-Ray Supplier"],
    '28' : ["AM","28 = Audiologist"],
    '29' : ["AM","29 = Physical Therapist in Private Practice"],
    '30' : ["AM","30 = Occupational Therapist in Private Practice"],
    '31' : ["AM","31 = Clinical Laboratory"],
    '32' : ["AM","32 = Clinic or Group Practice"],
    '33' : ["AM","33 = Registered Dietitian or Nutrition Professional"],
    '34' : ["AM","34 = Mass Immunizer Roster Biller"],
    '35' : ["AM","35 = Radiation Therapy Center"],
    '36' : ["AM","36 = Slide Preparation Facility"],
    '37' : ["AM","37 = Licensed Clinical Social Worker"],
    '38' : ["AM","38 = Certified Clinical Nurse Specialist"],
    '39' : ["AM","39 = Advance Diagnostic Imaging"],
    '40' : ["AM","40 = Optician"],
    '41' : ["AM","41 = Physician Assistant"],
    '42' : ["AM","42 = Hospital-General"],
    '43' : ["NF","43 = Skilled Nursing Facility"],
    '44' : ["NF","44 = Intermediate Care Nursing Facility"],
    '45' : ["NF","45 = Other Nursing Facility"],
    '46' : ["HH","46 = Home Health Agency"],
    '47' : ["AM","47 = Pharmacy"],
    '48' : ["OTH","48 = Medical Supply Company with Respiratory Therapis"],
    '49' : ["OTH","49 = Department Store"],
    '50' : ["OTH","50 = Grocery Store"],
    '51' : ["AM","51 = Indian Health Service Facility"],
    '52' : ["OTH","52 = Oxygen supplier"],
    '53' : ["AM","53 = Pedorthic personnel"],
    '54' : ["OTH","54 = Medical supply company with pedorthic personnel"],
    '55' : ["AM","55 = Rehabilitation Agency"],
    '56' : ["AM","56 = Ocularist"],
    '57' : ["AM","57 = All Other"],
    '' : ["OTH",'OTH']
}
#BLG_PRVDR_SPCLTY_CD
taf_ot_spc_cd_dict = {
    "01": ["AM", 'General Practice'],
    "02": ["AM", 'General Surgery'],
    "03": ["AM", 'Allergy/Immunology'],
    "04": ["AM", 'Otolaryngology'],
    "05": ["AM", 'Anesthesiology'],
    "06": ["AM", 'Cardiology'],
    "07": ["AM", 'Dermatology'],
    "08": ["AM", 'Family Practice'],
    "09": ["AM", 'Interventional Pain Management'],
    "10": ["AM", 'Gastroenterology'],
    "11": ["AM", 'Internal Medicine'],
    "12": ["AM", 'Osteopathic Manipulative Therapy'],
    "13": ["AM", 'Neurology'],
    "14": ["AM", 'Neurosurgery'],
    "15": ["AM", 'Speech Language Pathologist'],
    "16": ["AM", 'Obstetrics/Gynecolog'],
    "17": ["NF", 'Hospice and Palliative Care'],
    "18": ["AM", 'Ophthalmology'],
    "19": ["DV", 'Oral Surgery (dentists only'],
    "20": ["AM", 'Orthopedic Surgery'],
    "21": ["AM", 'Cardiac Electrophysiology'],
    "22": ["AM", 'Pathology'],
    "23": ["AM", 'Sports Medicine'],
    "24": ["AM", 'Plastic and Reconstructive Surgery'],
    "25": ["AM", 'Physical Medicine and Rehabilitation'],
    "26": ["AM", 'Psychiatry'],
    "27": ["AM", 'Geriatric Psychiatry'],
    "28": ["AM", 'Colorectal Surgery (formerly proctology)'],
    "29": ["AM", 'Pulmonary Disease'],
    "30": ["AM", 'Diagnostic Radiology'],
    "31": ["AM", 'Cardiac Rehabilitation & Intensive Cardiac Rehabilitation'],
    "32": ["AM", 'Anesthesiologist Assistant'],
    "33": ["AM", 'Thoracic Surgery'],
    "34": ["AM", 'Urology'],
    "35": ["AM", 'Chiropractic'],
    "36": ["AM", 'Nuclear Medicine'],
    "37": ["AM", 'Pediatric Medicine'],
    "38": ["AM", 'Geriatric Medicine'],
    "39": ["AM", 'Nephrology'],
    "40": ["AM", 'Hand Surgery'],
    "41": ["AM", 'Optometry'],
    "42": ["AM", 'Certified Nurse Midwife'],
    "43": ["AM", 'Certified Registered Nurse Anesthetist (CRNA)'],
    "44": ["AM", 'Infectious Disease'],
    "45": ["AM", 'Mammography Center'],
    "46": ["AM", 'Endocrinology'],
    "47": ["AM", 'Independent Diagnostic Testing Facility (IDTF)'],
    "48": ["AM", 'Podiatr'],
    "49": ["AM", 'Ambulatory Surgical Center'],
    "50": ["AM", 'Nurse Practitioner'],
    "51": ["OTH", 'Medical Supply Company with Orthotist'],
    "52": ["OTH", 'Medical Supply Company with Prosthetist'],
    "53": ["OTH", 'Medical Supply Company with Orthotist-Prosthetist'],
    "54": ["OTH", 'Other Medical Supply Company'],
    "55": ["AM", 'Individual Certified Orthotist'],
    "56": ["AM", 'Individual Certified Prosthetist'],
    "57": ["AM", 'Individual Certified Orthotist-Prosthetist'],
    "58": ["AM", 'Medical Supply Company with Pharmacis'],
    "59": ["OTH", 'Ambulance Service Provider'],
    "60": ["OTH", 'Public Health or Welfare Agency'],
    "61": ["AM", 'Voluntary Health or Charitable Agency'],
    "62": ["AM", 'Psychologist, Clinical'],
    "63": ["AM", 'Portable X-Ray Supplier'],
    "64": ["AM", 'Audiologist'],
    "65": ["AM", 'Physical Therapist in Private Practice'],
    "66": ["AM", 'Rheumatology'],
    "67": ["AM", 'Occupational Therapist in Private Practice'],
    "68": ["AM", 'Psychologist, Clinical'],
    "69": ["AM", 'Clinical Laboratory'],
    "70": ["AM", 'Single or Multispecialty Clinic or Group Practice'],
    "71": ["AM", 'Registered Dietitian or Nutrition Professional'],
    "72": ["AM", 'Pain Management'],
    "73": ["AM", 'Mass Immunization Roster Biller'],
    "74": ["AM", 'Radiation Therapy Center'],
    "75": ["AM", 'Slide Preparation Facility'],
    "76": ["AM", 'Peripheral Vascular Disease'],
    "77": ["AM", 'Vascular Surger'],
    "78": ["AM", 'Cardiac Surgery'],
    "79": ["AM", 'Addiction Medicine'],
    "80": ["AM", 'Licensed Clinical Social Worker'],
    "81": ["AM", 'Critical Care (Intensivists)'],
    "82": ["AM", 'Hematology'],
    "83": ["AM", 'Hematology/Oncology'],
    "84": ["AM", 'Preventive Medicine'],
    "85": ["AM", 'Maxillofacial Surgery'],
    "86": ["AM", 'Neuropsychiatry'],
    "87": ["OTH", 'All Other Suppliers'],
    "88": ["OTH", 'Unknown Supplier/Provider Specialty '],
    "89": ["AM", 'Certified Clinical Nurse Specialist'],
    "90": ["AM", 'Medical Oncology'],
    "91": ["AM", 'Surgical Oncology'],
    "92": ["AM", 'Radiation Oncology'],
    "93": ["ED", 'Emergency Medicine'],
    "94": ["AM", 'Interventional Radiology'],
    "95": ["AM", 'Advance Diagnostic Imaging'],
    "96": ["AM", 'Optician'],
    "97": ["AM", 'Physician Assistant'],
    "98": ["AM", 'Gynecological/Oncology'],
    "99": ["AM", 'Undefined physician type (provider is an MD)'],
    "A0": ["AM", 'Hospital-General'],
    "A1": ["NF", 'Skilled Nursing Facility'],
    "A2": ["NF", 'Intermediate Care Nursing Facility'],
    "A3": ["NF", 'Other Nursing Facility'],
    "A4": ["HH", 'Home Health Agency'],
    "A5": ["AM", 'Pharmacy'],
    "A6": ["OTH", 'Medical Supply Company with Respiratory Therapis'],
    "A7": ["OTH", 'Department Store'],
    "A8": ["OTH", 'Grocery Store'],
    "A9": ["AM", 'Indian Health Service facility'],
    "B1": ["OTH", 'Oxygen supplier'],
    "B2": ["AM", 'Pedorthic personnel'],
    "B3": ["OTH", 'Medical supply company with pedorthic personnel'],
    "B4": ["AM", 'Rehabilitation Agency'],
    "B5": ["AM", 'Ocularist'],
    '' : ["OTH",'OTH']
}

#https://resdac.org/cms-data/variables/bill-type-code - uses 2 and third digit of this column
taf_ot_bill_cd_dict={
    '18' : "IP",
    '15' : "IP",
    '16' : "IP",
    '11' : "IP",
    '12' : "IP",
    '17' : "IP",
    '14' : "IP",
    '13' : "AM",
    '28' : "NF",
    '25' : "NF",
    '26' : "NF",
    '21' : "NF",
    '22' : "NF",
    '27' : "NF",
    '24' : "NF",
    '23' : "AM",
    '68' : "NF",
    '65' : "NF",
    '66' : "NF",
    '61' : "NF",
    '62' : "NF",
    '67' : "NF",
    '64' : "NF",
    '63' : "NF",
    '38' : "HH",
    '35' : "HH",
    '36' : "HH",
    '31' : "HH",
    '32' : "HH",
    '37' : "HH",
    '34' : "HH",
    '33' : "HH",
    '48' : "IP",
    '45' : "IP",
    '46' : "IP",
    '41' : "IP",
    '42' : "IP",
    '47' : "IP",
    '44' : "IP",
    '43' : "AM",
    '82' : "IP",
    '81' : "NF",
    '83' : "AM",
    '89' : "AM",
    '84' : "AM",
    '85' : "AM",
    '88' : "AM",
    '86' : "IP",
    '87' : "AM",
    '71' : "AM",
    '73' : "AM",
    '76' : "AM",
    '72' : "AM",
    '75' : "AM",
    '74' : "AM",
    '79' : "AM",
    '78' : "ED",
    '-1' : "UNK"
}

#Also used for MDCD TAF OT BILL_TYPE_CD https://resdac.org/cms-data/variables/bill-type-code, in stage 2 and 3
mdcr_hh_toc_dict_1_4={
    #https://resdac.org/cms-data/variables/claim-facility-type-code-ffs
    #https://resdac.org/cms-data/variables/claim-service-classification-type-code-ffs
    1 : ['IP','Inpatient'],
    2 : ['IP','Inpatient or Home Health (covered on Part B)'],
    3 : ['AM','Outpatient (or HHA â€” covered on Part A)'],
    4 : ['IP','Other (Part B) â€” (includes HHA medical and other health services, e.g., SNF osteoporosis injectable drugs)'],
    5 : ['IP','Intermediate care â€” level I'],
    6 : ['IP','Intermediate care â€” level II'],
    7 : ['IP','Subacute Inpatient (revenue code 019X required) (formerly Intermediate care â€” level III)'],
    8 : ['IP','Swing Bed'],
    -1: ['UNK',np.nan],  
}
mdcr_hh_toc_dict_2={
    #https://resdac.org/cms-data/variables/claim-facility-type-code-ffs
    #https://resdac.org/cms-data/variables/claim-service-classification-type-code-ffs
    1 : ['NF','Inpatient'],
    2 : ['NF','Inpatient or Home Health (covered on Part B)'],
    3 : ['AM','Outpatient (or HHA â€” covered on Part A)'],
    4 : ['NF','Other (Part B) â€” (includes HHA medical and other health services, e.g., SNF osteoporosis injectable drugs)'],
    5 : ['NF','Intermediate care â€” level I'],
    6 : ['NF','Intermediate care â€” level II'],
    7 : ['NF','Subacute Inpatient (revenue code 019X required) (formerly Intermediate care â€” level III)'],
    8 : ['NF','Swing Bed'],
    -1: ['UNK',np.nan],  
}
mdcr_hh_toc_dict_3={
    #https://resdac.org/cms-data/variables/claim-facility-type-code-ffs
    #https://resdac.org/cms-data/variables/claim-service-classification-type-code-ffs
    1 : ['HH','Inpatient'],
    2 : ['HH','Inpatient or Home Health (covered on Part B)'],
    3 : ['HH','Outpatient (or HHA â€” covered on Part A)'],
    4 : ['HH','Other (Part B) â€” (includes HHA medical and other health services, e.g., SNF osteoporosis injectable drugs)'],
    5 : ['HH','Intermediate care â€” level I'],
    6 : ['HH','Intermediate care â€” level II'],
    7 : ['HH','Subacute Inpatient (revenue code 019X required) (formerly Intermediate care â€” level III)'],
    8 : ['HH','Swing Bed'],
    -1: ['UNK',np.nan],  
}
mdcr_hh_toc_dict_6={
    #https://resdac.org/cms-data/variables/claim-facility-type-code-ffs
    #https://resdac.org/cms-data/variables/claim-service-classification-type-code-ffs
    1 : ['NF','Inpatient'],
    2 : ['NF','Inpatient or Home Health (covered on Part B)'],
    3 : ['NF','Outpatient (or HHA â€” covered on Part A)'],
    4 : ['NF','Other (Part B) â€” (includes HHA medical and other health services, e.g., SNF osteoporosis injectable drugs)'],
    5 : ['NF','Intermediate care â€” level I'],
    6 : ['NF','Intermediate care â€” level II'],
    7 : ['NF','Subacute Inpatient (revenue code 019X required) (formerly Intermediate care â€” level III)'],
    8 : ['NF','Swing Bed'],
    -1: ['UNK',np.nan],  
}
mdcr_hh_toc_dict_7={
    1 : ['AM','Rural Health Clinic (RHC)'],
    2 : ['AM','Hospital based or independent renal dialysis facility'],
    3 : ['AM','Free-standing provider based federally qualified health center (FQHC)'],
    4 : ['AM','Other Rehabilitation Facility (ORF)'],
    5 : ['AM','Comprehensive Rehabilitation Center (CORF)'],
    6 : ['AM','Community Mental Health Center (CMHC)'],
    7 : ['AM','Federally Qualified Health Center (FQHC)'],
    -1: ['UNK',np.nan],  
}
mdcr_hh_toc_dict_8={
    1 : ['NF','Hospice (non-hospital based)'],
    2 : ['IP','Hospice (hospital based)'],
    3 : ['AM','Ambulatory surgical center (ASC) in hospital outpatient department'],
    4 : ['AM','Freestanding birthing center'],
    5 : ['AM','Critical Access Hospital â€” outpatient services'],
    7 : ['AM','Freestanding Non-residential Opioid Treatment Programs (eff. 1/2021)'],
    -1: ['UNK',np.nan],
}
#MDCD diff fict for 7 and 8
mdcd_ot_toc_dict_7={
    1 : ['AM','Rural Health Clinic (RHC)'],
    2 : ['AM','Hospital based or independent renal dialysis facility'],
    3 : ['AM','Free-standing provider based federally qualified health center (FQHC)'],
    4 : ['AM','Other Rehabilitation Facility (ORF)'],
    5 : ['AM','Comprehensive Rehabilitation Center (CORF)'],
    6 : ['AM','Community Mental Health Center (CMHC)'],
    7 : ['AM','Federally Qualified Health Center (FQHC)'],
    8 : ['ED', 'Licenseed Freestanding Emergency Medical Facility'],
    9 : ['AM','Other'],
    -1: ['UNK',np.nan],  
}
mdcd_ot_toc_dict_8={
    1 : ['NF','Hospice (non-hospital based)'],
    2 : ['IP','Hospice (hospital based)'],
    3 : ['AM','Ambulatory surgical center (ASC) in hospital outpatient department'],
    4 : ['AM','Freestanding birthing center'],
    5 : ['AM','Critical Access Hospital â€” outpatient services'],
    6 : ['IP', 'Residential facility'],
    7 : ['AM','Freestanding Non-residential Opioid Treatment Programs (eff. 1/2021)'],
    8 : ['AM', 'Reserved for National Assignment'],
    9 : ['AM','Other'],
    -1: ['UNK',np.nan],
}

dual_dict={#https://resdac.org/cms-data/variables/medicare-dual-code-mar #unsure if I coded binary correctly # 50-59 added in 2005, 2000 doesn't have this variable at all
    0 : [0, 'IN MSIS, ELIGIBLE IS NOT A MEDICARE BENEFICIARY'],
    1 : [0,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-QMB ONLY'],
    2 : [1,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-QMB AND FULL MEDICAID COVERAGE'],
    3 : [0,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-SLMB ONLY'],
    4 : [1,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-SLMB AND FULL MEDICAID COVERAGE'], 
    5 : [0,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-QDWI'],
    6 : [0,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-QUALIFYING INDIVIDUALS (1)'],
    7 : [0,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-QUALIFYING INDIVIDUALS (2)'], 
    8 : [1,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-OTHER DUAL ELIGIBLES'],
    9 : [1,'IN MSIS, ELIGIBLE IS ENTITLED TO MEDICARE-DUAL ELIGIBILITY CATEGORY UNKNOWN'],
    50 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODES 01-09 DO NOT APPLY'],
    51 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 01 APPLIES'], 
    52 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 02 APPLIES'],
    53 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 03 APPLIES'],
    54 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 04 APPLIES'],
    55 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 05 APPLIES'],
    56 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 06 APPLIES'], 
    57 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 07 APPLIES'], 
    58 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 08 APPLIES'], 
    59 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE ELIGIBLE AND CODE 09 APPLIES'], 
    60 : [1,'A RECORD WAS FOUND IN THE MEDICARE ENROLLMENT DATA BASE (EDB) FOR THE S-CHIP ELIGIBLE AND CODE 10 APPLIES'], 
    99 : [np.nan, 'IN MSIS, ELIGIBLES MEDICARE STATUS IS UNKNOWN'],
    -1 : [np.nan, "UNKNOWN"]
}

denom_combo_mc_dict= {
    0 : [0,'INDIVIDUAL WAS NOT ELIGIBLE FOR MEDICAID THIS MONTH'],
    1 : [1,'COMPREHENSIVE PLAN ONLY'],
    2 : [0,'DENTAL PLAN ONLY'],
    3 : [1,'BEHAVIORAL PLAN ONLY'],
    4 : [1,'PRIMARY CARE CASE MANAGEMENT (PCCM) PLAN ONLY'],
    5 : [1,'OTHER MANAGED CARE PLAN ONLY'],
    6 : [1,'COMPREHENSIVE PLAN AND DENTAL PLAN'],
    7 : [1,'COMPREHENSIVE PLAN AND BEHAVIORAL PLAN'],
    8 : [1,'COMPREHENSIVE PLAN AND OTHER MANAGED CARE PLAN'],
    9 : [1,'COMPREHENSIVE PLAN, DENTAL PLAN AND BEHAVIORAL PLAN'],
    10 : [1,'PRIMARY CARE CASE MANAGEMENT (PCCM) AND DENTAL PLAN'],
    11 : [1,'PRIMARY CARE CASE MANAGEMENT (PCCM) AND BEHAVIORAL PLAN'],
    12 : [1,'PRIMARY CARE CASE MANAGEMENT (PCCM) AND OTHER MANAGED CARE PLAN'],
    13 : [1,'PRIMARY CARE CASE MANAGEMENT (PCCM), DENTAL PLAN AND BEHAVIORAL PLAN'], 
    14 : [1,'DENTAL PLAN AND BEHAVIORAL PLAN'],
    15 : [1,'OTHER COMBINATIONS'],
    16 : [0,'FEE FOR SERVICE (NO MANAGED CARE PLAN REPORTED)'],
    99 : [np.nan,'ELIGIBLES MANAGED CARE PLAN STATUS IS UNKNOWN THIS MONTH'],
    -1 : [np.nan, "UNKNOWN"]
}

mc_dict = { #https://resdac.org/cms-data/variables/managed-care-type-plan-code
    0 : [0, "INDIVIDUAL WAS NOT ELIGIBLE FOR MEDICAID THIS MONTH"], 
    1 : [1, "ELIGIBLE IS ENROLLED IN A MEDICAL OR COMPREHENSIVE MANAGED CARE PLAN THIS MONTH (E.G. HMO)"],
    2 : [0, "ELIGIBLE IS ENROLLED IN A DENTAL MANAGED CARE PLAN THIS MONTH."],
    #including mental illnesses and substance use disorder (SUD) #https://www.kff.org/medicaid/issue-brief/state-policies-expanding-access-to-behavioral-health-care-in-medicaid/
    3 : [1, "ELIGIBLE IS ENROLLED IN A BEHAVIORAL MANAGED CARE PLAN THIS MONTH."],
    4 : [1, "ELIGIBLE IS ENROLLED IN A PRENATAL/DELIVERY MANAGED CARE PLAN THIS MONTH."],
    5 : [1, "ELIGIBLE IS ENROLLED IN A LONG-TERM CARE MANAGED CARE PLAN THIS MONTH."],
    6 : [1, "ELIGIBLE IS ENROLLED IN A PROGRAM FOR ALL-INCLUSIVE CARE FOR THE ELDERLY (PACE) THIS MONTH."],
    7 : [1, "ELIGIBLE IS ENROLLED IN A PRIMARY CARE CASE MANAGEMENT MANAGED CARE PLAN THIS MONTH."],
    8 : [1, "ELIGIBLE IS ENROLLED IN AN OTHER MANAGED CARE PLAN THIS MONTH."],
   77 : [np.nan, "THIS RECORD IS AN ENCOUNTER RECORD, BUT THERE WAS NO MATCH BETWEEN THE PLAN IDENTIFICATION NUMBER (DATA ELEMENT #22) AND THE PLAN IDENTIFIERS IN THE ELIGIBILITY RECORD FOR THIS PERSON IN THIS MONTH."],
    88: [0, "NOT APPLICABLE, THIS RECORD IS NOT AN ENCOUNTER RECORD OR THIS RECORD’S PLAN ID IS 8-FILLED."],
    99: [np.nan, "ELIGIBLE'S MANAGED CARE PLAN STATUS IS UNKNOWN."],
    66: [np.nan, "UNKNOWN"]
} 

denom_mc_dict = {
    "partial_mc" : 1,
    "mc_only" : 1,
    "ffs_only" : 0,
    "no_coverage" : 0
}

#Community based long term care flag https://resdac.org/cms-data/variables/community-based-lt-care-cltc-flag

cltc_list = [11,12,13,14,15,16,17,18,19,20,30,31,32,33,34,35,36,37,38,39,40]


states=['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN',
        'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH',
        'NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT',
        'VT','VA','WA','WV','WI','WY','-1']


#-----------------------------------------
# - list of medicare-medicaid dual values
#-----------------------------------------
#https://resdac.org/cms-data/variables/monthly-medicare-medicaid-dual-eligibility-code-january
#https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-january
#TAF: https://resdac.org/sites/datadocumentation.resdac.org/files/CCW%20Codebook%20TAF%20Demographic%20Eligibility_Version%20022023.pdf
#MDCR & MDCD TAF
full_dual= [2,4,10]
part_dual=[1,3,5,6]
all_dual = full_dual + part_dual
# MDCD MAX: https://resdac.org/sites/datadocumentation.resdac.org/files/CCW%20Codebook%20MAX%20Person%20Summary_Version%202014.pdf
max_full_dual = [2,4,10,52,54,60]
max_part_dual = [1,3,5,6,7,8,9,51,53,55,56,57,58,59]
max_all_dual = max_full_dual + max_part_dual

#https://resdac.org/cms-data/variables/hmo-indicator-january
mdcr_hmo_ind=["2","B","C"]