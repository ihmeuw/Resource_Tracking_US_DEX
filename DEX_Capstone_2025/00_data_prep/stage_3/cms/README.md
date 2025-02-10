How to process CMS stage 3

There are 5 sets of scripts:
    
    1. MDCD MAX (this processes MDCD MAX IP and OT subdatasets)
        Launcher: mdcd_max_launcher.py
        Helper: /helpers/mdcd/mdcd_max_stg3.py
    2. MDCD TAF (this processes MDCD TAF IP, OT, LTC, and RX subdatasets)
        Launcher: mdcd_taf_launcher.py
        Helper: /helpers/mdcd/mdcd_taf_stg3.py
    3. MDCR FFS (all MDCR FFS INLCUDING RX. So Carrier, HHA, Hosp, IP, NF, HOP, RX subdatasets)
        Launcher: mdcr_ffs_launcher.py
        Helper: /helpers/mdcr/mdcd_ffs_stg3.py
    4. MDCR Part C (this is managed care, this processes MDCR part C Carrier, HHA, HOP, IP, NF subdatasets)
        Launcher: mdcr_partc_launcher.py
        Helper: /helpers/mdcr/mdcd_part_c_stg3.py
    5. MDCR NF Medpar (just processes the medpar subdataset, this has a different data structure than the rest of MDCR so in its own script), its small enough to not need to be parallelized so only has a helper script that loops over year
        Helper: /helpers/mdcr_nf_medpar_stg3.py
    6. MDCR NF SAF (we aren't using this data anymore bc its old - moved scripts to archive folder)
    
Each set of scripts has a helper script (w/ _1 in script name), under the helper folder and a launcher script.
The archive folder contains the old helper and launcher scripts from when there was one per subdataset (so about 20 helper and launcher scripts in total). It also incluseds scripts to run MDCR NF SAF 
    
    
Processing stage 3 data: Run the launchers for each subdataset set (except for MDCR NF Medpar - run interactively instead of parallelized)
        
Other notes:
    
    -all the helper scripts reference functions in helpers/column_formatter.py
    -filepaths and dictionaries can be found under the constants folder
    


    
