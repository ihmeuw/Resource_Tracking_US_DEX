How to process CMS stage 2 - Main steps

There are  6 sets of scripts:
    
    1. MDCD MAX (this processes MDCD MAX IP and OT subdatasets)
    2. MDCD TAF (this processes MDCD TAF IP, OT, LTC, and RX subdatasets)
    3. MDCR FFS (all MDCR FFS except RX. So Carrier, HHA, Hosp, IP, NF, HOP subdatasets)
    4. MDCR RX (just processes the RX subdataset, this needs to be parallelized differently for stage 2 bc doesn't have state info, so in its  own script), but stage 3 processing is the same script as all the other MDCR FFS scripts
    5. MDCR Part C (this is managed care, this processes MDCR part C Carrier, HHA, HOP, IP, NF subdatasets)
    6. MDCR NF Medpar (just processes the medpar subdataset, this has a different data structure than the rest of MDCR so in its own script)
    7. MDCR NF SAF (we aren't using this data anymore bc its old - moved scripts to archive folder)
    
Each set of scripts has a helper script (w/ _1 in script name), under the helper folder and a launcher script.
    
    
Steps to processing stage 2 data:
        
        - to launch jobs make sure you have an srun going.
        - activate dex environment
        - cd into cms folder w/ launchers
        - run launcher: python {launcher_script_name}
        
Other notes:
    
    -all the helper scripts reference functions in helpers/processing_functions.py
    -filepaths and dictionaries can be found under the constants folder
     
