## RDP 

Folder maintained by Haley Lescinsky 


### Overview

RDP (ReDistribution Processing) is one of the necessary adjustments in the DEX county pipeline. It uses predetermined fixed proportions, and sometimes proportions observed in the data, to probabilistically reassign an ICD code that is either garbage or not specific enough. After RDP all rows of the data will directly map to one of the DEX causes, instead of one of the intermediate causes ("_gc" or "XX_NEC").

Step 1: Make maps (use packages and data)  

Step 2: Apply maps (use maps and data to actually adjust data)

Required inputs:
* RDP package version, accessible via the database and the config  [see `RDP_PACKAGES` folder for details on this]

* (At least) primary cause adjusted data with a run_id, accessible via the database and config


### When to run

Whenever there is a new run_id of the DEX model.

### How to run

Currently set up through array jobs and parallelized over the unit of analysis (year, TOC, source). 

1. Run `/main/launch_make_map.R` which will run `/main/make_map/make_map.R` and save a bunch of versioned maps to `config$RDP$map_output_dir`. 

2. Run `/main/launch_apply_map.R` after the maps are done. This will run `/main/apply_map/apply_map.R` followed by `/post/visualize_data.R`. This is when the maps are applied to the data, the adjusted data saved to `config$RDP$data_output_dir`, and some visualizations of the RDP saved in the RDP map version folder. 

### Other

A separate process is needed to adjust implausible codes in HCCI where the data is collapsed while exporting from the enclave. See the 'HCCI' folder for details. 
