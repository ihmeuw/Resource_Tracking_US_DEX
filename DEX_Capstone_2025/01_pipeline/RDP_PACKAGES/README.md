## RDP packages

Folder maintained by Haley Lescinsky


### High level overview
RDP packages store all of the metadata - input codes (garbage/NEC), target codes, proportions - necessary to create RDP maps. Thus, the `RDP package version` is a required input for making and applying RDP maps. Because we don't expect the packages to change as much as maps and data, RDP packages are versioned independently (`map_name == RDP_PACKAGES`).

The DEX redistribution uses the Global Burden of Disease Cause of Death (CoD) RDP packages as a starting point. The code in the RDP_PACKAGES folder basically 1) makes some slight modifications to the CoD packages and saves them in our folder, 2) creates custom DEX packages for a small set of causes + NECs and combines these packages with the rest, and 3) saves some metadata to make RDP easier


### Detailed overview

* `/pre/[special_dex_package]`: The code for custom DEX packages.
* `/main/01_knockout_cod_packages.R`: This script “knocks out” the CoD packages by updating the inputs and outputs so they are aligned with the DEX cause map. This involves: 1) Reducing the input code list to just those that are garbage codes or NEC in DEX hierarchy, 2) Reducing the target code list to just those that are not garbage codes or NEC in dex hierarchy 3) Increasing the input code list for the final “residual all-cause GC” package to include possible garbage codes and NEC codes. 4) Adding DEX custom packages to the list of packages, either in addition to the CoD packages or in place of a specific package. 5) Some additional processing steps to remove weight groups that are for demographics outside of the USA
* `/main/02_create_trunc_map.R`: To be run after packages are knocked out and there is a new package version. It combines the ICD map with the package inputs to save a truncation map that will reduce complexity of ICD codes prior to redistribution.
* `/main/03_make_package_summary.R`: To be run after packages are knocked out and there is a new package version. It saves a package summary and a list of RDP target codes that are required inputs to RDP map application and vetting. 

### When to run

Packages should be rerun if **either** the DEX ICD code map is updated *or* there have been changes made to the package inputs or package processing scripts.  Note - if there is a change made to the custom dex package inputs, the knockout script may need to be updated so it correctly pulls in the new packages. 


### How to run

Open up `launch_make_rdp_packages.R`  and create a new RDP package version id and register in database; confirm other versions (causemap + phase run) are also correct. Then launch the scripts which should all finish in several minutes. 
