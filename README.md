# CLIF-OMOP and OMOP-CLIF
This repository provides code that converts healthcare data from OMOP to CLIF and from CLIF to OMOP (bidirectional). It enables standardized data translation between the OMOP CDM and the CLIF longitudinal ICU format for analytical and research purposes.

# Getting Started
Before running any of the scripts, please specify the local file paths for the 'clif_parquet_dir' and 'omop_parquet_dir' in the config.json file. Please ensure that you use a parquet file. 

# Authors
Dariush Mokhlesi (GitHub: DariushMokhlesi) and Kian Mokhlesi (GitHub: KianMokhlesi). 

# Note
This repository converts several Omop tables (person, visit_occurence, measurement, and drug_exposure) to their corresponding CLIF tables (patient, hospitalization, vitals, medication_admin_continuous) and vice versa. After each file is run, it creates its converted corresponding parquet file.
