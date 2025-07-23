# src/person.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
from pathlib import Path
import json

PERSON_COL_NAMES = [
"person_id", 
"gender_concept_id", 
"year_of_birth",
"month_of_birth",
"day_of_birth",
"birth_datetime",
"race_concept_id",
"ethnicity_concept_id",
"location_id",
"provider_id",
"care_site_id",
"person_source_value",
"gender_source_value",
"gender_source_concept_id",
"race_source_value",
"race_source_concept_id",
"ethnicity_source_value",
"ethnicity_source_concept_id",
]

with open('config.json', 'r') as f:
    config = json.load(f)

clif_parquet_dir = config["clif_parquet_dir"]
file_path = Path(clif_parquet_dir) / "clif_patient.parquet"


def rename_person():
    try:
        patint_df = pd.read_parquet(file_path)
        patient_df = patient_df.rename(columns={'patient_id': 'person_id'})    
        patient_df = patient_df.rename(columns={'sex_cetegory': 'gender_concept_id'})        
        patient_df = patient_df.rename(columns={'birth_date': 'year_of_birth'})        
        patient_df = patient_df.rename(columns={'race_category': 'race_concept_id'})        
        patient_df = patient_df.rename(columns={'ethnicity_category': 'ethnicity_concept_id'})            

        print(patient_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")
def adding_columns_person():
    try:
        patient_df = pd.read_parquet(file_path)
        patient_df = patient_df.assign(location_id=None, provider_id=None, care_site_id=None, person_source_value=None, gender_source_value=None, gender_source_concept_id=None, race_source_value=None, race_source_concept_id=None, ethnicity_source_value=None, ethnicity_source_concept_id=None)
        print(patient_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")


if __name__ == "__main__":
    rename_person()
    adding_columns_person()