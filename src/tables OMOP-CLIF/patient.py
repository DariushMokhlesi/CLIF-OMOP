# src/person.py
import numpy as np
import pandas as pd
from importlib import reload
from pathlib import Path
import json

PATIENT_COL_NAMES = [
    "patient_id",
    "sex_cetegory",
    "birth_date",
    "race_category",
    "ethnicity_category",
    "sex_name",
    "race_name",
    "ethnicity_name",
    "death_dttm",
    "language_category",
    "language_name",
]

with open('config.json', 'r') as f:
    config = json.load(f)

omop_parquet_dir = config["omop_parquet_dir"]
file_path = Path(omop_parquet_dir) / "omop_person.parquet"
output_file = Path(omop_parquet_dir) / "clif_patient.parquet"

def rename_patient():
    try:
        person_df = pd.read_parquet(file_path)
        person_df = person_df.rename(columns={'person_id': 'patient_id'})
        person_df = person_df.rename(columns={'gender_concept_id': 'sex_cetegory'})        
        person_df = person_df.rename(columns={'birth_datetime': 'birth_date'})        
        person_df = person_df.rename(columns={'race_concept_id': 'race_category'})        
        person_df = person_df.rename(columns={'ethnicity_concept_id': 'ethnicity_category'})             
        person_df = person_df.rename(columns={'gender_source_value': 'sex_name'})   
        person_df = person_df.rename(columns={'race_source_value': 'race_name'})             
        person_df = person_df.rename(columns={'ethnicity_source_value': 'ethnicity_name'})             

        print(person_df.head())
    except Exception as e:
        print(f"Error renaming to patient file: {e}")

def remove_columns_patient():
    try:
        person_df = pd.read_parquet(file_path)
        person_df = person_df.drop(columns=['year_of_birth', 
        'month_of_birth', 
        'day_of_birth', 
        'location_id', 
        'provider_id', 
        'care_site_id', 
        'person_source_value', 
        'gender_source_concept_id', 
        'race_source_concept_id', 
        'ethnicity_source_concept_id'])        
        print(person_df.head())
    
    except Exception as e:
        print(f"Error removing columns to patient file: {e}")

def adding_columns_patient():
    try:
        person_df = pd.read_parquet(file_path)
        person_df = person_df.assign(death_dttm=None, 
        language_name=None, 
        language_category=None)
        person_df.to_parquet(output_file, index=False)
        print(person_df.head())


    except Exception as e:
        print(f"Error adding columns to patient file: {e}")

if __name__ == "__main__":
    rename_patient()
    remove_columns_patient()
    adding_columns_patient()