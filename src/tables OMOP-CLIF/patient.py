# src/person.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
from pathlib import Path
import json

PATIENT_COL_NAMES = [
"patient_id",
"race_name",
"ethnicity_name",
"sex_name",
"death_dttm",
"language_name",
"language_category",
"sex_cetegory",
"birth_date",
"race_category",
"ethnicity_category",
]

with open('config.json', 'r') as f:
    config = json.load(f)

clif_parquet_dir = config["clif_parquet_dir"]
file_path = Path(clif_parquet_dir) / "clif_patient.parquet" #CORRECT FILE NEEDED


def rename_patient():
    try:
        person_df = pd.read_parquet(file_path)
        person_df = person_df.rename(columns={'person_id': 'patient_id'})
        person_df = person_df.rename(columns={'gender_concept_id': 'sex_cetegory'})        
        person_df = person_df.rename(columns={'year_of_birth': 'birth_date'})        
        person_df = person_df.rename(columns={'race_concept_id': 'race_category'})        
        person_df = person_df.rename(columns={'ethnicity_concept_id': 'ethnicity_category'})             
        print(person_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")

def adding_columns_patient():
    try:
        person_df = pd.read_parquet(file_path)
        person_df = person_df.assign(race_name=None, ethnicity_name=None, sex_name=None, death_dttm=None, language_name=None, language_category=None)
        print(person_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    rename_patient()
    adding_columns_patient()