# src/visit_occurence.py
import numpy as np
import pandas as pd
from importlib import reload
from pathlib import Path
import json

HOSPITALIZATION_COL_NAMES = [
    "hospitalization_id",
    "patient_id",
    "admission_type_category",
    "admission_dttm",
    "discharge_dttm",
    "admission_type_name",
    "discharge_category",
    "discharge_name",
    "hospitalization_joined_id",
    "zipcode_nine_digit",
    "zipcode_five_digit",
    "census_block_code",
    "census_block_group_code",
    "census_tract",
    "state_code",
    "county_code",
    "age_at_admission",
]

with open('config.json', 'r') as f:
    config = json.load(f)

omop_parquet_dir = config["omop_parquet_dir"]
file_path = Path(omop_parquet_dir) / "omop_visit_occurrence.parquet"
output_file = Path(omop_parquet_dir) / "clif_hospitalization.parquet"

def rename_hospitalization():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.rename(columns={'visit_occurence_id': 'hospitalization_id'})       
        hospitalization_df = hospitalization_df.rename(columns={'person_id': 'patient_id'})       
        hospitalization_df = hospitalization_df.rename(columns={'visit_concept_id': 'admission_type_category'})       
        hospitalization_df = hospitalization_df.rename(columns={'visit_start_datetime': 'admission_dttm'})       
        hospitalization_df = hospitalization_df.rename(columns={'visit_end_datetime': 'discharge_dttm'})       
        hospitalization_df = hospitalization_df.rename(columns={'visit_source_value': 'admission_type_name'})       
        hospitalization_df = hospitalization_df.rename(columns={'discharged_to_concept_id': 'discharge_category'})       
        hospitalization_df = hospitalization_df.rename(columns={'discharged_to_source_value': 'discharge_name'})            
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error renaming columns to hospitalization file: {e}")

def remove_columns_hospitalization():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.drop(columns=['preceding_visit_occurence_id'])
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error removing columns to hospitalization file: {e}")

def adding_columns_hospitalization():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.assign(visit_start_date=None, 
        visit_end_date=None, 
        visit_type_concept_id=None, 
        provider_id=None, 
        care_site_id=None, 
        visit_source_concept_id=None, 
        admitted_from_concept_id=None, 
        admitted_from_source_value=None,
        preceding_visit_occurence_id=None,
        hospitalization_joined_id=None,
        zipcode_nine_digit=None,
        zipcode_five_digit=None,
        census_block_code=None,
        census_block_group_code=None,
        census_tract=None,
        state_code=None,
        county_code=None,
        age_at_admission=None)
        print(hospitalization_df.head())
        hospitalization_df.to_parquet(output_file, index=False)

    except Exception as e:
        print(f"Error adding columns to hospitalization file: {e}")

if __name__ == "__main__":
    rename_hospitalization()
    remove_columns_hospitalization()
    adding_columns_hospitalization()