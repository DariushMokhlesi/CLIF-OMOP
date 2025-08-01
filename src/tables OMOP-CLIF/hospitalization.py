# src/visit_occurence.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
from pathlib import Path
import json

VISIT_OCCURRENCE_COL_NAMES = [
"visit_occurence_id",
"person_id",
"visit_concept_id",
"visit_start_date",
"visit_start_datetime",
"visit_end_date",
"visit_end_datetime",
"visit_type_concept_id",
"provider_id",
"care_site_id",
"visit_source_value",
"visit_source_concept_id",
"admitted_from_concept_id",
"admitted_from_source_value",
"discharged_to_concept_id",
"discharged_to_source_value",
"preceding_visit_occurence_id",
]

with open('config.json', 'r') as f:
    config = json.load(f)

omop_parquet_dir = config["omop_parquet_dir"]
file_path = Path(omop_parquet_dir) / "omop_visit_occurrence.parquet"
output_file = Path(omop_parquet_dir) / "clif_hospitalization.parquet"


def rename_person():
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
        print(f"Error processing file: {e}")

def remove_columns_hospital():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.drop(columns=['preceding_visit_occurence_id'])
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")


def adding_columns_hospital():
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
        print(f"Error processing file: {e}")


if __name__ == "__main__":
    rename_person()
    remove_columns_hospital()
    adding_columns_hospital()
