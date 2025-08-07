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

clif_parquet_dir = config["clif_parquet_dir"]
file_path = Path(clif_parquet_dir) / "clif_hospitalization.parquet"
output_file = Path(clif_parquet_dir) / "omop_visit_occurrence.parquet"

def rename_visit_occurrence():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.rename(columns={'hospitalization_id': 'visit_occurence_id'})       
        hospitalization_df = hospitalization_df.rename(columns={'patient_id': 'person_id'})       
        hospitalization_df = hospitalization_df.rename(columns={'admission_type_category': 'visit_concept_id'})       
        #hospitalization_df = hospitalization_df.rename(columns={'admission_dttm': 'visit_start_date'})       
        hospitalization_df = hospitalization_df.rename(columns={'admission_dttm': 'visit_start_datetime'})       
        #hospitalization_df = hospitalization_df.rename(columns={'discharge_dttm': 'visit_end_date'})       
        hospitalization_df = hospitalization_df.rename(columns={'discharge_dttm': 'visit_end_datetime'})       
        #hospitalization_df = hospitalization_df.rename(columns={'admission_type_name': 'visit_source_value'})       
        hospitalization_df = hospitalization_df.rename(columns={'admission_type_name': 'admitted_from_source_value'})       
        hospitalization_df = hospitalization_df.rename(columns={'discharge_category': 'discharged_to_concept_id'})       
        hospitalization_df = hospitalization_df.rename(columns={'discharge_name': 'discharged_to_source_value'})               
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error renaming to visit_occurrence file: {e}")

def remove_columns_visit_occurrence():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.drop(columns=['hospitalization_joined_id', 'zipcode_nine_digit', 'zipcode_five_digit' , 'census_block_code', 'census_block_group_code', 'census_tract', 'state_code', 'state_code', 'county_code', 'age_at_admission'])
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error removing columns to visit_occurrence file: {e}")

def adding_columns_visit_occurrence():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.assign(visit_type_concept_id=None, 
        provider_id=None, 
        care_site_id=None, 
        visit_source_concept_id=None, 
        admitted_from_concept_id=None, 
        preceding_visit_occurence_id=None)
        hospitalization_df.to_parquet(output_file, index=False)
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error adding columns to visit_occurrence file: {e}")

if __name__ == "__main__":
    rename_visit_occurrence()
    remove_columns_visit_occurrence()
    adding_columns_visit_occurrence()
