# src/measurement.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
from pathlib import Path
import json

MEASUREMENT_COL_NAMES = [
"measurement_id", 
"person_id", 
"measurement_concept_id",
"measurement_date",
"measurement_datetime",
"measurement_time",
"measurement_type_concept_id",
"operator_concept_id",
"value_as_number",
"value_as_concept_id",
"unit_concept_id",
"range_low",
"range_high",
"provider_id",
"visit_occurrence_id",
"visit_detail_id",
"measurement_source_value",
"measurement_source_concept_id",
"unit_source_value",
"unit_source_concept_id",
"value_source_value",
"measurement_event_id",
"meas_event_field_concept_id",
]

with open('config.json', 'r') as f:
    config = json.load(f)

clif_parquet_dir = config["clif_parquet_dir"]
file_path = Path(clif_parquet_dir) / "clif_vitals.parquet"


def rename_measurement():
    try:
        vitals_df = pd.read_parquet(file_path)
        vitals_df = vitals_df.rename(columns={'vital_category': 'measurement_concept_id'})   
        #vitals_df = vitals_df.rename(columns={'recorded_dttm': 'measurement_date'})         
        vitals_df = vitals_df.rename(columns={'recorded_dttm': 'measurement_datetime'})        
        #vitals_df = vitals_df.rename(columns={'recorded_dttm': 'measurement_time'})        
        vitals_df = vitals_df.rename(columns={'vital_value': 'value_as_number'})        
        vitals_df = vitals_df.rename(columns={'meas_site_name': 'value_as_concept_Id'})        
        #vitals_df = vitals_df.rename(columns={'vital_category': 'unit_concept_id'})  
        vitals_df = vitals_df.rename(columns={'hospitalization_id': 'visit_occurence_id'})            
        vitals_df = vitals_df.rename(columns={'vital_name': 'measurement_source_value'})            
          
        print(vitals_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")

def adding_columns_measurement():
    try:
        vitals_df = pd.read_parquet(file_path)
        vitals_df = vitals_df.assign(measurement_id=None, person_id=None, measurement_type_concept_id=None, operator_concept_id=None, range_low=None, range_high=None, provider_id=None, visit_detail_id=None, measurement_source_concept_id=None, unit_source_value=None, unit_source_concept_id=None, value_source_value=None, measurement_event_id=None, meas_event_field_concept_id=None)
        print(vitals_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    rename_measurement()
    adding_columns_measurement()

