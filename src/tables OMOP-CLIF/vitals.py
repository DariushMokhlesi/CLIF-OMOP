# src/measurement.py
import numpy as np
import pandas as pd
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

omop_parquet_dir = config["omop_parquet_dir"]
file_path = Path(omop_parquet_dir) / "omop_measurement.parquet"
output_file = Path(omop_parquet_dir) / "clif_vitals.parquet"

def rename_vitals():
    try:
        measurement_df = pd.read_parquet(file_path)
        measurement_df = measurement_df.rename(columns={'measurement_concept_id': 'vital_category'})   
        measurement_df = measurement_df.rename(columns={'value_as_number': 'vital_value'})        
        measurement_df = measurement_df.rename(columns={'value_as_concept_id': 'meas_site_name'})        
        measurement_df = measurement_df.rename(columns={'visit_occurence_id': 'hospitalization_id'})        
        measurement_df = measurement_df.rename(columns={'measurement_source_value': 'vital_name'})            
        
        print(measurement_df.head())
    except Exception as e:
        print(f"Error renaming to vitals file: {e}")

def remove_columns_vitals():
    try:
        measurement_df = pd.read_parquet(file_path)
        measurement_df = measurement_df.drop(columns=['measurement_id', 
        'person_id', 
        'measurement_date', 
        'measurement_time', 
        'measurement_type_concept_id', 
        'operator_concept_id', 
        'range_low', 
        'range_high', 
        'provider_id', 
        'visit_detail_id', 
        'measurement_source_concept_id', 
        'unit_source_value', 
        'unit_source_concept_id', 
        'value_source_value', 
        'measurement_event_id', 
        'meas_event_field_concept_id'])
        print(measurement_df.head())
    except Exception as e:
        print(f"Error removing columns to vitals file: {e}")


def adding_columns_vitals():
    try:
        measurement_df = pd.read_parquet(file_path)
        measurement_df = measurement_df.assign(measurement_id=None, 
        person_id=None, 
        measurement_type_concept_id=None, 
        operator_concept_id=None, 
        range_low=None, 
        range_high=None, 
        provider_id=None, 
        visit_detail_id=None, 
        measurement_source_concept_id=None, 
        unit_source_value=None, 
        unit_source_concept_id=None, 
        value_source_value=None, 
        measurement_event_id=None, 
        meas_event_field_concept_id=None)
        print(measurement_df.head())

        measurement_df.to_parquet(output_file, index=False)
    except Exception as e:
        print(f"Error adding columns to vitals file: {e}")

if __name__ == "__main__":
    rename_vitals()
    remove_columns_vitals()
    adding_columns_vitals()