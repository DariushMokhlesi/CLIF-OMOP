# src/drug_exposure.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
from pathlib import Path
import json

DRUG_EXPOSURE_COL_NAMES = [
]

with open('config.json', 'r') as f:
    config = json.load(f)


omop_parquet_dir = config["omop_parquet_dir"]
file_path = Path(omop_parquet_dir) / "omop_drug_exposure.parquet"
output_file = Path(omop_parquet_dir) / "clif_medication_admin_continuous.parquet"


def rename_medication_admin_continuous():
    try:
        medication_df = pd.read_parquet(file_path)
        medication_df = medication_df.rename(columns={'drug_concept_id': 'med_category'})  
        medication_df = medication_df.rename(columns={'drug_exposure_start_datetime': 'admin_dttm'})  
        medication_df = medication_df.rename(columns={'quantity': 'med_dose'})  
        medication_df = medication_df.rename(columns={'visit_occurence_id': 'hospitalization_id'})  
        medication_df = medication_df.rename(columns={'drug_source_value': 'med_name'})  
        medication_df = medication_df.rename(columns={'route_source_value': 'med_route_name'})  
        medication_df = medication_df.rename(columns={'dose_unit_source_value': 'med_dose_unit'})  
        
        df = pd.read_csv("MappingMedicationConceptID.csv")
        df['med_category'] = df['med_category'].str.lower().str.strip()
        mapping = dict(zip(df["Concept ID"], df["med_category"]))
        print(type(medication_df["med_category"]))
        print(medication_df["med_category"].head())
        #medication_df["med_category"] = medication_df["med_category"].map(mapping)
        medication_df["med_category"] = medication_df["med_category"].apply(
    lambda x: mapping.get(x, x)
)
        print(medication_df["med_category"].unique())

    except Exception as e:
        print(f"Error processing file: {e}")

 
def remove_columns_medication_admin_continuous():
    try:
        medication_df = pd.read_parquet(file_path)
        medication_df = medication_df.drop(columns=['drug_exposure_id', 
        'person_id', 
        'drug_exposure_start_date', 
        'drug_exposure_end_date', 
        'drug_exposure_end_datetime', 
        'verbatim_end_date', 
        'drug_type_concept_id', 
        'stop_reason', 
        'refills', 
        'days_supply', 
        'sig', 
        'route_concept_id', 
        'lot_number', 
        'provider_id', 
        'visit_detail_id'])
        print(medication_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")

def adding_columns_medication_admin_continuous():
    try:
        medication_df = pd.read_parquet(file_path)
        medication_df = medication_df.assign(med_order_id=None, 
        med_group=None, 
        med_route_category = None,
        mar_action_name = None,
        mar_action_category = None)
        print(medication_df.head())
        medication_df.to_parquet(output_file, index=False)
    except Exception as e:
        print(f"Error processing file: {e}")


if __name__ == "__main__":
    rename_medication_admin_continuous()
    remove_columns_medication_admin_continuous()
    adding_columns_medication_admin_continuous()