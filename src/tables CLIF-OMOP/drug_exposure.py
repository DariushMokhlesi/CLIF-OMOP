# src/drug_exposure.py
import numpy as np
import pandas as pd
from importlib import reload
from pathlib import Path
import json

with open('config.json', 'r') as f:
    config = json.load(f)

clif_parquet_dir = config["clif_parquet_dir"]
file_path = Path(clif_parquet_dir) / "clif_medication_admin_continuous.parquet"
output_file = Path(clif_parquet_dir) / "omop_drug_exposure.parquet"

def rename_drug_exposure():
    try:
        drug_df = pd.read_parquet(file_path)
        drug_df = drug_df.rename(columns={'med_category': 'drug_concept_id'})  
        drug_df = drug_df.rename(columns={'admin_dttm': 'drug_exposure_start_datetime'})   
        drug_df = drug_df.rename(columns={'med_dose': 'quantity'})  
        drug_df = drug_df.rename(columns={'hospitalization_id': 'visit_occurence_id'})    
        drug_df = drug_df.rename(columns={'med_name': 'drug_source_value'})   
        drug_df = drug_df.rename(columns={'med_route_name': 'route_source_value'})   
        drug_df = drug_df.rename(columns={'med_dose_unit': 'dose_unit_source_value'})   
        
        print(drug_df.head())

        df = pd.read_csv("MappingMedicationConceptID.csv")
        df['med_category'] = df['med_category'].str.lower().str.strip()
        mapping = dict(zip(df["med_category"], df["Concept ID"]))
        drug_df["drug_concept_id"] = drug_df["drug_concept_id"].map(mapping)
    except Exception as e:
        print(f"Error renaming to drug exposure file: {e}")

def adding_columns_drug_exposure():
    try:
        drug_df = pd.read_parquet(file_path)
        drug_df = drug_df.assign(drug_exposure_id=None, 
        person_id=None, 
        drug_exposure_start_date = None,
        drug_exposure_end_date = None,
        drug_exposure_end_datetime = None,
        verbatim_end_date = None,
        drug_type_concept_id=None,
        stop_reason=None, 
        refills=None, 
        days_supply=None,
        sig=None,
        route_concept_id=None,
        lot_number=None,
        provider_id=None,
        visit_detail_id=None)

        drug_df.to_parquet(output_file, index=False)
        print(drug_df.head())
    except Exception as e:
        print(f"Error adding columns to drug exposure file: {e}")

if __name__ == "__main__":
    rename_drug_exposure()
    adding_columns_drug_exposure()