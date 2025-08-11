# src/drug_exposure.py
import numpy as np
import pandas as pd
from importlib import reload
from pathlib import Path
import json

DRUG_EXPOSURE_COL_NAMES = [
    "drug_exposure_id",
    "person_id",
    "drug_concept_id",
    "drug_exposure_start_date",
    "drug_exposure_start_datetime",
    "drug_exposure_end_date",
    "drug_exposure_end_datetime",
    "verbatim_end_date",
    "drug_type_concept_id",
    "stop_reason",
    "refills",
    "quantity",
    "days_supply",
    "sig",
    "route_concept_id",
    "lot_number",
    "provider_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "drug_source_value",
    "drug_source_concept_id",
    "route_source_value",
    "dose_unit_source_value"
]

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
        drug_df['drug_exposure_start_datetime'] = pd.to_datetime(drug_df['drug_exposure_start_datetime'], errors='coerce', utc=True)
        drug_df['drug_exposure_start_date'] = drug_df['drug_exposure_start_datetime'].dt.date
        df = pd.read_csv("MappingMedicationConceptID.csv")
        df['med_category'] = df['med_category'].str.lower().str.strip()
        mapping = dict(zip(df["med_category"], df["Concept ID"]))
        drug_df["drug_concept_id"] = drug_df["drug_concept_id"].map(mapping)

        drug_df.to_parquet(output_file, index=False)
        print(drug_df.head())
    except Exception as e:
        print(f"Error renaming to drug exposure file: {e}")


def remove_columns_drug_exposure():
    try:
        drug_df = pd.read_parquet(output_file)
        drug_df = drug_df.drop(columns=[
        'med_order_id',
        'med_group',
        'med_route_category',
        'mar_action_name',
        'mar_action_category'])
        drug_df.to_parquet(output_file, index=False)
        print(drug_df.head())
    except Exception as e:
        print(f"Error removing columns to drug exposure file: {e}")



def adding_columns_drug_exposure():
    try:
        drug_df = pd.read_parquet(output_file)
        drug_df = drug_df.assign(drug_exposure_id=None, 
        person_id=None, 
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
        visit_detail_id=None,
        drug_source_concept_id=None,
        )
        drug_df.to_parquet(output_file, index=False)
        print(drug_df.head())
    except Exception as e:
        print(f"Error adding columns to drug exposure file: {e}")

if __name__ == "__main__":
    rename_drug_exposure()
    remove_columns_drug_exposure()
    adding_columns_drug_exposure()