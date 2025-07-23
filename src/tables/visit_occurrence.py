# src/visit_occurence.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
from pathlib import Path
import json

PERSON_COL_NAMES = [
"visit_occurence_id",
]

with open('config.json', 'r') as f:
    config = json.load(f)

clif_parquet_dir = config["clif_parquet_dir"]
file_path = Path(clif_parquet_dir) / "clif_hospitalization.parquet"


def rename_person():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        hospitalization_df = hospitalization_df.rename(columns={'hospitalization_id': 'visit_occurence_id'})       
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")
def adding_columns_hospital():
    try:
        hospitalization_df = pd.read_parquet(file_path)
        #hospitalization_df = hospitalization_df.assign(location_id=None, provider_id=None, care_site_id=None, person_source_value=None, gender_source_value=None, gender_source_concept_id=None, race_source_value=None, race_source_concept_id=None, ethnicity_source_value=None, ethnicity_source_concept_id=None)
        print(hospitalization_df.head())
    except Exception as e:
        print(f"Error processing file: {e}")


if __name__ == "__main__":
    rename_person()
    adding_columns_hospital()
