
# src/ETL_project/definitions.py

import dagster as dg
from dagster import Definitions, load_assets_from_modules
from src.ETL_project.assets import assets 
#import src.ETL_project.resources as resource_module
from src.ETL_project import resources


# Load all assets defined in the asset module
all_assets = dg.load_assets_from_modules([assets])
database_resources = resources.db_resource()


# Register them with Dagster
defs = dg.Definitions(
    assets=all_assets,
    resources=database_resources
)
