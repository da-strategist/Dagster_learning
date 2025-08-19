
# src/ETL_project/definitions.py

import dagster as dg
from dagster import Definitions, load_assets_from_modules

from ETL_project.assets import data_ext #as asset_module
#import src.ETL_project.assets.assets as assets_module 
#import src.ETL_project.resources as resource_module
from src.ETL_project import resources


# Load all assets defined in the asset module
all_assets = load_assets_from_modules([data_ext])
# resources.db_resource is the dict mapping resource names -> ResourceDefinition
database_resources = resources.db_resource


# Register them with Dagster
defs = dg.Definitions(
    assets=all_assets,
    resources=database_resources
)
