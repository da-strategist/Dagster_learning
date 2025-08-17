
# src/ETL_project/definitions.py
from dagster import Definitions, load_assets_from_modules
import src.ETL_project.assets.asset as asset_module

# Load all assets defined in the asset module
all_assets = load_assets_from_modules([asset_module])

# Register them with Dagster
defs = Definitions(
    assets=all_assets,
)
