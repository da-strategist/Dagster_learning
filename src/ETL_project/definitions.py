from pathlib import Path
import dagster as dg
from dagster import definitions, load_from_defs_module
import pandas as pd


@definitions
def defs():
    return load_from_defs_module(project_root=Path(__file__).parent.parent.parent)
