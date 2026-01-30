#!python3
# -*- coding: utf-8 -*-
"""
Function to load the example package
"""
import pandas as pd

from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.builder.example import example_package_xls

def load_example_package() -> BuilderPackage:
    BuilderPackage.unlock_external_code_execution()
    mdl = BuilderPackage.from_excel(example_package_xls)
    return BuilderPackage(src=mdl)

def load_help_page_df(*, engine:str=None) -> pd.DataFrame:
    with pd.ExcelFile(example_package_xls, engine=engine) as help_file:
        help_df = pd.read_excel(help_file, sheet_name="help", header=None)
        help_file.close()
    return help_df

