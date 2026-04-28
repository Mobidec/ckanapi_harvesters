#!python3
# -*- coding: utf-8 -*-
"""
Function to load the example package
"""
from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.builder.example import example_package_xls

def load_example_package() -> BuilderPackage:
    BuilderPackage.unlock_external_code_execution()
    mdl = BuilderPackage.from_excel(example_package_xls)
    return BuilderPackage(src=mdl)

from ckanapi_harvesters.builder.builder_package_1_basic import load_aux_pages_df

