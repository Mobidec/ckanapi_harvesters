#!python3
# -*- coding: utf-8 -*-
"""
Tests to perform after the example package was uploaded
"""
from typing import Tuple
import os
import re
import getpass
import json

import pandas as pd
import numpy as np

from ckanapi_harvesters.auxiliary import CkanMap
from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.ckan_api import CkanApi

from ckanapi_harvesters.builder.example import example_package_xls
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


if __name__ == '__main__':
    BuilderPackage.unlock_external_code_execution()  # comment to test if the safety feature is enabled

    mdl = BuilderPackage.from_excel(example_package_xls)

    ckan = CkanApi()
    ckan = mdl.init_ckan(ckan)
    # mdl.update_ckan_map(ckan)
    # map_from_mdl = ckan.map.copy()

    # Test re-encoding the Excel file from the loaded model
    example_package_xls_reencoded = os.path.abspath("builder_package_example-reencoded.xlsx")
    mdl.to_excel(example_package_xls_reencoded)

    # export mdl to dict and re-import
    base_dir = os.path.abspath(".")
    mdl_dict = mdl.to_dict(base_dir=base_dir)
    mdl_from_dict = BuilderPackage.from_dict(mdl_dict, base_dir=base_dir)
    mdl_dict_bis = mdl_from_dict.to_dict(base_dir=base_dir)
    assert(mdl_dict == mdl_dict_bis)

    # test json serialization
    example_package_json_reencoded = os.path.abspath("builder_package_example-reencoded.json")
    with open(example_package_json_reencoded, "w") as json_file:
        json.dump(mdl_dict, json_file, indent=4)

    # test copy constructors
    ckan_copy = ckan.copy()
    mdl_copy = mdl.copy()
    mdl_copy_dict = mdl_copy.to_dict(base_dir=base_dir)
    assert(mdl_dict == mdl_copy_dict)

    print("Tests done.")


