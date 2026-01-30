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


def run(ckan:CkanApi = None):
    BuilderPackage.unlock_external_code_execution()  # comment to test if the safety feature is enabled

    mdl = BuilderPackage.from_excel(example_package_xls)
    ckan = mdl.init_ckan(ckan)
    ckan.initialize_from_cli_args()
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_verbosity(True)

    # Test re-encoding the Excel file from the loaded model
    example_package_xls_reencoded = os.path.abspath("builder_package_example-reencoded.xlsx")
    mdl.to_excel(example_package_xls_reencoded)

    # map package
    ckan.map_resources(mdl.package_name, datastore_info=True, license_list=True)
    map_init = ckan.map.copy()
    ckan.purge(purge_map=True)

    # Test using the model to update CKAN map (update_ckan_map)
    mdl.info_request_full(ckan)
    map_queried = ckan.map.copy()
    ckan.purge(purge_map=True)
    mdl.update_ckan_map(ckan)
    map_from_mdl = ckan.map.copy()

    # Test saving the map to a dictionary
    dict_map_queried = map_queried.to_dict()
    map_queried_from_dict = CkanMap.from_dict(dict_map_queried)

    dict_map_from_mdl = map_from_mdl.to_dict()
    map_from_mdl_from_dict = CkanMap.from_dict(dict_map_from_mdl)

    # test the function that recreates the Excel file from the online information
    ckan.purge(purge_map=True)
    mdl_api = BuilderPackage.from_ckan(ckan, mdl.package_name)
    example_package_xls_from_api = os.path.abspath("builder_package_example-from-api.xlsx")
    mdl_api.to_excel(example_package_xls_from_api)

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
        json_file.close()

    # test copy constructors
    ckan_copy = ckan.copy()
    mdl_copy = mdl.copy()
    mdl_copy_dict = mdl_copy.to_dict(base_dir=base_dir)
    assert(mdl_dict == mdl_copy_dict)

    print("Tests done.")


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)

