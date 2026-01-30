#!python3
# -*- coding: utf-8 -*-
"""
Example code to upload the builder example to a CKAN server
"""
from typing import Tuple
import os
import re
import getpass

import pandas as pd
import numpy as np

from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.ckan_api import CkanApi

from ckanapi_harvesters.builder.example import example_package_xls
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


def run(ckan:CkanApi = None):
    BuilderPackage.unlock_external_code_execution()

    mdl = BuilderPackage.from_excel(example_package_xls)
    ckan = mdl.init_ckan(ckan)
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_limits(10000)  # reduce if server hangs up
    ckan.set_submit_timeout(5)
    ckan.set_verbosity(True)
    # ckan.set_default_map_mode(datastore_info=True)  # uncomment to query DataStore information

    # Test re-encoding the Excel file from the loaded model
    example_package_xls_reencoded = os.path.abspath("builder_package_example-reencoded.xlsx")
    mdl.to_excel(example_package_xls_reencoded)

    # Patch package: apply metadata and upload small resources
    reupload = True  # True: reuploads all documents and resets large datasets to the first document
    mdl.patch_request_full(ckan, reupload=reupload)

    # Upload large datasets
    threads = 3  # > 1: multi-threading mode - reduce if HTTP 502 errors
    mdl.upload_large_datasets(ckan, threads=threads)

    print("Update done.")


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)

