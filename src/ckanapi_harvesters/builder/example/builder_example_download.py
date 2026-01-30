#!python3
# -*- coding: utf-8 -*-
"""
Example code to download the builder example from a CKAN server
"""
from typing import Tuple
import os
import re

import pandas as pd
import numpy as np

from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.ckan_api import CkanApi

from ckanapi_harvesters.builder.example import example_package_xls
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
example_package_download_dir = os.path.abspath("package_download")


def run(ckan:CkanApi = None):
    BuilderPackage.unlock_external_code_execution()

    mdl = BuilderPackage.from_excel(example_package_xls)
    ckan = mdl.init_ckan(ckan)
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_verbosity(True)

    # download into example_package_download_dir
    threads = 3  # > 1: number of threads to download large datasets
    mdl.download_request_full(ckan, example_package_download_dir, full_download=True, threads=threads,
                              skip_existing=False, rm_dir=True)

    print("Package downloaded in")
    print(example_package_download_dir)


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)



