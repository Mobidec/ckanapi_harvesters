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
from ckanapi_harvesters.auxiliary.ckan_action import CkanNotFoundError, CkanSqlCapabilityError

from ckanapi_harvesters.builder.example import example_package_xls
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


def run(ckan:CkanApi = None):
    BuilderPackage.unlock_external_code_execution()  # comment to test if the safety feature is enabled

    mdl = BuilderPackage.from_excel(example_package_xls)
    ckan = mdl.init_ckan(ckan)
    ckan.initialize_from_cli_args()
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_verbosity(True)

    # ckan.apikey.clear()  # uncomment for specific test

    capability = ckan.test_sql_capabilities()
    try:
        ckan.api_help_show("datastore_search_sql")
    except CkanNotFoundError:
        print("No datastore_search_sql help")
    users_table_id = mdl.get_or_query_resource_id(ckan, "users.csv")
    try:
        ckan.datastore_search_sql(f'SELECT * FROM "{users_table_id}"')
    except CkanSqlCapabilityError:
        print("datastore_search_sql is not accessible")

    print("Tests done.")


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)

