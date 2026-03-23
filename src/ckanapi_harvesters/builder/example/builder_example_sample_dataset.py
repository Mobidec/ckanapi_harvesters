#!python3
# -*- coding: utf-8 -*-
"""
Upload a sample dataset from the example package (needs to be online)

Samples are used to expose a portion of a dataset to a user, which can be publicly exposed,
so he can contact the data provider to demand full access.
"""

import os

import pandas as pd

from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.ckan_api import CkanApi

from ckanapi_harvesters.builder.example import example_package_xls
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


def run(ckan:CkanApi = None):
    BuilderPackage.unlock_external_code_execution()

    mdl_xls = BuilderPackage.from_excel(example_package_xls)
    ckan = mdl_xls.init_ckan(ckan)
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_limits(10000)  # reduce if server hangs up
    ckan.set_submit_timeout(5)
    ckan.set_verbosity(True)
    ckan.test_ckan_login(raise_error=True, verbose=True)
    # ckan.set_default_map_mode(datastore_info=True)  # uncomment to query DataStore information

    mdl_ckan = BuilderPackage.from_ckan(ckan, mdl_xls.package_name)

    sample_mdl = mdl_ckan.setup_sample_package(ckan)
    sample_df_dict = mdl_ckan.download_sample(ckan, limit=10)  # download the 10 first lines of each resource
    # sample_df_dict = mdl_ckan.download_sample(ckan, limit=10, include_files=False, empty_files=True)  # these options empty the file resources

    sample_df_dict["traces.csv"] = mdl_ckan.resource_builders["traces.csv"].download_sample_df(ckan, limit=120)  # specific request for this resource: take 120 lines
    # sample_df_dict["users.csv"] = pd.DataFrame()  # erase DataStore contents
    # sample_df_dict["generate_example.py"] = bytes()  # erase file contents

    sample_mdl.patch_request_full(ckan, reupload=True, sample_df_dict=sample_df_dict)
    sample_mdl.patch_request_final(ckan)

    print("Update done.")


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)


