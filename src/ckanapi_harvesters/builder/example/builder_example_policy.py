#!python3
# -*- coding: utf-8 -*-
"""
Tests to perform after the example package was uploaded
"""
from typing import Tuple
import os
import re
import json
import io

import pandas as pd
import numpy as np

from ckanapi_harvesters.auxiliary import CkanMap
from ckanapi_harvesters.builder.builder_package import BuilderPackage
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.policies.data_format_policy import (SingleValueListPolicy, ValueListPolicy, StringValueSpecification,
                                                            ListChoiceMode, CustomFieldsPolicy, CustomFieldSpecification,
                                                            GroupedValueListPolicy, ErrorLevel, DataPolicyError,
                                                            StringMatchMode, TagListPolicy, TagGroupsListPolicy)

from ckanapi_harvesters.builder.specific.configuration_builder import ConfigurationBuilder
from ckanapi_harvesters.builder.example import example_package_xls
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


enable_write = False  # be careful, setting this to True could erase your CKAN data format policy !!!


def run(ckan:CkanApi = None):
    BuilderPackage.unlock_external_code_execution()  # comment to test if the safety feature is enabled

    mdl = BuilderPackage.from_excel(example_package_xls)
    ckan = mdl.init_ckan(ckan)
    ckan.initialize_from_cli_args()
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_verbosity(False)
    ckan.verbose_policy = True

    policy = CkanPackageDataFormatPolicy()
    policy.resource_format = SingleValueListPolicy(ValueListPolicy([StringValueSpecification("CSV")]), extra_values=ListChoiceMode.NoExtra)
    policy.package_custom_fields = CustomFieldsPolicy([
                                                        CustomFieldSpecification(key="New field", mandatory=True),
                                                        CustomFieldSpecification(key="Algorithm", values=["Random"], match_mode=StringMatchMode.Match, mandatory=True)], error_level=ErrorLevel.Error)
    policy.package_tags = TagGroupsListPolicy([TagListPolicy([StringValueSpecification("Test")], group_name="Vocabulary_Test")])
    policy.package_mandatory_attributes = {"description", "author"}
    ckan.policy = policy

    policy_dict = policy.to_dict(sets_as_lists=False)
    policy_from_dict = CkanPackageDataFormatPolicy.from_dict(policy_dict)
    policy_dict_bis = policy_from_dict.to_dict(sets_as_lists=False)
    assert(policy_dict == policy_dict_bis)

    # serialisation
    policy_dict = policy.to_dict(sets_as_lists=True)
    policy_json_file = os.path.abspath("policy_py.json")
    with open(policy_json_file, "w") as f:
        json.dump(policy_dict, f, indent=4)
    with open(policy_json_file, "r") as f:
        policy_json_dict = json.load(f)
    # policy_json = CkanPackageDataFormatPolicy.from_dict(policy_json_dict)
    policy_json = CkanPackageDataFormatPolicy.from_json(policy_json_file)
    assert(policy_dict == policy_json_dict)

    # test on local definition (offline)
    buffer = {}
    success = mdl.local_policy_check(policy, buffer=buffer)
    # test if an error is raised (this mode does not display all messages)
    try:
        success = mdl.local_policy_check(policy)
    except DataPolicyError as e:
        print("Exception: " + str(e))
        assert(not success)
    else:
        print(f"No exception / success={success}")

    # test on remote definition (CKAN API)
    print("Test on remote")
    buffer = {}
    ckan.map_resources(mdl.package_name)
    success = ckan.policy_check(buffer=buffer)

    ckan.set_verbosity(True)

    # update vocabularies: deprecated
    # if enable_write:
    #     ckan.vocabularies_clear()
    #     ckan.initiate_vocabularies_from_policy(policy, remove_others=True)

    # upload default policy
    config_ckan = ConfigurationBuilder(ckan, ckan.owner_org)
    if enable_write:
        config_ckan.patch_policy(ckan, policy, reduced_size=False)

    # download default policy
    # default_policy = ckan.load_default_policy(force=True)
    config_ckan.load_default_policy(ckan)

    # check all packages against policy
    ckan.owner_org = None
    print(" ")
    buffer = {}
    config_ckan.policy_check(ckan, policy=policy, buffer=buffer, verbose=True)

    print("Tests done.")


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)

