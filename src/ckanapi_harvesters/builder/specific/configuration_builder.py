#!python3
# -*- coding: utf-8 -*-
"""
CKAN configuration builder
"""
from typing import Union, List, Dict

from ckanapi_harvesters.auxiliary import ckan_configuration
from ckanapi_harvesters.auxiliary.ckan_model import CkanState
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError
from ckanapi_harvesters.builder.builder_resource import BuilderResourceUnmanaged
from ckanapi_harvesters.builder.builder_resource_datastore_file import BuilderDataStoreFile
from ckanapi_harvesters.builder.specific_builder_abc import SpecificBuilderABC
from ckanapi_harvesters.builder.builder_package_1_basic import example_package_resources_dir


class ConfigurationBuilder(SpecificBuilderABC):
    def __init__(self, ckan:CkanApi, organization_name:str):
        super().__init__(ckan, package_name=ckan_configuration.configuration_package_name, organization_name=organization_name,
                         title="Configuration for scripts",
                         description="Configuration for use with Python scripts",
                         private=True,
                         )
        self.package_attributes.state = CkanState.Active
        self.package_attributes.private = False
        self.set_resources_base_dir(example_package_resources_dir)
        self.resource_builders[ckan_configuration.policy_resource] = \
            BuilderResourceUnmanaged(parent=self, name=ckan_configuration.policy_resource, format="JSON",
                                     description="CKAN Data format policy for use with Python scripts")
        self.resource_builders[ckan_configuration.datastore_sample_resource] = \
            BuilderDataStoreFile(parent=self, name=ckan_configuration.datastore_sample_resource, format="CSV",
                                 description="Sample DataStore",
                                 file_name="users_local.csv")

    def patch_policy(self, ckan:CkanApi, policy: CkanPackageDataFormatPolicy,
                     *, reduced_size:bool=None, full_patch:bool=True, update_ckan:bool=True):
        package_info = self.patch_request_package(ckan)
        package_id = package_info.id
        if full_patch:
            self.patch_request_full(ckan, reupload=True)
            self.upload_large_datasets(ckan)
        if policy is not None:
            payload = policy.to_jsons(reduced_size=reduced_size).encode()
            policy_builder: BuilderResourceUnmanaged = self.resource_builders[ckan_configuration.policy_resource]
            policy_builder.patch_request(ckan, package_id, payload=payload, reupload=True)
        else:
            # delete data format policy
            self.resource_builders[ckan_configuration.policy_resource].delete_request(ckan)
        if update_ckan:
            ckan.policy = policy

    def load_default_policy(self, ckan:CkanApi) -> CkanPackageDataFormatPolicy:
        return ckan.load_default_policy(force=True)

    def policy_check(self, ckan: CkanApi,
                    package_list: Union[str, List[str]] = None, *, owner_org:str=None,
                    policy:CkanPackageDataFormatPolicy=None, buffer:Dict[str, List[DataPolicyError]]=None,
                    raise_error:bool=False, verbose:bool=None) -> bool:
        """
        Check package list against currently loaded data format policy loaded in CKAN (or the one provided by argument).
        If not provided, the package list is the full list of packages, restrained to an organization (requires an API request).
        :param ckan:
        :param package_list:
        :param owner_org:
        :param policy:
        :param buffer:
        :param raise_error:
        :param verbose:
        :return:
        """
        # recommended to run load_default_policy before
        package_list = ckan.complete_package_list(package_list, owner_org=owner_org)
        ckan.map_resources(package_list, owner_org=owner_org)
        return ckan.policy_check(package_list, policy=policy, buffer=buffer, verbose=verbose, raise_error=raise_error)


if __name__ == '__main__':
    ckan = CkanApi()
    ckan.initialize_from_cli_args()
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=True)
    ckan.set_verbosity(True)

    mdl = ConfigurationBuilder(ckan, organization_name=ckan.owner_org)
    ckan = mdl.init_ckan(ckan)
    ckan.test_ckan_login(raise_error=True, verbose=True)

    mdl.patch_request_full(ckan)
    mdl.patch_request_final(ckan)

    print("Done.")


