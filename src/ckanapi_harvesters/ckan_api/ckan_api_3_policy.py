#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Dict, Tuple, Generator, Any, Union
import argparse

from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary import ckan_configuration
from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy

from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.ckan_api.ckan_api_2_readonly import CkanApiReadOnlyParams
from ckanapi_harvesters.ckan_api.ckan_api_2_readonly import CkanApiReadOnly

ckan_default_policy_keyword = "default"


class CkanApiPolicyParams(CkanApiReadOnlyParams):
    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None):
        super().__init__(proxies=proxies, ckan_headers=ckan_headers, http_headers=http_headers)
        self.policy_check_pre: bool = False
        self.policy_check_post: bool = True
        self.verbose_policy: bool = True

    def copy(self, new_identifier:str=None, *, dest=None):
        if dest is None:
            dest = CkanApiPolicyParams()
        super().copy(dest=dest)
        dest.policy_check_pre = self.policy_check_pre
        dest.policy_check_post = self.policy_check_post
        dest.verbose_policy = self.verbose_policy
        return dest


class CkanApiPolicy(CkanApiReadOnly):
    def __init__(self, url: str = None, *, proxies:Union[str,dict,ProxyConfig] = None,
                 apikey: Union[str,CkanApiKey] = None, apikey_file: str = None,
                 owner_org: str = None, params:CkanApiPolicyParams=None,
                 map:CkanMap=None, policy: CkanPackageDataFormatPolicy = None, policy_file: str = None,
                 identifier=None):
        """
        CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.

        :param url: url of the CKAN server
        :param proxies: proxies to use for requests
        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        :param policy: data format policy to use with policy_check function
        :param policy_file: path to a JSON file containing the data format policy to use with policy_check function
        :param owner_org: name of the organization to limit package_search (optional)
        :param params: other connection/behavior parameters
        :param map: map of known resources
        :param policy: data format policy to be used with the policy_check function.
        :param policy_file: path to a JSON file containing the data format policy to load.
        :param identifier: identifier of the ckan client
        """
        super().__init__(url=url, proxies=proxies, apikey_file=apikey_file, apikey=apikey,
                         owner_org=owner_org, map=map, identifier=identifier)
        self.policy: Union[CkanPackageDataFormatPolicy,None] = policy
        self.policy_source: Union[str,None] = None
        if policy_file is not None:
            self.load_policy(policy_file, base_dir=None)
        self.default_policy_load_on_map: bool = False
        if params is None:
            params = CkanApiPolicyParams()
        if proxies is not None:
            params.proxies = proxies
        self.params: CkanApiPolicyParams = params

    def copy(self, new_identifier: str = None, *, dest=None):
        if dest is None:
            dest = CkanApiPolicy()
        super().copy(new_identifier=new_identifier, dest=dest)
        if self.policy is not None:
            dest.policy = self.policy.copy()
        dest.default_policy_load_on_map = self.default_policy_load_on_map
        return dest

    def set_verbosity(self, verbosity:bool=True, verbose_extra:bool=None) -> None:
        """
        Enable/disable full verbose output

        :param verbosity: boolean. Cannot be None
        :return:
        """
        super().set_verbosity(verbosity=verbosity, verbose_extra=verbose_extra)
        if verbose_extra is not None:
            self.params.verbose_policy = verbose_extra

    def _setup_cli_ckan_parser(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        # overload adding support to load a policy from a file
        parser = super()._setup_cli_ckan_parser(parser=parser)
        parser.add_argument("--policy-file", type=str,
                            help="Path to a file containing the CKAN data format policy (json format)")
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None,
                             error_not_found:bool=True, default_proxies:dict=None, proxy_headers:dict=None,
                             proxies:dict=None, headers:dict=None) -> None:
        # overload adding support to load a policy from a file
        super()._cli_ckan_args_apply(args=args, base_dir=base_dir, error_not_found=error_not_found,
                                     default_proxies=default_proxies, proxy_headers=proxy_headers)
        if proxies is None:
            proxies = self.params.proxies
        if args.policy_file is not None:
            self.load_policy(args.policy_file, proxies=proxies, headers=headers, error_not_found=error_not_found)

    def query_default_policy(self, *, error_not_found:bool=False, load_error:bool=True) -> Union[CkanPackageDataFormatPolicy,None]:
        """
        Download default policy and return it without loading it in the policy attribute.

        :param error_not_found:
        :return:
        """
        self.map_resources(ckan_configuration.configuration_package_name, error_not_found=error_not_found, load_policy=False)  # load_policy=False or else infinite loop
        resource_info = self.map.get_resource_info(ckan_configuration.policy_resource, ckan_configuration.configuration_package_name,
                                                   error_not_mapped=error_not_found)
        if resource_info is not None:
            resource_id = resource_info.id
            url = resource_info.download_url
            _, response = self.resource_download(resource_id)
            payload = response.text
            return CkanPackageDataFormatPolicy.from_jsons(payload, source_file=url, load_error=load_error)
        else:
            return None

    def load_default_policy(self, *,
                            error_not_found:bool=False, load_error:bool=True, cancel_if_present:bool=False,
                            force:bool=False) -> Union[CkanPackageDataFormatPolicy,None]:
        """
        Function to load the default data format policy from the CKAN server.
        The default policy is defined in ckan_configuration

        :param error_not_found:
        :param cancel_if_present:
        :param force:
        :return:
        """
        if force:
            self.policy = None
        if self.policy_source == ckan_default_policy_keyword and cancel_if_present:
            return self.policy
        self.policy = self.query_default_policy(error_not_found=error_not_found, load_error=load_error)
        self.policy_source = ckan_default_policy_keyword
        return self.policy

    def load_policy(self, policy_file: str, base_dir: str = None, proxies:dict=None, headers:dict=None,
                    error_not_found: bool = True) -> CkanPackageDataFormatPolicy:
        """
        Load the CKAN data format policy from file (JSON format).

        :param policy_file: path to the policy file
        :param base_dir: base directory, if the apikey_file is a relative path
        :return:
        """
        if proxies is None:
            proxies = self.params.proxies
        if policy_file is None:
            policy_file = ckan_default_policy_keyword  # set to "default"
        if policy_file is not None and policy_file.lower() == ckan_default_policy_keyword:  # if equals "default"
            return self.load_default_policy(error_not_found=error_not_found, force=True, cancel_if_present=False)
        self.policy = CkanPackageDataFormatPolicy.from_json(policy_file, base_dir=base_dir, proxies=proxies, headers=headers,
                                                            error_not_found=error_not_found)
        self.policy_source = policy_file
        return self.policy

    def policy_check(self, package_list:Union[str,List[str]]=None, policy: CkanPackageDataFormatPolicy=None,
                     *, buffer:Dict[str, List[DataPolicyError]]=None, raise_error:bool=False,
                     verbose:bool=None) -> bool:
        """
        Enforce policy on mapped packages

        :param policy:
        :return:
        """
        success = True
        if package_list is None:
            package_list = self.map.packages.keys()  # check on all packages
        elif isinstance(package_list, str):
            package_list = [package_list]
        if policy is None:
            policy = self.policy
        if verbose is None:
            verbose = self.params.verbose_policy
        if policy is None:
            # no policy loaded at all
            return True
        if verbose:
            print(f"Testing policy {policy.label}")
        for package_name in package_list:
            package_info = self.get_package_info_or_request(package_name)
            package_buffer: List[DataPolicyError] = []
            success &= policy.policy_check_package(package_info, display_message=verbose,
                                                   package_buffer=package_buffer, raise_error=raise_error)
            if buffer is not None:
                buffer[package_info.name] = package_buffer
        if verbose:
            print(f"Data format policy {policy.label} success: {success}")
        return success

    def set_default_map_mode(self, datastore_info:bool=None, resource_view_list:bool=None,
                             organization_info:bool=None, license_list:bool=None,
                             load_policy:bool=None) -> None:
        super().set_default_map_mode(datastore_info=datastore_info, resource_view_list=resource_view_list,
                                     organization_info=organization_info, license_list=license_list)
        if load_policy is None:
            load_policy = self.default_policy_load_on_map
        self.default_policy_load_on_map = load_policy

    def map_resources(self, package_list:Union[str, List[str]]=None, *, params:dict=None,
                      datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None, license_list:bool=None,
                      only_missing:bool=True, error_not_found:bool=True,
                      owner_org:str=None, load_policy:bool=None) -> CkanMap:
        # overload including a call to load the default data format policy
        self.set_default_map_mode(load_policy=load_policy)
        map = super().map_resources(package_list=package_list, params=params, datastore_info=datastore_info,
                              resource_view_list=resource_view_list, organization_info=organization_info,
                              license_list=license_list, only_missing=only_missing, error_not_found=error_not_found,
                              owner_org=owner_org)
        load_policy = self.default_policy_load_on_map
        if load_policy:
            self.load_default_policy(cancel_if_present=True, load_error=False)
        return map

