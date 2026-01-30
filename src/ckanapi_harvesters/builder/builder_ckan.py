#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements the ckan connection definition.
"""
from typing import Union
import os
import json

import pandas as pd

from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_defs import environ_keyword
from ckanapi_harvesters.auxiliary.path import make_path_relative, path_rel_to_dir
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.auxiliary.ckan_auxiliary import ca_file_rel_to_dir, ca_arg_to_str
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy


class BuilderCkan:
    def __init__(self, url:str=None, apikey_file:str=None, proxy:ProxyConfig=None):
        if proxy is None:
            proxy = ProxyConfig()
        self.url: str = url
        self.apikey_file: str = apikey_file
        self._proxy_config: ProxyConfig = proxy
        self._policy_file: Union[str, None] = None
        self._policy: Union[CkanPackageDataFormatPolicy, None] = None
        self.ckan_ca: Union[bool, str, None] = None
        self.extern_ca: Union[bool, str, None] = None
        self._ckan_ca_src: Union[str, None] = None
        self._extern_ca_src: Union[str, None] = None
        self.options_string: Union[str, None] = None
        self.comment: Union[str, None] = None

    def __str__(self):
        return f"CKAN builder"

    def __copy__(self):
        return self.copy()

    def copy(self) -> "BuilderCkan":
        dest = BuilderCkan()
        dest.url = self.url
        dest.apikey_file = self.apikey_file
        dest._proxy_config = self._proxy_config.copy()
        dest._policy_file = self._policy_file
        if self._policy is not None:
            dest._policy = self._policy.copy()
        dest.ckan_ca = self.ckan_ca
        dest.extern_ca = self.extern_ca
        dest._ckan_ca_src = self._ckan_ca_src
        dest._extern_ca_src = self._extern_ca_src
        dest.options_string = self.options_string
        dest.comment = self.comment
        return dest

    @property
    def proxies(self) -> dict:
        return self._proxy_config.proxies
    @proxies.setter
    def proxies(self, proxies:dict) -> None:
        self._proxy_config.proxies = proxies
    @property
    def proxy_string(self) -> str:
        return self._proxy_config.proxy_string
    @proxy_string.setter
    def proxy_string(self, proxies:str) -> None:
        self._proxy_config.proxy_string = proxies

    @property
    def policy_file(self) -> str:
        return self._policy_file
    def set_policy_file(self, policy_file:str, *, ckan:CkanApi=None, base_dir:str=None, proxies:dict=None,
                        error_not_found:bool=True) -> None:
        if proxies is None:
            proxies = self._proxy_config.proxies
        self._policy_file = policy_file
        if policy_file is not None:
            self._policy = None
            if ckan is None:
                ckan = self.init_ckan(base_dir=base_dir)  # initiate a temporary ckan object to enable the load of the default policy
                # self._policy = CkanPackageDataFormatPolicy.from_json(policy_file, base_dir=base_dir, proxies=proxies, error_not_found=error_not_found)
            self._policy = ckan.load_policy(policy_file, base_dir=base_dir, proxies=proxies, error_not_found=error_not_found)
            self._policy_file = self._policy.source_file
        else:
            self._policy = None
    @property
    def policy(self) -> CkanPackageDataFormatPolicy:
        return self._policy

    def _load_from_df(self, ckan_df: pd.DataFrame, base_dir: str,
                      proxies:dict, error_not_found:bool=True) -> None:
        """
        Function to load builder parameters from a DataFrame, usually from an Excel worksheet

        :param ckan_df:
        :return:
        """
        ckan_df.columns = ckan_df.columns.map(str.lower)
        ckan_df.columns = ckan_df.columns.map(str.strip)
        # order is important here:
        if "ckan url" in ckan_df.columns:
            self.url = _string_from_element(ckan_df.pop("ckan url"))
        if "ckan api key file" in ckan_df.columns:
            self.apikey_file = _string_from_element(ckan_df.pop("ckan api key file"))
        if "proxies" in ckan_df.columns:
            self._proxy_config.proxy_string = _string_from_element(ckan_df.pop("proxies"))
        if "proxy authentication file" in ckan_df.columns:
            proxy_auth_file = _string_from_element(ckan_df.pop("proxy authentication file"))
            if proxy_auth_file is not None:
                self._proxy_config.load_proxy_auth_from_file(proxy_auth_file, base_dir=base_dir)
        self.ckan_ca = None
        self._ckan_ca_src = None
        if "ckan remote ca" in ckan_df.columns:
            ca_cert = _string_from_element(ckan_df.pop("ckan remote ca"))
            self.ckan_ca, self._ckan_ca_src = ca_file_rel_to_dir(ca_cert, base_dir)
        self.extern_ca = None
        self._extern_ca_src = None
        if "external ca" in ckan_df.columns:
            ca_cert = _string_from_element(ckan_df.pop("external ca"))
            self.extern_ca, self._extern_ca_src = ca_file_rel_to_dir(ca_cert, base_dir)
        if "data format policy file" in ckan_df.columns:
            policy_file = _string_from_element(ckan_df.pop("data format policy file"))
            self.set_policy_file(policy_file, base_dir=base_dir, proxies=proxies,
                                 error_not_found=error_not_found)
        if "options" in ckan_df.columns:
            self.options_string = _string_from_element(ckan_df.pop("options"))
        if "comment" in ckan_df.columns:
            self.comment = _string_from_element(ckan_df.pop("comment"))

    def _to_dict(self, base_dir:str) -> dict:
        """
        Function to export builder parameters to an Excel worksheet, using the same fields as the input format

        :see: _load_from_df
        :see: to_xls
        :return:
        """
        ckan_dict = dict()
        ckan_dict["CKAN URL"] = self.url
        ckan_dict["CKAN API key file"] = self.apikey_file
        ckan_dict["Proxies"] = self._proxy_config.proxy_string
        ckan_dict["Proxy authentication file"] = make_path_relative(self._proxy_config.proxy_auth_file, base_dir)
        ckan_dict["CKAN remote CA"] = ca_arg_to_str(self.ckan_ca, base_dir=base_dir, source_string=self._ckan_ca_src)
        ckan_dict["External remote CA"] = ca_arg_to_str(self.extern_ca, base_dir=base_dir, source_string=self._extern_ca_src)
        ckan_dict["Data format policy file"] = make_path_relative(self.policy_file, base_dir)
        ckan_dict["Options"] = self.options_string
        ckan_dict["Comment"] = self.comment
        return ckan_dict

    def _get_builder_df_help_dict(self) -> dict:
        ckan_help_dict = {
            "CKAN URL": "URL of the CKAN server e.g. https://demo.ckan.org/",
            "CKAN API key file": "Path to a file containing the API key in the first line",
            "Proxies": 'Proxies configuration, either one unique url or {"http": "http://proxy:8082", "https": "http://proxy:8082"}',
            "Proxy authentication file": "Path to a text file containing 3 lines with the proxy authentication method, username and password, relative to this Excel workbook folder. "
                                         + "This applies to all connexions (to CKAN server and external resources)",
            "CKAN remote CA": "Path to a custom CA certificate for the CKAN server (.pem), relative to this Excel workbook folder",
            "External CA": "Path to a custom CA certificate used for connexions other than the CKAN server, relative to this Excel workbook folder (.pem)",
            "Data format policy file": "Path to a JSON file containing the CKAN data format policy, relative to this Excel workbook folder",
            "Options": "List of options to initialize the CKAN API object in CLI format",
        }
        return ckan_help_dict

    def _load_from_dict(self, ckan_dict: dict, base_dir: str, proxies:dict=None) -> None:
        ckan_df = pd.DataFrame([ckan_dict], index=["Value"])
        ckan_df = ckan_df.transpose()
        ckan_df.index.name = "Attribute"
        ckan_df = ckan_df.transpose()
        self._load_from_df(ckan_df, base_dir=base_dir, proxies=proxies)

    def _get_builder_df(self, base_dir:str) -> pd.DataFrame:
        """
        Converts the result of method _to_dict() into a DataFrame

        :return:
        """
        ckan_dict = self._to_dict(base_dir=base_dir)
        ckan_help_dict = self._get_builder_df_help_dict()
        ckan_df = pd.DataFrame([ckan_dict, ckan_help_dict], index=["Value", "Help"])
        ckan_df = ckan_df.transpose()
        ckan_df.index.name = "Attribute"
        return ckan_df

    def from_ckan(self, ckan: CkanApi) -> None:
        """
        Initialize fields from a CKAN instance.
        """
        self.url = ckan.url
        self.apikey_file = ckan.apikey.apikey_file
        self._proxy_config = ckan.params._proxy_config
        self.ckan_ca = ckan.params.ckan_ca
        self.extern_ca = ckan.params.extern_ca
        if ckan.policy is not None and ckan.policy_source is not None:
            self.set_policy_file(ckan.policy_source)

    def init_ckan(self, base_dir:str, ckan:CkanApi=None, default_proxies:dict=None,
                  proxies:Union[str,dict,ProxyConfig]=None) -> CkanApi:
        """
        Initialize a CKAN instance, following the parameters of the Excel workbook.
        The parameters from Excel have precedence on the values already contained in the CKAN object.
        However, the Excel workbook might not contain sufficient information.

        :param base_dir:
        :param ckan:
        :param default_proxies:
        :param proxies:
        :return:
        """
        if ckan is None:
            ckan = CkanApi(url=self.url)
        if self.url is not None:
            ckan.url = self.url.strip()
        if self.apikey_file is not None:
            ckan.load_apikey(self.apikey_file, base_dir=base_dir)
        if self.ckan_ca is not None:
            ckan.ckan_ca = self.ckan_ca
        if self.extern_ca is not None:
            ckan.extern_ca = self.extern_ca
        if proxies is not None:
            # proxies given by argument are prioritary over those specified in the builder
            ckan.set_proxies(proxies)
        elif self._proxy_config.is_defined():
            ckan._proxy_config = self._proxy_config
            ckan._proxy_config.replace_default_proxy(default_proxies)
        elif default_proxies is not None:
            ckan._proxy_config.proxies = default_proxies
        if self.policy is not None:
            ckan.policy = self.policy
        if self.options_string is not None:
            ckan.initialize_from_options_string(self.options_string,
                                                base_dir=base_dir, default_proxies=default_proxies)
        return ckan

