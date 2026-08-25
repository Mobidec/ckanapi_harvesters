#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Union
from contextlib import contextmanager

from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC

from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.auxiliary.ckan_model import CkanApiTokenInfo
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType
from ckanapi_harvesters.auxiliary.ckan_errors import (ArgumentError)
from ckanapi_harvesters.ckan_api.ckan_api_7_sysadmin import CkanApiSysadminParams
from ckanapi_harvesters.ckan_api.ckan_api_7_sysadmin import CkanApiSysadmin



class CkanApiSysadminParamsDeprecated(CkanApiSysadminParams):
    default_token_name = "CkanApiSysadmin"


class CkanApiSysadminDeprecated(CkanApiSysadmin):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements methods only available to sysadmins among which:
    - requests to impersonate another user
    """

    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org:str=None, params:CkanApiSysadminParams=None,
                 map:CkanMap=None, policy: CkanPackageDataFormatPolicy = None, policy_file:str=None,
                 data_cleaner_upload:CkanDataCleanerABC=None,
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
        :param data_cleaner_upload: data cleaner object to use before uploading to a CKAN DataStore.
        :param identifier: identifier of the ckan client
        """
        super().__init__(url=url, proxies=proxies, apikey=apikey, apikey_file=apikey_file,
                         owner_org=owner_org, map=map, policy=policy, policy_file=policy_file,
                         data_cleaner_upload=data_cleaner_upload, identifier=identifier)
        if params is None:
            params = CkanApiSysadminParams()
        if proxies is not None:
            params.proxies = proxies
        self.params: CkanApiSysadminParams = params

    def copy(self, new_identifier: str = None, *, dest=None):
        if dest is None:
            dest = CkanApiSysadmin()
        super().copy(new_identifier=new_identifier, dest=dest)
        return dest

    ## User impersonation: does not work
    @contextmanager
    def user_impersonate(self, user_name:str, *, token_name:str=None) -> "CkanApiSysadmin":
        user_id = self.get_user_id_or_request(user_name)
        if token_name is None:
            token_name = self.params.default_token_name
        # create token for the impersonation
        token = self._api_api_token_create(user_id, token_name)
        # initialize CKAN instance
        try:
            user_ckan = self.copy()
            user_ckan.apikey = CkanApiKey(token)
            yield user_ckan
        finally:
            # when finished or error, destroy token
            self._api_api_token_revoke(token=token)

    def api_tokens_clear_user(self, user_name:str, *, token_name:str=None):
        """
        Remove tokens of a given name for a user

        :param user_name: name or id of the user
        :param token_name: name of the token to be removed - if not specified, params.default_token_name is used
        """
        user_id = self.get_user_id_or_request(user_name)
        if token_name is None:
            token_name = self.params.default_token_name
        token_list = self._api_api_token_list(user_id)
        token_ids = [token.id for token in token_list if token.name == token_name]
        for token_id in token_ids:
            self._api_api_token_revoke(token_id=token_id)

    def api_tokens_clear_full(self, *, token_name: str = None):
        """
        Remove tokens of a given name for all mapped users

        :param token_name: name of the token to be removed - if not specified, params.default_token_name is used
        """
        if token_name is None:
            token_name = self.params.default_token_name
        for user_id in self.map.users.keys():
            token_list = self._api_api_token_list(user_id)
            token_ids = [token.id for token in token_list if token.name == token_name]
            for token_id in token_ids:
                self._api_api_token_revoke(token_id=token_id)


