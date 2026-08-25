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
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiExtendedParams
from ckanapi_harvesters.ckan_api.ckan_api_6_user_access import CkanApiUserAccess



class CkanApiSysadminParams(CkanApiExtendedParams):
    pass


class CkanApiSysadmin(CkanApiUserAccess):
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

    ## User impersonation
    def _api_api_token_list(self, user_id: str, params:dict=None) -> List[CkanApiTokenInfo]:
        """
        Call to API api_token_list
        Lists existing API tokens for a given user (with names and ids, not the actual tokens).

        :param user_id: user id
        """
        if params is None:
            params = {}
        params["user_id"] = user_id
        response = self._api_action_request(f"api_token_list", method=RequestType.Get, params=params)
        if response.success:
            # get token
            token_list = [CkanApiTokenInfo(element) for element in response.result]
            return token_list
        else:
            raise response.default_error(self)

    def _api_api_token_create(self, user_id: str, token_name:str, params:dict=None) -> str:
        """
        Call to API api_token_create
        Creates a token for a given user.

        :param user_id: user id
        :param token_name: unique name for the token to be created
        """
        if params is None:
            params = {}
        params["user"] = user_id
        params["name"] = token_name
        response = self._api_action_request(f"api_token_create", method=RequestType.Post, json=params)
        if response.success:
            # get token
            token = response.result["token"]
            return token
        else:
            raise response.default_error(self)

    def _api_api_token_revoke(self, *, token:str=None, token_id:str=None, params:dict=None) -> bool:
        """
        Call to API api_token_revoke
        Removes a token for a given user. Requires either token or token_id.

        :param token: token to be removed
        :param token_id: ID of the token to be removed
        """
        if params is None:
            params = {}
        if token is not None:
            params["token"] = token
        elif token_id is not None:
            params["jti"] = token_id
        else:
            raise ArgumentError("Either token or token_id is required.")
        response = self._api_action_request(f"api_token_revoke", method=RequestType.Post, json=params)
        if response.success:
            return True
        else:
            raise response.default_error(self)

