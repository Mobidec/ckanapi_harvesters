#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Union
from contextlib import contextmanager

from ckanapi_harvesters.auxiliary.ckan_action import CkanNotFoundError
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC

from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.auxiliary.ckan_model import CkanApiTokenInfo, CkanUserInfo, CkanGroupInfo, CkanState
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType
from ckanapi_harvesters.auxiliary.ckan_errors import (ArgumentError, DuplicateNameError)
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiExtendedParams
from ckanapi_harvesters.ckan_api.ckan_api_6_user_access import CkanApiUserAccess



class CkanApiSysadminParams(CkanApiExtendedParams):
    pass


class CkanApiSysadmin(CkanApiUserAccess):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This extension implements methods which bear no direct relation with data manipulation.
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

    #% ---------- User management
    def _api_user_create(self, *, name: str, email: str, password: str, fullname: str=None,
                         about: str = None, image_url: str = None, plugin_extras: dict=None, params:dict=None) -> CkanUserInfo:
        """
        Call to API user_create
        Creates a new user (requires sysadmin user)
        """
        if params is None:
            params = {}
        params["name"] = name
        params["email"] = email
        params["password"] = password
        if fullname is not None:
            params["fullname"] = fullname
        if about is not None:
            params["about"] = about
        if image_url is not None:
            params["image_url"] = image_url
        if plugin_extras is not None:
            params["plugin_extras"] = plugin_extras
        response = self._api_action_request(f"user_create", method=RequestType.Post, json=params)
        if response.success:
            user_info = CkanUserInfo.from_dict(response.result)
            # update map
            self.map._update_user_info(user_info.copy())
            return user_info
        else:
            raise response.default_error(self)

    def user_create(self, *, name: str, email: str, password: str, fullname: str = None,
                    about: str = None, image_url: str = None, plugin_extras: dict = None,
                    params:dict = None, error_exists: bool = True) -> CkanUserInfo:
        """
        Call to API user_create, checking at first if the user does not exist.
        """
        user_found = True
        try:
            _ = self.user_show(name)
        except CkanNotFoundError:
            user_found = False
        if user_found and error_exists:
            raise DuplicateNameError("user", name)
        return self._api_user_create(name=name, email=email, password=password, fullname=fullname,
                                     about=about, image_url=image_url, plugin_extras=plugin_extras,
                                     params=params)

    #% ---------- Group management
    def _api_group_create(self, *, name: str, title : str = None, description: str = None,
                          image_url: str = None, state: Union[CkanState, str] = None, params:dict=None) -> Union[CkanGroupInfo, str]:
        """
        Call to API group_create
        Creates a new group (requires sysadmin user)
        """
        if params is None:
            params = {}
        params["name"] = name
        if title is not None:
            params["title"] = title
        if description is not None:
            params["description"] = description
        if image_url is not None:
            params["image_url"] = image_url
        if state is not None:
            params["state"] = str(state) if isinstance(state, CkanState) else state
        response = self._api_action_request(f"group_create", method=RequestType.Post, json=params)
        if response.success:
            if params.get("return_id_only", False):
                return response.result  # group id
            else:
                group_info = CkanGroupInfo.from_dict(response.result)
                # update map
                self.map._update_user_info(group_info.copy())
                return group_info
        else:
            raise response.default_error(self)

    def group_create(self, *, name: str, title : str = None, description: str = None,
                     image_url: str = None, state: Union[CkanState, str] = None,
                     params:dict = None, error_exists: bool = True) -> Union[CkanGroupInfo, str]:
        """
        Call to API group_create, checking at first if the group does not exist.
        """
        group_found = True
        try:
            _ = self.group_show(name)
        except CkanNotFoundError:
            group_found = False
        if group_found and error_exists:
            raise DuplicateNameError("group", name)
        return self._api_group_create(name=name, title=title, description=description,
                                      image_url=image_url, state=state,
                                      params=params)

