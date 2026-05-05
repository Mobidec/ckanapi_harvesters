#!python3
# -*- coding: utf-8 -*-
"""
Version of CkanApi to control a local CKAN server
"""
# see: https://github.com/ckan/ckanapi/blob/master/ckanapi/localckan.py
from typing import Union, List
from tempfile import TemporaryFile
from warnings import warn
import argparse

from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType, import_args_kwargs_dict
from ckanapi_harvesters.auxiliary.ckan_action import CkanActionResponse, LocalCkanActionResponse
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_errors import LocalApiKeyError, CkanActionNotFoundError
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.ckan_api.ckan_api import CkanApi, CkanApiParams

COPY_CHUNK = 1024*1024


class LocalCkanApiParams(CkanApiParams):
    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None,
                 username:str=None, context:dict=None):
        super().__init__(proxies=proxies, ckan_headers=ckan_headers, http_headers=http_headers)
        self.username: Union[str,None] = username
        if context is None:
            context = {}
        self.context: Union[dict,None] = context

    def _setup_cli_ckan_parser__params(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        # overload adding support to trigger admin mode
        parser = super()._setup_cli_ckan_parser__params(parser=parser)
        parser.add_argument("--username", type=str,
                            help="CKAN user name (local CKAN only)")
        parser.add_argument("--context", nargs="*",
                            help="Keyword arguments for context arguments (local CKAN only)")
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None,
                             error_not_found:bool=True, default_proxies:dict=None, proxy_headers:dict=None,
                             proxies:dict=None, headers:dict=None) -> None:
        # overload adding support to trigger admin mode
        super()._cli_ckan_args_apply(args=args, base_dir=base_dir, error_not_found=error_not_found,
                                     default_proxies=default_proxies, proxy_headers=proxy_headers)
        if args.username:
            self.username = args.username
        if args.context:
            self.context.update(import_args_kwargs_dict(args.context))


class LocalCkanApi(CkanApi):
    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org:str=None, params:LocalCkanApiParams=None,
                 map:CkanMap=None, policy: CkanPackageDataFormatPolicy = None, policy_file:str=None,
                 data_cleaner_upload:CkanDataCleanerABC=None,
                 identifier=None, username:str=None, context:dict=None):
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
        msg = "Not implemented: module was not tested"  # TODO: test the implementation of LocalCkanApi
        raise(NotImplementedError(msg))
        if url is None:
            # URL is used for local HTTP requests (_ckan_url_request, download_url_proxy, API DataStore dump)
            url = "http://localhost"  # TODO: confirm, special default port used?, HTTPS?
        super().__init__(url=url, proxies=proxies, apikey=apikey, apikey_file=apikey_file,
                         owner_org=owner_org, map=map, policy=policy, policy_file=policy_file,
                         data_cleaner_upload=data_cleaner_upload, identifier=identifier)
        if params is None:
            params = LocalCkanApiParams()
        self.params: LocalCkanApiParams = params

        from ckan.logic import get_action
        self._get_action = get_action

        if username is None and self.params.username is None:
            username = self.get_site_username()
        if username is not None:
            self.params.username = username
        if context is not None:
            self.params.context = context
        self.params.context["user"] = self.params.username

    def _api_get_site_user(self, *, ignore_auth:bool=True, params:dict=None) -> Union[dict,None]:
        """
        API call to get_site_user. With no params, returns the name of the current user logged in.

        Only internal services allowed to use this action

        :return: dict with information on the current user
        """
        if params is None: params = {}
        if ignore_auth is not None:
            params["ignore_auth"] = ignore_auth
        response = self._api_action_request("get_site_user", method=RequestType.Get, params=params, timeout=5)
        if response.success:
            user_dict = response.result
            return user_dict
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanActionNotFoundError(self, "User", response)
        else:
            raise response.default_error(self)

    def get_site_username(self, *, ignore_auth:bool=False, params:dict=None) -> str:
        user_info = self._api_get_site_user(ignore_auth=ignore_auth, params=params)
        return user_info["name"]

    def _api_action_request(self, action: str, *, method: RequestType, params: dict = None,
                            headers: dict = None, data: Union[dict, str, bytes] = None, json: dict = None,
                            files: List[tuple] = None,
                            timeout: float = None, _attempt_counts: int = 0,
                            _attempt_traceback: List[str] = None,
                            context:dict=None) -> CkanActionResponse:
        # copy dicts because actions may modify the dicts they are passed
        # (CKAN...you so crazy)
        data_dict = dict(data or [])
        if json is not None:
            data_dict.update(json)
        context = dict(self.params.context if context is None else context)
        if not self.apikey.is_empty():
            # FIXME: allow use of apikey to set a user in context?
            raise LocalApiKeyError()

        to_close = []
        try:
            for fieldname in files or []:
                f = files[fieldname]
                if isinstance(f, tuple):
                    # requests accepts (filename, file...) tuples
                    filename, f = f[:2]
                else:
                    filename = f.name
                try:
                    f.seek(0)
                except (AttributeError, IOError):
                    f = _write_temp_file(f)
                    to_close.append(f)

                from werkzeug.datastructures import FileStorage

                file_storage = FileStorage()
                file_storage.stream = f
                file_storage.filename = filename
                data_dict[fieldname] = file_storage

            result = self._get_action(action)(context, data_dict)
        finally:
            for f in to_close:
                f.close()
        return LocalCkanActionResponse(result, dry_run=False)


def _write_temp_file(f):
    """
    Pull all data from stream f into a temporary file

    Caller must close file returned.
    """
    out = TemporaryFile()
    while True:  # FIXME: check for maximum size?
        chunk = f.read(COPY_CHUNK)
        if not chunk:
            break
        out.write(chunk)
    return out
