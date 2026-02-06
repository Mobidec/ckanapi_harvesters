#!python3
# -*- coding: utf-8 -*-
"""

"""
from abc import ABC
from typing import List, Dict, Callable, Union, Any, Generator, Sequence, Tuple, Collection
from collections import OrderedDict
import time
import copy
from warnings import warn
import argparse
import shlex
import os

import requests
from requests.auth import AuthBase
import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.external_code_import import unlock_external_code_execution
from ckanapi_harvesters.auxiliary.ckan_configuration import download_external_resource_urls, \
    unlock_external_url_resource_download, allow_no_ca, unlock_no_ca
from ckanapi_harvesters.auxiliary.ckan_defs import environ_keyword
from ckanapi_harvesters.auxiliary.path import path_rel_to_dir
from ckanapi_harvesters.auxiliary.urls import urlsep, url_join
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType, max_len_debug_print, assert_or_raise
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_action import CkanActionResponse, CkanActionError, CkanNotFoundError
from ckanapi_harvesters.auxiliary.ckan_errors import (MaxRequestsCountError, UnexpectedError, InvalidParameterError,
                                                      ExternalUrlLockedError, UrlError, NoCAVerificationError, RequestError)
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.ckan_api.ckan_api_params import CkanApiParamsBasic, CkanApiDebug

CKAN_API_VERSION = 3

use_ckan_owner_org_as_default:bool = True  # the owner_org field of CkanApi is destined to default the owner organization (or else it should be None)
ckan_request_proxy_default_auth_if_ckan:bool = True  # fill authentification headers for requests with CkanApi requests proxy method if same domain is used by default

## Abstract class
class CkanApiABC(ABC):
    pass



## Main class ------------------
class CkanApiBase(CkanApiABC):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements the basic parameters and request functions.
    """
    CKAN_URL_ENVIRON = "CKAN_URL"

    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org:str=None, params:CkanApiParamsBasic=None,
                 identifier=None):
        """
        CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.

        :param url: url of the CKAN server
        :param proxies: proxies to use for requests
        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        :param owner_org: name of the organization to limit package_search (optional)
        :param params: other connection/behavior parameters
        :param identifier: identifier of the ckan client
        """
        if identifier is None: identifier = ""
        if apikey is None or not isinstance(apikey, CkanApiKey):
            apikey = CkanApiKey(apikey=apikey, apikey_file=apikey_file)
        if params is None:
            params = CkanApiParamsBasic()
        if proxies is not None:
            params.proxies = proxies
        self.identifier = identifier  # variable for debugging purposes
        self._ckan_url: str = ""
        self.apikey: CkanApiKey = apikey
        self.owner_org: Union[str, None] = owner_org  # name of the organization to limit package_search (optional)
        self.params: CkanApiParamsBasic = params
        self.ckan_session: Union[requests.Session, None] = None
        self.extern_session: Union[requests.Session, None] = None
        if apikey_file is not None and apikey is None:
            self.load_apikey()
        self.debug: CkanApiDebug = CkanApiDebug()
        # properties
        self.url = url  # url of the CKAN server (property)

    def __del__(self):
        self.disconnect()
        self.apikey.__del__()

    def __copy__(self):
        return self.copy()

    def copy(self, new_identifier:str=None, *, dest=None):
        """
        Returns a copy of the current instance.
        Useful to use an initialized ckan object in a multithreaded context. Each thread would have its own copy.
        It is recommended to purge the last response before doing a copy (with purge_map=False)
        """
        if dest is None:
            dest = CkanApiBase()
        dest._ckan_url = self._ckan_url
        dest.params = self.params.copy()
        dest.ckan_session = None
        dest.extern_session = None
        dest.owner_org = self.owner_org
        dest.debug = CkanApiDebug()
        dest.apikey = self.apikey.copy()
        # post-copy operations
        if new_identifier is not None:
            dest.identifier = new_identifier
        dest.purge()
        # this only sets the session objects to None but lets the original instance's session open
        dest.session = None
        dest.extern_session = None
        return dest

    def __str__(self) -> str:
        """
        String representation of the instance, for debugging purposes.

        :return: URL representing the CKAN server
        """
        return f"CKAN <{self.url}> {str(self.identifier)}"

    @property
    def url(self) -> str:
        return self._ckan_url
    @url.setter
    def url(self, url:str) -> None:
        # ensure the ckan url ends with '/' (see is_url_internal)
        if url is None:
            self._ckan_url = None
        elif url.lower().strip() == environ_keyword:  # keyword
            self.init_from_environ(init_api_key=False)
        elif not url.endswith(urlsep):
            self._ckan_url = url + urlsep
        else:
            self._ckan_url = url

    def _init_session(self, *, internal:bool=False):
        """
        Initialize the session objects which are used to perform requests with this CKAN instance.
        This method can be overloaded to fit your needs (proxies, certificates, cookies, headers, etc.).

        :param internal:
        :return:
        """
        if internal:
            if self.ckan_session is None:
                # the use of a session object will improve performance
                self.ckan_session = requests.Session()
                if self.params.proxies is not None:
                    self.ckan_session.proxies.update(self.params.proxies)
                self.ckan_session.auth = self.params.proxy_auth
                self.ckan_session.verify = self.params.ckan_ca
                self.ckan_session.headers = self.params.ckan_headers
                # API key is applied in the headers of each request
        else:
            if self.extern_session is None:
                self.extern_session = requests  # do not persist cookies between domains & requests to external resources are not meant to be numerous
                # self.extern_session = requests.Session()
                # if self.params.proxies is not None:
                #     self.extern_session.proxies.update(self.params.proxies)
                # self.extern_session.auth = self.params.proxy_auth
                # self.extern_session.verify = self.params.extern_ca
                # self.extern_session.headers = self.params.http_headers

    def connect(self):
        self.test_ckan_login(raise_error=True)

    def disconnect(self):
        if self.ckan_session is not None:
            self.ckan_session.close()
        if self.extern_session is not None and isinstance(self.extern_session, requests.Session):
            self.extern_session.close()
        self.ckan_session = None
        self.extern_session = None

    def full_unlock(self, unlock:bool=True,
                    *, no_ca:bool=None, external_url_resource_download:bool=None) -> None:
        """
        Function to unlock full capabilities of the CKAN API

        :param unlock:
        :return:
        """
        if no_ca is not None:
            unlock_no_ca(no_ca)
        if external_url_resource_download is not None:
            unlock_external_url_resource_download(external_url_resource_download)

    def prepare_for_multithreading(self, mode_reduced:bool=True) -> None:
        """
        This method disables unnecessary writes to this object.
        It is recommended to enable the reduced writes mode in a multithreaded context.
        Do not forget to reset sessions at the beginning of each thread.

        :param mode_reduced:
        :return:
        """
        self.debug.store_last_response = not mode_reduced
        self.debug.store_last_response_debug_info = not mode_reduced
        if mode_reduced:
            self.disconnect()

    def purge(self) -> None:
        """
        Erase temporary data stored in this object

        :param purge_map: whether to purge the map created with map_resources
        """
        self.debug.last_response = None
        self.debug.ckan_request_counter = 0
        self.debug.extern_request_counter = 0
        self.debug.last_response_request_count = 0
        self.debug.multi_requests_last_successful_offset = 0
        self.debug.last_response_elapsed_time = 0.0

    def set_limits(self, limit_read:Union[int,None]) -> None:
        """
        Set default query limits. If only one argument is provided, it applies to both limits.

        :param limit_read: default limit for read requests
        :return:
        """
        self.params.default_limit_read = limit_read
        self.params.default_limit_list = limit_read

    def set_verbosity(self, verbosity:bool=True, verbose_extra:bool=None) -> None:
        """
        Enable/disable full verbose output
        :param verbosity: boolean. Cannot be None
        :return:
        """
        self.params.verbose_multi_requests = verbosity
        self.params.verbose_request = verbosity
        self.params.verbose_request_error = verbosity
        if verbose_extra is not None:
            self.params.verbose_extra = verbose_extra

    def set_proxies(self, proxies:Union[str,dict,ProxyConfig], *, default_proxies:dict=None, proxy_headers:dict=None) -> None:
        """
        Set up the proxy configuration

        :param proxies: string or proxies dict or ProxyConfig object.
        If a string is provided, it must be an url to a proxy or one of the following values:
            - "environ": use the proxies specified in the environment variables "http_proxy" and "https_proxy"
            - "noproxy": do not use any proxies
            - "unspecified": do not specify the proxies
            - "default": use value provided by default_proxies
        :param default_proxies: proxies used if proxies="default"
        :param proxy_headers: headers used to access the proxies, generally for authentication
        :return:
        """
        self.params._proxy_config = ProxyConfig.from_str_or_config(proxies,
                                                            default_proxies=default_proxies, proxy_headers=proxy_headers)

    def set_requests_timeout(self, requests_timeout:float, multi_requests_timeout=None) -> None:
        """
        Set timeout for requests.

        :param requests_timeout: timeout for each request (seconds)
        :param multi_requests_timeout: timeout for grouped request (seconds)
        :return:
        """
        self.params.requests_timeout = requests_timeout
        if multi_requests_timeout is not None:
            self.params.multi_requests_timeout = multi_requests_timeout

    def init_from_environ(self, *, init_api_key:bool=True, error_not_found:bool=False) -> None:
        """
        Initialize CKAN from environment variables.

        - `CKAN_URL` for the url of the CKAN server.

        And optionally:
        - `CKAN_API_KEY`: for the raw API key (it is not recommended to store API key in an environment variable)
        - `CKAN_API_KEY_FILE`: path to a file containing a valid API key in the first line of text

        :param error_not_found: raise an error if the API key file was not found
        :return:
        """
        ckan_url = os.environ.get(self.CKAN_URL_ENVIRON)            # "CKAN_URL"
        if ckan_url is not None:
            assert not ckan_url.lower().strip() == environ_keyword  # this value would create an infinite loop
            self.url = ckan_url
        if init_api_key:
            self.apikey.load_from_environ(error_not_found=error_not_found)

    def _setup_cli_ckan_parser(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        """
        Define or add CLI arguments to initialize a CKAN API connection
        parser help message:

        CKAN API connection parameters initialization

        options:
          -h, --help            show this help message and exit
          --ckan-url CKAN_URL   CKAN URL
          --apikey APIKEY       CKAN API key
          --apikey-file APIKEY_FILE
                                Path to a file containing the CKAN API key (first line)
          --policy-file POLICY_FILE
                                Path to a file containing the CKAN data format policy (json format)
          --owner-org OWNER_ORG
                                CKAN Owner Organization
          --default-limit DEFAULT_LIMIT
                                Default number of rows per request
          --verbose VERBOSE     Option to set verbosity

        :param parser: option to provide an existing parser to add the specific fields needed to initialize a CKAN API connection
        :return:
        """
        if parser is None:
            parser = argparse.ArgumentParser(description="CKAN API connection parameters initialization")
        parser.add_argument("--ckan-url", type=str,
                            help="CKAN URL")
        CkanApiKey._setup_cli_parser(parser)  # add arguments --apikey-file --apikey
        self.params._setup_cli_ckan_parser__params(parser)
        # parser.add_argument("--external-code", action="store_true",
        #                     help="Enable external code execution for builder (only enable for trusted sources)")
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None, error_not_found:bool=True,
                             default_proxies:dict=None, proxy_headers:dict=None) -> None:
        """
        Apply the arguments parsed by the argument parser defined by _setup_cli_ckan_parser

        :param args:
        :param base_dir: base directory to find the CKAN API key file, if a relative path is provided
        (recommended: leave None to use cwd)
        :param error_not_found: option to raise an exception if the CKAN API key file is not found
        :param default_proxies: proxies used if proxies="default"
        :param proxy_headers: headers used to access the proxies, generally for authentication
        :return:
        """
        if args.ckan_url is not None:
            self.url = args.ckan_url
        self.apikey._cli_args_apply(args, base_dir=base_dir, error_not_found=error_not_found)
        self.params._cli_ckan_args_apply(args, base_dir=base_dir, error_not_found=error_not_found,
                                         default_proxies=default_proxies, proxy_headers=proxy_headers)
        if args.default_limit is not None:
            self.set_limits(args.default_limit)
        if args.verbose is not None:
            self.set_verbosity(args.verbose)
        # if args.external_code:
        #     unlock_external_code_execution()
        print(args)

    def initialize_from_cli_args(self, *, args:Sequence[str]=None, base_dir:str=None,
                                 error_not_found:bool=True, parser:argparse.ArgumentParser=None,
                                 default_proxies:dict=None, proxy_headers:dict=None) -> None:
        """
        Intialize the CKAN API connection from command line arguments.

        :param args: Option to provide arguments from another source.
        :return:
        """
        parser = self._setup_cli_ckan_parser(parser)
        args_parsed = parser.parse_args(args)
        self._cli_ckan_args_apply(args_parsed, base_dir=base_dir, error_not_found=error_not_found,
                                  default_proxies=default_proxies, proxy_headers=proxy_headers)

    def input_cli_args(self, *, base_dir:str=None, error_not_found:bool=True, only_if_necessary:bool=False,
                       default_proxies:dict=None, proxy_headers:dict=None):
        """
        Initialize the query for initialization parameters in the command-line format in the console window.

        :return:
        """
        if only_if_necessary and (self.url is not None and not self.apikey.is_empty()):  # and self.proxy_object.is_defined()):
            return
        options_string = input("Please enter CKAN connection CLI arguments: ")
        self.initialize_from_options_string(options_string, base_dir=base_dir, error_not_found=error_not_found,
                                            default_proxies=default_proxies, proxy_headers=proxy_headers)

    def initialize_from_options_string(self, options_string:str=None, base_dir:str=None,
                                       error_not_found:bool=True, parser:argparse.ArgumentParser=None,
                                       default_proxies:dict=None, proxy_headers:dict=None) -> None:
        parser = self._setup_cli_ckan_parser(parser)
        args = parser.parse_args(shlex.split(options_string))
        self._cli_ckan_args_apply(args, base_dir=base_dir, error_not_found=error_not_found,
                                  default_proxies=default_proxies, proxy_headers=proxy_headers)

    def input_missing_info(self, *, base_dir:str=None, input_args:bool=False, input_args_if_necessary:bool=False,
                           input_apikey:bool=True, error_not_found:bool=True):
        """
        Ask user information in the console window.

        :param input_owner_org: option to ask for the owner organization.
        :return:
        """
        if input_args or input_args_if_necessary:
            self.input_cli_args(base_dir=base_dir, error_not_found=error_not_found, only_if_necessary=input_args_if_necessary)
        if self.url is None:
            ckan_url = input("Please enter the CKAN URL: ")
            self.url = ckan_url
        if self.apikey.is_empty() and input_apikey:
            self.apikey.input()


    ## Error management ------------------
    def _error_print_debug_response(self, response:requests.Response, *,
                                    url:str=None, params:dict=None, json:dict=None, error:Exception=None, headers:dict=None):
        if self.params.verbose_request_error:
            print(f"{self.identifier} CKAN Response error details ({str(self)})")
            print(" ")
            if response is None:
                print(f"Problematic request did not obtain response ({url})")
            else:
                print(f"Problematic response code {response.status_code}:")
            if response is None:
                pass
            elif isinstance(response.content, bytes):
                print_str = response.content.decode()
            else:
                print_str = response.content
                print(print_str[:max_len_debug_print])
                if len(print_str) > max_len_debug_print:
                    print("[...]")
            if error is not None:
                print(" ")
                print("Exception error message:")
                print(str(error))
            if response is None:
                if params is not None:
                    print(" ")
                    print("Request params:")
                    print(params)
                if json is not None:
                    print(" ")
                    print("Request json:")
                    print(json)
            else:
                print(" ")
                print("Request URL:")
                print(response.request.url)
                print(" ")
                print("Request body:")
                if isinstance(response.request.body, bytes):
                    print_str = response.request.body.decode()
                elif response.request.body is not None:
                    print_str = response.request.body
                else:
                    print_str = "None"
                print(print_str[:max_len_debug_print])
                if len(print_str) > max_len_debug_print:
                    print("[...]")
                print(" ")
                print("Response body:")
                if response.text is not None:
                    print_str = response.text
                else:
                    print_str = "None"
                print(print_str[:max_len_debug_print])
                if len(print_str) > max_len_debug_print:
                    print("[...]")
            print(" ")


    ## Authentification ------------------
    def load_apikey(self, apikey_file:str=None, base_dir:str=None, error_not_found:bool=True):
        """
        Load the CKAN API key from file.
        The file should contain a valid API key in the first line of text.

        :param apikey_file: API key file (optional if specified at the creation of the object)
        :param base_dir: base directory, if the apikey_file is a relative path
        :return:
        """
        self.apikey.load_apikey(apikey_file=apikey_file, base_dir=base_dir, error_not_found=error_not_found)

    def _prepare_headers(self, headers:dict=None, include_ckan_auth:bool=False) -> dict:
        """
        Prepare headers for a request. If the request is destined to the CKAN server,
        include authentication headers, if API key was provided.

        :param headers: initial headers
        :param include_ckan_auth: boolean to include CKAN authentication headers
        :return:
        """
        if headers is None:
            headers = {}
        if self.params.user_agent is not None:
            headers["User-Agent"] = str(self.params.user_agent)
        headers.update(self.params._proxy_config.proxy_headers)
        headers.update(self.params.http_headers)
        if include_ckan_auth:
            headers.update(self.params.ckan_headers)
            headers.update(self.apikey.get_auth_header())
        return headers

    @staticmethod
    def unlock_no_ca(value:bool=True):
        """
        This function enables you to disable the CA verification of the CKAN server.

        __Warning__:
        Only allow in a local environment!
        """
        unlock_no_ca(value)

    @staticmethod
    def unlock_external_url_resource_download(value:bool=True):
        """
        This function enables the download of resources external from the CKAN server.
        """
        unlock_external_url_resource_download(value)

    def prepare_arguments_for_url_download_request(self, url:str, *,
                                                   auth_if_ckan:bool=None, headers:dict=None, verify:Union[bool,str,None]=None) \
            -> Tuple[bool, dict]:
        """
        Include CKAN authentication headers only if the URL points to the CKAN server.

        :param url: target URL
        :param headers: initial headers
        :param auth_if_ckan: option to include CKAN authentication headers if the url is recognized as part of the CKAN server.
        :return:
        """
        if auth_if_ckan is None:
            auth_if_ckan = ckan_request_proxy_default_auth_if_ckan
        verify_ca = verify
        url_is_internal = self.is_url_internal(url)
        if url_is_internal:
            headers = self._prepare_headers(headers, include_ckan_auth=auth_if_ckan)
            if verify is None:
                verify_ca = self.params.ckan_ca
        elif not download_external_resource_urls:
            raise ExternalUrlLockedError(url)
        else:
            headers = self._prepare_headers(headers, include_ckan_auth=False)
            if verify is None:
                verify_ca = self.params.extern_ca
            msg = f"Request to external url: {url}"
            warn(msg)
        request_kwargs = dict(headers=headers, verify=verify_ca)
        return url_is_internal and auth_if_ckan, request_kwargs

    def download_url_proxy(self, url:str, *, method:str=None, auth_if_ckan:bool=None,
                           proxies:dict=None, headers:dict=None, auth: Union[AuthBase, Tuple[str,str]]=None, verify:Union[bool,str,None]=None) -> requests.Response:
        """
        Download a URL using the CKAN parameters (proxy, authentication etc.)

        :param url:
        :param proxies:
        :param headers:
        :return:
        """
        if proxies is None: proxies = self.params.proxies
        if method is None:
            method = "GET"
        if auth is None:
            auth = self.params.proxy_auth
        url_is_internal_auth, request_kwargs = self.prepare_arguments_for_url_download_request(url, auth_if_ckan=auth_if_ckan,
                                                                                              headers=headers, verify=verify)
        response = None
        self._init_session(internal=url_is_internal_auth)
        try:
            if self.params.dry_run:
                response = requests.Response()
            elif url_is_internal_auth:
                self.debug.ckan_request_counter += 1
                response = self.ckan_session.request(method, url, timeout=self.params.requests_timeout,
                                                     proxies=proxies, **request_kwargs, auth=auth)
            else:
                self.debug.extern_request_counter += 1
                response = self.extern_session.request(method, url, timeout=self.params.requests_timeout,
                                                       proxies=proxies, **request_kwargs, auth=auth)
        except Exception as e:
            self._error_print_debug_response(response, url=url, headers=headers, error=e)
            raise e from e
        self.debug.last_response_request_count = 1
        if self.params.store_last_response:
            self.debug.last_response = response
        return response

    def download_url_proxy_test_head(self, url:str, *, raise_error:bool=False, auth_if_ckan:bool=None,
                                     proxies:dict=None, headers:dict=None, auth: Union[AuthBase, Tuple[str,str]]=None,
                                     verify:Union[bool,str,None]=None, context:str=None) \
            -> Union[None,ContextErrorLevelMessage]:
        """
        This sends a HEAD request to the url using the CKAN connexion parameters via download_url_proxy.
        The resource is not downloaded but the headers indicate if the url is valid.

        :return: None if successful
        """
        if context is None:
            context = "URL"
        try:
            response = self.download_url_proxy(url, method="HEAD", auth_if_ckan=auth_if_ckan, proxies=proxies, headers=headers, auth=auth, verify=verify)
        except Exception as e:
            if raise_error:
                raise e from e
            return ContextErrorLevelMessage(context, ErrorLevel.Error, f"Failed to query url {url}: {str(e)}")
        if response.ok and response.status_code == 200:
            return None
        else:
            if raise_error:
                raise RequestError(f"Failed to query url {url}: status {response.status_code} {response.reason}")
            return ContextErrorLevelMessage(context, ErrorLevel.Error, f"Failed to query url: {url}: status {response.status_code} {response.reason}")

    ## API calls ------------------
    def _error_empty_url(self, raise_error:bool=True) -> bool:
        if self.url is None or self.url == "":
            if raise_error:
                raise UrlError("CKAN URL was not specified")
            return True
        return False

    def _get_api_url(self, category:str=None):
        """
        Returns the base API url and appends the category

        :param category: usually, "action"
        :return:
        """
        self._error_empty_url()
        base = url_join(self.url, "api/3")
        if category is not None:
            return base + urlsep + category
        else:
            return base

    def _api_action_request(self, action:str, *, method:RequestType, params:dict=None,
                            headers:dict=None,
                            data:Union[dict,str,bytes]=None, json:dict=None, files:List[tuple]=None) -> CkanActionResponse:
        """
        Send API action request and return response.

        :param action: action name
        :param method: GET / POST
        :param params: params to set in the url
        :param data: information to encode in the request body (only for POST method)
        :param json: information to encode as JSON in the request json (only for POST method)
        :param files: files to upload in the request (only for POST method)
        :param headers: headers for the request (authentication tokens are added by the function)
        :return:
        """
        if params is None: params = {}
        base = self._get_api_url("action")
        url = base + urlsep + action
        headers = self._prepare_headers(headers, include_ckan_auth=True)
        if self.params.verbose_request:
            if json is not None:
                params_str = "json=" + str(json) + " / "
            else:
                params_str = ""
            params_str = params_str + str(params)
            if data is not None:
                params_str = params_str + "data=" + str(data)[:max_len_debug_print] + " / "
                if len(str(data)) > max_len_debug_print:
                    params_str = params_str + "[...]"
            params_str = params_str[:max_len_debug_print]
            print(f"{self.identifier} API action '{action}' with arguments {params_str}")
        start = time.time()
        self.debug.ckan_request_counter += 1
        response = None
        self._init_session(internal=True)
        try:
            if self.params.dry_run:
                response = requests.Response()
            elif method == RequestType.Get:
                assert_or_raise(data is None, UnexpectedError("data"))
                response = self.ckan_session.get(url, params=params, headers=headers, timeout=self.params.requests_timeout,
                                                 proxies=self.params.proxies, verify=self.params.ckan_ca, auth=self.params.proxy_auth)
            else:
                response = self.ckan_session.post(url, data=data, headers=headers, params=params, files=files, json=json,
                                                  timeout=self.params.requests_timeout,
                                                  proxies=self.params.proxies, verify=self.params.ckan_ca, auth=self.params.proxy_auth)
        except Exception as e:
            self._error_print_debug_response(response, url=url, params=params, headers=headers, json=json, error=e)
            raise e from e
        end = time.time()
        if self.params.verbose_request and not self.params.dry_run:
            print(f"{self.identifier} API action '{action}' done in {end-start} seconds. Received {len(response.content)} bytes")
        if self.params.store_last_response:
            self.debug.last_response = response
        if self.params.store_last_response_debug_info:
            self.debug.last_response_elapsed_time = end - start
            self.debug.last_response_request_count = 1
        return CkanActionResponse(response, self.params.dry_run)

    def api_action_call(self, action:str, *, method:RequestType, params:dict=None,
                            headers:dict=None,
                            data:dict=None, json:dict=None, files:List[tuple]=None) -> CkanActionResponse:
        # function alias of _api_action_request
        return self._api_action_request(action=action, method=method, params=params, headers=headers, data=data, json=json, files=files)

    def _url_request(self, path:str, *, method:RequestType, params:dict=None, headers:dict=None,
                            data:dict=None, json:dict=None, files:List[tuple]=None) -> requests.Response:
        """
        Send request to server and return response.

        :param path: relative path to server url
        :param method: GET / POST
        :param params: params to set in the url
        :param data: information to encode in the request body (only for POST method)
        :param headers: headers for the request (authentication tokens are added by the function)
        :return:
        """
        if params is None: params = {}
        self._error_empty_url()
        url = url_join(self.url, path)
        headers = self._prepare_headers(headers, include_ckan_auth=True)
        if self.params.verbose_request:
            if json is not None:
                params_str = str(json) + " / "
            else:
                params_str = ""
            params_str = params_str + str(params)
            params_str = params_str[:min(len(params_str), max_len_debug_print)]
            print(f"{self.identifier} URL call {url} with arguments {params_str}")
        start = time.time()
        self.debug.ckan_request_counter += 1
        response = None
        self._init_session(internal=True)
        try:
            if self.params.dry_run:
                response = requests.Response()
            elif method == RequestType.Get:
                response = self.ckan_session.get(url, params=params, headers=headers, timeout=self.params.requests_timeout,
                                                 proxies=self.params.proxies, verify=self.params.ckan_ca, auth=self.params.proxy_auth)
            else:
                response = self.ckan_session.post(url, data=data, headers=headers, params=params, timeout=self.params.requests_timeout,
                                                  json=json, files=files,
                                                  proxies=self.params.proxies, verify=self.params.ckan_ca, auth=self.params.proxy_auth)
        except Exception as e:
            self._error_print_debug_response(response, url=url, params=params, headers=headers, json=json, error=e)
            raise e from e
        end = time.time()
        if self.params.verbose_request:
            print(f"{self.identifier} URL call {url} done in {end-start} seconds. Received {len(response.content)} bytes")
        if self.params.store_last_response:
            self.debug.last_response = response
        if self.params.store_last_response_debug_info:
            self.debug.last_response_elapsed_time = end - start
            self.debug.last_response_request_count = 1
        return response

    def api_help_show(self, action_name:str, *, print_output:bool=True) -> str:
        """
        API help command on a given action.

        :param action_name:
        :param print_output: Option to print the output in the command line
        :return:
        """
        response = self._api_action_request("help_show", method=RequestType.Get, params={"name": action_name})
        if response.success:
            if print_output:
                print(action_name + " help:")
                print(response.result)
            return response.result
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "Action", response)
        else:
            if print_output:
                print(f"No documentation found for action '{action_name}'")
            raise response.default_error(self)


    ## Multiple queries with limited responses until full contents are obtained ------------------
    def _request_all_results_generator(self, api_fun:Callable, *, params:dict=None,
                                          limit:int=None, offset:int=0, search_all:bool=True,
                                          **kwargs) -> Generator[Any, Any, None]:
        """
        Multiply request with a limited length until no more data is transmitted thanks to the offset parameter.
        Lazy auxiliary function which yields a result for each request.

        :param api_fun: function to call, typically a unitary request function
        :param params: api_fun must accept params argument in order to transmit other values and enforce the offset parameter
        :param limit: api_fun must accept limit argument in order to update the limit value
        :param offset: api_fun must accept offset argument in order to update the offset value
        :param search_all: if False, only the first request is operated
        :param kwargs: additional keyword arguments to pass to api_fun
        :return:
        """
        if params is None:
            params = {}
        if limit is None:
            limit = self.params.default_limit_read
        if limit is not None:
            # params["limit"] = limit
            assert_or_raise(limit > 0, InvalidParameterError("limit"))
        if offset is None:
            offset = 0
        # params["offset"] = offset
        if self.params.store_last_response_debug_info:
            self.debug.multi_requests_last_successful_offset = offset
        start = time.time()
        requests_count = 1
        n_received = 0
        if self.params.verbose_multi_requests:
            print(f"{self.identifier} Multi-requests no. {requests_count} - Requesting {limit} results from {api_fun.__name__}...")
        result_add: Union[pd.DataFrame, CkanActionResponse, Collection] = api_fun(params=params, limit=limit, offset=offset, **kwargs)
        if self.params.store_last_response_debug_info:
            self.debug.multi_requests_last_successful_offset = offset
            self.debug.last_response_request_count = requests_count
        offset += len(result_add)
        n_received += len(result_add)
        yield result_add
        current = time.time()
        timeout = (current - start) > self.params.multi_requests_timeout
        flag = search_all and len(result_add) > 0 and requests_count < self.params.max_requests_count and not timeout
        while flag:
            if self.params.multi_requests_time_between_requests > 0:
                time.sleep(self.params.multi_requests_time_between_requests)
            # params["offset"] = offset
            requests_count += 1
            if self.params.verbose_multi_requests:
                print(f"{self.identifier} Multi-requests no. {requests_count} - Requesting {limit} results from {api_fun.__name__}...")
            result_add = api_fun(params=params, limit=limit, offset=offset, **kwargs)
            if self.params.store_last_response_debug_info:
                self.debug.multi_requests_last_successful_offset = offset
                self.debug.last_response_request_count = requests_count
            offset += len(result_add)
            n_received += len(result_add)
            yield result_add
            current = time.time()
            timeout = (current - start) > self.params.multi_requests_timeout
            flag = len(result_add) > 0 and requests_count < self.params.max_requests_count and not timeout
        if timeout:
            raise TimeoutError()
        if requests_count >= self.params.max_requests_count:
            raise MaxRequestsCountError()
        current = time.time()
        if self.params.verbose_multi_requests:
            print(f"{self.identifier} Multi-requests {api_fun.__name__} done in {requests_count} calls and {round(current - start, 2)} seconds. {n_received} lines received.")
        return

    def _request_all_results_df(self, api_fun:Callable, *, params:dict=None, list_attrs:bool=True,
                                limit:int=None, offset:int=0, search_all:bool=True,
                                **kwargs) -> pd.DataFrame:
        """
        Multiply request with a limited length until no more data is transmitted thanks to the offset parameter.
        DataFrame implementation returns the concatenated DataFrame from the unitary function calls.

        :param api_fun: function to call, typically a unitary request function
        :param params: api_fun must accept params argument in order to transmit other values and enforce the offset parameter
        :param limit: api_fun must accept limit argument in order to update the limit value
        :param offset: api_fun must accept offset argument in order to update the offset value
        :param search_all: if False, only the first request is operated
        :param list_attrs: option to aggregate DataFrame attrs field into lists. # False not tested
        :param kwargs: additional keyword arguments to pass to api_fun
        :return:
        """
        start = time.time()
        iter = self._request_all_results_generator(api_fun=api_fun, params=params,
                                                   limit=limit, offset=offset, search_all=search_all, **kwargs)
        requests_count = 1
        df = next(iter)
        if list_attrs:
            df.attrs = {key: [value] for key, value in df.attrs.items()}
        for df_add in iter:
            requests_count += 1
            if len(df_add) > 0:
                if list_attrs:
                    attrs = df.attrs
                df = pd.concat([df, df_add])
                if list_attrs:
                    df.attrs = {key: value + [df_add.attrs[key]] for key, value in attrs.items()}
        current = time.time()
        df.attrs["requests_count"] = requests_count
        df.attrs["elapsed_time"] = (current - start)
        return df

    def _request_all_results_list(self, api_fun:Callable, *, params:dict=None,
                                  limit:int=None, offset:int=0, search_all:bool=True, **kwargs) -> Union[List[CkanActionResponse], list]:
        """
        Multiply request with a limited length until no more data is transmitted thanks to the offset parameter.
        List implementation returns the list of the unitary function return values.

        :param api_fun: function to call, typically a unitary request function
        :param params: api_fun must accept params argument in order to transmit other values and enforce the offset parameter
        :param limit: api_fun must accept limit argument in order to update the limit value
        :param offset: api_fun must accept offset argument in order to update the offset value
        :param search_all: if False, only the first request is operated
        :param kwargs: additional keyword arguments to pass to api_fun
        :return:
        """
        return list(self._request_all_results_generator(api_fun=api_fun, params=params, limit=limit, offset=offset,
                                                            search_all=search_all, **kwargs))

    def is_url_internal(self, url:str) -> bool:
        """
        Tests whether a url points to the same server as the CKAN url.

        :param url:
        :return:
        """
        # TODO: improve the url matching test
        return url.startswith(self.url)

    def test_ckan_url_reachable(self, raise_error:bool=False) -> bool:
        """
        Test if the CKAN URL is reachable with a HEAD request.
        This does not check it is really a CKAN server and does not check authentication.
        """
        error_message = self.download_url_proxy_test_head(self.url, raise_error=raise_error, context="CKAN URL test")
        return error_message is None

