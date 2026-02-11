#!python3
# -*- coding: utf-8 -*-
"""
Basic parameters for the CkanApi class
"""
from typing import Union, Tuple
import copy
from warnings import warn
import argparse

import requests
from requests.auth import AuthBase

from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_auxiliary import CkanIdFieldTreatment
from ckanapi_harvesters.auxiliary.ckan_configuration import allow_no_ca
from ckanapi_harvesters.auxiliary.ckan_errors import NoCAVerificationError
from ckanapi_harvesters.auxiliary.path import path_rel_to_dir

default_df_download_id_field_treatment: CkanIdFieldTreatment = CkanIdFieldTreatment.SetIndex


class CkanApiParamsBasic:
    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None):
        """
        :param proxies: proxies to use for requests
        :param ckan_headers: headers to use for requests, only to the CKAN server
        :param http_headers: headers to use for requests, for all requests, including external requests and to the CKAN server
        """
        if ckan_headers is None: ckan_headers = {}
        if http_headers is None: http_headers = {}
        # HTTP parameters
        self._proxy_config: ProxyConfig = ProxyConfig.from_str_or_config(proxies)
        self.user_agent: Union[str,None] = None
        self.http_headers: dict = http_headers
        self.ckan_headers: dict = ckan_headers
        self._ckan_ca: Union[bool, str, None] = True  # use to specify a path to a custom CA certificate for the CKAN server (see also environment variable REQUESTS_CA_BUNDLE)
        self._extern_ca: Union[bool, str, None] = True  # use to specify a path to a custom CA certificate for external connexions (see also environment variable REQUESTS_CA_BUNDLE)
        # debug parameters
        self.store_last_response: bool = True
        self.store_last_response_debug_info: bool = True
        # modes
        self.dry_run: bool = False  # if True, no requests are sent to the server (for debugging purposes)
        # limits
        self.default_limit_list:Union[int,None] = 100   # limit the number of entries per list response (used as default value)
        self.default_limit_read:Union[int,None] = 5000  # limit the number of entries per response (used as default value)
        self.max_requests_count:int = 1000000  # when automating multiple requests, the number of requests is limited by this parameter
        # timeouts
        self.multi_requests_timeout:float = 3600*10  # when automating multiple requests, the total time elapsed is limited by this parameter (evaluated between each request)
        self.multi_requests_time_between_requests:float = 0  # when automating multiple requests, wait this additional time (in seconds) between each request
        self.requests_timeout:Union[float,None] = 100  # timeout per request sent to the requests module
        # verbosity
        self.verbose_multi_requests:bool = False
        self.verbose_request:bool = False
        self.verbose_request_error:bool = True
        self.verbose_extra:bool = True

    def copy(self, *, dest=None):
        if dest is None:
            dest = CkanApiParamsBasic()
        dest._proxy_config = self._proxy_config.copy()
        dest.user_agent = copy.deepcopy(self.user_agent)
        dest.http_headers = copy.deepcopy(self.http_headers)
        dest.ckan_headers = copy.deepcopy(self.ckan_headers)
        dest._ckan_ca = self._ckan_ca
        dest._extern_ca = self._extern_ca
        dest.dry_run = self.dry_run
        dest.store_last_response = self.store_last_response
        dest.store_last_response_debug_info = self.store_last_response_debug_info
        dest.default_limit_list = self.default_limit_list
        dest.default_limit_read = self.default_limit_read
        dest.max_requests_count = self.max_requests_count
        dest.multi_requests_timeout = self.multi_requests_timeout
        dest.multi_requests_time_between_requests = self.multi_requests_time_between_requests
        dest.requests_timeout = self.requests_timeout
        dest.verbose_multi_requests = self.verbose_multi_requests
        dest.verbose_request = self.verbose_request
        dest.verbose_request_error = self.verbose_request_error
        dest.verbose_extra = self.verbose_extra
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
    def proxy_auth(self) -> Union[AuthBase, Tuple[str,str]]:
        return self._proxy_config.proxy_auth
    @proxy_auth.setter
    def proxy_auth(self, proxy_auth:Union[AuthBase, Tuple[str,str]]) -> None:
        self._proxy_config.proxy_auth = proxy_auth
    @property
    def ckan_ca(self) -> Union[bool,str,None]:
        return self._ckan_ca
    @ckan_ca.setter
    def ckan_ca(self, ca_cert:Union[bool,str,None]) -> None:
        if ca_cert is not None and isinstance(ca_cert, bool) and not ca_cert:
            if not allow_no_ca:
                raise NoCAVerificationError()
            else:
                msg = "CA verification has been disabled. Only allow in a local environment!"
                warn(msg)
        self._ckan_ca = ca_cert
    @property
    def extern_ca(self) -> Union[bool,str,None]:
        return self._extern_ca
    @extern_ca.setter
    def extern_ca(self, ca_cert:Union[bool,str,None]) -> None:
        if ca_cert is not None and isinstance(ca_cert, bool) and not ca_cert:
            if not allow_no_ca:
                raise NoCAVerificationError()
            else:
                msg = "CA verification has been disabled. Only allow in a local environment!"
                warn(msg)
        self._extern_ca = ca_cert

    @staticmethod
    def _setup_cli_ckan_parser__params(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        """
        Define or add CLI arguments to initialize a CKAN API connection
        parser help message:

        CKAN API connection parameters initialization

        :param parser: option to provide an existing parser to add the specific fields needed to initialize a CKAN API connection
        :return:
        """
        if parser is None:
            parser = argparse.ArgumentParser(description="CKAN API connection parameters initialization")
        ProxyConfig._setup_cli_proxy_parser(parser)  # add arguments --proxy --http-proxy --https-proxy --no-proxy --proxy-auth-file
        parser.add_argument("--ckan-ca", type=str,
                            help="CKAN CA certificate location (.pem file)")
        parser.add_argument("--extern-ca", type=str,
                            help="CA certificate location for extern connexions (.pem file)")
        parser.add_argument("--user-agent", type=str,
                            help="User agent for HTTP requests")
        parser.add_argument("-l", "--default-limit", type=int,
                            help="Default number of rows per request")
        parser.add_argument("-v", "--verbose",
                            help="Option to set verbosity", action="store_true", default=False)
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
        proxy_config = ProxyConfig.from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                                 default_proxies=default_proxies, proxy_headers=proxy_headers)
        if proxy_config is not None:
            self._proxy_config = proxy_config
        if args.ckan_ca is not None:
            self.ckan_ca = path_rel_to_dir(args.ckan_ca, base_dir=base_dir)
        if args.extern_ca is not None:
            self.extern_ca = path_rel_to_dir(args.extern_ca, base_dir=base_dir)
        if args.user_agent is not None:
            self.user_agent = args.user_agent
        # if args.default_limit is not None:
        #     self.set_limits(args.default_limit)
        # if args.verbose is not None:
        #     self.set_verbosity(args.verbose)
        # if args.external_code:
        #     unlock_external_code_execution()
        print(args)

class CkanApiDebug:
    def __init__(self):
        self.ckan_request_counter: int = 0
        self.extern_request_counter: int = 0
        self.last_response: Union[requests.Response, None] = None  # field containing the last response, for debug purposes
        self.last_response_request_count: int = 0
        self.multi_requests_last_successful_offset: int = 0  # last used offset when multiple queries are performed. This can be used in order to restart an update/download sequence in case of an error.

