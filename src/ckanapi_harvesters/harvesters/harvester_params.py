#!python3
# -*- coding: utf-8 -*-
"""
Harvester parameters. The base names of the parameters are shared between harvesters.
"""
from typing import Union, Tuple, List, Any, Callable
from collections import OrderedDict
from abc import ABC, abstractmethod
import argparse
import shlex
from warnings import warn
import copy

import pandas as pd
from requests.auth import AuthBase

from ckanapi_harvesters.auxiliary.ckan_configuration import default_ckan_has_postgis, default_ckan_target_epsg
from ckanapi_harvesters.auxiliary.ckan_configuration import unlock_external_url_resource_download, allow_no_ca, unlock_no_ca
from ckanapi_harvesters.auxiliary.ckan_errors import NoCAVerificationError
from ckanapi_harvesters.auxiliary.ckan_auxiliary import ca_file_rel_to_dir, assert_or_raise
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_api_key import ApiKey
from ckanapi_harvesters.auxiliary.login import Login, SSHLogin
from ckanapi_harvesters.harvesters.harvester_errors import HarvestMethodRequiredError

harvester_enforce_ca_verification: bool = False


class DatabaseParams:
    """
    Class representing parameters to connect to a database.
    This class mangages the connection parameters such as proxy and CA.
    It also manages authentication parameters.
    """
    def __init__(self, source: "DatabaseParams" = None):
        self.options_string: Union[str,None] = None
        self.file_url_attr: Union[str, None] = None
        self.base_dir: Union[str,None] = None
        self.harvest_method: str = ""
        self._proxy_config: ProxyConfig = ProxyConfig()
        self._verify_ca: Union[str, bool, None] = None
        self._verify_ca_src: Union[str, None] = None
        self.timeout: Union[float, None] = None
        self.host: Union[str, None] = None
        self.port: Union[int, None] = None
        self.auth_url_suffix: Union[str, None] = None
        self.auth_url: Union[str, None] = None
        self.url: Union[str, None] = None
        self.apikey: ApiKey = ApiKey()
        self.login: Login = Login()
        self.database: Union[str, None] = None
        self.verbose_harvester: bool = True
        self.ckan_postgis: Union[bool,None] = default_ckan_has_postgis
        self.ckan_default_target_epsg:Union[int,None] = default_ckan_target_epsg
        if source is not None:
            source.copy(dest=self)

    @abstractmethod
    def copy(self, *, dest=None):
        dest.options_string = self.options_string
        dest.file_url_attr = self.file_url_attr
        dest.base_dir = self.base_dir
        dest.harvest_method = self.harvest_method
        dest._proxy_config = self._proxy_config
        dest._verify_ca = self._verify_ca
        dest._verify_ca_src = self._verify_ca_src
        dest.timeout = self.timeout
        dest.host = self.host
        dest.port = self.port
        dest.auth_url_suffix = self.auth_url_suffix
        dest.auth_url = self.auth_url
        dest.url = self.url
        dest.apikey = self.apikey
        dest.login = self.login
        dest.database = self.database
        dest.verbose_harvester = self.verbose_harvester
        dest.ckan_postgis = self.ckan_postgis
        return dest

    @staticmethod
    def setup_cli_harvester_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="Harvester parameters")
        parser.add_argument("--harvester", type=str,
                            help="Type of harvester to use", required=True)
        ProxyConfig._setup_cli_proxy_parser(parser)  # add arguments --proxy --http-proxy --https-proxy --no-proxy --proxy-auth-file
        parser.add_argument("--ca", type=str,
                            help="Server CA certificate location (.pem file)")
        parser.add_argument("--timeout", type=float,
                            help="Server timeout (seconds)")
        parser.add_argument("--host", type=str,
                            help="Host for queries")
        parser.add_argument("--port", type=int,
                            help="Port for queries")
        parser.add_argument("--auth-url-suffix", type=str,
                            help="URL suffix used to authenticate user")
        parser.add_argument("--auth-url", type=str,
                            help="URL to authenticate user")
        parser.add_argument("--url", type=str,
                            help="Base URL for queries")
        ApiKey._setup_cli_parser(parser)  # add arguments --apikey-file --apikey
        Login._setup_cli_parser(parser)  # add argument --login-file
        parser.add_argument("-v", "--verbose",
                            help="Option to set verbosity", action="store_true", default=False)
        parser.add_argument("--database", type=str,
                            help="Database name")
        parser.add_argument("--ckan-postgis", action="store_true",
                            help="Option to use CKAN with PostGIS geometric types")  # default=default_ckan_has_postgis
        parser.add_argument("--ckan-epsg", type=int,
                            help="Default EPSG for CKAN", default=default_ckan_target_epsg)
        return parser

    def initialize_from_cli_args(self, args: argparse.Namespace, base_dir: str = None, error_not_found: bool = True,
                                 default_proxies: dict = None, proxy_headers: dict = None) -> None:
        self.harvest_method = args.harvester
        proxy_config = ProxyConfig.from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                                 default_proxies=default_proxies, proxy_headers=proxy_headers)
        if proxy_config is not None:
            self._proxy_config = proxy_config
        ca_cert = args.ca
        verify_ca, self._verify_ca_src = ca_file_rel_to_dir(ca_cert, base_dir=base_dir)
        self.set_verify_ca(verify_ca)
        self.timeout = args.timeout
        self.host = args.host
        self.port = args.port
        self.auth_url_suffix = args.auth_url_suffix
        self.auth_url = args.auth_url
        self.url = args.url
        self.apikey._cli_args_apply(args, base_dir=base_dir, error_not_found=error_not_found)
        self.login._cli_args_apply(args, base_dir=base_dir, error_not_found=error_not_found)
        self.database = args.database
        if args.verbose is not None:
            self.verbose_harvester = args.verbose
        if args.ckan_postgis:
            self.ckan_postgis = args.ckan_postgis
        if args.ckan_epsg:
            self.ckan_default_target_epsg = args.ckan_epsg

    def _update_from_ckan(self, ckan):
        # aim: make these values accessible to the harvester algorithms (for the rest, Harvesters are independent of CkanApi)
        if self.ckan_postgis is None:
            self.ckan_postgis = ckan.params.ckan_has_postgis
        if self.ckan_default_target_epsg is None:
            self.ckan_default_target_epsg = ckan.params.ckan_default_target_epsg

    @staticmethod
    def parse_harvest_method(options_string: str) -> str:
        # parser = DatabaseParams.setup_cli_harvester_parser()
        parser = argparse.ArgumentParser(description="Harvester selection")
        parser.add_argument("--harvester", type=str,
                            help="Type of harvester to use", required=True)
        args, _ = parser.parse_known_args(shlex.split(options_string))
        assert_or_raise(args.harvester is not None, HarvestMethodRequiredError())
        return args.harvester.lower().strip()

    def parse_options_string(self, options_string: str, *, base_dir: str = None, file_url_attr: str=None,
                             parser:argparse.ArgumentParser=None):
        self.file_url_attr = file_url_attr
        parser = self.setup_cli_harvester_parser(parser)
        args, _ = parser.parse_known_args(shlex.split(options_string))
        self.options_string = options_string
        self.base_dir = base_dir
        self.initialize_from_cli_args(args, base_dir=base_dir)

    @property
    def proxies(self) -> dict:
        return self._proxy_config.proxies

    @proxies.setter
    def proxies(self, proxies: dict) -> None:
        self._proxy_config.proxies = proxies

    @property
    def proxy_string(self) -> str:
        return self._proxy_config.proxy_string

    @proxy_string.setter
    def proxy_string(self, proxies: str) -> None:
        self._proxy_config.proxy_string = proxies

    @property
    def proxy_auth(self) -> Union[AuthBase, Tuple[str, str]]:
        return self._proxy_config.proxy_auth

    @proxy_auth.setter
    def proxy_auth(self, proxy_auth: Union[AuthBase, Tuple[str, str]]) -> None:
        self._proxy_config.proxy_auth = proxy_auth

    @property
    def verify_ca(self) -> Union[bool, str, None]:
        return self._verify_ca

    def set_verify_ca(self, ca_cert: Union[bool, str, None], enforce_ca_safety: bool = None) -> None:
        if enforce_ca_safety is None:
            enforce_ca_safety = harvester_enforce_ca_verification
        if ca_cert is not None and isinstance(ca_cert, bool) and not ca_cert:
            if enforce_ca_safety and not allow_no_ca:
                raise NoCAVerificationError()
            else:
                msg = "CA verification has been disabled. Only allow in a local environment!"
                warn(msg)
        self._verify_ca = ca_cert

    @staticmethod
    def unlock_no_ca(value: bool = True):
        """
        This function enables you to disable the CA verification of the CKAN server.

        __Warning__:
        Only allow in a local environment!
        """
        unlock_no_ca(value)

    @staticmethod
    def unlock_external_url_resource_download(value: bool = True):
        """
        This function enables the download of resources external from the CKAN server.
        """
        unlock_external_url_resource_download(value)


class DatasetParams(DatabaseParams):
    def __init__(self, source: "DatasetParams" =None):
        super().__init__(source)
        self.dataset: Union[str, None] = None
        if source is not None:
            source.copy(dest=self)

    def copy(self, *, dest=None):
        if dest is None:
            dest = DatasetParams()
        super().copy(dest=dest)
        dest.dataset = self.dataset
        return dest

    @staticmethod
    def setup_cli_harvester_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="Harvester parameters")
            DatabaseParams.setup_cli_harvester_parser(parser=parser)
        parser.add_argument("--dataset", type=str,
                            help="Dataset name")
        return parser

    def initialize_from_cli_args(self, args: argparse.Namespace, base_dir: str = None, error_not_found: bool = True,
                                 default_proxies: dict = None, proxy_headers: dict = None) -> None:
        super().initialize_from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                         default_proxies=default_proxies, proxy_headers=proxy_headers)
        self.dataset = args.dataset


class TableParams(DatasetParams):
    def __init__(self, source: "TableParams" =None):
        super().__init__(source)
        self.output_dir: Union[str, None] = None
        self.enable_download: Union[bool, None] = None
        self.resource_url: Union[str, None] = None
        self.table: Union[str, None] = None
        self.query_string: Union[str, None] = None
        self.limit: Union[int, None] = None
        self.single_request: bool = False
        if source is not None:
            source.copy(dest=self)

    def copy(self, *, dest=None):
        if dest is None:
            dest = TableParams()
        super().copy(dest=dest)
        dest.output_dir = self.output_dir
        dest.enable_download = self.enable_download
        dest.resource_url = self.resource_url
        dest.table = self.table
        dest.query_string = self.query_string
        dest.limit = self.limit
        dest.single_request = self.single_request
        dest.ckan_postgis = self.ckan_postgis
        return dest

    @staticmethod
    def setup_cli_harvester_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="Harvester parameters")
            DatabaseParams.setup_cli_harvester_parser(parser)
            DatasetParams.setup_cli_harvester_parser(parser)
        parser.add_argument("-o", "--output-dir", type=str,
                            help="Output directory of download, relative to the download directory (normally provided by File/URL attribute)")  # applies to parent (builder)
        parser.add_argument("--no-download", type=bool,
                            help="Option to disable download")  # applies to parent (builder)
        parser.add_argument("--resource-url", type=str,
                            help="URL of resource")
        parser.add_argument("--table", type=str,
                            help="Table name")  # normally specified in the File/URL attribute of builder
        parser.add_argument("--query", type=str,
                            help="Query to restrict the lines of the table")
        parser.add_argument("-l", "--limit", type=int,
                            help="Number of rows per request", default=10000)
        parser.add_argument("--once",
                            help="Option to perform only one request with the default limit. This will limit the size of the Data.",
                            action="store_true", default=False)
        return parser

    def initialize_from_cli_args(self, args: argparse.Namespace, base_dir: str = None, error_not_found: bool = True,
                                 default_proxies: dict = None, proxy_headers: dict = None) -> None:
        super().initialize_from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                         default_proxies=default_proxies, proxy_headers=proxy_headers)
        self.output_dir = args.output_dir  # applies to parent (builder)
        self.enable_download = not args.no_download if args.no_download is not None else None  # applies to parent (builder)
        self.resource_url = args.resource_url
        self.table = args.table
        self.limit = args.limit
        if args.once is not None:
            self.single_request = args.once

    def parse_options_string(self, options_string: str, *, base_dir: str = None, file_url_attr: str=None,
                             parser:argparse.ArgumentParser=None):
        self.file_url_attr = file_url_attr
        parser = self.setup_cli_harvester_parser(parser)
        args = parser.parse_args(shlex.split(options_string))
        self.options_string = options_string
        self.base_dir = base_dir
        self.initialize_from_cli_args(args, base_dir=base_dir)


