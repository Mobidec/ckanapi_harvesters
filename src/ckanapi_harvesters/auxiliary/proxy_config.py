#!python3
# -*- coding: utf-8 -*-
"""
Setting the proxy from simple command line arguments
"""
import urllib.request
from typing import Union, Sequence, Tuple
import os
import argparse
from warnings import warn
import copy
import json

from ckanapi_harvesters.auxiliary.ckan_defs import environ_keyword
from ckanapi_harvesters.auxiliary.path import sanitize_path, path_rel_to_dir
from ckanapi_harvesters.auxiliary.login import Login

import requests
from requests.auth import AuthBase, HTTPProxyAuth, HTTPBasicAuth

PROXY_AUTH_ENVIRON = "PROXY_AUTH_FILE"

class HttpsProxyDefError(Exception):
    def __init__(self):
        super().__init__("Only one of http_proxy or https_proxy is set")

def get_proxies_from_environ() -> dict:
    proxies = urllib.request.getproxies()
    return proxies
    # http_proxy = os.environ.get("http_proxy")
    # https_proxy = os.environ.get("https_proxy")
    # no_proxy = os.environ.get("no_proxy")
    # if http_proxy is not None and https_proxy is not None:
    #     proxies = {"http": http_proxy, "https": https_proxy}
    # elif http_proxy is not None:
    #     proxies = {"http": http_proxy, "https": http_proxy}
    # elif https_proxy is not None:
    #     raise HttpsProxyDefError()
    # else:
    #     proxies = None
    # if proxies is not None and no_proxy is not None:
    #     proxies["no"] = no_proxy
    # return proxies

def host_port_sep(url:Union[str,None], *, default_port:int=None) -> Tuple[Union[str,None],Union[int,None]]:
    if url is None:
        return None, None
    if ':' in url:
        host_prefix, host_suffix = "", ""
        if '@' in url:
            host_prefix, url = url.split('@')
        if '?' in url:
            url, host_suffix = url.split('?')
        host, port_str = url.split(':')
        host = host_prefix + host + host_suffix
        port = int(port_str)
    else:
        host, port = url, default_port
    return host, port

def _define_proxies(proxy_string:Union[str, dict], default_proxies:dict=None) -> dict:
    if proxy_string is None:
        proxies = None
    elif isinstance(proxy_string, dict):
        proxies = proxy_string
    elif isinstance(proxy_string, str):
        proxy_string = proxy_string.strip()
        proxy_mode = proxy_string.lower()
        if proxy_mode == environ_keyword:
            proxies = get_proxies_from_environ()
            if proxies is None:
                proxies = default_proxies
        elif proxy_mode == "unspecified":
            proxies = None  # do not specify the proxies - is equivalent to "environ"
        elif proxy_mode == "noproxy":
            proxies = {"http": "", "https": ""}  # do not use any proxy
        elif proxy_mode == "default":
            proxies = default_proxies  # default proxies, provided in argument
        elif proxy_string.startswith('{'):
            # proxy string is a string representation of proxy dictionary
            proxies = json.loads(proxy_string)
        else:
            # suppose string contains an url to a proxy server
            proxies = {"http": proxy_string, "https": proxy_string}
            # if "http" not in proxy_string:
            #     # url without http
            #     proxies = {"http": f"http://{proxy_string}", "https": f"http://{proxy_string}"}
            # else:
            #     proxies = {"http": proxy_string, "https": proxy_string}
    else:
        raise TypeError("proxy must be str or dict")
    return proxies


class ProxyConfig:
    def __init__(self, proxy_string:Union[str,dict]=None, default_proxies:dict=None,
                 proxy_headers:dict = None, proxy_auth:Union[AuthBase, Tuple[str,str]]=None) -> None:
        """
        :param proxy_string: string or proxies dict or ProxyConfig object.
        If a string is provided, it must be an url to a proxy or one of the following values:
            - "environ": use the proxies specified in the environment variables "http_proxy" and "https_proxy"
            - "noproxy": do not use any proxies
            - "unspecified": do not specify the proxies
            - "default": use value provided by default_proxies
        :param default_proxies: proxies used if proxies="default"
        :param proxy_headers: headers used to access the proxies, generally for authentication
        """
        if proxy_headers is None: proxy_headers = {}
        self._proxy_string:Union[str, dict, None] = None
        self._proxies:Union[dict,None] = None
        self._is_defined:bool = False
        self._default_proxies:Union[dict,None] = default_proxies
        self.proxy_headers: dict = proxy_headers
        self._proxy_auth: Union[AuthBase, Tuple[str,str], None] = proxy_auth
        self.proxy_auth_file: Union[str,None] = None
        self.proxy_auth_from_env: bool = False
        self.proxy_string = proxy_string  # property
        # self.load_proxy_auth_environ(error_not_found=False)  # recommended to base these parameters on user demand (confirm if there is a risk of leakage)

    def __str__(self):
        return str(self._proxies)

    def __copy__(self):
        return self.copy()

    def copy(self) -> "ProxyConfig":
        dest = ProxyConfig()
        dest._proxies = copy.deepcopy(self._proxies)
        dest._proxy_string = copy.deepcopy(self._proxy_string)
        dest._default_proxies = copy.deepcopy(self._default_proxies)
        dest.proxy_headers = copy.deepcopy(self.proxy_headers)
        dest._proxy_auth = copy.deepcopy(self._proxy_auth)
        dest._is_defined = self._is_defined
        dest.proxy_auth_file = self.proxy_auth_file
        dest.proxy_auth_from_env = self.proxy_auth_from_env
        return dest


    @property
    def proxy_string(self) -> Union[str, dict, None]:
        return self._proxy_string
    @proxy_string.setter
    def proxy_string(self, proxy_string:Union[str, dict, None]):
        self._proxy_string = proxy_string
        self._is_defined = proxy_string is not None
        self._proxies = _define_proxies(proxy_string, default_proxies=self._default_proxies)
    @property
    def proxies(self) -> dict:
        return self._proxies
    @proxies.setter
    def proxies(self, proxies:dict):
        self._proxy_string = proxies
        self._proxies = proxies
        self._is_defined = True
    @property
    def proxy_auth(self) -> Union[AuthBase, Tuple[str,str]]:
        return self._proxy_auth
    @proxy_auth.setter
    def proxy_auth(self, proxy_auth:Union[AuthBase, Tuple[str,str]]):
        self._proxy_auth = proxy_auth
        self.proxy_auth_file = None
        self.proxy_auth_from_env = False

    def get_host_port(self) -> Tuple[Union[str,None],Union[int,None]]:
        # special mode
        if self._proxies is None:
            return None, None
        elif "http_proxy" in self._proxies.keys() and self._proxies["http_proxy"] is not None:
            return host_port_sep(self._proxies["http_proxy"])
        else:
            return None, None

    def get_proxy_login(self) -> Login:
        if self._proxy_auth is None:
            return Login()
        else:
            assert(isinstance(self._proxy_auth, HTTPBasicAuth))  # HTTPProxyAuth is a super class of HTTPBasicAuth
            return Login(self._proxy_auth.username, self._proxy_auth.password)

    @staticmethod
    def from_str_or_config(proxies:Union[str,dict, "ProxyConfig"],
                           *, default_proxies:dict=None, proxy_headers:dict=None) -> "ProxyConfig":
        if proxies is None:
            return ProxyConfig(None, default_proxies=default_proxies , proxy_headers=proxy_headers)
        elif isinstance(proxies, ProxyConfig):
            if proxy_headers is not None:
                proxies.proxy_headers = proxy_headers
            return proxies
        else:
            return ProxyConfig(proxies, default_proxies=default_proxies, proxy_headers=proxy_headers)

    def replace_default_proxy(self, default_proxies:dict) -> None:
        if self._proxy_string is not None and self._proxy_string.lower() == "default":
            self._proxies = default_proxies

    def reset(self) -> None:
        self._proxy_string = None
        self._proxies = None
        self._is_defined = False

    def is_defined(self) -> bool:
        return self._is_defined

    def load_proxy_auth_environ(self, *, error_not_found:bool=False) -> bool:
        proxy_auth_file = sanitize_path(os.environ.get(PROXY_AUTH_ENVIRON))  # "PROXY_AUTH_FILE"
        if proxy_auth_file is not None:
            proxy_keyword = proxy_auth_file.strip().lower()
            assert(not proxy_keyword == environ_keyword)  # this value would create an infinite loop
            if self.load_proxy_auth_from_file(proxy_auth_file, error_not_found=error_not_found):
                self.proxy_auth_from_env = True
                return True
        return False

    def load_proxy_auth_from_file(self, file_path:str, *, base_dir:str=None, error_not_found:bool=True) -> bool:
        file_path = path_rel_to_dir(file_path, base_dir=base_dir, keyword_exceptions={environ_keyword})
        proxy_keyword = file_path.strip().lower()
        if proxy_keyword == environ_keyword:
            # this keyword is not very useful if proxy authentication file is loaded from environment anyway
            return self.load_proxy_auth_environ(error_not_found=error_not_found)
        if (not error_not_found) and (not os.path.isfile(file_path)):
            msg = f"Proxy authentication file does not exist: {file_path}"
            warn(msg)
            return False
        self.proxy_auth_file = file_path
        self.proxy_auth_from_env = False
        with open(file_path, "r") as f:
            auth_type = f.readline().strip().lower()
            username = f.readline().strip()
            password = f.readline().strip()
            if auth_type == "basic" or auth_type == "httpbasicauth":
                self._proxy_auth = requests.auth.HTTPBasicAuth(username, password)
            elif auth_type == "proxy" or auth_type == "httpproxyauth":
                self._proxy_auth = requests.auth.HTTPProxyAuth(username, password)
            elif auth_type == "digest" or auth_type == "httpdigestauth":
                self._proxy_auth = requests.auth.HTTPDigestAuth(username, password)
            elif auth_type == "none":
                self._proxy_auth = None
            else:
                raise KeyError(f"Unknown auth type {auth_type}")
        return True


    @staticmethod
    def _setup_cli_proxy_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        """
        Define or add CLI arguments to initialize the proxy
        parser help message:

        Proxy parameters initialization

        options:
          -h, --help            show this help message and exit
          --proxy PROXY         Proxy for HTTP and HTTPS

        :param parser: option to provide an existing parser to add the specific fields needed to initialize a CKAN API connection
        :return:
        """
        if parser is None:
            parser = argparse.ArgumentParser(description="Proxy parameters initialization")
        parser.add_argument("--proxy", type=str,
                            help="Proxy for HTTP and HTTPS")
        parser.add_argument("--http-proxy", type=str,
                            help="HTTP proxy")
        parser.add_argument("--https-proxy", type=str,
                            help="HTTPS proxy")
        parser.add_argument("--no-proxy", type=str,
                            help="Proxy exceptions")
        parser.add_argument("--proxy-auth-file", type=str,
                            help="Path to a proxy authentication file with 3 lines (authentication method, username, password)")
        return parser

    @staticmethod
    def from_cli_args(args: argparse.Namespace, *, base_dir:str=None, error_not_found:bool=True,
                      default_proxies:dict=None, proxy_headers:dict=None) -> "ProxyConfig":
        proxy_string, proxies = None, None
        if args.proxy is not None:
            proxy_string = args.proxy
        elif args.http_proxy is not None and args.https_proxy is not None:
            proxies = {"http": args.http_proxy, "https": args.https_proxy}
        elif args.http_proxy is not None:
            proxies = {"http": args.http_proxy, "https": args.http_proxy}
        if proxies is not None and args.no_proxy is not None:
            proxies["no"] = args.no_proxy
        elif args.https_proxy is not None:
            raise HttpsProxyDefError()
        if proxy_string is not None:
            proxy_config = ProxyConfig(proxy_string, default_proxies=default_proxies, proxy_headers=proxy_headers)
        elif proxies is not None:
            proxy_config = ProxyConfig(proxies, default_proxies=default_proxies, proxy_headers=proxy_headers)
        else:
            proxy_config = None
        if args.proxy_auth_file is not None:
            if proxy_config is not None:
                proxy_config.load_proxy_auth_from_file(args.proxy_auth_file, base_dir=base_dir, error_not_found=error_not_found)
            else:
                raise Exception(f"Proxy authentication file specified without proxy specification: {args.proxy_auth_file}")
        return proxy_config

