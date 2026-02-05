#!python3
# -*- coding: utf-8 -*-
"""
Methods to load an API key
"""

import os
from warnings import warn
from typing import Dict, Union, Iterable
import getpass
import argparse

from ckanapi_harvesters.auxiliary.ckan_errors import ApiKeyFileError
from ckanapi_harvesters.auxiliary.path import sanitize_path, path_rel_to_dir
from ckanapi_harvesters.auxiliary.ckan_defs import environ_keyword



class ApiKey:
    """
    API key storage class.
    """

    def __init__(self, *, apikey:str=None, apikey_file:str=None,
                 api_key_header_name:Union[str, Iterable[str]]=None):
        """
        CKAN Database API key storage class.

        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        """
        if api_key_header_name is None:
            api_key_header_name = "Authorization"
        self.apikey_file: str = apikey_file  # path to a file containing a valid API key in the first line of text (optional)
        self._apikey: str = apikey  # API key used for restricted package access
        self.api_key_header_name = api_key_header_name

    def __del__(self):
        self.clear()

    def __copy__(self):
        return self.copy()

    def copy(self, *, dest=None):
        if dest is None:
            dest = ApiKey()
        dest.apikey_file = self.apikey_file
        dest._apikey = self._apikey
        return dest

    def __str__(self):
        if self._apikey is None:
            return "None"
        elif self._apikey == "":
            return "<empty string>"
        else:
            return "*****"

    @property
    def value(self) -> Union[str,None]:
        return self._apikey
    @value.setter
    def value(self, value:Union[str,None]):
        self._apikey = value

    def is_empty(self):
        return self._apikey is None

    def clear(self) -> None:
        self._apikey = None

    def load_from_environ(self, *, error_not_found:bool=False) -> bool:
        """
        Load CKAN API key from environment variables, by order of priority:

        By default, no environment variables are used.
        """
        return False

    def load_apikey(self, apikey_file:str=None, *, base_dir:str=None, error_not_found:bool=True) -> bool:
        """
        Load the API key from file.
        The file should contain a valid API key in the first line of text.

        :param apikey_file: path to the API key file. The following keywords are accepted:
            - "environ": the API key will be looked up in the environment variable with load_from_environ
        :param base_dir: base directory to find the API key file, if a relative path is provided
        :param error_not_found: option to raise an exception if the API key file is not found
        :return:
        """
        if apikey_file is None:
            apikey_file = self.apikey_file
        apikey_file = path_rel_to_dir(apikey_file, base_dir=base_dir, keyword_exceptions={environ_keyword})
        if apikey_file is None:
            raise ApiKeyFileError('apikey_file is required')
        api_keyword = apikey_file.strip().lower()
        if api_keyword == environ_keyword:
            return self.load_from_environ(error_not_found=error_not_found)
        if not(os.path.isfile(apikey_file)) and not error_not_found:
            msg = f"API key file does not exist: {apikey_file}"
            warn(msg)
            return False
        with open(apikey_file, 'r') as f:
            apikey = f.readline().strip()
            f.close()
        self.value = apikey
        self.apikey_file = apikey_file
        return True

    def get_auth_header(self) -> Dict[str, str]:
        """
        Returns the correct header with the API key for the requests needing it.
        If no API key was loaded, returns an empty dictionary.
        """
        if self.value is not None:
            apikey_encoded = self.value
            if isinstance(self.api_key_header_name, str):
                return {self.api_key_header_name: apikey_encoded}
            else:
                return {key: apikey_encoded for key in self.api_key_header_name}
        else:
            return {}

    def input(self):
        """
        Prompt the user to input the API key in the console window.

        :return:
        """
        api_key = getpass.getpass("Please enter the API key: ")
        self._apikey = api_key

    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="API key initialization")
        parser.add_argument("--apikey", type=str,
                            help="API key")
        parser.add_argument("--apikey-file", type=str,
                            help="Path to a file containing the API key (first line)")
        return parser

    def _cli_args_apply(self, args: argparse.Namespace, *, base_dir: str = None, error_not_found: bool = True) -> None:
        if args.apikey is not None:
            self.value = args.apikey
        if args.apikey_file is not None:
            self.load_apikey(args.apikey_file, base_dir=base_dir, error_not_found=error_not_found)


class CkanApiKey(ApiKey):
    """
    CKAN Database API key storage class.
    """
    CKAN_API_KEY_HEADER_NAME = {"Authorization", "X-CKAN-API-Key"}  # match apikey_header_name of your CKAN instance
    CKAN_API_KEY_ENVIRON = "CKAN_API_KEY"  # not recommended to store sensitive information in environment variables
    API_KEY_FILE_ENVIRON = "CKAN_API_KEY_FILE"
    API_KEY_FILE_DEFAULT = os.path.expanduser(r"~/.ckan/__CKAN_API_KEY__.txt")  # default API key file location for CKAN

    def __init__(self, *, apikey:str=None, apikey_file:str=None, apikey_auto_load:bool=True):
        """
        CKAN Database API key storage class.

        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        :param apikey_auto_load: option to automatically load the API key from file or envrionment variables

        Order of priority:
        ====
        1. Value of `apikey`
        2. Contents of file pointed by `apikey_file`
        3. Value of environment variable `CKAN_API_KEY`
        4. Contents of file pointed by the environment variable `CKAN_API_KEY_FILE`
        5. Contents of the file at the default location: `~/.ckan/__CKAN_API_KEY__.txt`
        """
        super().__init__(apikey=apikey, apikey_file=apikey_file, api_key_header_name=self.CKAN_API_KEY_HEADER_NAME)
        if apikey_auto_load:
            if self.is_empty() and apikey_file is not None:
                self.load_apikey(error_not_found=False)
            if self.is_empty():
                if self.load_from_environ(error_not_found=False, empty_warning=False):
                    print(f"API key loaded from file: {self.apikey_file}")

    def copy(self, *, dest=None) -> "CkanApiKey":
        if dest is None:
            dest = CkanApiKey()
        super().copy(dest=dest)
        return dest

    @staticmethod
    def get_default_apikey_file() -> Union[str, None]:
        env_apikey_file = os.environ.get(CkanApiKey.API_KEY_FILE_ENVIRON)   # "CKAN_API_KEY_FILE"
        if env_apikey_file:
            assert not env_apikey_file.strip().lower() == environ_keyword  # this value would create an infinite loop
            return sanitize_path(env_apikey_file)
        elif os.path.exists(CkanApiKey.API_KEY_FILE_DEFAULT):
            return CkanApiKey.API_KEY_FILE_DEFAULT
        else:
            return None

    def load_from_environ(self, *, error_not_found:bool=False, empty_warning:bool=True) -> bool:
        """
        Load CKAN API key from environment variables, by order of priority:

        - `CKAN_API_KEY`: for the raw API key (it is not recommended to store API key in an environment variable)
        - `CKAN_API_KEY_FILE`: path to a file containing a valid API key in the first line of text

        :param error_not_found: raise an error if the API key file was not found
        :return:
        """
        apikey = os.environ.get(self.CKAN_API_KEY_ENVIRON)            # "CKAN_API_KEY"
        default_apikey_file = CkanApiKey.get_default_apikey_file()
        if apikey is not None:
            msg = f"It is not recommended to store sensitive information in environment variables such as the API key ({self.CKAN_API_KEY_ENVIRON})"
            warn(msg)
            self.value = apikey
            return True
        elif default_apikey_file is not None:
            return self.load_apikey(default_apikey_file, error_not_found=error_not_found)
        else:
            if empty_warning:
                msg = f"No API key was found in the environment variable {self.CKAN_API_KEY_ENVIRON}"
                warn(msg)
            return False

    def input(self):
        """
        Prompt the user to input the API key in the console window.

        :return:
        """
        api_key = getpass.getpass("Please enter the CKAN API key: ")
        self._apikey = api_key

    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="CKAN API key initialization")
        ApiKey._setup_cli_parser(parser=parser)
        return parser

