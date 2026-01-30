#!python3
# -*- coding: utf-8 -*-
"""
Methods to load authentication credentials (user, password)
"""
from typing import Union, Tuple, Dict
import getpass
from warnings import warn
import os
import argparse
import shlex

from ckanapi_harvesters.auxiliary.path import path_rel_to_dir
from ckanapi_harvesters.auxiliary.ckan_defs import environ_keyword
from ckanapi_harvesters.auxiliary.ckan_errors import ApiKeyFileError


class Login:
    def __init__(self, username:str=None, password:str=None, login_file:str=None):
        self._username:Union[str,None] = username
        self._password:Union[str,None] = password
        self.login_file: str = login_file  # path to a file containing a valid API key in the first line of text (optional)

    def __del__(self):
        self.clear()

    def __copy__(self):
        return self.copy()

    def copy(self, *, dest=None):
        if dest is None:
            dest = Login()
        dest.login_file = self.login_file
        dest._username = self._username
        dest._password = self._password
        return dest

    def clear(self) -> None:
        self._username = None
        self._password = None

    def is_empty(self):
        return self._username is None or self._password is None

    def __str__(self):
        if self.is_empty():
            return "None"
        elif self._username == "" and self._password == "":
            return "<empty string>"
        elif self._username == "":
            return "<empty user>"
        elif self._password == "":
            return "<empty password>"
        else:
            return "*****"

    @property
    def username(self) -> Union[str,None]:
        return self._username
    @username.setter
    def username(self, value:Union[str,None]):
        self._username = value
    @property
    def password(self) -> Union[str,None]:
        return self._password
    @password.setter
    def password(self, value:Union[str,None]):
        self._password = value

    def load_from_file(self, login_file:str=None, *, base_dir:str=None, error_not_found:bool=True) -> bool:
        """
        Load the credentials from file.
        The file should contain username in first line and password in second line.

        :param login_file: path to the API key file. The following keywords are accepted:
            - "environ": the API key will be looked up in the environment variable with load_from_environ
        :param base_dir: base directory to find the API key file, if a relative path is provided
        :param error_not_found: option to raise an exception if the API key file is not found
        :return:
        """
        if login_file is None:
            login_file = self.login_file
        login_file = path_rel_to_dir(login_file, base_dir=base_dir, keyword_exceptions={environ_keyword})
        if login_file is None:
            raise ApiKeyFileError('login_file is required')
        api_keyword = login_file.strip().lower()
        if api_keyword == environ_keyword:
            return False  # self.load_from_environ(error_not_found=error_not_found)
        if not(os.path.isfile(login_file)) and not error_not_found:
            msg = f"Login file does not exist: {login_file}"
            warn(msg)
            return False
        with open(login_file, 'r') as f:
            self.username = f.readline().strip()
            self.password = f.readline().rstrip('\n')
            f.close()
        self.login_file = login_file
        return True

    def input(self):
        """
        Prompt the user to input the login credentials in the console window.

        :return:
        """
        value = input("Please enter user name: ")
        self.username = value
        value = getpass.getpass("Please enter password: ")
        self.password = value

    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="Login credentials initialization")
        parser.add_argument("--login-file", type=str,
                            help="Path to a text file containing login credentials for authentification (user, password)")
        return parser

    def _cli_args_apply(self, args: argparse.Namespace, *, base_dir: str = None, error_not_found: bool = True) -> None:
        if args.login_file is not None:
            self.load_from_file(args.login_file, base_dir=base_dir, error_not_found=error_not_found)

    def to_tuple(self) -> Tuple[str,str]:
        return self.username, self.password

    @staticmethod
    def from_tuple(values: Tuple[str,str]) -> "Login":
        login = Login(*values)
        return login

    def to_dict(self) -> Dict[str,str]:
        return {"username": self.username, "password": self.password}

    @staticmethod
    def from_dict(values: Dict[str,str]) -> "Login":
        login = Login(**values)
        return login


class SSHLogin(Login):
    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="SSH login credentials initialization")
        parser.add_argument("--ssh-login-file", type=str,
                            help="Path to a text file containing SSH login credentials for authentification (user, password)")
        return parser

    def _cli_args_apply(self, args: argparse.Namespace, *, base_dir: str = None, error_not_found: bool = True) -> None:
        if args.login_file is not None:
            self.load_from_file(args.ssh_login_file, base_dir=base_dir, error_not_found=error_not_found)

    def input(self):
        """
        Prompt the user to input the login credentials in the console window.

        :return:
        """
        value = input("Please enter SSH user name: ")
        self.username = value
        value = getpass.getpass("Please enter SSH password: ")
        self.password = value

