#!python3
# -*- coding: utf-8 -*-
"""
Class to parameterize and establish an SSH tunnel to a distant server
"""
from typing import Union
import io
import argparse
import getpass

try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = None

from ckanapi_harvesters.auxiliary.path import path_rel_to_dir
from ckanapi_harvesters.auxiliary.ckan_errors import RequirementError
from ckanapi_harvesters.auxiliary.login import Login
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig


class SshLogin(Login):
    LOGIN_FILE_ENVIRON = "SSH_AUTH_FILE"

    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="SSH login credentials initialization", add_help=False)
        parser.add_argument("--ssh-login-file", type=str,
                            help="Path to a text file containing SSH login credentials for authentification (user, password)")
        parser.add_argument("--ssh-user", type=str,
                            help="SSH user name (prefer using a file)")
        parser.add_argument("--ssh-password", type=str,
                            help="SSH user password (prefer using a file)")
        return parser

    def _cli_args_apply(self, args: argparse.Namespace, *, base_dir: str = None, error_not_found: bool = True) -> None:
        if args.login_file is not None:
            self.load_from_file(args.ssh_login_file, base_dir=base_dir, error_not_found=error_not_found)
        if args.ssh_user is not None:
            self.username = args.ssh_user
        if args.ssh_password is not None:
            self.password = args.ssh_password

    def input(self):
        """
        Prompt the user to input the login credentials in the console window.

        :return:
        """
        value = input("Please enter SSH user name: ")
        self.username = value
        value = getpass.getpass("Please enter SSH password: ")
        self.password = value


class SshTunnel:
    def __init__(self, *, remote_host: str = None, remote_port: int = None,
                 ssh_host: str=None, ssh_port: int=None, ssh_login: SshLogin=None,
                 ssh_login_file: str=None, ssh_pkey_file: str=None,
                 proxy:ProxyConfig=None) -> None:
        """
        SSH Tunnel parameterization functions.

        SSH remote is to be configured by the caller. The other attributes can be configured by the CLI.

        :param remote_host: Remote bind host. This is the service which is not exposed in clear, on server side.
        :param remote_port: Remote bind port.
        :param ssh_host: Remote SSH server host.
        :param ssh_port: Remote SSH server port.
        :param ssh_login_file: Login to connect to the SSH server.
        :param ssh_pkey_file: Path to the SSH private key file.
        """
        if ssh_login is None:
            ssh_login = SshLogin()
        if proxy is None:
            proxy = ProxyConfig()
        self.remote_host: str = remote_host
        self.remote_port: int = remote_port
        self.ssh_host: str = ssh_host
        self.ssh_port: int = ssh_port
        self.ssh_pkey_file = ssh_pkey_file
        self.ssh_login: SshLogin = ssh_login
        self.socks_proxy: ProxyConfig = proxy
        self.server: Union[SSHTunnelForwarder, None] = None
        if ssh_login_file is not None:
            self.ssh_login.load_from_file(ssh_login_file)
        self.verbose: bool = False
        raise NotImplementedError("Not tested")

    def __str__(self):
        if self.server is None:
            return f"SSH Tunnel to {self.remote_host}:{self.remote_port} (disconnected)"
        else:
            return f"SSH Tunnel to {self.remote_host}:{self.remote_port} ({self.get_tunnel_url()})"

    def __copy__(self):
        return self.copy()

    def copy(self) -> "SshTunnel":
        dest = SshTunnel()
        dest.remote_host = self.remote_host
        dest.remote_port = self.remote_port
        dest.ssh_host = self.ssh_host
        dest.ssh_port = self.ssh_port
        dest.ssh_login = self.ssh_login.copy()
        dest.socks_proxy = self.socks_proxy.copy()
        dest.ssh_pkey_file = self.ssh_pkey_file
        dest.verbose = self.verbose
        return dest

    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="SSH tunnel initialization", add_help=False)
        SshLogin._setup_cli_parser(parser)
        parser.add_argument("--ssh-host", type=str,
                            help="Remote SSH server host")
        parser.add_argument("--ssh-port", type=int,
                            help="Remote SSH server port")
        parser.add_argument("--ssh-key-file", type=str,
                            help="Path to the private key file")
        return parser

    def print_help_cli(self, display:bool=True) -> str:
        parser = self._setup_cli_parser()
        if display:
            parser.print_help()
        with io.StringIO() as stream:
            parser.print_help(stream)
            return stream.getvalue()

    def _cli_args_apply(self, args: argparse.Namespace, *, base_dir: str = None, error_not_found: bool = True) -> None:
        if args.ssh_host is not None:
            self.ssh_host = args.ssh_host
        if args.ssh_port is not None:
            self.ssh_port = args.ssh_port
        if args.ssh_key_file is not None:
            self.ssh_pkey_file = path_rel_to_dir(args.ssh_key_file, base_dir=base_dir)

    def start_tunnel(self):
        if SSHTunnelForwarder is None:
            raise RequirementError("sshtunnel", "SshTunnel")
        ssh_proxy = self.socks_proxy.get_host_port() if self.socks_proxy.is_defined() else None
        self.server = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_login.username,
            ssh_password=self.ssh_login.password,
            ssh_pkey=self.ssh_pkey_file,
            remote_bind_address=(self.remote_host, self.remote_port),
            ssh_proxy=ssh_proxy,
        )
        self.server.start()
        print(f"SSH tunnel established: localhost:{self.server.local_bind_port} -> {self.remote_host}:{self.remote_port}")

    def get_tunnel_host(self) -> str:
        if self.server is None:
            raise ConnectionError("SSH tunnel was not opened")
        return "localhost"
    def get_tunnel_port(self) -> int:
        if self.server is None:
            raise ConnectionError("SSH tunnel was not opened")
        return self.server.local_bind_port
    def get_tunnel_url(self) -> str:
        if self.server is None:
            raise ConnectionError("SSH tunnel was not opened")
        return f"localhost:{self.server.local_bind_port}"

    def is_connected(self) -> bool:
        return self.server is not None

    def is_defined(self) -> bool:
        return self.ssh_host is not None

    def close_tunnel(self):
        """
        Close SSH tunnel. Please close underlying connections before.
        """
        if self.server is not None:
            self.server.stop()
            self.server = None

    def __del__(self):
        self.ssh_login.__del__()
        self.close_tunnel()

if __name__ == '__main__':
    tunnel = SshTunnel(remote_host="www.google.com", remote_port=443,
                       ssh_host="", ssh_port=22,  # TODO: define host
                       ssh_login=SshLogin("demo", "password"))
    print("SSH tunnel CLI-format options:")
    tunnel.print_help_cli()

    print("Test connection")
    import requests
    tunnel.start_tunnel()
    A = requests.get(f"{tunnel.remote_host}:{tunnel.remote_port}")
    B = requests.get(tunnel.get_tunnel_url())
    tunnel.close_tunnel()
    print("Finished")

