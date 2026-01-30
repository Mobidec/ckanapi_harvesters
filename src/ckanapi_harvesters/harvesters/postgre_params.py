#!python3
# -*- coding: utf-8 -*-
"""
Harvest from a PostgreSQL database
"""
import argparse

from ckanapi_harvesters.harvesters.harvester_params import DatasetParams, TableParams


class DatasetParamsPostgreSchema(DatasetParams):
    """
    A CKAN dataset corresponds to a PostgreSQL schema (set of tables).
    This subclass of DatasetParams implements an alias attribute for dataset name called schema.
    """
    def __init__(self, source: "DatasetParamsPostgreSchema" =None):
        super().__init__(source)
        if source is not None:
            source.copy(dest=self)

    # alias property for the dataset name setting: schema in PostgreSQL
    @property
    def schema(self) -> str:
        return self.dataset
    @schema.setter
    def schema(self, value: str):
        self.dataset = value

    def copy(self, *, dest=None):
        if dest is None:
            dest = DatasetParamsPostgreSchema()
        super().copy(dest=dest)
        return dest

    @staticmethod
    def setup_cli_harvester_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        parser = DatasetParams.setup_cli_harvester_parser(parser=parser)
        parser.add_argument("--schema", type=str,
                            help="PostgreSQL schema name")
        # parser.add_argument("--dataset", help=argparse.SUPPRESS)  # do not display in help ==> conflict
        return parser

    def initialize_from_cli_args(self, args: argparse.Namespace, base_dir: str = None, error_not_found: bool = True,
                                 default_proxies: dict = None, proxy_headers: dict = None) -> None:
        super().initialize_from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                         default_proxies=default_proxies, proxy_headers=proxy_headers)
        if args.schema is not None:
            self.schema = args.schema


class TableParamsPostgre(TableParams): #, DatasetParamsPostgreSchema):
    def __init__(self, source: "TableParamsPostgre" =None):
        super().__init__(source)

    def copy(self, *, dest=None):
        if dest is None:
            dest = TableParamsPostgre()
        super().copy(dest=dest)
        return dest

    # DatasetParamsPostgreSchema:
    @property
    def schema(self) -> str:
        return self.dataset
    @schema.setter
    def schema(self, value: str):
        self.dataset = value

    @staticmethod
    def setup_cli_harvester_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        parser = TableParams.setup_cli_harvester_parser(parser=parser)
        # DatasetParamsPostgreSchema:
        # parser = DatasetParamsPostgreSchema.setup_cli_harvester_parser(parser=parser):
        parser.add_argument("--schema", type=str,
                            help="PostgreSQL schema name")
        # parser.add_argument("--dataset", help=argparse.SUPPRESS)  # do not display in help ==> conflict
        return parser

    def initialize_from_cli_args(self, args: argparse.Namespace, base_dir: str = None, error_not_found: bool = True,
                                 default_proxies: dict = None, proxy_headers: dict = None) -> None:
        super().initialize_from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                         default_proxies=default_proxies, proxy_headers=proxy_headers)
        # DatasetParamsPostgreSchema:
        if args.schema is not None:
            self.schema = args.schema

