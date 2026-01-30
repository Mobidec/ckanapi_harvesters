#!python3
# -*- coding: utf-8 -*-
"""
Harvest from a mongo database using pymongo
"""
import argparse

from ckanapi_harvesters.harvesters.harvester_params import TableParams


class TableParamsMongoCollection(TableParams):
    """
    A table (CKAN DataStore) corresponds to a MongoDB collection.
    This subclass of TableParams implements an alias attribute for table name called collection.
    """
    def __init__(self, source: "TableParamsMongoCollection" =None):
        super().__init__(source)
        self.dbref_expand:bool = False
        if source is not None:
            source.copy(dest=self)

    # alias property for the table name setting: collection in MongoDB
    @property
    def collection(self) -> str:
        return self.table
    @collection.setter
    def collection(self, value: str):
        self.table = value

    def copy(self, *, dest=None):
        if dest is None:
            dest = TableParamsMongoCollection()
        super().copy(dest=dest)
        dest.dbref_expand = self.dbref_expand
        return dest

    @staticmethod
    def setup_cli_harvester_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        parser = TableParams.setup_cli_harvester_parser(parser=parser)
        parser.add_argument("--collection", type=str,
                            help="MongoDB collection name")  # normally specified in the File/URL attribute of builder
        # parser.add_argument("--table", help=argparse.SUPPRESS)  # do not display in help ==> conflict
        parser.add_argument("--dbref-expand",
                            help="Option to expand DBRefs",
                            action="store_true", default=False)  # applies to data cleaner
        return parser

    def initialize_from_cli_args(self, args: argparse.Namespace, base_dir: str = None, error_not_found: bool = True,
                                 default_proxies: dict = None, proxy_headers: dict = None) -> None:
        super().initialize_from_cli_args(args, base_dir=base_dir, error_not_found=error_not_found,
                                         default_proxies=default_proxies, proxy_headers=proxy_headers)
        self.dbref_expand = args.dbref_expand
        if args.collection is not None:
            self.collection = args.collection
