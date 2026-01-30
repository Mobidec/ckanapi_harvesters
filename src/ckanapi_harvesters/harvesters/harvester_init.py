#!python3
# -*- coding: utf-8 -*-
"""
Harvester initialization from the options_string arguments
"""
from ckanapi_harvesters.harvesters.harvester_abc import TableHarvesterABC, DatasetHarvesterABC
from ckanapi_harvesters.harvesters.harvester_params import TableParams
from ckanapi_harvesters.harvesters.harvester_params import DatasetParams
from ckanapi_harvesters.harvesters.postgre_harvester import TableHarvesterPostgre, DatasetHarvesterPostgre
from ckanapi_harvesters.harvesters.pymongo_harvester import TableHarvesterMongoCollection, DatasetHarvesterMongoDatabase


def init_table_harvester_from_options_string(options_string:str, *, file_url_attr:str, base_dir:str=None) -> TableHarvesterABC:
    harvest_method = TableParams.parse_harvest_method(options_string)
    if harvest_method == "pymongo":
        return TableHarvesterMongoCollection.init_from_options_string(options_string, file_url_attr=file_url_attr, base_dir=base_dir)
    elif harvest_method == "postgre":
        return TableHarvesterPostgre.init_from_options_string(options_string, file_url_attr=file_url_attr, base_dir=base_dir)
    else:
        raise NotImplementedError(f"harvester method {harvest_method} not implemented")


def init_dataset_harvester_from_options_string(options_string:str, *, base_dir:str=None) -> DatasetHarvesterABC:
    harvest_method = DatasetParams.parse_harvest_method(options_string)
    if harvest_method == "pymongo":
        return DatasetHarvesterMongoDatabase.init_from_options_string(options_string, base_dir=base_dir)
    elif harvest_method == "postgre":
        return DatasetHarvesterPostgre.init_from_options_string(options_string, base_dir=base_dir)
    else:
        raise NotImplementedError(f"harvester method {harvest_method} not implemented")
