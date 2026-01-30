#!python3
# -*- coding: utf-8 -*-
"""
Harvest from a mongo database using pymongo
"""
from typing import Union, List, Any, Dict
from types import SimpleNamespace
from collections import OrderedDict
import json
import argparse


try:
    import pymongo
    import pymongo.client_session
    import pymongo.database
except ImportError:
    pymongo = SimpleNamespace(MongoClient=None, client_session=SimpleNamespace(ClientSession=None),
                              database=SimpleNamespace(Database=None), collection=SimpleNamespace(Collection=None))


from ckanapi_harvesters.harvesters.harvester_errors import (HarvesterRequirementError, HarvesterArgumentRequiredError, ResourceNotFoundError)
from ckanapi_harvesters.harvesters.harvester_abc import TableHarvesterABC, DatasetHarvesterABC, DatabaseHarvesterABC
from ckanapi_harvesters.harvesters.harvester_model import TableMetadata, DatasetMetadata
from ckanapi_harvesters.harvesters.harvester_params import DatasetParams, DatabaseParams
from ckanapi_harvesters.harvesters.pymongo_data_cleaner import pymongo_default_data_cleaner, pymongo_default_df_conversion
from ckanapi_harvesters.harvesters.pymongo_data_cleaner import mongodb_keep_id_column_trace, mongodb_id_new_column
from ckanapi_harvesters.harvesters.pymongo_params import TableParamsMongoCollection
from ckanapi_harvesters.auxiliary.urls import url_join
from ckanapi_harvesters.auxiliary.ckan_auxiliary import ssl_arguments_decompose, assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_errors import UrlError
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel


mongodb_excluded_collections = {"system.profile"}


class DatabaseHarvesterMongoServer(DatabaseHarvesterABC):
    """
    This class manages the connection to a MongoDB server.
    It can list datasets (MongoDB databases) but this call could lead to an error.
    """
    def __init__(self, params:DatabaseParams=None):
        super().__init__(params)
        if pymongo.MongoClient is None:
            raise HarvesterRequirementError("pymongo", "pymongo")
        self.params.harvest_method = "Pymongo"
        self.mongo_client: Union[pymongo.MongoClient,None] = None
        self.mongo_session: Union[pymongo.client_session.ClientSession,None] = None
        if self.params.auth_url is None and self.params.port is None and self.params.host is None:
            raise HarvesterArgumentRequiredError("auth-url", "pymongo", "This argument defines the url used to authenticate.")

    @staticmethod
    def init_from_options_string(options_string:str, base_dir:str=None) -> "DatabaseHarvesterMongoServer":
        params = DatabaseParams()
        params.parse_options_string(options_string, base_dir=base_dir)
        return DatabaseHarvesterMongoServer(params)

    def copy(self, *, dest=None):
        if dest is None:
            dest = DatabaseHarvesterMongoServer()
        return super().copy(dest=dest)

    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        if cancel_if_connected and self.mongo_client is not None:
            return self.mongo_client
        else:
            if self.mongo_client is not None:
                self.mongo_session.end_session()
                self.mongo_client.close()
                self.mongo_session = None
                self.mongo_client = None
            ssl, ssl_certfile = ssl_arguments_decompose(self.params.verify_ca)
            auth_url = self.params.auth_url
            if auth_url is None:
                if self.params.url is not None:
                    auth_url = self.params.url
                elif self.params.host is not None:
                    auth_url = f"mongodb://{self.params.host}"
                    if self.params.port is not None:
                        auth_url += f":{self.params.port}"
                else:
                    raise UrlError("No Mongo URL provided")
                if self.params.auth_url_suffix is not None:
                    auth_url = url_join(auth_url, self.params.auth_url_suffix)
                self.params.auth_url = auth_url
            self.mongo_client = pymongo.MongoClient(auth_url, username=self.params.login.username, password=self.params.login.password,
                                                    ssl=ssl, tlscafile=ssl_certfile,
                                                    timeoutMS=self.params.timeout*1000.0 if self.params.timeout is not None else None)
            self.mongo_session = self.mongo_client.start_session()
            if self.params.host is None and self.params.port is None:
                # complete with host and port parsed by MongoClient
                mongo_address = self.mongo_client.address
                if mongo_address is not None:
                    self.params.host, self.params.port = mongo_address
            return self.mongo_client

    def is_connected(self) -> bool:
        return self.mongo_client is not None

    def disconnect(self) -> None:
        if self.mongo_client is not None:
            if self.mongo_session is not None:
                self.mongo_session.end_session()
            self.mongo_client.close()
            self.mongo_client = None
            self.mongo_session = None

    def check_connection(self, *, new_connection:bool=False, raise_error:bool=False) -> Union[None, ContextErrorLevelMessage]:
        try:
            self.connect(cancel_if_connected=not new_connection)
            # the following line requires specific admin rights (unexpected)
            # remote_collections = self.mongo_client.list_database_names(self.mongo_session)  # this tests the database connection
        except Exception as e:
            if raise_error:
                raise e from e
            else:
                return ContextErrorLevelMessage("Mongo Harvester", ErrorLevel.Error, f"Failed to connect to {self.params.auth_url}: {e}")

    def get_dataset_harvester(self, dataset_name: str) -> "DatasetHarvesterMongoDatabase":
        params_dataset = self.params.copy(dest=DatasetParams())
        params_dataset.dataset = dataset_name
        dataset_harvester = DatasetHarvesterMongoDatabase(params_dataset)
        self.copy(dest=dataset_harvester)
        dataset_harvester.params = params_dataset
        dataset_harvester._finalize_connection()
        return dataset_harvester

    def list_datasets(self, return_metadata: bool = True) -> Union[List[str], OrderedDict[str, DatasetMetadata]]:
        # this would raise an unauthorized error !
        self.connect()
        dataset_list = self.mongo_client.list_database_names(self.mongo_session)
        if return_metadata:
            return OrderedDict([(name, self.get_dataset_harvester(name).query_dataset_metadata()) for name in dataset_list])
        else:
            return dataset_list


class DatasetHarvesterMongoDatabase(DatabaseHarvesterMongoServer, DatasetHarvesterABC):
    """
    A CKAN dataset corresponds to a MongoDB database (set of collections).
    """
    def __init__(self, params:DatasetParams=None):
        super().__init__(params)
        self.mongo_database: Union[pymongo.database.Database,None] = None
        self.dataset_metadata: Union[DatasetMetadata, None] = None  # DatasetHarvesterABC
        # use database attribute if dataset is not specified (ambiguity on name - database attribute is not used above)
        if self.params.dataset is None:
            self.params.dataset = self.params.database
        if self.params.dataset is None:
            raise HarvesterArgumentRequiredError("dataset", "pymongo", "This argument defines the mongo database used")

    @staticmethod
    def init_from_options_string(options_string:str, base_dir:str=None) -> "DatasetHarvesterMongoDatabase":
        params = DatasetParams()
        params.parse_options_string(options_string, base_dir=base_dir)
        return DatasetHarvesterMongoDatabase(params)

    def _finalize_connection(self):
        if super().is_connected() and self.mongo_database is None:
            # remote_datasets = self.mongo_client.list_database_names(self.mongo_session)
            # assert_or_raise(self.params.dataset in remote_datasets, ResourceNotFoundError("Database", self.params.dataset, self.params.auth_url))
            self.mongo_database = self.mongo_client[self.params.dataset]

    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        if not (cancel_if_connected and self.is_connected()):
            super().connect(cancel_if_connected=cancel_if_connected)
            self._finalize_connection()
        return self.mongo_client

    def is_connected(self) -> bool:
        return super().is_connected()

    def disconnect(self) -> None:
        if super().is_connected():
            self.mongo_database = None
            super().disconnect()

    def check_connection(self, *, new_connection: bool = False, raise_error: bool = False) -> Union[None, ContextErrorLevelMessage]:
        try:
            super().check_connection(new_connection=new_connection, raise_error=raise_error)
            remote_collections = self.mongo_database.list_collection_names(self.mongo_session)  # this tests the database connection
        except Exception as e:
            if raise_error:
                raise e from e
            else:
                return ContextErrorLevelMessage("Mongo Harvester", ErrorLevel.Error,
                                                f"Failed to connect to {self.params.auth_url}: {e}")
        if self.mongo_database is None:
            return ContextErrorLevelMessage("Mongo Harvester", ErrorLevel.Error, f"Failed to connect to {self.params.auth_url}: <no error message>")
        else:
            return None

    def query_dataset_metadata(self, cancel_if_present:bool=True) -> DatasetMetadata:
        self.connect()
        if cancel_if_present and self.dataset_metadata is not None:
            return self.dataset_metadata
        else:
            self.dataset_metadata = DatasetMetadata()
            self.dataset_metadata.name = self.mongo_database.name
            self.dataset_metadata.tables = self.list_tables(return_metadata=True)
            return self.dataset_metadata

    def clean_dataset_metadata(self) -> DatasetMetadata:
        return self.query_dataset_metadata().copy()

    def get_table_harvester(self, table_name:str) -> "TableHarvesterMongoCollection":
        params_table = self.params.copy(dest=TableParamsMongoCollection())
        if self.params.options_string is not None:
            # reparse options_string for table-specific arguments
            params_table.parse_options_string(self.params.options_string, base_dir=self.params.base_dir)
        params_table.table = table_name
        table_harvester = TableHarvesterMongoCollection(params_table)
        self.copy(dest=table_harvester)
        table_harvester.params = params_table
        table_harvester._finalize_connection()
        return table_harvester

    def list_tables(self, return_metadata:bool=True) -> Union[List[str], OrderedDict[str, TableMetadata]]:
        self.connect()
        remote_collections = [collection_name for collection_name in self.mongo_database.list_collection_names(session=self.mongo_session) if collection_name not in mongodb_excluded_collections]
        if return_metadata:
            return OrderedDict([(name, self.get_table_harvester(name).query_table_metadata()) for name in remote_collections])
        else:
            return remote_collections


class TableHarvesterMongoCollection(DatasetHarvesterMongoDatabase, TableHarvesterABC):
    """
    A table (CKAN DataStore) corresponds to a MongoDB collection.
    """
    _default_upload_fun = pymongo_default_df_conversion
    _default_primary_key = [mongodb_id_new_column]

    def __init__(self, params:TableParamsMongoCollection=None):
        super().__init__(params)
        self.params: TableParamsMongoCollection = params
        self.mongo_collection: Union[pymongo.collection.Collection,None] = None
        self.table_metadata: Union[TableMetadata, None] = None  # TableHarvesterABC
        if self.params.file_url_attr is not None:
            # File/URL attribute has priority over CLI
            self.params.table = self.params.file_url_attr
        if self.params.table is None:
            raise HarvesterArgumentRequiredError("table", "pymongo", "This argument defines the mongo collection used")

    @staticmethod
    def init_from_options_string(options_string:str, *, base_dir:str=None, file_url_attr:str=None) -> "TableHarvesterMongoCollection":
        params = TableParamsMongoCollection()
        params.parse_options_string(options_string, file_url_attr=file_url_attr, base_dir=base_dir)
        return TableHarvesterMongoCollection(params)

    def copy(self, *, dest=None):
        if dest is None:
            dest = TableHarvesterMongoCollection()
        super().copy(dest=dest)
        return dest

    def disconnect(self) -> None:
        if super().is_connected():
            self.mongo_collection = None
            super().disconnect()

    def _finalize_connection(self):
        super()._finalize_connection()
        if super().is_connected() and self.mongo_collection is None:
            mongo_database = self.mongo_database
            remote_collections = mongo_database.list_collection_names()
            collection_name = self.params.table
            assert_or_raise(collection_name in remote_collections, ResourceNotFoundError("Collection", self.params.dataset + '.' + collection_name, self.params.auth_url))
            collection = mongo_database[collection_name]
            self.mongo_collection = collection

    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        if not (cancel_if_connected and self.is_connected()):
            super().connect()
            self._finalize_connection()
        return self.mongo_client

    def check_connection(self, *, new_connection:bool=False, raise_error:bool=False) -> Union[None, ContextErrorLevelMessage]:
        super().check_connection(new_connection=new_connection, raise_error=raise_error)
        if self.mongo_collection is None:
            return ContextErrorLevelMessage("Mongo Harvester", ErrorLevel.Error, f"Failed to connect to {self.params.auth_url}: <no error message>")
        else:
            return None

    def query_table_metadata(self, cancel_if_present:bool=True) -> TableMetadata:
        self.connect()
        if cancel_if_present and self.table_metadata is not None:
            return self.table_metadata
        else:
            # TODO: query at least primary key and indexes
            self.table_metadata = TableMetadata()
            index_dict = self.mongo_collection.index_information(session=self.mongo_session)
            self.table_metadata.name = self.mongo_collection.name
            self.table_metadata.indexes = sum([[key[0] for key in index["key"]] for index in index_dict.values()], [])
            return self.table_metadata

    def clean_table_metadata(self) -> TableMetadata:
        clean_metadata = self.query_table_metadata().copy()
        if clean_metadata.indexes is not None:
            i_rm = []
            for i, name in enumerate(clean_metadata.indexes):
                if name == "_id":
                    if mongodb_keep_id_column_trace:
                        clean_metadata.indexes[i] = mongodb_id_new_column
                    else:
                        i_rm.append(i)
                elif "." in name:
                    i_rm.append(i)
            for i in reversed(i_rm):
                clean_metadata.indexes.pop(i)
        clean_metadata.indexes = None  # finally, do not specify indexes at all
        return clean_metadata

    def get_default_primary_key(self) -> List[str]:
        table_metadata = self.query_table_metadata()
        if table_metadata.primary_key is not None:
            return table_metadata.primary_key
        else:
            return TableHarvesterMongoCollection._default_primary_key

    def get_default_data_cleaner(self) -> Union[CkanDataCleanerABC, None]:
        data_cleaner = pymongo_default_data_cleaner()
        data_cleaner.param_mongodb_dbref_as_one_column = not self.params.dbref_expand
        return data_cleaner

    def list_queries(self, *, new_connection:bool=False) -> List[Dict[str,Any]]:
        self.connect(cancel_if_connected=not new_connection)
        assert(self.mongo_collection is not None)
        query = json.loads(self.params.query_string) if self.params.query_string is not None else {}
        if self.params.verbose_harvester:
            print(f"Counting documents of table {self.params.table}")
        # num_rows = self.mongo_collection.count_documents(query, session=self.mongo_session)
        num_rows = self.mongo_collection.estimated_document_count()
        num_queries = num_rows // self.params.limit + 1
        if self.params.single_request:
            return [OrderedDict([("$match", query), ("$skip", i * self.params.limit), ("$limit", self.params.limit)]) for i in range(1)]
        else:
            queries_exact = [OrderedDict([("$match", query), ("$skip", i * self.params.limit), ("$limit", self.params.limit)]) for i in range(num_queries)]
            query_extra = OrderedDict([("$match", query), ("$skip", num_queries * self.params.limit)])
            return queries_exact + [query_extra]

    def query_data(self, query:Dict[str,Any]) -> List[dict]:
        assert(self.mongo_collection is not None)
        if isinstance(query, str):
            query = json.loads(query)
        if self.params.verbose_harvester:
            print(f"Pymongo request {query} on table {self.params.table}")
        cursor = self.mongo_collection.find(query["$match"], session=self.mongo_session).skip(query["$skip"])
        if "$limit" in query.keys():
            cursor = cursor.limit(query["$limit"])
        documents = list(cursor)
        return documents

