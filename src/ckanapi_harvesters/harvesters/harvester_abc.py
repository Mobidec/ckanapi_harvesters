#!python3
# -*- coding: utf-8 -*-
"""
Harvester base class
"""
from typing import Union, List, Any, Callable
from collections import OrderedDict
from abc import ABC, abstractmethod

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage
from ckanapi_harvesters.harvesters.harvester_model import DatasetMetadata, TableMetadata
from ckanapi_harvesters.harvesters.harvester_params import DatabaseParams, DatasetParams, TableParams
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC


class HarvesterConnectABC(ABC):
    def __del__(self):
        self.disconnect()

    @abstractmethod
    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def _finalize_connection(self):
        raise NotImplementedError()

    @abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def check_connection(self, *, new_connection:bool=False, raise_error:bool=False) -> Union[None, ContextErrorLevelMessage]:
        raise NotImplementedError()

    @abstractmethod
    def disconnect(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update_from_ckan(self, ckan):
        raise NotImplementedError()


class DatabaseHarvesterABC(HarvesterConnectABC, ABC):
    def __init__(self, params:DatabaseParams=None):
        if params is None:
            params = DatabaseParams()
        self.params: DatabaseParams = params

    @abstractmethod
    def copy(self, *, dest=None):
        dest.params = self.params.copy()
        return dest

    @staticmethod
    @abstractmethod
    def init_from_options_string(options_string:str, *, base_dir:str=None) -> "DatabaseHarvesterABC":
        raise NotImplementedError()

    def _finalize_connection(self):
        pass

    ## query methods interface ---------------
    @abstractmethod
    def get_dataset_harvester(self, dataset_name:str) -> "DatasetHarvesterABC":
        raise NotImplementedError()

    @abstractmethod
    def list_datasets(self, return_metadata:bool=True) -> Union[List[str], OrderedDict[str, DatasetMetadata]]:
        raise NotImplementedError()

    def update_from_ckan(self, ckan):
        self.params._update_from_ckan(ckan)


class DatasetHarvesterABC(DatabaseHarvesterABC, ABC):
    def __init__(self, params:DatasetParams=None):
        if params is None:
            params = DatasetParams()
        super().__init__(params)
        self.params: DatasetParams = params
        self.dataset_metadata: Union[DatasetMetadata, None] = None

    def __del__(self):
        self.disconnect()

    @abstractmethod
    def _finalize_connection(self):
        raise NotImplementedError()

    @abstractmethod
    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.dataset_metadata = self.dataset_metadata.copy() if self.dataset_metadata is not None else None
        return dest

    @staticmethod
    @abstractmethod
    def init_from_options_string(options_string:str, *, base_dir:str=None) -> "DatasetHarvesterABC":
        raise NotImplementedError()

    ## metadata interface ---------------
    @abstractmethod
    def query_dataset_metadata(self, cancel_if_present:bool=True) -> DatasetMetadata:
        self.connect()
        if cancel_if_present and self.dataset_metadata is not None:
            return self.dataset_metadata
        else:
            self.dataset_metadata = DatasetMetadata()
            # user needs to complete here
            self.dataset_metadata.tables = self.list_tables(return_metadata=True)
            return self.dataset_metadata

    def clean_dataset_metadata(self) -> DatasetMetadata:
        return self.query_dataset_metadata().copy()

    ## query methods interface ---------------
    @abstractmethod
    def get_table_harvester(self, table_name:str) -> "TableHarvesterABC":
        raise NotImplementedError()

    @abstractmethod
    def list_tables(self, return_metadata:bool=True) -> Union[List[str], OrderedDict[str, TableMetadata]]:
        raise NotImplementedError()


class TableHarvesterABC(DatasetHarvesterABC, ABC):
    _default_upload_fun: Union[Callable[[Any], pd.DataFrame], None] = None
    _default_primary_key: Union[List[str], None] = None

    def __init__(self, params:TableParams=None):
        if params is None:
            params = TableParams()
        super().__init__(params)
        self.params: TableParams = params
        self.table_metadata: Union[TableMetadata, None] = None

    def __del__(self):
        self.disconnect()

    @abstractmethod
    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.table_metadata = self.table_metadata.copy() if self.table_metadata is not None else None
        return dest

    @staticmethod
    @abstractmethod
    def init_from_options_string(options_string:str, *, base_dir:str=None, file_url_attr:str=None) -> "TableHarvesterABC":
        raise NotImplementedError()

    ## metadata interface ---------------
    @abstractmethod
    def query_table_metadata(self, cancel_if_present:bool=True) -> TableMetadata:
        self.connect()
        if cancel_if_present and self.table_metadata is not None:
            return self.table_metadata
        else:
            self.table_metadata = TableMetadata()
            # user needs to complete here
            return self.table_metadata

    def clean_table_metadata(self) -> TableMetadata:
        return self.query_table_metadata().copy()

    @classmethod
    def get_default_df_upload_fun(cls) -> Union[Callable[[Any], pd.DataFrame], None]:
        return cls._default_upload_fun

    def get_default_data_cleaner(self) -> Union[CkanDataCleanerABC, None]:
        return None

    @abstractmethod
    def get_default_primary_key(self) -> List[str]:
        return []

    ## query methods interface ---------------
    @abstractmethod
    def list_queries(self, *, new_connection:bool=False) -> List[Any]:
        self.connect(cancel_if_connected=not new_connection)
        raise NotImplementedError()

    @abstractmethod
    def query_data(self, query:Any) -> Union[List[dict], pd.DataFrame]:
        raise NotImplementedError()


