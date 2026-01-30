#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to convert formats between database and local files.
"""
from typing import Dict, List, Callable, Any, Tuple, Union, Set
import copy

import pandas as pd

from ckanapi_harvesters.builder.builder_errors import MissingCodeFileError
from ckanapi_harvesters.auxiliary.external_code_import import PythonUserCode
from ckanapi_harvesters.auxiliary.list_records import ListRecords


def simple_upload_fun(df_local: pd.DataFrame) -> None:
    for field in df_local.columns:
        if df_local[field].dtype == pd.Timestamp:
            df_local[field] = df_local[field].apply(pd.Timestamp.isoformat)  # ISO-8601 format


class DataSchemeConversion:
    def __init__(self, *, df_upload_fun:Callable[[pd.DataFrame], Any] = None,
                 df_download_fun:Callable[[pd.DataFrame], Any] = None):
        """
        Class to convert between local data formats and database formats

        :param df_upload_fun:
        :param df_download_fun:
        """
        self.df_upload_fun:Union[Callable[[Any, Any], Union[ListRecords, pd.DataFrame]], None] = df_upload_fun
        self.df_download_fun:Union[Callable[[pd.DataFrame, Any], pd.DataFrame], None] = df_download_fun

    def copy(self):
        return copy.deepcopy(self)

    def df_upload_alter(self, df_local: Union[pd.DataFrame, Any], file_name:str=None, mapper_kwargs:dict=None, **kwargs) -> pd.DataFrame:
        """
        Apply used-defined df_upload_fun if present

        :param df_local: the dataframe to upload
        :return: the dataframe ready for upload, converted in the format of the database
        """
        if mapper_kwargs is None: mapper_kwargs = {}
        mapper_kwargs["file_name"] = file_name
        if file_name is not None and isinstance(df_local, pd.DataFrame):
            df_local.attrs["source"] = file_name
        df_database = df_local
        if self.df_upload_fun is not None:
            # df_database = df_local.copy()  # unnecessary copy
            df_upload_fun = self.df_upload_fun
            df_database = df_upload_fun(df_database, **mapper_kwargs, **kwargs)
        if not isinstance(df_database, pd.DataFrame):
            if isinstance(df_database, ListRecords):
                pass  # also accept ListRecords (List[dict])
            elif self.df_upload_fun is None:
                raise TypeError("No upload function was defined to convert the data format to a DataFrame")
            else:
                raise TypeError("df_upload_fun must return a DataFrame")
        return df_database

    def df_download_alter(self, df_database:pd.DataFrame, file_query:dict=None, mapper_kwargs:dict=None, **kwargs) -> pd.DataFrame:
        """
        Apply used-defined df_download_fun if present.
        df_download_fun should be the reverse function of df_upload_fun

        :param df_database: the downloaded dataframe from the database
        :return: the dataframe ready to save, converted in the local format
        """
        if mapper_kwargs is None: mapper_kwargs = {}
        mapper_kwargs["file_query"] = file_query
        if file_query is not None:
            df_database.attrs["query"] = file_query
        df_local = df_database
        if self.df_download_fun is not None:
            # df_local = df_database.copy()  # unnecessary copy
            df_download_fun = self.df_download_fun
            df_local = df_download_fun(df_local, **mapper_kwargs, **kwargs)
        return df_local

    def _connect_aux_functions(self, module: PythonUserCode, aux_upload_fun_name:str, aux_download_fun_name:str) -> None:
        if (aux_upload_fun_name or aux_download_fun_name) and module is None:
            raise MissingCodeFileError()
        if aux_upload_fun_name:
            self.df_upload_fun = module.function_pointer(aux_upload_fun_name)
        if aux_download_fun_name:
            self.df_download_fun = module.function_pointer(aux_download_fun_name)

    def get_necessary_fields(self) -> Set[str]:
        return set()

