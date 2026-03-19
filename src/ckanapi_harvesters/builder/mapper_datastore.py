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

from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_errors import MissingCodeFileError
from ckanapi_harvesters.auxiliary.external_code_import import PythonUserCode
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.auxiliary.ckan_model import CkanField


class DataSchemeConversion:
    def __init__(self, *, df_upload_fun:Callable[[GeneralDataFrame,Any], GeneralDataFrame] = None,
                 df_download_fun:Callable[[GeneralDataFrame,Any], GeneralDataFrame] = None):
        """
        Class to convert between local data formats and database formats

        :param df_upload_fun:
        :param df_download_fun:
        """
        self.df_upload_fun:Union[Callable[[GeneralDataFrame, Any], GeneralDataFrame], None] = df_upload_fun
        self.df_download_fun:Union[Callable[[GeneralDataFrame, Any], GeneralDataFrame], None] = df_download_fun
        self.upload_upload_index_column: str = ""

    def copy(self):
        return copy.deepcopy(self)

    def df_upload_alter(self, df_local: Union[pd.DataFrame, List[dict], Any], *,
                        total_lines_read: int, fields:Dict[str, CkanField], file_query:str,
                        mapper_kwargs:dict=None, **kwargs) -> Union[pd.DataFrame, ListRecords]:
        """
        Apply used-defined df_upload_fun if present

        :param df_local: the DataFrame to upload
        :param total_lines_read: total number of lines read, including the current DataFrame
        :param fields: the known fields metadata.
        :param file_query: the name of the file the data originates from (or query)
        :param mapper_kwargs: extra arguments passed to df_upload_fun
        :return: the DataFrame ready for upload, converted in the format of the database
        """
        if mapper_kwargs is None: mapper_kwargs = {}
        mapper_kwargs["file_query"] = file_query
        mapper_kwargs["fields"] = fields
        mapper_kwargs["total_lines_read"] = total_lines_read
        if self.upload_upload_index_column:
            # insert an extra column keeping track of the last read line (default primary key)
            index_offset = total_lines_read - len(df_local)
            if isinstance(df_local, pd.DataFrame):
                index_offset -= df_local.index[0]  # index of DataFrame in file, not 0 if the file is read by chunks
                assert_or_raise(not(self.upload_upload_index_column in df_local.keys()), KeyError(f"{self.upload_upload_index_column} already exists"))
                df_local[self.upload_upload_index_column] = df_local.index + index_offset
            else:
                for index, line in enumerate(df_local):
                    assert_or_raise(not(self.upload_upload_index_column in line.keys()), KeyError(f"{self.upload_upload_index_column} already exists"))
                    line[self.upload_upload_index_column] = index + index_offset
        if file_query is not None and (isinstance(df_local, pd.DataFrame) or isinstance(df_local, ListRecords)):
            df_local.attrs["source"] = file_query
        df_database = df_local
        if self.df_upload_fun is not None:
            # df_database = df_local.copy()  # unnecessary copy
            df_upload_fun = self.df_upload_fun
            df_database = df_upload_fun(df_database, **mapper_kwargs, **kwargs)
        if not isinstance(df_database, pd.DataFrame):
            if isinstance(df_database, ListRecords):
                pass  # also accept ListRecords (List[dict])
            elif isinstance(df_database, list):
                df_database = ListRecords(df_database)
            elif self.df_upload_fun is None:
                raise TypeError("No upload function was defined to convert the data format to a DataFrame")
            else:
                raise TypeError("df_upload_fun must return a DataFrame")
        return df_database

    def df_download_alter(self, df_database:Union[pd.DataFrame, List[dict], Any], file_query:dict=None,
                          fields:Dict[str, CkanField]=None, mapper_kwargs:dict=None, **kwargs) -> Union[pd.DataFrame, ListRecords]:
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
            df_local = df_download_fun(df_local, fields=fields, **mapper_kwargs, **kwargs)
        if not isinstance(df_local, pd.DataFrame):
            if isinstance(df_local, ListRecords):
                pass  # also accept ListRecords (List[dict])
            elif isinstance(df_local, list):
                df_local = ListRecords(df_local)
            elif self.df_download_fun is None:
                raise TypeError("No download function was defined to convert the received DataFrame")
            else:
                raise TypeError("df_download_fun must return a DataFrame")
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

