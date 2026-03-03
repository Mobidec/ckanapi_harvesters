#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Set, Any, Generator
import os
import io
from warnings import warn
from collections import OrderedDict
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC
from ckanapi_harvesters.harvesters.file_formats.file_format_init import init_file_format_datastore
from ckanapi_harvesters.builder.mapper_datastore import DataSchemeConversion
from ckanapi_harvesters.builder.builder_resource import BuilderResourceABC
from ckanapi_harvesters.auxiliary.ckan_errors import DuplicateNameError
from ckanapi_harvesters.auxiliary.path import resolve_rel_path
from ckanapi_harvesters.builder.builder_errors import RequiredDataFrameFieldsError, ResourceFileNotExistMessage, IncompletePatchError
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, find_duplicates, datastore_id_col
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreFileHeadABC
from ckanapi_harvesters.builder.builder_resource_multi_abc import BuilderMultiABC, FileChunkDataFrame

class BuilderDataStoreFile(BuilderDataStoreFileHeadABC, BuilderMultiABC):
    """
    Implementation supporting the reading of a file by chunks
    """
    def __init__(self, *, name: str = None, format: str = None, description: str = None,
                 resource_id: str = None, download_url: str = None, file_name: str = None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, file_name=file_name)
        self.file_size:int = 0
        self.upsert_method: UpsertChoice = UpsertChoice.Upsert

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreFile()
        super().copy(dest=dest)
        dest.file_name = self.file_name
        return dest

    ## upload chunks ----------------
    def upsert_request_df(self, ckan: CkanApi, df_upload:pd.DataFrame,
                          method:UpsertChoice=UpsertChoice.Upsert,
                          apply_last_condition:bool=None, always_last_condition:bool=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Call to ckan datastore_upset.
        Before sending the DataFrame, a call to df_upload_alter is made.
        This method is overloaded in BuilderDataStoreMultiABC and BuilderDataStoreFolder

        :param ckan:
        :param df_upload:
        :param method:
        :return:
        """
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=True)
        df_upload_transformed = self.df_mapper.df_upload_alter(df_upload, fields=self._get_fields_info())
        ret_df = ckan.datastore_upsert(df_upload_transformed, resource_id, method=method,
                                       apply_last_condition=apply_last_condition,
                                       always_last_condition=always_last_condition, data_cleaner=self.data_cleaner_upload)
        return df_upload_transformed, ret_df

    def upsert_request_df_no_return(self, ckan: CkanApi, df_upload:pd.DataFrame,
                                    method:UpsertChoice=UpsertChoice.Upsert,
                                    apply_last_condition:bool=None, always_last_condition:bool=None) -> None:
        """
        Calls upsert_request_df but does not return anything

        :return:
        """
        self.upsert_request_df(ckan=ckan, df_upload=df_upload, method=method,
                               apply_last_condition=apply_last_condition, always_last_condition=always_last_condition)
        return None

    def init_local_files_list(self, resources_base_dir:str, cancel_if_present:bool=True, **kwargs) -> List[str]:
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        self.file_size = os.path.getsize(file_path)
        return [file_path]

    def get_local_file_offset(self, file_chunk: FileChunkDataFrame) -> int:
        return file_chunk.file_position

    def get_local_file_total_size(self) -> int:
        return self.file_size

    def get_local_file_count(self) -> int:
        return 1

    def upload_request_final(self, ckan:CkanApi, *, force:bool=False) -> None:
        return self.upsert_request_final(ckan=ckan, force=force)

    def _unit_upload_apply(self, *, ckan: CkanApi, file_chunk: FileChunkDataFrame,
                           upload_alter:bool=True, overall_chunk_index: int, file_count: int, start_index: int, end_index: int,
                           method: UpsertChoice, **kwargs) -> None:
        file_index = file_chunk.file_index
        if file_index == 0 and file_chunk.chunk_index == 0 and self.upsert_method == UpsertChoice.Insert:
            return  # do not reupload the first document, which was used for the initialization of the dataset
        if start_index <= file_index and file_index < end_index:
            if upload_alter:
                file_chunk.df = self.df_mapper.df_upload_alter(file_chunk.df, self.sample_data_source, fields=self._get_fields_info(), **kwargs)
            self._call_progress_callback(self.get_local_file_offset(file_chunk),
                                         self.get_local_file_total_size(), info=file_chunk,
                                         file_index=file_index, file_count=file_count,
                                         context=f"{ckan.identifier} single-thread upload")
            self.upsert_request_df_no_return(ckan=ckan, df_upload=file_chunk.df, method=method,
                                             apply_last_condition=False)
        else:
            # self._call_progress_callback(index, total, info=df_upload_local, context=f"{ckan.identifier} single-thread skip")
            pass