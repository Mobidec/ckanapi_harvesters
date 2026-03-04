#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Set, Any, Generator, Collection
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
from ckanapi_harvesters.builder.mapper_datastore_multi import RequestMapperABC
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreABC
from ckanapi_harvesters.builder.builder_resource_datastore_multi_folder import BuilderDataStoreFolder
from ckanapi_harvesters.builder.builder_resource_multi_abc import BuilderMultiABC, FileChunkDataFrame


class BuilderDataStoreFile(BuilderDataStoreFolder):
    """
    Implementation supporting the reading of a file by chunks
    """
    def __init__(self, *, name: str = None, format: str = None, description: str = None,
                 resource_id: str = None, download_url: str = None, file_name: str = None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, dir_name="")
        self.file_size:int = 0
        self.upsert_method: UpsertChoice = UpsertChoice.Upsert
        self.file_name = file_name

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreFile()
        super().copy(dest=dest)
        dest.file_name = self.file_name
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.file_name: str = _string_from_element(row["file/url"])

    @staticmethod
    def resource_mode_str() -> str:
        return "DataStore from File"

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.file_name
        return d

    # def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
    #     file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
    #     if os.path.isfile(file_path):
    #         return None
    #     else:
    #         return ResourceFileNotExistMessage(self.name, ErrorLevel.Error, f"Missing file for resource {self.name}: {file_path}")

    ## upload chunks ----------------
    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        if os.path.isfile(file_path):
            return None
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error, f"Missing file for resource {self.name}: {file_path}")

    def get_sample_file_path(self, resources_base_dir:str, file_index:int=0) -> str:
        return resolve_rel_path(resources_base_dir, self.file_name, field=f"File/URL of resource {self.name}")

    def list_local_files(self, resources_base_dir:str, cancel_if_present:bool=True) -> List[str]:
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        self.file_size = os.path.getsize(file_path)
        self.local_file_list = [file_path]
        self.local_file_size = [self.file_size]
        self.local_file_size_sum = self.file_size
        return self.local_file_list

    def get_local_file_offset(self, file_chunk: FileChunkDataFrame) -> int:
        return file_chunk.file_position

    def get_local_file_total_size(self) -> int:
        return self.file_size

    def get_local_file_count(self) -> int:
        return 1

    ## download ----------------
    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download:bool=True,
                         force:bool=False, threads:int=1, return_data:bool=False) -> Union[pd.DataFrame,None]:
        if (not self.enable_download) and (not force):
            msg = f"Did not download resource {self.name} because download was disabled."
            warn(msg)
            return None
        if out_dir is not None:
            self.downloaded_destination = resolve_rel_path(out_dir, self.file_name, field=f"File/URL of resource {self.name}")
            if self.download_skip_existing and os.path.exists(self.downloaded_destination):
                return None
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        if self.local_file_format.append_allowed() and not return_data:
            download_generator = ckan.datastore_dump_generator(resource_id, search_all=full_download)
            first_df = None
            for df_download in download_generator:
                df = self.df_mapper.df_download_alter(df_download, fields=self._get_fields_info())
                if out_dir is not None:
                    if first_df is None:
                        self.local_file_format.write_file(df, self.downloaded_destination, fields=self._get_fields_info())
                        first_df = df  # maybe the first DataFrame could be used in the append function to reproduce treatments
                    else:
                        self.local_file_format.append_file(df, self.downloaded_destination, fields=self._get_fields_info())
        else:
            df_download = ckan.datastore_dump_generator(resource_id, search_all=full_download)
            df = self.df_mapper.df_download_alter(df_download, fields=self._get_fields_info())
            if out_dir is not None:
                os.makedirs(out_dir, exist_ok=True)
                self.local_file_format.write_file(df, self.downloaded_destination, fields=self._get_fields_info())
            if return_data:
                return df
        return None

    def download_request_full(self, ckan: CkanApi, out_dir: str, threads:int=1, external_stop_event=None,
                              start_index:int=0, end_index:int=None, force:bool=False) -> None:
        return self.download_request(ckan=ckan, out_dir=out_dir, full_download=True, force=force)

    def to_builder_datastore_folder(self,
                                    *, dir_name:str=None, primary_key:List[str]=None,
                                    file_query_list:Collection[Tuple[str,dict]]=None) -> BuilderDataStoreFolder:
        resource_folder = BuilderDataStoreFolder()
        resource_folder._load_from_df_row(self._to_row())
        resource_folder.field_builders = self.field_builders
        if dir_name is not None:
            resource_folder.dir_name = dir_name
        elif isinstance(self, BuilderDataStoreFolder):
            resource_folder.dir_name = self.dir_name
        else:
            resource_folder.dir_name, _ = os.path.splitext(self.file_name)
        resource_folder.package_name = self.package_name
        if isinstance(self.df_mapper, RequestMapperABC):
            resource_folder.df_mapper = self.df_mapper.copy()
        else:
            resource_folder.df_mapper.df_upload_fun = self.df_mapper.df_upload_fun
            resource_folder.df_mapper.df_download_fun = self.df_mapper.df_download_fun
        if primary_key is not None or file_query_list is not None:
            resource_folder.setup_default_file_mapper(primary_key=primary_key, file_query_list=file_query_list)
        resource_folder.downloaded_file_query_list = file_query_list
        return resource_folder