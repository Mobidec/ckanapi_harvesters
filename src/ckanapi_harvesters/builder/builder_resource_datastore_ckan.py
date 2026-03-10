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


class BuilderDataStoreCkan(BuilderDataStoreFolder):
    """
    Merge of existing CKAN DataStores (on the same server) into a single DataStore
    """
    def __init__(self, *, name: str = None, format: str = None, description: str = None,
                 resource_id: str = None, download_url: str = None, file_name: str = None, options_string:str=None, base_dir:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, dir_name="", options_string=options_string, base_dir=base_dir)
        self.upsert_method: UpsertChoice = UpsertChoice.Upsert
        self.resource_ids: List[str] = [resource_id.strip() for resource_id in ckan_tags_sep.split(file_name)]
        raise NotImplementedError()

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreCkan()
        super().copy(dest=dest)
        dest.resource_ids = self.resource_ids
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        file_name_field: str = _string_from_element(row["file/url"])
        if file_name_field is not None:
            self.resource_ids = [resource_id.strip() for resource_id in ckan_tags_sep.split(file_name_field)]
        else:
            self.resource_ids = []

    @staticmethod
    def resource_mode_str() -> str:
        return "Ckan DataStore merge"

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return True

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = ckan_tags_sep.join(self.resource_ids)
        return d

    ## upload chunks ----------------
    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        # TODO: check resource ids exist on CKAN
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        if os.path.isfile(file_path):
            return None
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error, f"Missing file for resource {self.name}: {file_path}")

    def get_sample_file_path(self, resources_base_dir:str, file_index:int=0) -> str:
        # TODO: return URL of first resource
        return resolve_rel_path(resources_base_dir, self.file_name, field=f"File/URL of resource {self.name}")

    def list_local_files(self, resources_base_dir:str, cancel_if_present:bool=True) -> List[str]:
        # TODO: obtain row count of each resource
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        self.file_size = os.path.getsize(file_path)
        self.local_file_list = [file_path]
        self.local_file_size = [self.file_size]
        self.local_file_size_sum = self.file_size
        return self.local_file_list

    def get_local_file_offset(self, file_chunk: FileChunkDataFrame) -> int:
        # TODO: update
        return file_chunk.file_position

    def get_local_file_total_size(self) -> int:
        # TODO: update
        return self.file_size

    def get_local_file_count(self) -> int:
        # TODO: update
        return 1

    def get_local_df_chunk_generator(self, resources_base_dir:str, allow_chunks:bool=False,
                                     **kwargs) -> Generator[FileChunkDataFrame, None, None]:
        # TODO: pass ckan argument
        for file_index, resource_id in enumerate(self.resource_ids):
            self.file_semaphore.acquire()
            file_position = 0
            chunk_index = 0
            generator = ckan.datastore_search_generator(resource_id=resource_id, search_all=True)
            self.file_semaphore.release()
            while True:
                try:
                    self.file_semaphore.acquire()
                    df = next(generator)
                    chunk_index += 1
                    file_position += len(df)
                    self.read_line_counter += len(df)
                    line_counter = self.read_line_counter
                    self.file_semaphore.release()
                except StopIteration:
                    self.file_semaphore.release()
                    return
                yield FileChunkDataFrame(df, resource_id, file_index, chunk_index,
                                         file_position=file_position, read_line_counter=line_counter)


