#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore.
"""
from typing import List, Union, Generator
from collections import OrderedDict

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.builder.builder_errors import ResourceFileNotExistMessage
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.builder.builder_resource_datastore_multi_folder import BuilderDataStoreFolder
from ckanapi_harvesters.builder.builder_resource_multi_abc import FileChunkDataFrame


class BuilderDataStoreCkan(BuilderDataStoreFolder):
    """
    Merge of existing CKAN DataStores (on the same server) into a single DataStore
    """
    def __init__(self, *, parent, name: str = None, format: str = None, description: str = None,
                 resource_id: str = None, download_url: str = None, file_name: str = None, options_string:str=None, base_dir:str=None):
        super().__init__(parent=parent, name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, dir_name="", options_string=options_string, base_dir=base_dir)
        self.upsert_method: UpsertChoice = UpsertChoice.Upsert
        self.resource_ids: List[str] = [resource_id.strip() for resource_id in ckan_tags_sep.split(file_name)]
        raise NotImplementedError()  # not tested

    def copy(self, *, dest=None, parent=None):
        if dest is None:
            dest = BuilderDataStoreCkan(parent=self.parent_package_builder)
        super().copy(dest=dest, parent=parent)
        dest.resource_ids = self.resource_ids
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row, base_dir=base_dir)
        file_name_field: str = _string_from_element(row["file/url"], strip=True)
        self._user_fields_used.add("file/url")
        if file_name_field is not None:
            self.resource_ids = [resource_id.strip() for resource_id in ckan_tags_sep.split(file_name_field)]
        else:
            self.resource_ids = []

    @staticmethod
    def resource_mode_str() -> str:
        return "CKAN DataStore merge"

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return True

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = ckan_tags_sep.join(self.resource_ids)
        return d

    ## upload chunks ----------------
    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        # check resource ids exist on CKAN
        missing_resources = []
        for resource_id in self.resource_ids:
            if ckan.get_resource_info_or_request(resource_id, error_not_found=False) is None:
                missing_resources.append(resource_id)
        if len(missing_resources) == 0:
            return None
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error, f"Missing source resources for {self.name}: {','.join(missing_resources)}")

    def get_sample_file_path(self, resources_base_dir:str, ckan:Union[CkanApi,None]=None, file_index:int=0) -> Union[str,None]:
        return None

    def list_local_files(self, resources_base_dir:str, ckan:CkanApi, cancel_if_present:bool=True) -> List[str]:
        # obtain row count of each resource
        self.local_file_size = [0] * len(self.resource_ids)
        for index, resource_id in enumerate(self.resource_ids):
            resource_info = ckan.get_resource_info_or_request(resource_id, error_not_found=False, datastore_info=True)
            if resource_info is not None and resource_info.datastore_info is not None and resource_info.datastore_info.row_count is not None:
                self.local_file_size[index] = resource_info.datastore_info.row_count
        self.local_file_size_sum = sum(self.local_file_size,0)
        return self.local_file_list

    def _update_metadata(self, ckan: CkanApi, *, base_dir:str=None):
        # obtain metadata from first resource of list
        super()._update_metadata(ckan, base_dir=base_dir)
        resource_info = ckan.get_resource_info_or_request(self.resource_ids[0], error_not_found=True, datastore_info=True)
        self.resource_attributes_data_source = None
        self.field_builders_data_source = None
        if resource_info is not None:
            self.resource_attributes_data_source = resource_info.copy()
            if resource_info.datastore_info is not None and resource_info.datastore_info.fields_dict is not None:
                self.field_builders_data_source = OrderedDict()
                for field_info in resource_info.datastore_info.fields_dict:
                    self.field_builders_data_source[field_info.field_name] = BuilderField._from_ckan_field(field_info)

    def get_local_df_chunk_generator(self, resources_base_dir:str, ckan:CkanApi, allow_chunks:bool=True,
                                     **kwargs) -> Generator[FileChunkDataFrame, None, None]:
        for file_index, resource_id in enumerate(self.resource_ids):
            self.file_semaphore.acquire()
            file_position = 0
            chunk_index = 0
            generator = ckan.datastore_search_page_generator(resource_id=resource_id, search_all=True)
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


