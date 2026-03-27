#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements the basic resources. See builder_datastore for specific functions to initiate datastores.
"""
from typing import Any, Generator, Union, Set, List, Dict, Tuple
import os
import requests
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, find_duplicates
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_errors import DuplicateNameError
from ckanapi_harvesters.auxiliary.path import resolve_rel_path, glob_rm_glob
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreABC
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.harvesters.file_formats.file_format_init import FileFormatABC, init_file_format_datastore
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanCallbackLevel
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.builder.builder_resource_datastore_file import BuilderDataStoreFile
from ckanapi_harvesters.builder.builder_resource_multi_abc import FileChunkDataFrame
from ckanapi_harvesters.builder.builder_resource_multi_file import BuilderMultiFile


class BuilderMultiDataStore(BuilderMultiFile, BuilderDataStoreABC):
    def __init__(self, *, parent, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None):
        super().__init__(parent=parent, name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.field_builders: Union[Dict[str, BuilderField],None] = None
        self.field_builders_user: Union[Dict[str, BuilderField],None] = None
        self.field_builders_data_source: Union[Dict[str, BuilderField],None] = None
        self.primary_key: Union[List[str],None] = None
        self.indexes: Union[List[str],None] = None
        self.aux_upload_fun_name:str = ""
        self.aux_download_fun_name:str = ""
        self.aux_read_fun_name:str = ""
        self.aux_write_fun_name:str = ""
        self.data_cleaner_upload:Union[CkanDataCleanerABC,None] = None
        self.local_file_format: FileFormatABC = init_file_format_datastore(self.resource_attributes_user.format, self.options_string, self.aux_read_fun_name, self.aux_write_fun_name)
        self.read_line_counter: int = 0

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderMultiDataStore(parent=self.parent_package)
        super().copy(dest=dest)
        dest.field_builders = copy.deepcopy(self.field_builders)
        dest.field_builders_user = copy.deepcopy(self.field_builders_user)
        dest.field_builders_data_source = copy.deepcopy(self.field_builders_data_source)
        dest.primary_key = copy.deepcopy(self.primary_key)
        dest.indexes = copy.deepcopy(self.indexes)
        dest.aux_upload_fun_name = self.aux_upload_fun_name
        dest.aux_download_fun_name = self.aux_download_fun_name
        dest.local_file_format = self.local_file_format.copy()
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row, base_dir=base_dir)
        primary_keys_string: str = _string_from_element(row["primary key"])
        indexes_string: str = _string_from_element(row["indexes"])
        if primary_keys_string is not None:
            if primary_keys_string.lower() == "none":
                self.primary_key = []
            else:
                self.primary_key = [field.strip() for field in primary_keys_string.split(ckan_tags_sep)]
        if indexes_string is not None:
            if indexes_string.lower() == "none":
                self.indexes = []
            else:
                self.indexes = [field.strip() for field in indexes_string.split(ckan_tags_sep)]
        if "upload function" in row.keys():
            self.aux_upload_fun_name: str = _string_from_element(row["upload function"], empty_value="")
        if "download function" in row.keys():
            self.aux_download_fun_name: str = _string_from_element(row["download function"], empty_value="")

    def _load_fields_df(self, fields_df: pd.DataFrame):
        fields_df.columns = fields_df.columns.map(str.lower)
        fields_df.columns = fields_df.columns.map(str.strip)
        self.field_builders_user = {}
        for row_loc, row in fields_df.iterrows():
            field_builder = BuilderField()
            field_builder._load_from_df_row(row=row)
            self.field_builders_user[field_builder.name] = field_builder

    def _check_field_duplicates(self):
        duplicates = find_duplicates([field_builder.name for field_builder in self.field_builders_user.values()])
        if len(duplicates) > 0:
            raise DuplicateNameError("Field", duplicates)

    def _get_fields_dict(self) -> Dict[str, dict]:
        self._check_field_duplicates()
        if self.field_builders_user is not None:
            fields_dict = {field_builder.name: field_builder._to_dict() for field_builder in self.field_builders_user.values()}
        else:
            fields_dict = None
        return fields_dict

    def _get_fields_df(self) -> Union[pd.DataFrame,None]:
        if self.field_builders_user is not None:
            fields_dict_list = [value for value in self._get_fields_dict().values()]
            fields_df = pd.DataFrame.from_records(fields_dict_list)
            return fields_df
        else:
            return None

    @staticmethod
    def resource_mode_str() -> str:
        return "MultiDataStore"

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["Primary key"] = ckan_tags_sep.join(self.primary_key) if self.primary_key else ""
        d["Indexes"] = ckan_tags_sep.join(self.indexes) if self.indexes is not None else ""
        return d

    def _data_store_builder_of_file(self, file_path:str) -> Tuple[BuilderDataStoreFile, str]:
        file_dir, file_name = os.path.split(file_path)
        ds_builder = BuilderDataStoreFile(parent=self.parent_package, name=file_name, description=self.resource_attributes_user.description, download_url=self.download_url,
                                          format=self.resource_attributes_user.format, file_name=file_name)
        ds_builder.field_builders = self.field_builders
        ds_builder.primary_key = self.primary_key
        ds_builder.indexes = self.indexes
        ds_builder.package_name = self.package_name
        ds_builder.aux_upload_fun_name = self.aux_upload_fun_name
        ds_builder.aux_download_fun_name = self.aux_download_fun_name
        ds_builder.aliases = None
        ds_builder.data_cleaner_upload = self.data_cleaner_upload
        ds_builder.local_file_format = self.local_file_format
        ds_builder.process_level = CkanCallbackLevel.MultiFileResource  # file of a multi-file resource
        return ds_builder, file_dir


    ## Upload ----------------
    def get_local_df_chunk_generator(self, resources_base_dir:str, ckan:CkanApi, excluded_files:Set[str]=None,
                                     allow_chunks:bool=True, **kwargs) -> Generator[FileChunkDataFrame, None, None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        for file_index, file_name in enumerate(self.local_file_list):
            self.file_semaphore.acquire()
            self.read_line_counter = 0
            self.file_semaphore.release()
            df_file = self.local_file_format.read_file(file_name, self.field_builders, allow_chunks=allow_chunks)
            if file_index == 0:
                self._merge_resource_attributes_from_file()
            if isinstance(df_file, pd.DataFrame) or isinstance(df_file, ListRecords):
                self.file_semaphore.acquire()
                self.read_line_counter += len(df_file)
                line_counter = self.read_line_counter
                self.file_semaphore.release()
                yield FileChunkDataFrame(df_file, file_name, file_index, 0, 0, line_counter)
            else:  # iterator
                with df_file as df_generator:
                    if hasattr(df_file, "handles"):
                        file_handle = df_file.handles.handle
                    else:
                        file_handle = None
                    previous_file_position = 0
                    # for chunk_index, df in enumerate(df_generator):
                    chunk_index = 0
                    file_position = 0
                    while True:
                        self.file_semaphore.acquire()
                        if file_handle is not None:
                            file_position = file_handle.buffer.tell()  # approximative position in file
                        else:
                            file_position = 0  # no position tracking available
                        try:
                            df = next(df_generator)
                            if file_handle is None and hasattr(df, "attrs") and "file_position" in df.attrs:
                                file_position = df.attrs["file_position"]
                            self.read_line_counter += len(df)
                            line_counter = self.read_line_counter
                        except StopIteration:
                            self.file_semaphore.release()
                            break
                        self.file_semaphore.release()
                        yield FileChunkDataFrame(df, file_name, file_index, chunk_index, previous_file_position, line_counter)
                        previous_file_position = file_position

    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True, file_index:int=0, allow_chunks:bool=True, **kwargs) -> GeneralDataFrame:
        generator = self.get_local_df_chunk_generator(resources_base_dir=resources_base_dir, ckan=None, allow_chunks=allow_chunks)
        chunk = next(generator)
        df_local = chunk.df
        generator.close()
        if upload_alter:
            df_upload = self.df_mapper.df_upload_alter(df_local, total_lines_read=chunk.read_line_counter,
                                                       fields=self._get_fields_info(), file_query=chunk.file_path)
            return df_upload
        else:
            return df_local

    def upload_file_chunk(self, ckan:CkanApi, package_id:str, file_chunk:FileChunkDataFrame, *,
                          reupload:bool=False, override_ckan:bool=False, cancel_if_present:bool=True) -> CkanResourceInfo:
        file_path = file_chunk.file_path
        ds_builder, file_dir = self._data_store_builder_of_file(file_path=file_path)
        if file_chunk.is_first_chunk:
            return ds_builder.patch_request(ckan=ckan, package_id=package_id, reupload=reupload,
                                            resources_base_dir=file_dir)
        else:
            ds_builder.upsert_request_df(ckan, file_chunk.df, file_name=file_path, total_lines_read=self.read_line_counter)
            return ckan.map.get_resource_info(resource_name=ds_builder.name, package_name=package_id)


    ## Download --------------
    def download_file_query_item_df(self, ckan: CkanApi, out_dir: str, file_query_item: str, full_download:bool=True) -> Tuple[str, pd.DataFrame]:
        resource_name = file_query_item
        ds_builder, _ = self._data_store_builder_of_file(file_path=resource_name)
        file_dir = resolve_rel_path(out_dir, glob_rm_glob(self.dir_name), field=f"File/URL of resource {self.name}")
        df = ds_builder.download_request(ckan, out_dir=file_dir, full_download=full_download)
        if df is not None:
            self.read_line_counter += len(df)
        return ds_builder.downloaded_destination, df

    def download_file_query_item(self, ckan: CkanApi, out_dir: str, file_query_item: str, full_download:bool=True) -> Tuple[Union[str,None], Union[requests.Response,None]]:
        downloaded_destination, df = self.download_file_query_item_df(ckan=ckan, out_dir=out_dir, file_query_item=file_query_item,full_download=full_download)
        return downloaded_destination, None

    def download_request_generator_df(self, ckan: CkanApi, out_dir: str,
                                   excluded_resource_names:Set[str]=None) -> Generator[Tuple[Union[str,None], Union[pd.DataFrame,None]], Any, None]:
        self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True,
                                           excluded_resource_names=excluded_resource_names)
        for file_query_item in self.get_file_query_generator():
            yield self.download_file_query_item_df(ckan=ckan, out_dir=out_dir, file_query_item=file_query_item)

