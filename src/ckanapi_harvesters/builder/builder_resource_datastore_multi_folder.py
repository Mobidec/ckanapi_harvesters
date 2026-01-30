#!python3
# -*- coding: utf-8 -*-
"""
Code to initiate a DataStore defined by a large number of files to concatenate into one table.
This concrete implementation is linked to the file system.
"""
from typing import Dict, List, Collection, Callable, Any, Tuple, Generator, Union
import os
from warnings import warn
import glob
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.builder.mapper_datastore import DataSchemeConversion
from ckanapi_harvesters.builder.builder_errors import ResourceFileNotExistMessage
from ckanapi_harvesters.builder.builder_resource_datastore_multi_abc import BuilderDataStoreMultiABC
from ckanapi_harvesters.builder.builder_resource_datastore_multi_abc import datastore_multi_apply_last_condition_intermediary
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.path import resolve_rel_path, glob_rm_glob, list_files_scandir
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.builder.mapper_datastore_multi import RequestMapperABC, RequestFileMapperABC
from ckanapi_harvesters.builder.mapper_datastore_multi import default_file_mapper_from_primary_key
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreFile


class BuilderDataStoreFolder(BuilderDataStoreMultiABC):
    def __init__(self, *, file_query_list: List[Tuple[str,dict]]=None, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, dir_name:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.dir_name = dir_name
        # Functions inputs/outputs
        self.df_mapper: RequestFileMapperABC = default_file_mapper_from_primary_key(self.primary_key, file_query_list)
        self.local_file_list_base_dir:Union[str,None] = None
        self.local_file_list:Union[List[str],None] = None
        self.downloaded_file_query_list:Collection[Tuple[str,dict]] = file_query_list

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreFolder()
        super().copy(dest=dest)
        dest.dir_name = self.dir_name
        dest.local_file_list_base_dir = self.local_file_list_base_dir
        dest.local_file_list = copy.deepcopy(self.local_file_list)
        dest.downloaded_file_query_list = copy.deepcopy(self.downloaded_file_query_list)
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None) -> None:
        super()._load_from_df_row(row=row)
        self.df_mapper = default_file_mapper_from_primary_key(self.primary_key)
        self.dir_name: str = _string_from_element(row["file/url"])

    def setup_default_file_mapper(self, primary_key:List[str]=None, file_query_list:Collection[Tuple[str, dict]]=None) -> None:
        """
        This function enables the user to define the primary key and initializes the default file mapper.
        :param primary_key: manually specify the primary key
        :return:
        """
        df_mapper_mem = self.df_mapper
        if primary_key is not None:
            self.primary_key = primary_key
        self.df_mapper = default_file_mapper_from_primary_key(self.primary_key, file_query_list)
        if file_query_list is not None:
            self.downloaded_file_query_list = file_query_list
        # preserve upload/download functions
        self.df_mapper.df_upload_fun = df_mapper_mem.df_upload_fun
        self.df_mapper.df_download_fun = df_mapper_mem.df_download_fun

    @staticmethod
    def resource_mode_str() -> str:
        return "DataStore from Folder"

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.dir_name
        return d

    @staticmethod
    def from_file_datastore(resource_file: BuilderDataStoreFile,
                            *, dir_name:str=None, primary_key:List[str]=None,
                            file_query_list:Collection[Tuple[str,dict]]=None) -> "BuilderDataStoreFolder":
        resource_folder = BuilderDataStoreFolder()
        resource_folder._load_from_df_row(resource_file._to_row())
        resource_folder.field_builders = resource_file.field_builders
        if dir_name is not None:
            resource_folder.dir_name = dir_name
        elif isinstance(resource_file, BuilderDataStoreFolder):
            resource_folder.dir_name = resource_file.dir_name
        else:
            resource_folder.dir_name, _ = os.path.splitext(resource_file.file_name)
        resource_folder.package_name = resource_file.package_name
        if isinstance(resource_file.df_mapper, RequestMapperABC):
            resource_folder.df_mapper = resource_file.df_mapper.copy()
        else:
            resource_folder.df_mapper.df_upload_fun = resource_file.df_mapper.df_upload_fun
            resource_folder.df_mapper.df_download_fun = resource_file.df_mapper.df_download_fun
        if primary_key is not None or file_query_list is not None:
            resource_folder.setup_default_file_mapper(primary_key=primary_key, file_query_list=file_query_list)
        resource_folder.downloaded_file_query_list = file_query_list
        return resource_folder


    ## upload ---------------------------------------------------
    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        if os.path.isdir(resolve_rel_path(resources_base_dir, glob_rm_glob(self.dir_name), field=f"File/URL of resource {self.name}")):
            if len(self.list_local_files(resources_base_dir=resources_base_dir)) > 0:
                return None
            else:
                return ResourceFileNotExistMessage(self.name, ErrorLevel.Error,
                    f"Empty resource directory for resource {self.name}: {os.path.join(resources_base_dir, self.dir_name)}")
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error,
                f"Missing directory for resource {self.name}: {os.path.join(resources_base_dir, self.dir_name)}")

    def get_sample_file_path(self, resources_base_dir:str, file_index:int=0) -> Union[str,None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        return self.local_file_list[file_index]

    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True, file_index:int=0, **kwargs) -> pd.DataFrame:
        file_path:str = self.get_sample_file_path(resources_base_dir, file_index=file_index)
        return self.load_local_df(file=file_path, upload_alter=upload_alter, **kwargs)

    def load_local_df(self, file: str, *, upload_alter:bool=True, **kwargs) -> pd.DataFrame:
        # self.sample_data_source = resolve_rel_path(resources_base_dir, self.dir_name, file, field=f"File/URL of resource {self.name}")
        self.sample_data_source = file
        df_local = self.local_file_format.read_file(self.sample_data_source, fields=self._get_fields_info())
        if isinstance(df_local, pd.DataFrame):
            df_local.attrs["source"] = self.sample_data_source
        if upload_alter:
            df_upload = self.df_mapper.df_upload_alter(df_local, self.sample_data_source, fields=self._get_fields_info())
            return df_upload
        else:
            return df_local

    def get_local_file_generator(self, resources_base_dir:str, **kwargs) -> Generator[str, None, None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        for file_name in self.local_file_list:
            yield file_name

    def get_local_df_generator(self, resources_base_dir:str, **kwargs) -> Generator[pd.DataFrame, None, None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        for file_name in self.local_file_list:
            yield self.load_local_df(file_name, **kwargs)

    def list_local_files(self, resources_base_dir:str, cancel_if_present:bool=True) -> List[str]:
        if cancel_if_present and self.local_file_list is not None and self.local_file_list_base_dir == resources_base_dir:
            return self.local_file_list
        dir_search_path = resolve_rel_path(resources_base_dir, self.dir_name, field=f"File/URL of resource {self.name}")
        # file_list = [os.path.join(dir_search_path, file_name) for file_name in os.listdir(dir_search_path)]
        # file_list = [os.path.join(file.path, file.name) for file in list(os.scandir(dir_search_path)) if file.is_file()]
        search_query = dir_search_path
        file_list = glob.glob(search_query)
        # file_list = list_files_scandir(dir_search_path)
        file_list.sort()
        self.local_file_list = file_list
        self.local_file_list_base_dir = resources_base_dir
        return file_list

    def init_local_files_list(self, resources_base_dir:str, cancel_if_present:bool=True, **kwargs) -> List[str]:
        return self.list_local_files(resources_base_dir=resources_base_dir, cancel_if_present=cancel_if_present)

    def get_local_file_len(self) -> int:
        if self.local_file_list is None:
            raise RuntimeError("You must call list_local_files first")
        return len(self.local_file_list)

    def upsert_request_df(self, ckan: CkanApi, df_upload:pd.DataFrame,
                          method:UpsertChoice=UpsertChoice.Upsert,
                          apply_last_condition:bool=None, always_last_condition:bool=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Call to ckan datastore_upsert.
        Before sending the DataFrame, a call to df_upload_alter is made.
        This implementation optionally checks for the last line of the DataFrame based on the first columns of the primary key.

        :param ckan:
        :param df_upload:
        :param method:
        :return:
        """
        if apply_last_condition is None:
            apply_last_condition = True  # datastore_multi_apply_last_condition_intermediary
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=True)
        df_upload_local = df_upload
        df_upload_transformed = self.df_mapper.df_upload_alter(df_upload_local, fields=self._get_fields_info())
        file_query = self.df_mapper.get_file_query_of_df(df_upload_transformed)
        if file_query is not None:
            i_restart, upload_needed, row_count, df_row = self.df_mapper.last_inserted_index_request(ckan=ckan,
                                     resource_id=resource_id, df_upload=df_upload_transformed, file_query=file_query)
        else:
            i_restart, upload_needed, row_count, df_row = 0, True, -1, None
        if upload_needed:
            if i_restart > 0 and ckan.params.verbose_extra:
                print(f"Starting transfer from index {i_restart}")
            ret_df = ckan.datastore_upsert(df_upload_transformed.iloc[i_restart:], resource_id, method=method,
                                           apply_last_condition=apply_last_condition,
                                           always_last_condition=always_last_condition, data_cleaner=self.data_cleaner_upload)
        elif 0 <= row_count and row_count < len(df_row):
            msg = f"Sending full dataframe because is was shorter on server side"
            warn(msg)
            ret_df = ckan.datastore_upsert(df_upload_transformed, resource_id, method=method,
                                           apply_last_condition=apply_last_condition,
                                           always_last_condition=always_last_condition, data_cleaner=self.data_cleaner_upload)
        else:
            if ckan.params.verbose_extra:
                print(f"File up to date on server side")
            ret_df = None
        return df_upload_transformed, ret_df


    ## download ---------------------------------------------------------------------------
    def download_file_query_list(self, ckan: CkanApi, cancel_if_present:bool=True) -> List[Tuple[str, dict]]:
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and self.download_error_not_found:
            self.downloaded_file_query_list = []
            return []
        if not(cancel_if_present and self.downloaded_file_query_list is not None):
            file_query_list = self.df_mapper.download_file_query_list(ckan=ckan, resource_id=resource_id)
            self.downloaded_file_query_list = [(self.df_mapper.get_file_name_of_query(file_query), file_query) for file_query in file_query_list]
        return self.downloaded_file_query_list

    def setup_download_file_query_list(self, file_query_list: List[Tuple[str,dict]]) -> None:
        self.downloaded_file_query_list = file_query_list

    def init_download_file_query_list(self, ckan: CkanApi, out_dir: str, cancel_if_present:bool=True, **kwargs) -> List[Any]:
        if out_dir is not None:
            dir_tables = resolve_rel_path(out_dir, glob_rm_glob(self.dir_name, default_rec_dir=self.name), field=f"File/URL of resource {self.name}")
            os.makedirs(dir_tables, exist_ok=True)
        return self.download_file_query_list(ckan=ckan, cancel_if_present=cancel_if_present)

    def get_file_query_len(self) -> int:
        if self.downloaded_file_query_list is None:
            raise RuntimeError("You must call download_file_query_list first")
        return len(self.downloaded_file_query_list)

    def get_file_query_generator(self) -> Generator[Tuple[str,dict], Any, None]:
        for file_name, file_query in self.downloaded_file_query_list:
            yield file_name, file_query

    def download_file_query(self, ckan: CkanApi, out_dir: str, file_name:str, file_query:dict) \
            -> Tuple[Union[str,None], Union[pd.DataFrame,None]]:
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and self.download_error_not_found:
            return None, None
        self.download_file_query_list(ckan, cancel_if_present=True)
        file_out = None
        if out_dir is not None:
            file_out = resolve_rel_path(out_dir, glob_rm_glob(self.dir_name, default_rec_dir=self.name), file_name, field=f"File/URL of resource {self.name}")
            if self.download_skip_existing and os.path.exists(file_out):
                if ckan.params.verbose_extra:
                    print(f"Skipping existing file {file_out}")
                return file_out, None
        df_download = self.df_mapper.download_file_query(ckan=ckan, resource_id=resource_id, file_query=file_query)
        df = self.df_mapper.df_download_alter(df_download, file_query=file_query, fields=self._get_fields_info())
        if out_dir is not None:
            self.local_file_format.write_file(df, file_out, fields=self._get_fields_info())
        else:
            file_out = None
        return file_out, df

    def download_file_query_item(self, ckan: CkanApi, out_dir: str, file_query_item: Tuple[str,dict]) -> Tuple[str, pd.DataFrame]:
        file_name, file_query = file_query_item
        return self.download_file_query(ckan=ckan, file_name=file_name, file_query=file_query, out_dir=out_dir)

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download:bool=False, force:bool=False, threads:int=1) -> None:
        # limit download to first page by default
        if not full_download:
            super().download_request(ckan=ckan, out_dir=out_dir, full_download=False, force=force, threads=threads)
        else:
            self.download_request_full(ckan=ckan, out_dir=out_dir, threads=threads, force=force)


