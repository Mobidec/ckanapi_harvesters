#!python3
# -*- coding: utf-8 -*-
"""
Code to initiate a DataStore defined by a large number of files to concatenate into one table.
This concrete implementation is linked to the file system.
"""
from typing import Dict, List, Collection, Any, Tuple, Generator, Union, Set
from collections import OrderedDict
from warnings import warn
import glob
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.builder.mapper_datastore import DataSchemeConversion
# from ckanapi_harvesters.auxiliary.path import list_files_scandir
from ckanapi_harvesters.builder.builder_errors import ResourceFileNotExistMessage
from ckanapi_harvesters.builder.builder_resource_datastore_multi_abc import BuilderDataStoreMultiABC
from ckanapi_harvesters.builder.builder_resource_datastore_multi_abc import datastore_multi_apply_last_condition_intermediary
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.auxiliary.ckan_model import CkanField, CkanResourceInfo, UpsertChoice
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.builder.mapper_datastore_multi import RequestMapperABC, RequestFileMapperABC
from ckanapi_harvesters.builder.mapper_datastore_multi import default_file_mapper_from_primary_key
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreFile
from ckanapi_harvesters.harvesters.harvester_abc import TableHarvesterABC
from ckanapi_harvesters.harvesters.harvester_init import init_table_harvester_from_options_string
from ckanapi_harvesters.builder.builder_resource_datastore_multi_folder import BuilderDataStoreFolder


class BuilderDataStoreHarvester(BuilderDataStoreFolder):
    def __init__(self, *, file_query_list: List[Tuple[str,dict]]=None, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, dir_name:str=None, file_url_attr:str=None, options_string:str=None, base_dir:str=None):
        super().__init__(file_query_list=file_query_list, dir_name=dir_name,
                         name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.options_string = options_string
        self.enable_multi_threaded_upload = False
        # specific attributes
        self.file_url_attr:Union[str,None] = file_url_attr
        self._harvester: Union[TableHarvesterABC,None] = None
        if self.options_string is not None and len(self.options_string) > 0:
            self._apply_options(base_dir=base_dir)

    @property
    def harvester(self) -> Union[TableHarvesterABC,None]:
        return self._harvester
    @harvester.setter
    def harvester(self, harvester: Union[TableHarvesterABC,None]):
        assert_or_raise(self._harvester is None, RuntimeError("You can only set the harvester once"))
        self._harvester = harvester
        self._apply_harvester_metadata()

    def _apply_options(self, base_dir: str = None):
        self.harvester = init_table_harvester_from_options_string(self.options_string, file_url_attr=self.file_url_attr, base_dir=base_dir)

    def init_options_from_ckan(self, ckan:CkanApi) -> None:
        super().init_options_from_ckan(ckan)
        self.harvester.update_from_ckan(ckan)

    def _apply_harvester_metadata(self, base_dir:str=None):
        self.dir_name = self.name  # by default, take the resource name
        if self.harvester.params.output_dir is not None:
            self.dir_name = self.harvester.params.output_dir
        if self.harvester.params.enable_download is not None:
            self.enable_download = self.harvester.params.enable_download
        # import default metadata
        table_metadata = self.harvester.clean_table_metadata()
        if self.df_mapper.df_upload_fun is None:
            self.df_mapper.df_upload_fun = self.harvester.get_default_df_upload_fun()
        if self.data_cleaner_upload is None:
            self.data_cleaner_upload = self.harvester.get_default_data_cleaner()
        if self.primary_key is None:
            self.primary_key = self.harvester.get_default_primary_key()
        if self.indexes is None:
            self.indexes = table_metadata.indexes
        if self.description is None:
            self.description = table_metadata.description
        if self.format is None:
            self.format = "CSV"
        if table_metadata.fields is not None:
            if self.field_builders is None:
                self.field_builders = OrderedDict()
            for field_name, field_metadata in table_metadata.fields.items():
                if field_name in self.field_builders.keys():
                    field_builder = self.field_builders[field_name]
                    if field_builder.type_override is None:
                        field_builder.type_override = field_metadata.data_type
                else:
                    field_builder = BuilderField(name=field_metadata.name,
                                                 type_override=field_metadata.data_type)
                if field_builder.label is None:
                    field_builder.label = field_metadata.label
                if field_builder.description is None:
                    field_builder.description = field_metadata.description
                if field_builder.uniquekey is None:
                    field_builder.uniquekey = field_metadata.uniquekey or (table_metadata.unique_keys is not None and field_name in table_metadata.unique_keys)
                if field_builder.is_index is None:
                    field_builder.is_index = field_metadata.is_index
                if field_builder.notnull is None:
                    field_builder.notnull = field_metadata.notnull
                field_builder.internal_attrs = field_metadata.internal_attrs.merge(field_builder.internal_attrs)
                self.field_builders[field_name] = field_builder
        if table_metadata.unique_keys is not None and len(table_metadata.unique_keys) > 0:
            if self.field_builders is None:
                self.field_builders = OrderedDict()
            for field_name in table_metadata.unique_keys:
                if field_name in self.field_builders.keys():
                    field_builder = self.field_builders[field_name]
                    if field_builder.uniquekey is None:
                        field_builder.uniquekey = True
                else:
                    pass  # because we do not know the data type
                    # field_builder = BuilderField(name=field_name)
                    # field_builder.uniquekey = field_name
                    # self.field_builders[field_name] = field_builder

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreHarvester()
        super().copy(dest=dest)
        dest.file_url_attr = self.file_url_attr
        dest.harvester = self.harvester
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None) -> None:
        super()._load_from_df_row(row=row)
        self.df_mapper = default_file_mapper_from_primary_key(self.primary_key)
        self.dir_name = ""
        self.file_url_attr: str = _string_from_element(row["file/url"])
        if self.options_string is not None and len(self.options_string) > 0:
            self._apply_options(base_dir=base_dir)

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.file_url_attr
        return d

    @staticmethod
    def resource_mode_str() -> str:
        return "DataStore from Harvester"

    @staticmethod
    def from_file_datastore(resource_file: BuilderDataStoreFile,
                            *, dir_name:str=None, primary_key:List[str]=None,
                            file_query_list:Collection[Tuple[str,dict]]=None) -> "BuilderDataStoreHarvester":
        """
        Do not initialize a BuilderDataStoreHarvester with this method. Rather initialize a new instance of the class.

        :raises NotImplementedError:
        """
        raise NotImplementedError("This method must not be called for a DataStore from Harvester. Rather initialize a new BuilderDataStoreHarvester.")


    ## upload is specific to this class ---------------------------------------------------
    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        return self.harvester.check_connection()

    def get_sample_file_path(self, resources_base_dir:str, file_index:int=0) -> Union[Any,None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        return self.local_file_list[file_index]

    def load_local_df(self, file: str, *, upload_alter:bool=True, fields:OrderedDict[str,CkanField]=None) -> pd.DataFrame:
        # self.sample_data_source = resolve_rel_path(resources_base_dir, self.dir_name, file, field=f"File/URL of resource {self.name}")
        self.sample_data_source = file
        data_local = self.harvester.query_data(query=file)
        if upload_alter:
            df_upload = self.df_mapper.df_upload_alter(data_local, self.sample_data_source, fields=self._get_fields_info())
            return df_upload
        else:
            raise RuntimeError("upload_alter must be True for a DataStore from Harvester.")

    def get_local_file_generator(self, resources_base_dir:str, **kwargs) -> Generator[Any, None, None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        for query in self.local_file_list:
            yield query

    def list_local_files(self, resources_base_dir:str, cancel_if_present:bool=True) -> List[Any]:
        if cancel_if_present and self.local_file_list is not None:
            return self.local_file_list
        self.local_file_list = self.harvester.list_queries(new_connection=not cancel_if_present)
        return self.local_file_list

    def init_local_files_list(self, resources_base_dir:str, cancel_if_present:bool=True, **kwargs) -> List[str]:
        return self.list_local_files(resources_base_dir=resources_base_dir, cancel_if_present=cancel_if_present)

    def get_local_df_generator(self, resources_base_dir:str, *, fields:OrderedDict[str,CkanField], **kwargs) -> Generator[pd.DataFrame, None, None]:
        return super().get_local_df_generator(resources_base_dir=resources_base_dir, fields=fields, **kwargs)

    def get_local_file_len(self) -> int:
        if self.local_file_list is None:
            raise RuntimeError("You must call list_local_files first")
        return len(self.local_file_list)

    # def patch_request(self, ckan: CkanApi, package_id: str, *,
    #                   df_upload: pd.DataFrame=None, reupload: bool = None, resources_base_dir:str=None) -> CkanResourceInfo:
    #     # apply same treatments as super method to determine df_upload
    #     if reupload is None: reupload = self.reupload_on_update
    #     if df_upload is None:
    #         if not reupload:
    #             resource_id = ckan.map.get_resource_id(self.name, self.package_name, error_not_mapped=False)
    #             if resource_id is not None:
    #                 fields = ckan.get_datastore_fields_or_request(resource_id, error_not_found=False)
    #             else:
    #                 fields = None
    #         else:
    #             fields = None
    #         df_upload = self.load_sample_df(resources_base_dir=resources_base_dir, upload_alter=True, fields=fields)
    #     return super().patch_request(ckan, package_id, df_upload=df_upload, reupload=reupload, resources_base_dir=resources_base_dir)

    # def upload_request_full(self, ckan:CkanApi, resources_base_dir:str, *,
    #                         method:UpsertChoice=UpsertChoice.Upsert,
    #                         threads:int=1, external_stop_event=None,
    #                         only_missing:bool=False,
    #                         start_index:int=0, end_index:int=None) -> None:
    #     resource_id = ckan.map.get_resource_id(self.name, self.package_name, error_not_mapped=False)
    #     if resource_id is not None:
    #         fields = ckan.get_datastore_fields_or_request(resource_id, error_not_found=False)
    #     else:
    #         fields = None
    #     super().upload_request_full(ckan=ckan, resources_base_dir=resources_base_dir,
    #                                 threads=threads, external_stop_event=external_stop_event,
    #                                 start_index=start_index, end_index=end_index,
    #                                 method=method, fields=fields)

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

        # resource_id = self.get_or_query_resource_id(ckan, error_not_found=True)
        # df_upload_transformed = self.df_mapper.df_upload_alter(df_upload)
        # ret_df = ckan.datastore_upsert(df_upload_transformed, resource_id, method=method,
        #                                apply_last_condition=apply_last_condition,
        #                                always_last_condition=always_last_condition)
        # return df_upload_transformed, ret_df

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


