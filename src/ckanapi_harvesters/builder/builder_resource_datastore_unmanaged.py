#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore without uploading any data.
"""
from typing import List, Tuple, Union, Generator
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage
from ckanapi_harvesters.builder.builder_resource_datastore_file import BuilderDataStoreFile
# from ckanapi_harvesters.builder.builder_resource import BuilderResourceUnmanagedABC
from ckanapi_harvesters.auxiliary.list_records import GeneralDataFrame
from ckanapi_harvesters.auxiliary.ckan_errors import NotMappedObjectNameError, DataStoreNotFoundError
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise, datastore_id_col


class BuilderDataStoreUnmanaged(BuilderDataStoreFile):  # , BuilderResourceUnmanagedABC):  # multiple inheritance can give undefined results
    """
    Class representing a DataStore (resource metadata and fields metadata) without managing its contents during the upload process.
    """
    def __init__(self, *, parent, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, options_string:str=None, base_dir:str=None):
        super().__init__(parent=parent, name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, options_string=options_string, base_dir=base_dir)
        self.reupload_on_update = False
        self.reupload_if_needed = True
        self.initiate_by_user:bool = False
        self.file_name = name
        self.default_df_upload: Union[pd.DataFrame,None] = None

    def copy(self, *, dest=None, parent=None):
        if dest is None:
            dest = BuilderDataStoreUnmanaged(parent=self.parent_package_builder)
        super().copy(dest=dest, parent=parent)
        dest.reupload_on_update = self.reupload_on_update
        dest.reupload_if_needed = self.reupload_if_needed
        dest.initiate_by_user = self.initiate_by_user
        dest.file_name = self.file_name
        dest.default_df_upload = copy.deepcopy(self.default_df_upload)
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row, base_dir=base_dir)
        self.file_name = self.name

    def get_sample_file_path(self, resources_base_dir: str, ckan:Union[CkanApi,None]=None, file_index:int=0) -> None:
        return None

    def init_local_files_list(self, resources_base_dir:str, cancel_if_present:bool=True, **kwargs) -> List[str]:
        self.file_size = 0
        self.local_file_list = []
        self.local_file_size = []
        self.local_file_size_sum = 0
        return []

    def get_local_df_chunk_generator(self, resources_base_dir:str, ckan:CkanApi, **kwargs) -> Generator[Tuple[GeneralDataFrame,int], None, None]:
        if False:
            yield None  # generator will not iterate once
        return

    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True, file_index:int=0, allow_chunks:bool=True, **kwargs) -> Union[pd.DataFrame,None]:
        return None

    @staticmethod
    def resource_mode_str() -> str:
        return "Unmanaged DataStore"

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = ""
        return d

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        return None

    def patch_request(self, ckan: CkanApi, *,
                      df_upload: pd.DataFrame=None,
                      reupload: bool = None, override_ckan:bool=False,
                      resources_base_dir:str=None, inhibit_datastore_patch_indexes:bool=False) -> CkanResourceInfo:
        """
        Specific implementation of patch_request which does not upload any data and only updates the fields currently present in the database

        :param resources_base_dir:
        :param ckan:
        :param reupload:
        :return:
        """
        package_id = self.parent_package_builder.get_or_query_package_id(ckan)
        self._merge_resource_attributes(override_ckan=override_ckan)
        if df_upload is None:
            df_upload = self.default_df_upload
        if reupload is None: reupload = self.reupload_on_update and df_upload is not None
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=False)
        if df_upload is None:
            try:
                df_download = self.download_resource_df(ckan, search_all=False, download_alter=False, limit_per_request=1)
                if df_download is None:
                    assert_or_raise(resource_id is None, RuntimeError("Unexpected: resource_id should be None"))
                    raise NotMappedObjectNameError(self.name)
                current_df_fields = set(df_download.columns)
            except NotMappedObjectNameError as e:
                df_download = None
                current_df_fields = set()
            except DataStoreNotFoundError as e:
                df_download = None
                current_df_fields = set()
            data_cleaner_fields = None
            data_cleaner_index = set()
        else:
            df_upload, data_cleaner_fields, data_cleaner_index = self._apply_data_cleaner_before_patch(ckan, df_upload, 
                                                                       reupload=reupload, override_ckan=override_ckan)
            df_download = df_upload
            current_df_fields = set(df_upload.columns)
        empty_datastore = df_download is None or len(df_download) == 0
        current_df_fields -= {datastore_id_col}  # _id does not require documentation
        execute_datastore_create = df_upload is not None or not (self.initiate_by_user and (df_download is None or df_download.empty))
        aliases = self._get_alias_list(ckan)
        self._check_necessary_fields(current_df_fields, raise_error=False, empty_datastore=empty_datastore)
        self._check_undocumented_fields(current_df_fields)
        primary_key, indexes = self._get_primary_key_indexes(data_cleaner_index, current_df_fields=current_df_fields,
                                                             error_missing=False, empty_datastore=empty_datastore)
        fields_update = self._get_fields_update(ckan, current_df_fields=current_df_fields, data_cleaner_fields=data_cleaner_fields,
                                                reupload=reupload, override_ckan=override_ckan)
        fields = list(fields_update.values()) if len(fields_update) > 0 else None
        resource_info = ckan.resource_create(package_id, name=self.name, format=self.resource_attributes.format, description=self.resource_attributes.description,
                                             state=self.resource_attributes.state,
                                             create_default_view=self.create_default_view,
                                             cancel_if_exists=True, update_if_exists=True, reupload=reupload and df_upload is not None,
                                             datastore_create=execute_datastore_create, records=df_upload, fields=fields,
                                             primary_key=primary_key, indexes=indexes, aliases=aliases,
                                             inhibit_datastore_patch_indexes=inhibit_datastore_patch_indexes,
                                             progress_callback=self.progress_callback,
                                             records_to_file=self.records_to_file)
        resource_id = resource_info.id
        self.known_id = resource_id
        self._compare_fields_to_datastore_info(resource_info, current_df_fields, ckan)
        return resource_info



