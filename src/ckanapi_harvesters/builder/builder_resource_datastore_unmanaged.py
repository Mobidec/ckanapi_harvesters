#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore without uploading any data.
"""
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Callable, Any, Tuple, Union, Set
import os
from io import StringIO
from warnings import warn
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreFile, num_rows_patch_first_upload_partial
# from ckanapi_harvesters.builder.builder_resource import BuilderResourceUnmanagedABC
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.ckan_errors import NotMappedObjectNameError, DataStoreNotFoundError
from ckanapi_harvesters.builder.builder_errors import RequiredDataFrameFieldsError, IncompletePatchError
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, assert_or_raise, find_duplicates, datastore_id_col


class BuilderDataStoreUnmanaged(BuilderDataStoreFile):  # , BuilderResourceUnmanagedABC):  # multiple inheritance can give undefined results
    """
    Class representing a DataStore (resource metadata and fields metadata) without managing its contents during the upload process.
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.reupload_on_update = False
        self.reupload_if_needed = True
        self.initiate_by_user:bool = False
        self.file_name = name
        self.default_df_upload: Union[pd.DataFrame,None] = None

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreUnmanaged()
        super().copy(dest=dest)
        dest.reupload_on_update = self.reupload_on_update
        dest.reupload_if_needed = self.reupload_if_needed
        dest.initiate_by_user = self.initiate_by_user
        dest.file_name = self.file_name
        dest.default_df_upload = copy.deepcopy(self.default_df_upload)
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.file_name = self.name

    def get_sample_file_path(self, resources_base_dir: str) -> None:
        return None

    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True) -> Union[pd.DataFrame,None]:
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

    def patch_request(self, ckan: CkanApi, package_id: str, *,
                      df_upload: pd.DataFrame=None,
                      reupload: bool = None, resources_base_dir:str=None) -> CkanResourceInfo:
        """
        Specific implementation of patch_request which does not upload any data and only updates the fields currently present in the database
        :param resources_base_dir:
        :param ckan:
        :param package_id:
        :param reupload:
        :return:
        """
        if df_upload is None:
            df_upload = self.default_df_upload
        if reupload is None: reupload = self.reupload_on_update and df_upload is not None
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=False)
        if df_upload is None:
            try:
                df_download = self.download_sample_df(ckan, search_all=False, download_alter=False, limit=1)
                if df_download is None:
                    assert_or_raise(resource_id is None, RuntimeError("Unexpected: resource_id should be None"))
                    raise NotMappedObjectNameError(self.name)
                current_fields = set(df_download.columns)
            except NotMappedObjectNameError as e:
                df_download = None
                current_fields = set()
            except DataStoreNotFoundError as e:
                df_download = None
                current_fields = set()
            df_upload_partial, df_upload_upsert = None, None
            data_cleaner_fields = None
            data_cleaner_index = set()
        else:
            df_upload, data_cleaner_fields, data_cleaner_index = self._apply_data_cleaner_before_patch(ckan, df_upload, reupload=reupload)
            df_download = df_upload
            current_fields = set(df_upload.columns)
            if num_rows_patch_first_upload_partial is not None and len(df_upload) > num_rows_patch_first_upload_partial:
                df_upload_partial = df_upload.iloc[:num_rows_patch_first_upload_partial]
                df_upload_upsert = df_upload.iloc[num_rows_patch_first_upload_partial:]
            else:
                df_upload_partial, df_upload_upsert = df_upload, None
        empty_datastore = df_download is None or len(df_download) == 0
        current_fields -= {datastore_id_col}  # _id does not require documentation
        execute_datastore_create = df_upload_partial is not None or not (self.initiate_by_user and (df_download is None or df_download.empty))
        aliases = self._get_alias_list(ckan)
        self._check_necessary_fields(current_fields, raise_error=False, empty_datastore=empty_datastore)
        self._check_undocumented_fields(current_fields)
        primary_key, indexes = self._get_primary_key_indexes(data_cleaner_index, current_fields=current_fields,
                                                             error_missing=False, empty_datastore=empty_datastore)
        fields_update = self._get_fields_update(ckan, current_fields, data_cleaner_fields, reupload=reupload)
        fields = list(fields_update.values()) if len(fields_update) > 0 else None
        resource_info = ckan.resource_create(package_id, name=self.name, format=self.format, description=self.description, state=self.state,
                                             create_default_view=self.create_default_view,
                                             cancel_if_exists=True, update_if_exists=True, reupload=reupload and df_upload_partial is not None,
                                             datastore_create=execute_datastore_create, records=df_upload_partial, fields=fields,
                                             primary_key=primary_key, indexes=indexes, aliases=aliases, data_cleaner=self.data_cleaner_upload)
        reupload = reupload or resource_info.newly_created
        resource_id = resource_info.id
        self.known_id = resource_id
        self._compare_fields_to_datastore_info(resource_info, current_fields, ckan)
        if df_upload_upsert is not None and reupload:
            if reupload:
                ckan.datastore_upsert(df_upload_upsert, resource_id, method=UpsertChoice.Insert,
                                      always_last_condition=None, data_cleaner=self.data_cleaner_upload)
            else:
                # case where a reupload was needed but is not permitted by self.reupload_if_needed
                msg = f"Did not upload the remaining part of the resource {self.name}."
                raise IncompletePatchError(msg)
        return resource_info



