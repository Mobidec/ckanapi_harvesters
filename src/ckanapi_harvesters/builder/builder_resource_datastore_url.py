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
import io
from warnings import warn

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.builder.builder_resource import builder_request_default_auth_if_ckan
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreFile
from ckanapi_harvesters.auxiliary.ckan_errors import NotMappedObjectNameError, DataStoreNotFoundError
from ckanapi_harvesters.builder.builder_errors import RequiredDataFrameFieldsError, ResourceFileNotExistMessage
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.auxiliary.ckan_errors import CkanArgumentError, FunctionMissingArgumentError, ExternalUrlLockedError
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, assert_or_raise, find_duplicates, datastore_id_col
from ckanapi_harvesters.ckan_api.ckan_api_2_readonly import df_download_read_csv_kwargs


class BuilderDataStoreUrl(BuilderDataStoreFile):  #, BuilderUrlABC):  # multiple inheritance can give undefined results
    """
    Class representing a DataStore (resource metadata and fields metadata) defined by a url.
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, url:str=None):
        super(BuilderDataStoreFile, self).__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        # super(BuilderUrlABC, self).__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url, url=url)
        self.reupload_on_update = False
        self.reupload_if_needed = False
        self.url:str = url
        self.file_name = name

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreUrl()
        super().copy(dest=dest)
        dest.reupload_on_update = self.reupload_on_update
        dest.reupload_if_needed = self.reupload_if_needed
        dest.url = self.url
        dest.file_name = self.file_name
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super(BuilderDataStoreFile, self)._load_from_df_row(row=row)
        # super(BuilderUrlABC, self)._load_from_df_row(row=row)
        self.url: str = _string_from_element(row["file/url"])
        self.file_name = self.name

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return True

    def get_sample_file_path(self, resources_base_dir: str) -> str:
        return self.url

    def load_sample_data(self, resources_base_dir:str, *, ckan:CkanApi=None,
                         proxies:dict=None, headers:dict=None) -> bytes:
        self.sample_source = self.url
        if ckan is None:
            raise FunctionMissingArgumentError("BuilderDataStoreUrl.load_sample_data", "ckan")
        return ckan.download_url_proxy(self.url, proxies=proxies, headers=headers, auth_if_ckan=builder_request_default_auth_if_ckan).content

    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True) -> pd.DataFrame:
        payload = self.load_sample_data(resources_base_dir=resources_base_dir)
        buffer = io.StringIO(payload.decode())
        response_df = self.local_file_format.read_buffer(buffer, fields=self._get_fields_info())
        if upload_alter:
            df_upload = self.df_mapper.df_upload_alter(response_df, self.sample_data_source, fields=self._get_fields_info())
            return df_upload
        else:
            return response_df

    @staticmethod
    def resource_mode_str() -> str:
        return "DataStore from URL"

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.url
        return d

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        if ckan is None:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Warning, "Could not determine if resource url exists because ckan argument was not provided.")
        else:
            return ckan.download_url_proxy_test_head(self.url, **kwargs)

    def patch_request(self, ckan: CkanApi, package_id: str, *,
                      df_upload:pd.DataFrame=None, payload:Union[bytes, io.BufferedIOBase]=None,
                      reupload: bool = None, resources_base_dir:str=None) -> CkanResourceInfo:
        """
        Specific implementation of patch_request which does not upload any data and only updates the fields currently present in the database
        :param resources_base_dir:
        :param ckan:
        :param package_id:
        :param reupload:
        :return:
        """
        if reupload is None: reupload = self.reupload_on_update
        if payload is not None or df_upload is not None:
            raise CkanArgumentError("payload", "datastore defined from URL patch")
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=False)
        try:
            df_download = self.download_sample_df(ckan, download_alter=False, search_all=False, limit=1)
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
        empty_datastore = df_download is None or len(df_download) == 0
        data_cleaner_fields = None
        data_cleaner_index = set()
        current_fields -= {datastore_id_col}  # _id does not require documentation
        aliases = self._get_alias_list(ckan)
        self._check_necessary_fields(current_fields, raise_error=False, empty_datastore=empty_datastore)
        self._check_undocumented_fields(current_fields)
        primary_key, indexes = self._get_primary_key_indexes(data_cleaner_index, current_fields=current_fields,
                                                             error_missing=False, empty_datastore=empty_datastore)
        fields_update = self._get_fields_update(ckan, current_fields, data_cleaner_fields, reupload=reupload)
        fields = list(fields_update.values()) if len(fields_update) > 0 else None
        resource_info = ckan.resource_create(package_id, name=self.name, format=self.format, description=self.description, state=self.state,
                                             url=self.url,
                                             datastore_create=False, auto_submit=False, create_default_view=self.create_default_view,
                                             cancel_if_exists=True, update_if_exists=True, aliases=aliases, reupload=False, data_cleaner=self.data_cleaner_upload)
        resource_id = resource_info.id
        self.known_id = resource_id
        self._compare_fields_to_datastore_info(resource_info, current_fields, ckan)
        if reupload:
            # re-initialize datastore to reupload from url
            # normally, data was automatically submitted to DataStore on resource_create (not needed)
            ckan.datastore_create(resource_id, fields=fields, primary_key=primary_key, indexes=indexes, aliases=aliases)
            ckan.datastore_submit(resource_id)
        return resource_info



