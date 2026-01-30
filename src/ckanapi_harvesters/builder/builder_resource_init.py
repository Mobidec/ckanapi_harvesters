#!python3
# -*- coding: utf-8 -*-
"""
Code to initialize a resource builder from a row
"""
from typing import Union

import pandas as pd

from ckanapi_harvesters.ckan_api import  CkanApiMap
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_errors import (UnexpectedError)
from ckanapi_harvesters.builder.builder_errors import MissingDataStoreInfoError
from ckanapi_harvesters.builder.builder_resource import (BuilderResourceABC, BuilderFileBinary, BuilderUrl,
                                                         BuilderResourceUnmanaged)
from ckanapi_harvesters.builder.builder_resource_multi_file import BuilderMultiFile
from ckanapi_harvesters.builder.builder_resource_datastore import (BuilderDataStoreABC, BuilderDataStoreFile,
                                                                   BuilderResourceIgnored)
from ckanapi_harvesters.builder.builder_resource_multi_datastore import BuilderMultiDataStore
from ckanapi_harvesters.builder.builder_resource_datastore_url import BuilderDataStoreUrl
from ckanapi_harvesters.builder.builder_resource_datastore_multi_harvester import BuilderDataStoreHarvester
from ckanapi_harvesters.builder.builder_resource_datastore_unmanaged import BuilderDataStoreUnmanaged
from ckanapi_harvesters.builder.builder_resource_datastore_multi_abc import BuilderDataStoreMultiABC
from ckanapi_harvesters.builder.builder_resource_datastore_multi_folder import BuilderDataStoreFolder
from ckanapi_harvesters.builder.builder_field import BuilderField


import_as_folder_row_count_threshold: Union[int,None] = None


def init_resource_from_df(row: pd.Series, base_dir:str=None) -> BuilderResourceABC:
    """
    Function mapping keywords to a resource builder type.

    :param row:
    :return:
    """
    mode = row["mode"].lower().strip()
    if mode == "file":
        resource_builder = BuilderFileBinary()
    elif mode == "url":
        resource_builder = BuilderUrl()
    elif mode == "datastore from file":
        resource_builder = BuilderDataStoreFile()
    elif mode == "datastore from folder":
        resource_builder = BuilderDataStoreFolder()
    elif mode == "datastore from url":
        resource_builder = BuilderDataStoreUrl()
    elif mode == "datastore from harvester":
        resource_builder = BuilderDataStoreHarvester()
    elif mode == "unmanaged":
        resource_builder = BuilderResourceUnmanaged()
    elif mode == "unmanaged datastore":
        resource_builder = BuilderDataStoreUnmanaged()
    elif mode == "multifile":
        resource_builder = BuilderMultiFile()
    elif mode == "multidatastore":
        resource_builder = BuilderMultiDataStore()
    elif mode == "ignored":
        resource_builder = BuilderResourceIgnored()
    else:
        raise ValueError(f"{mode} is not a valid mode")
    resource_builder._load_from_df_row(row=row, base_dir=base_dir)
    return resource_builder


def init_resource_from_ckan(ckan: CkanApiMap, resource_info: CkanResourceInfo) -> BuilderResourceABC:
    """
    Function initiating a resource builder based on information provided by the CKAN API.

    :return:
    """
    # assert_or_raise(ckan.map._mapping_query_datastore_info, MissingDataStoreInfoError())
    assert_or_raise(resource_info.datastore_queried(), MissingDataStoreInfoError())
    d = {
        "name": resource_info.name,
        "format": resource_info.format,
        "description": resource_info.description,
        "state": resource_info.state.name if resource_info.state is not None else "",
        "file/url": resource_info.name,
        "primary key": "",
        "indexes": "",
        "known id": resource_info.id,
        "known url": resource_info.download_url,
    }
    if (isinstance(resource_info.datastore_info, CkanDataStoreInfo)
            and resource_info.datastore_info.row_count is not None
            and len(resource_info.datastore_info.fields_id_list) > 0):
        # DataStore
        d["indexes"] = ckan_tags_sep.join(resource_info.datastore_info.index_fields)
        d["aliases"] = ckan_tags_sep.join(resource_info.datastore_info.aliases)
        if len(resource_info.download_url) > 0 and not ckan.is_url_internal(resource_info.download_url):
            d["file/url"] = resource_info.download_url
            row = pd.Series(d)
            resource = BuilderDataStoreUrl()
            resource._load_from_df_row(row=row)
        elif resource_info.format.lower() == "csv":
            row = pd.Series(d)
            resource = BuilderDataStoreUnmanaged()
            resource._load_from_df_row(row=row)
            if import_as_folder_row_count_threshold is not None and resource_info.datastore_info.row_count > import_as_folder_row_count_threshold:
                resource = BuilderDataStoreFolder.from_file_datastore(resource)
        else:
            raise UnexpectedError(f"Format of data store {resource_info.name} ({resource_info.format}) is not recognized")
        # load fields information
        resource.field_builders = {}
        for field_id in resource_info.datastore_info.fields_id_list:
            field_info = resource_info.datastore_info.fields_dict[field_id]
            resource.field_builders[field_id] = BuilderField._from_ckan_field(field_info)
    elif len(resource_info.download_url) > 0 and not ckan.is_url_internal(resource_info.download_url):
        # external resource
        d["file/url"] = resource_info.download_url
        row = pd.Series(d)
        resource = BuilderUrl()
        resource._load_from_df_row(row=row)
        assert_or_raise(not resource_info.datastore_active and not isinstance(resource_info.datastore_info, CkanResourceInfo), UnexpectedError())
    else:
        # file
        row = pd.Series(d)
        resource = BuilderResourceUnmanaged()
        resource._load_from_df_row(row=row)
    resource.package_name = resource_info.package_id
    return resource

