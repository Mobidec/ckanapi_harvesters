#!python3
# -*- coding: utf-8 -*-
"""
Code to initialize a resource builder from a row
"""
from typing import Union
from warnings import warn

import pandas as pd

from ckanapi_harvesters.ckan_api import  CkanApiMap
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise, _string_from_element
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_errors import (UnexpectedError, IntegrityError)
from ckanapi_harvesters.builder.builder_errors import MissingDataStoreInfoError
from ckanapi_harvesters.builder.builder_resource import (BuilderResourceABC, BuilderFileBinary, BuilderUrl,
                                                         BuilderResourceUnmanaged)
from ckanapi_harvesters.builder.builder_resource_multi_file import BuilderMultiFile
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderResourceIgnored
from ckanapi_harvesters.builder.builder_resource_datastore_multi_folder import BuilderDataStoreFolder
from ckanapi_harvesters.builder.builder_resource_datastore_multi_ckan import BuilderDataStoreCkan
from ckanapi_harvesters.builder.builder_resource_datastore_file import BuilderDataStoreFile
from ckanapi_harvesters.builder.builder_resource_multi_datastore import BuilderMultiDataStore
from ckanapi_harvesters.builder.builder_resource_datastore_url import BuilderDataStoreUrl
from ckanapi_harvesters.builder.builder_resource_datastore_multi_harvester import BuilderDataStoreHarvester
from ckanapi_harvesters.builder.builder_resource_datastore_unmanaged import BuilderDataStoreUnmanaged
from ckanapi_harvesters.builder.builder_field import BuilderField


import_as_folder_row_count_threshold: Union[int,None] = None


def init_resource_from_df(row: pd.Series, parent, base_dir:str=None) -> Union[BuilderResourceABC,None]:
    """
    Function mapping keywords to a resource builder type.

    :param row:
    :return:
    """
    mode = _string_from_element(row["mode"], empty_value="")
    mode = mode.lower().strip()
    if mode == "file":
        resource_builder = BuilderFileBinary(parent=parent)
    elif mode == "url":
        resource_builder = BuilderUrl(parent=parent)
    elif mode == "datastore from file":
        resource_builder = BuilderDataStoreFile(parent=parent)
    elif mode == "datastore from folder":
        resource_builder = BuilderDataStoreFolder(parent=parent)
    elif mode == "datastore from url":
        resource_builder = BuilderDataStoreUrl(parent=parent)
    elif mode == "datastore from harvester":
        resource_builder = BuilderDataStoreHarvester(parent=parent)
    elif mode == "unmanaged":
        resource_builder = BuilderResourceUnmanaged(parent=parent)
    elif mode == "unmanaged datastore":
        resource_builder = BuilderDataStoreUnmanaged(parent=parent)
    elif mode == "multifile":
        resource_builder = BuilderMultiFile(parent=parent)
    elif mode == "ckan datastore merge":
        resource_builder = BuilderDataStoreCkan(parent=parent)
    elif mode == "multidatastore":
        resource_builder = BuilderMultiDataStore(parent=parent)
    elif mode == "ignored":
        resource_builder = BuilderResourceIgnored(parent=parent)
    elif mode == "":
        resource_name = _string_from_element(row["name"], empty_value="", strip=True)
        if resource_name == "":
            msg = "Ignoring line with empty resource name and empty resource mode: " + row.to_json()
            warn(msg)
            return None
        else:
            raise ValueError(f"Empty resource mode for resource '{resource_name}'")
    else:
        raise ValueError(f"{mode} is not a valid mode")
    resource_builder._load_from_df_row(row=row, base_dir=base_dir)
    resource_builder._user_fields_used.add("mode")
    return resource_builder


def init_resource_from_ckan(ckan: CkanApiMap, resource_info: CkanResourceInfo, parent) -> BuilderResourceABC:
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
            resource_builder = BuilderDataStoreUrl(parent=parent)
            resource_builder._load_from_df_row(row=row)
        elif resource_info.format.lower() == "csv":
            row = pd.Series(d)
            resource_builder = BuilderDataStoreUnmanaged(parent=parent)
            resource_builder._load_from_df_row(row=row)
            if import_as_folder_row_count_threshold is not None and resource_info.datastore_info.row_count > import_as_folder_row_count_threshold:
                resource_builder = resource_builder.to_builder_datastore_folder()
        else:
            raise UnexpectedError(f"Format of data store {resource_info.name} ({resource_info.format}) is not recognized")
        # load fields information
        resource_builder.field_builders = {}
        for field_id in resource_info.datastore_info.fields_id_list:
            field_info = resource_info.datastore_info.fields_dict[field_id]
            resource_builder.field_builders[field_id] = BuilderField._from_ckan_field(field_info)
    elif len(resource_info.download_url) > 0 and not ckan.is_url_internal(resource_info.download_url):
        # external resource_builder
        d["file/url"] = resource_info.download_url
        row = pd.Series(d)
        resource_builder = BuilderUrl(parent=parent)
        resource_builder._load_from_df_row(row=row)
        assert_or_raise(not resource_info.datastore_active and not isinstance(resource_info.datastore_info, CkanResourceInfo), UnexpectedError())
    else:
        # file
        row = pd.Series(d)
        resource_builder = BuilderResourceUnmanaged(parent=parent)
        resource_builder._load_from_df_row(row=row)
    assert_or_raise(resource_info.package_id == parent.package_attributes.id, IntegrityError("Package id is inconsistent"))
    return resource_builder

