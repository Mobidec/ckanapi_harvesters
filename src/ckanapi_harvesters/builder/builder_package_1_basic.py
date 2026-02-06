#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements the package definition.
"""
from typing import Dict, List, Tuple, Union, Callable
from warnings import warn
import os
import shutil
import json
import re
from collections import OrderedDict

import pandas as pd
import numpy as np

from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.ckan_api import CkanApi, CkanApiMap
from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_model import CkanVisibility, CkanState, CkanPackageInfo, CkanResourceInfo, CkanDataStoreInfo, CkanLicenseInfo
from ckanapi_harvesters.auxiliary.path import sanitize_path, path_rel_to_dir, make_path_relative
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, assert_or_raise, find_duplicates
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_errors import (UnexpectedError, DuplicateNameError, ForbiddenNameError, MissingIdError,
                                                      MandatoryAttributeError, FileOrDirNotExistError)
from ckanapi_harvesters.auxiliary.ckan_configuration import unlock_external_url_resource_download, unlock_no_ca
from ckanapi_harvesters.builder.builder_errors import MissingDataStoreInfoError, UnsupportedBuilderVersionError
from ckanapi_harvesters.builder import BUILDER_FILE_FORMAT_VERSION as BUILDER_VER
from ckanapi_harvesters.builder.builder_resource import BuilderResourceABC
from ckanapi_harvesters.builder.builder_resource_multi_file import BuilderMultiFile, multi_file_exclude_other_files
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreABC
from ckanapi_harvesters.builder.builder_resource_multi_datastore import BuilderMultiDataStore
from ckanapi_harvesters.builder.builder_resource_datastore_multi_abc import BuilderDataStoreMultiABC
from ckanapi_harvesters.builder.builder_resource_datastore_multi_harvester import BuilderDataStoreHarvester
from ckanapi_harvesters.builder.builder_resource_init import init_resource_from_df, init_resource_from_ckan
from ckanapi_harvesters.builder.builder_ckan import BuilderCkan
from ckanapi_harvesters.auxiliary.external_code_import import PythonUserCode, unlock_external_code_execution

self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
example_package_xls = os.path.join(self_dir, "builder_package_example.xlsx")

forbidden_resource_names = {"ckan", "info", "package", "resources", "validation", "help"}
excel_subs_characters_re = r"[\*\?\[\]\+]"  # characters used in wildcards (MultiFile & MultiDataStore), forbidden in Excel sheet names
excel_subs_dest_character = '#'


def excel_name_of_sheet(resource_name: str) -> str:
        return re.sub(excel_subs_characters_re, excel_subs_dest_character, resource_name)

def excel_name_of_builder(resource_builder: BuilderResourceABC) -> str:
    if isinstance(resource_builder, BuilderMultiDataStore):
        return excel_name_of_sheet(resource_builder.name)
    else:
        return resource_builder.name


class BuilderPackageBasic:
    """
    Class to store an image of a CKAN package defined by an Excel worksheet


    __NB__: There are several paths to distinguish:

    - the path of the Excel worksheet
    - base_dir: the base directory for relative paths
    - resources_base_dir: the base directory for resources (for upload), which is generally defined relative to base_dir
    - out_dir: the output directory, for download, absolute or relative to the cwd (current working directory)

    __NB__: A builder can refer to the following external files:

    - CKAN API key file (.txt)
    - Proxy authentication file (.txt)
    - CKAN CA certificate file (.pem)
    - CA certificate for external connexions (.pem)
    - Data format policy file (.json)
    - External Python module (.py) containing DataFrame modification functions for upload/download of a DataStore
    """
    default_to_json_reduced_size:bool = False

    def __init__(self, package_name:str=None, *, package_id:str=None,
                 title: str = None, description: str = None, private: bool = None, state: CkanState = None,
                 version: str = None,
                 url: str = None, tags: List[str] = None,
                 organization_name:str=None, license_name:str=None, src=None):
        if src is not None:
            src.copy(dest=self)
        self.builder_source_file: Union[str, None] = None
        self.builder_format_version: Union[str, None] = None
        # package attributes
        self.package_attributes: CkanPackageInfo = CkanPackageInfo(package_name=package_name, package_id=package_id,
                                                                   title=title, description=description, private=private, state=state,
                                                                   version=version, url=url, tags=tags)
        if state is None:
            self.package_attributes.state = CkanState.Active  # activate by default - if the package was deleted, it reappears
        # if private is None:
        #     self.package_attributes.private = True  # package private by default
        self.organization_name: Union[str, None] = organization_name
        self.license_name: Union[str, None] = license_name
        # package resources
        self._resources_base_dir_src: Union[str, None] = None  # source of the resources_base_dir
        self._resources_base_dir: Union[str, None] = None
        self.resource_builders:OrderedDict[str,BuilderResourceABC] = OrderedDict()
        self._default_out_dir_src: Union[str, None] = None
        self._default_out_dir: Union[str, None] = None
        # auxiliary builders
        self.ckan_builder: BuilderCkan = BuilderCkan()
        self.external_python_code: Union[PythonUserCode, None] = None
        self.comment: str = ""

    def __str__(self):
        return f"Package builder for {self.package_name} ({len(self.resource_builders)} resources)"

    def copy(self, dest=None) -> "BuilderPackageBasic":
        if dest is None:
            dest = BuilderPackageBasic()
        dest.builder_source_file = self.builder_source_file
        dest.builder_format_version = self.builder_format_version
        dest.package_attributes = self.package_attributes.copy()
        dest.organization_name = self.organization_name
        dest.license_name = self.license_name
        dest._resources_base_dir_src = self._resources_base_dir_src
        dest._resources_base_dir = self._resources_base_dir
        dest._default_out_dir_src = self._default_out_dir_src
        dest._default_out_dir = self._default_out_dir
        dest.resource_builders = OrderedDict()
        dest.comment = self.comment
        for key, value in self.resource_builders.items():
            dest.resource_builders[key] = value.copy()
        dest.ckan_builder = self.ckan_builder.copy()
        if self.external_python_code is not None:
            dest.external_python_code = self.external_python_code.copy()
        return dest

    def _check_mandatory_attributes(self):
        if self.package_name is None:
            raise MandatoryAttributeError("Package", "name")
        # organization can be non-mandatory depending on CKAN configuration
        # if self.organization_name is None:
        #     raise MissingMandatoryAttributeError("Package", "owner_org")

    def clear_ids(self):
        """
        Clear all known ids from package and resource builders
        :return:
        """
        self.package_attributes.id = None
        for resource_builder in self.resource_builders.values():
            resource_builder.known_id = None

    @staticmethod
    def unlock_external_code_execution(value:bool=True):
        """
        This function enables external code execution for the PythonUserCode class.
        It is necessary to load builders which specify an Auxiliary functions file.

        __Warning__:
        only run code if you trust the source!

        :return:
        """
        unlock_external_code_execution(value)

    @staticmethod
    def unlock_no_ca(value:bool=True):
        """
        This function enables you to disable the CA verification of the CKAN server.

        __Warning__:
        Only allow in a local environment!

        """
        unlock_no_ca(value)

    @staticmethod
    def unlock_external_url_resource_download(value:bool=True):
        """
        This function enables the download of resources external from the CKAN server.
        """
        unlock_external_url_resource_download(value)

    @property
    def package_name(self) -> str:
        return self.package_attributes.name
    @package_name.setter
    def package_name(self, value:str):
        self.package_attributes.name = value
        self.update_package_name_in_resources()

    @property
    def resources_base_dir(self) -> str:
        return self._resources_base_dir
    def set_resources_base_dir(self, value:str, base_dir:str=None):
        self._resources_base_dir_src = value
        self._apply_resources_base_dir_src(base_dir=self.get_base_dir(base_dir=base_dir))

    @property
    def default_out_dir(self) -> str:
        return self._default_out_dir
    def set_default_out_dir(self, value:str, base_dir:str=None):
        self._default_out_dir_src = value
        self._apply_out_dir_src(base_dir=self.get_base_dir(base_dir=base_dir))

    def update_package_name_in_resources(self):
        """
        Update package_name attribute in resource_builders
        Call before any operation on resources
        """
        package_name = self.package_name
        for resource_builder in self.resource_builders.values():
            resource_builder.package_name = package_name

    def update_ckan_options_name_in_resources(self, ckan:CkanApi):
        """
        Update ckan options in resource_builders
        Call before any operation on resources
        """
        for resource_builder in self.resource_builders.values():
            resource_builder.init_options_from_ckan(ckan)

    def _apply_resources_base_dir_src(self, base_dir:str):
        """
        The resources base directory is specified in a field of the Excel workbook.
        This function resolves the directory name, based on the location of the Excel file
        or the base_dir, if provided.

        :param base_dir:
        :return:
        """
        resources_base_dir_src = self._resources_base_dir_src
        if resources_base_dir_src is None:
            resources_base_dir = base_dir
        else:
            resources_base_dir_src = os.path.expanduser(resources_base_dir_src)
            if os.path.isabs(resources_base_dir_src):
                resources_base_dir = resources_base_dir_src
            else:
                assert(base_dir is not None)
                self._resources_base_dir_src = os.path.join(base_dir, resources_base_dir_src)
                resources_base_dir = self._resources_base_dir_src
        if resources_base_dir is not None and not os.path.isdir(resources_base_dir):
            if not os.path.exists(resources_base_dir):
                raise FileOrDirNotExistError(resources_base_dir)
            # the field points to a text file containing the resources_base_dir
            with open(resources_base_dir, "r") as f:
                resources_base_dir = f.readline().strip()
                f.close()
        self._resources_base_dir = sanitize_path(resources_base_dir)

    def _get_resources_base_dir_src(self, base_dir:str):
        return make_path_relative(self._resources_base_dir, base_dir)
        # elif self._resources_base_dir_src is not None and os.path.exists(self._resources_base_dir_src) and not os.path.isdir(self._resources_base_dir_src):
        #     return self._resources_base_dir_src if base_dir is None else os.path.relpath(self._resources_base_dir_src, base_dir)
        # else:
        #     return self._resources_base_dir if base_dir is None else os.path.relpath(self._resources_base_dir, base_dir)

    def _apply_out_dir_src(self, base_dir:str, not_exist_error:bool=False):
        """
        The default download directory is specified in a field of the Excel workbook.
        This function resolves the directory name, based on the location of the Excel file
        or the base_dir, if provided.

        :param base_dir:
        :return:
        """
        out_dir_src = self._default_out_dir_src
        if out_dir_src is None:
            out_dir = None  # by default, do not define an output dir
        else:
            out_dir_keyword = out_dir_src.lower().strip()
            out_dir_src = os.path.expanduser(out_dir_src)
            if out_dir_keyword == "none":
                out_dir = None  # by default, do not define an output dir
            elif os.path.isabs(out_dir_src):
                out_dir = out_dir_src
            else:
                assert(base_dir is not None)
                self._default_out_dir_src = os.path.join(base_dir, out_dir_src)
                out_dir = self._default_out_dir_src
        if out_dir is not None and not os.path.isdir(out_dir):
            if not os.path.exists(out_dir):
                if not_exist_error:
                    raise FileOrDirNotExistError(out_dir)
                else:
                    msg = f"Default output directory {out_dir} does not exist! It will be created if you call the download function with no out_dir."
                    warn(msg)
                    self._default_out_dir = out_dir
                    return
            # the field points to a text file containing the out_dir
            with open(out_dir, "r") as f:
                out_dir = f.readline().strip()
                f.close()
        self._default_out_dir = sanitize_path(out_dir)

    def _get_out_dir_src(self, base_dir:str):
        return make_path_relative(self._default_out_dir_src, base_dir)


    def _load_from_df(self, info_df: pd.DataFrame, package_df: pd.DataFrame, base_dir:str=None) -> None:
        """
        Function to load builder parameters from a DataFrame, usually from an Excel worksheet

        :param package_df:
        :return:
        """
        if info_df is not None:
            package_df = pd.concat([package_df, info_df], axis=1)
        original_columns = list(package_df.columns)
        package_df.columns = package_df.columns.map(str.lower)
        package_df.columns = package_df.columns.map(str.strip)
        renamed_columns = list(package_df.columns)
        # info
        base_dir = self.get_base_dir(base_dir=base_dir)
        if "builder format version" in package_df.columns:
            self.builder_format_version = _string_from_element(package_df.pop("builder format version")).strip()
            assert_or_raise(self.builder_format_version == BUILDER_VER, UnsupportedBuilderVersionError(self.builder_format_version))
        if "resources local directory" in package_df.columns:
            resources_base_dir_src = sanitize_path(_string_from_element(package_df.pop("resources local directory")))
        else:
            resources_base_dir_src = None
        self._resources_base_dir_src = resources_base_dir_src
        self._apply_resources_base_dir_src(base_dir=base_dir)
        if "download directory" in package_df.columns:
            out_dir_src = sanitize_path(_string_from_element(package_df.pop("download directory")))
        else:
            out_dir_src = None
        self._default_out_dir_src = out_dir_src
        self._apply_out_dir_src(base_dir=base_dir)
        if "auxiliary functions file" in package_df.columns:
            auxiliary_functions_file = sanitize_path(_string_from_element(package_df.pop("auxiliary functions file")))
            if auxiliary_functions_file is not None:
                self.external_python_code = PythonUserCode(auxiliary_functions_file, base_dir=base_dir)
        if "comment" in package_df.columns:
            self.comment = _string_from_element(package_df.pop("comment"), empty_value="")
        # package attributes
        self.package_attributes: CkanPackageInfo
        self.package_name = _string_from_element(package_df.pop("name")).strip()
        self.package_attributes.title = _string_from_element(package_df.pop("title"))
        if "known id" in package_df.columns:
            self.package_attributes.id = _string_from_element(package_df.pop("known id"))
        if "description" in package_df.columns:
            self.package_attributes.description = _string_from_element(package_df.pop("description"))
        if "version" in package_df.columns:
            self.package_attributes.version = _string_from_element(package_df.pop("version"))
        if "visibility" in package_df.columns:
            visibility = _string_from_element(package_df.pop("visibility"))
            if visibility is not None:
                self.package_attributes.private = CkanVisibility.from_str(visibility).to_bool_is_private()
        if "state" in package_df.columns:
            state = _string_from_element(package_df.pop("state"))
            if state is not None:
                self.package_attributes.state = CkanState.from_str(state)
        if "url" in package_df.columns:
            # field not in the default Excel file
            self.package_attributes.url = _string_from_element(package_df.pop("url"))
        if "tags" in package_df.columns:
            tags_string = _string_from_element(package_df.pop("tags"))
            if tags_string is not None:
                self.package_attributes.tags = [label.strip() for label in tags_string.split(ckan_tags_sep)]
        if "author" in package_df.columns:
            self.package_attributes.author = _string_from_element(package_df.pop("author"))
        if "author email" in package_df.columns:
            self.package_attributes.author_email = _string_from_element(package_df.pop("author email"))
        if "maintainer" in package_df.columns:
            self.package_attributes.maintainer = _string_from_element(package_df.pop("maintainer"))
        if "maintainer email" in package_df.columns:
            self.package_attributes.maintainer_email = _string_from_element(package_df.pop("maintainer email"))
        # fields which may require additional CKAN requests to obtain ids of the designated objects
        if "license" in package_df.columns:
            self.license_name = _string_from_element(package_df.pop("license"))
        if "organization" in package_df.columns:
            self.organization_name = _string_from_element(package_df.pop("organization"))
        # other fields = user custom fields
        if "attribute" in package_df.columns:
            package_df.pop("attribute")  # reserved name for table header
        remaining_columns = list(package_df.columns)
        for column in remaining_columns:
            original_column = original_columns[renamed_columns.index(column)]
            self.package_attributes.custom_fields[original_column] = _string_from_element(package_df[column])
        self._check_mandatory_attributes()

    def _to_dict(self, base_dir:str=None, include_id:bool=True) -> Tuple[dict, dict]:
        """
        Function to export builder parameters to an Excel worksheet, using the same fields as the input format

        :see: _load_from_df
        :see: to_xls
        :return:
        """
        info_dict = dict()
        info_dict["Builder format version"] = BUILDER_VER
        info_dict["Auxiliary functions file"] = make_path_relative(self.external_python_code.python_file, to_base_dir=base_dir) if self.external_python_code is not None else ""
        info_dict["Resources local directory"] = self._get_resources_base_dir_src(base_dir=base_dir)
        info_dict["Download directory"] = self._get_out_dir_src(base_dir=base_dir)
        info_dict["Comment"] = self.comment
        package_dict = dict()
        package_dict["Name"] = self.package_name
        package_dict["Title"] = self.package_attributes.title
        if include_id and self.package_attributes.id:
            package_dict["Known Id"] = self.package_attributes.id
        package_dict["Description"] = self.package_attributes.description if self.package_attributes.description is not None else ""
        package_dict["Version"] = self.package_attributes.version if self.package_attributes.version is not None else ""
        package_dict["Visibility"] = CkanVisibility.from_bool_is_private(self.package_attributes.private).name if self.package_attributes.private is not None else ""
        package_dict["State"] = self.package_attributes.state.name if self.package_attributes.state is not None else ""
        package_dict["Organization"] = self.organization_name if self.organization_name is not None else ""
        package_dict["License"] = self.license_name if self.license_name is not None else ""
        package_dict["URL"] = self.package_attributes.url if self.package_attributes.url is not None else ""
        package_dict["Tags"] = ckan_tags_sep.join(self.package_attributes.tags) if self.package_attributes.tags is not None else ""
        package_dict["Author"] = self.package_attributes.author if self.package_attributes.author is not None else ""
        package_dict["Author Email"] = self.package_attributes.author_email if self.package_attributes.author_email is not None else ""
        package_dict["Maintainer"] = self.package_attributes.maintainer if self.package_attributes.maintainer is not None else ""
        package_dict["Maintainer Email"] = self.package_attributes.maintainer_email if self.package_attributes.maintainer_email is not None else ""
        for key, value in self.package_attributes.custom_fields.items():
            package_dict[key] = value if value is not None else ""
        return info_dict, package_dict

    def _get_builder_df_help_dict(self) -> Tuple[dict, dict]:
        info_help_dict = {
            "Builder format version": "Version of the file format for the script that processes this file",
            "Auxiliary functions file": "Path to a Python file containing auxiliary functions, relative to this Excel workbook folder\n"
                                        + "Warning: only execute code if you trust the source !",
            "Resources local directory": "Path to the local directory containing the resources to upload or text file defining this directory, relative to this Excel workbook folder",
            "Download directory": "Default path to download the resources to, relative to this Excel workbook folder",
            "Comment": "Place to add a comment on this file",
        }
        package_help_dict = {
            "Name": "Name used in the URL (short name)",
            "Title": "Title of the resource",
            "Description": "Description can use Markdown formatting",
            "Visibility": "Private/Public",
            "State": "Active/Draft/Deleted",
            "Organization": "Organization title, name or ID (mandatory)",
            "License": "License title or ID",
            "URL": "A URL for the dataset's source",
            "Tags": "Comma-separated list of tags (refer to data format policy)",
        }
        if self.package_attributes.id:
            package_help_dict["Known Id"] = "ID of the resource in the CKAN database, last requested"
        package_help_dict.update({key: "Custom key-value pair (refer to data format policy)" for key in self.package_attributes.custom_fields.keys()})
        return info_help_dict, package_help_dict

    def _load_from_dict(self, info_dict: dict, package_dict: dict, base_dir:str=None) -> None:
        if info_dict is not None:
            info_df = pd.DataFrame([info_dict], index=["Value"])
            info_df = info_df.transpose()
            info_df.index.name = "Attribute"
            info_df = info_df.transpose()
        else:
            info_df = None
        package_df = pd.DataFrame([package_dict], index=["Value"])
        package_df = package_df.transpose()
        package_df.index.name = "Attribute"
        package_df = package_df.transpose()
        self._load_from_df(info_df, package_df, base_dir=base_dir)

    def _get_builder_df(self, base_dir:str=None, include_id:bool=True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Converts the result of method _to_dict() into a DataFrame

        :return:
        """
        info_dict, package_dict = self._to_dict(base_dir=base_dir, include_id=include_id)
        info_help_dict, package_help_dict = self._get_builder_df_help_dict()
        package_df = pd.DataFrame([package_dict, package_help_dict], index=["Value", "Help"])
        package_df = package_df.transpose()
        package_df.index.name = "Attribute"
        info_df = pd.DataFrame([info_dict, info_help_dict], index=["Value", "Help"])
        info_df = info_df.transpose()
        info_df.index.name = "Attribute"
        return info_df, package_df

    def _check_resource_duplicates(self):
        duplicates = find_duplicates([resource_builder.name for resource_builder in self.resource_builders.values()])
        if len(duplicates) > 0:
            raise DuplicateNameError("Resource", duplicates)

    def _get_resources_dict(self, include_id:bool=True) -> Dict[str, dict]:
        self._check_resource_duplicates()
        resources_dict = {resource_builder.name: resource_builder._to_dict(include_id=include_id) for resource_builder in self.resource_builders.values()}
        return resources_dict

    def _get_resources_df(self, include_id:bool=True) -> pd.DataFrame:
        """
        Calls the method _to_dict() on all resources and returns the DataFrame listing the resources of the package

        :return:
        """
        resources_dict_list = [value for value in self._get_resources_dict(include_id=include_id).values()]
        resources_df = pd.DataFrame.from_records(resources_dict_list)
        return resources_df

    def _get_datastores_dict(self) -> Dict[str, dict]:
        """
        Calls the method _get_fields_dict() on all resources which are DataStores and returns a DataFrame per DataStore
        listing the fields of the DataStore with their metadata

        :return:
        """
        return {resource.name: resource._get_fields_dict() for resource in self.resource_builders.values()
                if (isinstance(resource, BuilderDataStoreABC) or isinstance(resource, BuilderMultiDataStore)) and resource.field_builders is not None}

    def _get_datastores_df(self) -> Dict[str, pd.DataFrame]:
        """
        Calls the method _get_fields_df() on all resources which are DataStores and returns a DataFrame per DataStore
        listing the fields of the DataStore with their metadata

        :return:
        """
        return {resource.name: resource._get_fields_df() for resource in self.resource_builders.values()
                if (isinstance(resource, BuilderDataStoreABC) or isinstance(resource, BuilderMultiDataStore)) and resource.field_builders is not None}

    def get_all_df(self, base_dir:str=None, include_id:bool=True) -> Dict[str, pd.DataFrame]:
        """
        Returns all the dataframes used to define the object and components

        :return:
        """
        info_df, package_df = self._get_builder_df(base_dir=base_dir, include_id=include_id)
        ckan_df = self.ckan_builder._get_builder_df(base_dir=base_dir)
        resources_df = self._get_resources_df(include_id=include_id)
        datastores_df = self._get_datastores_df()
        df_dict = {"info": info_df, "ckan": ckan_df, "package": package_df, "resources": resources_df}
        df_dict.update(datastores_df)
        return df_dict

    def to_excel(self, path_or_buffer, *, engine:str=None, include_id:bool=True, include_help:bool=True, **kwargs) -> None:
        """
        Call this function to export the builder parameters to an Excel worksheet

        :param path_or_buffer:
        :param engine:
        :return:
        """
        if isinstance(path_or_buffer, str):
            base_dir, _ = os.path.split(path_or_buffer)
        else:
            base_dir = None
        info_df, package_df = self._get_builder_df(base_dir=base_dir, include_id=include_id)
        ckan_df = self.ckan_builder._get_builder_df(base_dir=base_dir)
        resources_df = self._get_resources_df(include_id=include_id)
        datastores_df = self._get_datastores_df()
        with pd.ExcelWriter(path_or_buffer, engine=engine, **kwargs) as writer:
            ckan_df.to_excel(writer, sheet_name="ckan", index=True)
            info_df.to_excel(writer, sheet_name="info", index=True)
            package_df.to_excel(writer, sheet_name="package", index=True)
            resources_df.to_excel(writer, sheet_name="resources", index=False)
            for name, df in datastores_df.items():
                df.to_excel(writer, sheet_name=excel_name_of_sheet(name), index=False)
            if include_help:
                with pd.ExcelFile(example_package_xls, engine=engine) as help_file:
                    help_df = pd.read_excel(help_file, sheet_name="help", header=None)
                    help_file.close()
                help_df.to_excel(writer, sheet_name="help", index=False, header=False)
            # writer.close()

    def to_dict(self, base_dir:str=None, include_id:bool=True) -> dict:
        """
        Call this function to export the builder parameters to an Excel worksheet

        :return:
        """
        d = dict()
        d["Info"], d["Package"] = self._to_dict(base_dir=base_dir, include_id=include_id)
        d["CKAN"] = self.ckan_builder._to_dict(base_dir=base_dir)
        resources_dict = self._get_resources_dict(include_id=include_id)
        datastores_dict = self._get_datastores_dict()
        for name, fields_dict in datastores_dict.items():
            resources_dict[name]["fields"] = list(fields_dict.values())
        d["Resources"] = list(resources_dict.values())
        return d

    def to_json(self, json_file:str, *, include_id:bool=True, reduced_size:bool=None) -> None:
        if reduced_size is None:
            reduced_size = self.default_to_json_reduced_size
        base_dir, _ = os.path.split(json_file)
        builder_dict = self.to_dict(base_dir=base_dir, include_id=include_id)
        with open(json_file, "w", encoding="utf-8") as f:
            if reduced_size:
                json.dump(builder_dict, f, ensure_ascii=False)
            else:
                json.dump(builder_dict, f, ensure_ascii=False, indent=4)
            f.close()

    def to_jsons(self, *, base_dir:str=None, include_id:bool=True, reduced_size:bool=None) -> str:
        if reduced_size is None:
            reduced_size = self.default_to_json_reduced_size
        builder_dict = self.to_dict(base_dir=base_dir, include_id=include_id)
        if reduced_size:
            return json.dumps(builder_dict, ensure_ascii=False)
        else:
            return json.dumps(builder_dict, ensure_ascii=False, indent=4)

    @staticmethod
    def from_ckan(ckan: CkanApiMap, package_info: Union[CkanPackageInfo, str]) -> "BuilderPackageBasic":
        """
        Function to initialize a BuilderPackageBasic from information requested by the CKAN API

        :param ckan:
        :param package_info: The package to import or the package name
        :return:
        """
        if isinstance(package_info, str):
            package_info = ckan.get_package_info_or_request(package_info, datastore_info=True)
        package_info: CkanPackageInfo
        mdl = BuilderPackageBasic()
        mdl.package_attributes = package_info
        mdl.organization_name = package_info.organization_info.get_owner_org() if package_info.organization_info is not None else None
        mdl.license_name = package_info.license_id if package_info.license_id else None
        mdl.license_name = mdl.get_license_name(ckan)
        for resource in package_info.package_resources.values():
            mdl.resource_builders[resource.name] = init_resource_from_ckan(ckan, resource)
        mdl.update_package_name_in_resources()
        mdl.update_ckan_options_name_in_resources(ckan)
        mdl.builder_source_file = "ckan"
        return mdl

    def update_from_ckan(self, ckan:CkanApiMap, *, error_not_found:bool=True) -> None:
        """
        Update IDs from CKAN mapped objects.
        Objects must be mapped first.
        """
        package_info = ckan.map.get_package_info(self.package_name, error_not_mapped=error_not_found)
        package_id = package_info.id
        self.package_attributes.id = package_id
        for resource_builder in self.resource_builders.values():
            resource_info = ckan.map.get_resource_info(resource_builder.name, package_id, error_not_mapped=error_not_found)
            resource_builder.id = resource_info.id if resource_info is not None else None

    def _init_resource_from_df_aux_fun(self, resource_builder: BuilderResourceABC) -> None:
        if isinstance(resource_builder, BuilderDataStoreABC):
            resource_builder.df_mapper._connect_aux_functions(self.external_python_code,
                                                              aux_upload_fun_name=resource_builder.aux_upload_fun_name,
                                                              aux_download_fun_name=resource_builder.aux_download_fun_name)

    def to_ckan_package_info(self, *, check_id:bool=True) -> CkanPackageInfo:
        """
        Function to insert the information coming from the builder into the CKAN map.
        Requires the IDs of the package and resources to be known.
        This enables to use the stored IDs instead of querying the CKAN API for these IDs.

        :return:
        """
        package_id = self.package_attributes.id
        package_info: CkanPackageInfo = self.package_attributes.copy()
        if package_id is None and check_id:
            msg = MissingIdError("package", self.package_name)
            raise(msg)
        for resource_builder in self.resource_builders.values():
            if isinstance(resource_builder, BuilderMultiFile):
                msg = f"Multi-resource builder is not compatible with updating CKAN resource ids from known ids because more than one id is expected (resource builder {resource_builder.name})"
                warn(msg)
            else:
                package_info.package_resources[resource_builder.known_id] = resource_builder._to_ckan_resource_info(package_id, check_id=check_id)
        package_info.resources_id_index = {resource_info.name: resource_info.id for resource_info in package_info.package_resources.values()}  # resource name -> id
        package_info.resources_id_index_counts = {}  # resource name -> counter
        for resource_info in package_info.package_resources.values():
            if resource_info.name not in package_info.resources_id_index_counts.keys():
                package_info.resources_id_index_counts[resource_info.name] = 1
            else:
                package_info.resources_id_index_counts[resource_info.name] += 1
        return package_info

    def update_ckan_map(self, ckan: CkanApiMap) -> CkanPackageInfo:
        """
        This function updates the CKAN map from the information contained in this builder.
        For this to work, the package and resource ids must be known.
        This is not the case if the package was not initialized.
        Use if the builder was initialized from ckan or use with precaution.

        :param ckan:
        :return:
        """
        package_info = self.to_ckan_package_info(check_id=True)
        ckan.map._update_package_info(package_info)
        return package_info.copy()

    def map_resources(self, ckan: CkanApiMap, *, error_not_found:bool=True, cancel_if_exists:bool=True,
                      datastore_info:bool=True) -> Union[CkanPackageInfo,None]:
        """
        proxy call to ckan.map_resources and returns package information from CKAN

        :param ckan:
        :param error_not_found:
        :param cancel_if_exists:
        :return:
        """
        ckan.map_resources(self.package_name, datastore_info=datastore_info, error_not_found=error_not_found, only_missing=cancel_if_exists)
        package_info = ckan.map.get_package_info(self.package_name, error_not_mapped=error_not_found)
        if package_info is None:
            return None
        self.package_attributes.id = package_info.id
        self.update_from_ckan(ckan, error_not_found=error_not_found)
        return package_info

    def _load_package_resources_list_df(self, resources_df: pd.DataFrame, base_dir:str=None) -> None:
        resources_df.columns = resources_df.columns.map(str.lower)
        resources_df.columns = resources_df.columns.map(str.strip)
        self.resource_builders = OrderedDict()
        for index, row in resources_df.iterrows():
            resource_builder = init_resource_from_df(row, base_dir=base_dir)
            self._init_resource_from_df_aux_fun(resource_builder)
            if resource_builder.name in self.resource_builders.keys():
                raise DuplicateNameError("resource_builder", resource_builder.name)
            if resource_builder.name.lower() in forbidden_resource_names:
                raise ForbiddenNameError("resource_builder", resource_builder.name)
            self.resource_builders[resource_builder.name] = resource_builder
        # self._update_package_name_resources()  # call after full init in caller function

    @staticmethod
    def from_excel(path_or_buffer, *, proxies:dict=None, engine:str=None, **kwargs) -> "BuilderPackageBasic":
        """
        Load package definition from an Excel workbook.

        :param path_or_buffer: path to the Excel workbook
        :param engine: Engine used by pandas.read_excel(). Supported engines: xlrd, openpyxl, odf, pyxlsb, calamine.
        openpyxl makes part of this package's optional requirements
        :return:
        """
        mdl = BuilderPackageBasic()
        mdl.builder_source_file = path_or_buffer
        with pd.ExcelFile(path_or_buffer, engine=engine, **kwargs) as xls:
            sheet_names = set(xls.sheet_names)
            sheet_names_lower_index = {sheet_name.lower().strip(): sheet_name for sheet_name in sheet_names}
            package_df = pd.read_excel(xls, sheet_name=sheet_names_lower_index["package"], header=None)
            package_df.set_index(0, inplace=True, verify_integrity=True)
            package_df = package_df.T
            if "info" in sheet_names_lower_index.keys():
                info_df = pd.read_excel(xls, sheet_name=sheet_names_lower_index["info"], header=None)
                info_df.set_index(0, inplace=True, verify_integrity=True)
                info_df = info_df.T
            else:
                info_df = None
            base_dir = mdl.get_base_dir(None)
            mdl._load_from_df(info_df, package_df, base_dir=base_dir)
            if "ckan" in sheet_names_lower_index.keys():
                ckan_df = pd.read_excel(xls, sheet_name=sheet_names_lower_index["ckan"], header=None)
                ckan_df.set_index(0, inplace=True, verify_integrity=True)
                ckan_df = ckan_df.T
                mdl.ckan_builder._load_from_df(ckan_df, base_dir=base_dir, proxies=proxies)
            resources_df = pd.read_excel(xls, sheet_name=sheet_names_lower_index["resources"])
            mdl._load_package_resources_list_df(resources_df, base_dir=base_dir)
            resource_sheets = sheet_names - {sheet_names_lower_index[name] for name in forbidden_resource_names if name in sheet_names_lower_index.keys()}
            for resource_builder in mdl.resource_builders.values():
                resource_sheet = None
                equiv_name = excel_name_of_builder(resource_builder)
                if resource_builder.name in resource_sheets:
                    resource_sheet = resource_builder.name
                elif equiv_name in resource_sheets:
                    resource_sheet = equiv_name
                if resource_sheet is not None:
                    fields_df = pd.read_excel(xls, sheet_name=resource_sheet)
                    assert(isinstance(resource_builder, BuilderDataStoreABC) or isinstance(resource_builder, BuilderMultiDataStore))
                    resource_builder._load_fields_df(fields_df)
                    resource_sheets.remove(resource_sheet)
            mdl.update_package_name_in_resources()
            if len(resource_sheets) > 0:
                msg = f"Sheets present but not used: {', '.join(resource_sheets)}"
                warn(msg)
            xls.close()
        return mdl

    @staticmethod
    def from_dict(d:dict, base_dir:str=None, *, proxies:dict=None) -> "BuilderPackageBasic":
        """
        Load package definition from a dictionary.
        In this case, the base directory used to specify the resources locations must be given manually.
        This is usually the directory of the file where the dictionary comes from.

        :param d:
        :param base_dir:
        :param proxies:
        :return:
        """
        mdl = BuilderPackageBasic()
        mdl.builder_source_file = None
        sheet_names = set(d.keys())
        sheet_names_lower_index = {sheet_name.lower().strip(): sheet_name for sheet_name in sheet_names}
        info_dict = d[sheet_names_lower_index["info"]] if "info" in sheet_names_lower_index.keys() else None
        mdl._load_from_dict(info_dict, d[sheet_names_lower_index["package"]], base_dir=base_dir)
        if "ckan" in sheet_names_lower_index.keys():
            ckan_dict = d[sheet_names_lower_index["ckan"]]
            mdl.ckan_builder._load_from_dict(ckan_dict, base_dir=base_dir, proxies=proxies)
        resources_dict = dict()
        for resource_dict in d[sheet_names_lower_index["resources"]]:
            resource_dict_alt = {k.lower().strip(): v for k, v in resource_dict.items()}
            resources_dict[resource_dict_alt["name"]] = resource_dict_alt
        resources_df = pd.DataFrame(list(resources_dict.values()))
        mdl._load_package_resources_list_df(resources_df, base_dir=base_dir)
        resource_sheets = sheet_names - {sheet_names_lower_index[name] for name in forbidden_resource_names if name in sheet_names_lower_index.keys()}
        for resource_builder in mdl.resource_builders.values():
            if "fields" in resources_dict[resource_builder.name]:
                assert(isinstance(resource_builder, BuilderDataStoreABC) or isinstance(resource_builder, BuilderMultiDataStore))
                fields_df = pd.DataFrame(resources_dict[resource_builder.name]["fields"])
                resource_builder._load_fields_df(fields_df)
            else:
                resource_sheet = None
                equiv_name = excel_name_of_builder(resource_builder)
                if resource_builder.name in resource_sheets:
                    resource_sheet = resource_builder.name
                elif equiv_name in resource_sheets:
                    resource_sheet = equiv_name
                if resource_sheet is not None:
                    assert(isinstance(resource_builder, BuilderDataStoreABC) or isinstance(resource_builder, BuilderMultiDataStore))
                    fields_df = pd.DataFrame(list(d[resource_sheet].values()))
                    resource_builder._load_fields_df(fields_df)
                    resource_sheets.remove(resource_sheet)
        mdl.update_package_name_in_resources()
        if len(resource_sheets) > 0:
            msg = f"Sheets present but not used: {', '.join(resource_sheets)}"
            warn(msg)
        return mdl

    @staticmethod
    def from_json(json_file, *, proxies:dict=None) -> "BuilderPackageBasic":
        base_dir, _ = os.path.split(json_file)
        with open(json_file, "r") as f:
            builder_dict = json.load(f)
            f.close()
        mdl = BuilderPackageBasic.from_dict(builder_dict, base_dir=base_dir, proxies=proxies)
        mdl.builder_source_file = json_file
        return mdl

    @staticmethod
    def from_jsons(stream:str, *, source_file:str=None, proxies:dict=None) -> "BuilderPackageBasic":
        base_dir, _ = os.path.split(source_file) if source_file is not None else (None, None)
        builder_dict = json.loads(stream)
        mdl = BuilderPackageBasic.from_dict(builder_dict, base_dir=base_dir, proxies=proxies)
        mdl.builder_source_file = source_file
        return mdl

    def get_owner_org(self, ckan: CkanApiMap) -> str:
        """
        Returns the owner organization for the package.
        The owner organization can be specified by its name, title or id

        :param ckan:
        :return:
        """
        if self.organization_name is not None:
            ckan.organization_list_all(cancel_if_present=True)
            # organization_info = ckan.get_organization_info_or_request(self.organization_name, error_not_found=True)
            organization_info = ckan.map.get_organization_info(self.organization_name, error_not_mapped=True)
            owner_org = organization_info.get_owner_org()
        else:
            owner_org = None
        return owner_org

    def get_license_id(self, ckan: CkanApiMap) -> str:
        """
        Returns the license for the package.
        The license can be specified by its title or id

        :param ckan:
        :return:
        """
        if self.license_name is not None:
            ckan.license_list(cancel_if_present=True)
            license_id = ckan.map.get_license_id(self.license_name, error_not_mapped=True)
        else:
            license_id = None
        return license_id

    def get_license_info(self, ckan: CkanApiMap) -> CkanLicenseInfo:
        license_id = self.get_license_id(ckan)
        license_info = ckan.map.get_license_info(license_id) if license_id is not None else None
        return license_info

    def get_license_name(self, ckan: CkanApiMap) -> str:
        license_info = self.get_license_info(ckan)
        return license_info.title if license_info is not None else None

    def patch_request_package(self, ckan:CkanApi) -> CkanPackageInfo:
        """
        Function to perform all the necessary requests to initiate/reupload the package on the CKAN server.
        This function does not upload the package resources.
        NB: the organization must be provided, especially if the package is private

        :param ckan:
        :return:
        """
        owner_org = self.get_owner_org(ckan)
        license_id = self.get_license_id(ckan)
        return ckan.package_create(self.package_name, private=self.package_attributes.private, state=self.package_attributes.state,
                                   title=self.package_attributes.title, notes=self.package_attributes.description, owner_org=owner_org,
                                   tags=self.package_attributes.tags, custom_fields=self.package_attributes.custom_fields,
                                   url=self.package_attributes.url, version=self.package_attributes.version,
                                   author=self.package_attributes.author, author_email=self.package_attributes.author_email,
                                   maintainer=self.package_attributes.maintainer, maintainer_email=self.package_attributes.maintainer_email,
                                   license_id=license_id,
                                   cancel_if_exists=True, update_if_exists=True)

    def patch_request_full(self, ckan:CkanApi, *,
                           reupload:bool=False, resources_base_dir:str=None,
                           create_default_view:bool=True) \
            -> Tuple[CkanPackageInfo, Dict[str, CkanResourceInfo]]:
        """
        Perform necessary requests to initiate/reupload the package and resources on the CKAN server.
        For folder resources, this only uploads the first file of the resource.

        :param ckan:
        :return:
        """
        # call to function update_request of package and update_request of resources
        if ckan.params.policy_check_pre:
            self.local_policy_check()
        resources_base_dir = self.get_resources_base_dir(resources_base_dir)
        self.upload_file_checks(resources_base_dir=resources_base_dir, ckan=ckan, verbose=True, raise_error=True)
        pkg_info = self.patch_request_package(ckan)
        ckan.map_resources(self.package_name, datastore_info=True)
        package_id = pkg_info.id
        self.package_attributes.id = package_id
        resource_info_dict: Dict[str, CkanResourceInfo] = {}
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        for resource_builder in self.resource_builders.values():
            if create_default_view is not None:
                resource_builder.create_default_view = create_default_view
            resource_info = resource_builder.patch_request(ckan, package_id, reupload=reupload, resources_base_dir=resources_base_dir)
            resource_info_dict[resource_builder.name] = resource_info
            if resource_info is not None:  # this would be the case for BuilderMultiFile
                pkg_info.update_resource(resource_info)
            else:
                assert(isinstance(resource_builder, BuilderMultiFile))
        self.package_resource_reorder(ckan)
        if ckan.params.policy_check_post:
            self.remote_policy_check(ckan)
        return pkg_info, resource_info_dict

    def _get_mono_resource_used_files(self, resources_base_dir:str):
        """
        List files used by mono-resource builders

        :param resources_base_dir:
        :return:
        """
        mono_resource_used_files = set()
        for resource_builder in self.resource_builders.values():
            if isinstance(resource_builder, BuilderDataStoreMultiABC):
                if not isinstance(resource_builder, BuilderDataStoreHarvester):
                    file_list = resource_builder.init_local_files_list(resources_base_dir=resources_base_dir)
                    mono_resource_used_files.update(set(file_list))
            elif not (isinstance(resource_builder, BuilderMultiFile)):
                if resource_builder.get_sample_file_path(resources_base_dir) is not None and not resource_builder.sample_file_path_is_url():
                    mono_resource_used_files.add(resource_builder.get_sample_file_path(resources_base_dir))
        return mono_resource_used_files

    def upload_file_checks(self, resource_name:Union[str, List[str]]=None, *, resources_base_dir:str=None,
                           messages:Dict[str, ContextErrorLevelMessage]=None,
                           verbose:bool=True, raise_error:bool=False, ckan:CkanApi=None, **kwargs) -> bool:
        """
        Method to check the presence of all needed files before uploading or patching resources.

        :param resources_base_dir:
        :param ckan: Optional CkanApi object used to parameterize the requests to test the presence of resources defined by an url.
        :param kwargs: keyword arguments to specify connexion parameters for querying the urls.
        :return:
        """
        if resource_name is None:
            resource_name = list(self.resource_builders.keys())
        elif isinstance(resource_name, str):
            resource_name = [resource_name]
        if messages is None:
            messages = {}
        self.update_package_name_in_resources()
        resources_base_dir = self.get_resources_base_dir(resources_base_dir)
        mono_resource_used_files = self._get_mono_resource_used_files(resources_base_dir)
        for resource_builder_name in resource_name:
            resource_builder = self.resource_builders[resource_builder_name]
            if isinstance(resource_builder, BuilderMultiFile):
                messages[resource_builder_name] = resource_builder.upload_file_checks(resources_base_dir=resources_base_dir, ckan=ckan,
                                                        excluded_files=mono_resource_used_files if multi_file_exclude_other_files else None, **kwargs)
            else:
                messages[resource_builder_name] = resource_builder.upload_file_checks(resources_base_dir=resources_base_dir, ckan=ckan, **kwargs)
        num_messages = len([1 for message in messages.values() if message is not None])
        success = len([1 for message in messages.values() if message is not None and message.error_level == ErrorLevel.Error]) == 0
        if verbose and num_messages > 0:
            print("\n".join([f"for resource {key}: {message}" for key, message in messages.items() if message is not None]))
        if raise_error and not success:
            raise FileNotFoundError("\n".join([f"for resource {key}: {message}" for key, message in messages.items() if message is not None and message.error_level == ErrorLevel.Error]))
        return success

    def upload_large_datasets(self, ckan:CkanApi, *, resources_base_dir:str=None, threads:int=1,
                              progress_callback:Callable=None, only_missing:bool=False) -> None:
        """
        Method to upload large datasets of the package.
        The small datasets are to be uploaded with the patch_request_full method.

        :param ckan:
        :param resources_base_dir:
        :param threads:
        :param progress_callback:
        :param only_missing: upsert only missing rows for DataStores and only missing files for MultiFile
        :return:
        """
        self.info_request_package(ckan=ckan)
        resources_base_dir = self.get_resources_base_dir(resources_base_dir)
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        resource_names = [key for key, resource_builder in self.resource_builders.items() if isinstance(resource_builder, BuilderDataStoreMultiABC)]
        self.upload_file_checks(resource_names, resources_base_dir=resources_base_dir, ckan=ckan, verbose=True, raise_error=True)
        mono_resource_used_files = self._get_mono_resource_used_files(resources_base_dir)
        for resource_builder in self.resource_builders.values():
            if isinstance(resource_builder, BuilderDataStoreMultiABC):
                if progress_callback is not None:
                    resource_builder.progress_callback = progress_callback
                resource_builder.upload_request_full(ckan=ckan, resources_base_dir=resources_base_dir, threads=threads,
                                                     only_missing=only_missing)
        for resource_builder in self.resource_builders.values():
            if isinstance(resource_builder, BuilderMultiFile):
                if progress_callback is not None:
                    resource_builder.progress_callback = progress_callback
                resource_builder.upload_request_full(ckan=ckan, resources_base_dir=resources_base_dir, threads=threads,
                                                     only_missing=only_missing,
                                                     excluded_files=mono_resource_used_files if multi_file_exclude_other_files else None)
        self.package_resource_reorder(ckan)

    def download_resource_df(self, ckan:CkanApi, resource_name:str, search_all:bool=False, **kwargs) -> pd.DataFrame:
        """
        Proxy for download_sample_df for a DataStore
        """
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        assert(isinstance(self.resource_builders[resource_name], BuilderDataStoreABC))
        return self.resource_builders[resource_name].download_sample_df(ckan=ckan, search_all=search_all, **kwargs)

    def download_resource(self, ckan:CkanApi, resource_name:str, full_download:bool=False, **kwargs) -> bytes:
        """
        Proxy for download_sample for a resource
        """
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        return self.resource_builders[resource_name].download_sample(ckan=ckan, full_download=full_download, **kwargs)

    def get_or_query_resource_id(self, ckan:CkanApi, resource_name:str, error_not_found:bool=True) -> str:
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        return self.resource_builders[resource_name].get_or_query_resource_id(ckan, error_not_found=error_not_found)

    def _get_mono_resource_names(self):
        """
        List resource names of mono-resource builders.

        :return:
        """
        return {resource_name for resource_name, resource_builder in self.resource_builders.items() if not isinstance(resource_builder, BuilderMultiFile)}

    def download_request_full(self, ckan:CkanApi, out_dir:str=None, enforce_none_out_dir:bool=False, resource_name:str=None, full_download:bool=False,
                              threads:int=1, skip_existing:bool=True, progress_callback:Callable=None,
                              force:bool=False, rm_dir:bool=False) -> None:
        """
        Downloads the full package resources into out_dir.

        :param ckan:
        :param out_dir: download directory
        :param rm_dir: remove directory if exists before downloading
        :param skip_existing: skip download of existing resources
        :param enforce_none_out_dir: if no out_dir is provided, True: files will not be saved after download, False: default output dir will be used, if defined
        :param resource_name:
        :param full_download: option to fully download the resources. If False, only a partial download is made.
        :param threads:
        :param progress_callback:
        :param force: option to bypass the enable_download attribute of resources
        :return:
        """
        out_dir = self.get_default_out_dir(out_dir, enforce_none=enforce_none_out_dir)
        if out_dir is not None and os.path.isdir(out_dir):
            if rm_dir:
                shutil.rmtree(out_dir)
        self.info_request_package(ckan=ckan)
        if resource_name is None:
            resource_builders = self.resource_builders
        else:
            resource_builders = {resource_name: self.resource_builders[resource_name]}
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        mono_resource_names = self._get_mono_resource_names()
        for resource_builder in resource_builders.values():
            if skip_existing is not None:
                resource_builder.download_skip_existing = skip_existing
            if not (isinstance(resource_builder, BuilderDataStoreMultiABC) or isinstance(resource_builder, BuilderMultiFile)):
                resource_builder.download_request(ckan, out_dir=out_dir, full_download=full_download,
                                                  threads=threads, force=force)
        for resource_builder in resource_builders.values():
            if isinstance(resource_builder, BuilderDataStoreMultiABC):
                if progress_callback is not None:
                    resource_builder.progress_callback = progress_callback
                resource_builder.download_request(ckan, out_dir=out_dir, full_download=full_download,
                                                  threads=threads, force=force)
        for resource_builder in resource_builders.values():
            if isinstance(resource_builder, BuilderMultiFile):
                if progress_callback is not None:
                    resource_builder.progress_callback = progress_callback
                resource_builder.download_request(ckan, out_dir=out_dir, full_download=full_download,
                                                  threads=threads, force=force, excluded_resource_names=mono_resource_names)

    def download_sample_df(self, ckan:CkanApi, resource_name:str=None, *, search_all:bool=False, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Download a sample DataFrame for the DataStore type resources.

        :param ckan:
        :param resource_name:
        :return:
        """
        self.info_request_package(ckan=ckan)
        if resource_name is None:
            resource_builders = self.resource_builders
        else:
            resource_builders = {resource_name: self.resource_builders[resource_name]}
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        df_dict = {}
        for resource_builder in resource_builders.values():
            if isinstance(resource_builder, BuilderDataStoreABC):
                df_dict[resource_builder.name] = resource_builder.download_sample_df(ckan, search_all=search_all, **kwargs)
        return df_dict

    def download_sample(self, ckan:CkanApi, resource_name:str=None, *, datastores_as_df:bool=True, search_all:bool=False, **kwargs) -> Dict[str, Union[bytes, pd.DataFrame]]:
        """
        Download samples from all resources.

        :param ckan:
        :param resource_name:
        :return:
        """
        self.info_request_package(ckan=ckan)
        if resource_name is None:
            resource_builders = self.resource_builders
        else:
            resource_builders = {resource_name: self.resource_builders[resource_name]}
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        df_dict = {}
        for resource_builder in resource_builders.values():
            if isinstance(resource_builder, BuilderDataStoreABC) and datastores_as_df:
                df_dict[resource_builder.name] = resource_builder.download_sample_df(ckan, search_all=search_all, **kwargs)
            else:
                df_dict[resource_builder.name] = resource_builder.download_sample(ckan, search_all=search_all, **kwargs)
        return df_dict

    def info_request_package(self, ckan:CkanApi) -> CkanPackageInfo:
        pkg_info = ckan.get_package_info_or_request(package_name=self.package_name)
        self.package_attributes.id = pkg_info.id
        return pkg_info

    def info_request_full(self, ckan:CkanApi) -> Tuple[CkanPackageInfo, List[CkanResourceInfo]]:
        pkg_info = self.info_request_package(ckan)
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        res_info = [resource_builder.resource_info_request(ckan) for resource_builder in self.resource_builders.values()]
        return pkg_info, res_info

    def get_base_dir(self, base_dir:str=None) -> str:
        """
        Returns the default base_dir if not specified. The base_dir is the location of the Excel workbook.
        If this was initialized from a dictionary, the current working directory will be used (cwd).

        :return:
        """
        if base_dir is None:
            if self.builder_source_file is not None:
                base_dir, _ = os.path.split(self.builder_source_file)
            else:
                base_dir = os.path.abspath(".")
        return base_dir

    def get_resources_base_dir(self, resources_base_dir:str) -> str:
        """
        This returns the base directory for the resource files.
        It is distinct from the base_dir and can be defined relative to the base_dir in the Excel workbook (see comment at the top of the class).

        :param resources_base_dir:
        :return:
        """
        if resources_base_dir is None:
            resources_base_dir = self._resources_base_dir
        return resources_base_dir

    def get_default_out_dir(self, out_dir:str, enforce_none:bool=False) -> str:
        """
        This returns the default download directory.

        :param out_dir:
        :return:
        """
        if out_dir is None and not enforce_none:
            out_dir = self._default_out_dir
        return out_dir

    def init_ckan(self, ckan:CkanApi=None, *, base_dir:str=None, set_owner_org:bool=False,
                  default_proxies:dict=None, proxies:Union[str,dict,ProxyConfig]=None) -> CkanApi:
        """
        Initialize the CKAN instance from the parameters defined in the "ckan" tab of the Excel workbook.

        :param ckan: 
        :param base_dir: 
        :param default_proxies: 
        :param set_owner_org: Option to set the owner_org of the CKAN instance.
        This can be problematic because it requires some requests as the proxies are not set.
        It can be omitted because it has no influence on the patch_request_package function.
        :return: 
        """
        base_dir = self.get_base_dir(base_dir)  # base_dir is necessary to find the API key file, if provided
        ckan = self.ckan_builder.init_ckan(base_dir, ckan=ckan, default_proxies=default_proxies,
                                           proxies=proxies)
        if set_owner_org and self.organization_name is not None:
            ckan.owner_org = self.get_owner_org(ckan)
        return ckan

    def get_or_query_package_id(self, ckan: CkanApi) -> str:
        package_info = ckan.get_package_info_or_request(self.package_name)
        self.package_attributes.id = package_info.id
        return package_info.id

    def list_resource_ids(self, ckan: CkanApi) -> List[str]:
        """
        List resource ids on CKAN server, following the order of the package builder

        :param ckan:
        :return:
        """
        self.update_package_name_in_resources()
        self.update_ckan_options_name_in_resources(ckan)
        mono_resource_names = {resource_name for resource_name, resource_builder in self.resource_builders.items() if not isinstance(resource_builder, BuilderMultiFile)}
        resource_ids = []
        for resource_builder in self.resource_builders.values():
            if not (isinstance(resource_builder, BuilderMultiFile)):
                resource_ids.append(resource_builder.get_or_query_resource_id(ckan))
            else:
                multi_resource_ids = resource_builder.list_remote_resource_ids(ckan, excluded_resource_names=mono_resource_names,
                                                                               cancel_if_present=False)
                resource_ids = resource_ids + multi_resource_ids
        np_resource_ids = np.array(resource_ids)
        _, I = np.unique(np_resource_ids, return_index=True)
        I.sort()
        np_resource_ids = np_resource_ids[I]
        resource_ids = np_resource_ids.tolist()
        return resource_ids

    def package_resource_reorder(self, ckan: CkanApi) -> None:
        """
        Apply the order of the resources defined in the Excel workbook.

        :param ckan: 
        :return: 
        """
        # OrderedDict ensures the order of resources is preserved
        package_id = self.get_or_query_package_id(ckan=ckan)
        resource_ids = self.list_resource_ids(ckan=ckan)
        ckan._api_package_resource_reorder(package_id=package_id, resource_ids=resource_ids)

    def remote_policy_check(self, ckan: CkanApi, policy:CkanPackageDataFormatPolicy=None,
                            *, buffer:Dict[str, List[DataPolicyError]]=None, raise_error:bool=False,
                            verbose:bool=None) -> bool:
        """
        Check the package defined by this builder against a data format policy, based on the information from the API.

        :param ckan:
        :param policy:
        :param buffer:
        :param raise_error:
        :param verbose:
        :return:
        """
        if policy is None:
            policy = self.ckan_builder.policy
        return ckan.policy_check(package_list=self.package_name, policy=policy, buffer=buffer,
                                 verbose=verbose, raise_error=raise_error)

    def local_policy_check(self, policy:CkanPackageDataFormatPolicy=None,
                           *, buffer:Dict[str, List[DataPolicyError]]=None, raise_error:bool=False,
                           verbose:bool=True) -> bool:
        """
        Check if the package builder respects a data format policy (only on local definition).

        :return:
        """
        if policy is None:
            policy = self.ckan_builder.policy
        if policy is None:
            # no policy loaded at all
            return True
        package_info = self.to_ckan_package_info(check_id=False)
        package_buffer: List[DataPolicyError] = []
        success = policy.policy_check_package(package_info, display_message=verbose,
                                              package_buffer=package_buffer, raise_error=raise_error)
        if buffer is not None:
            buffer[package_info.name] = package_buffer
        if verbose:
            print(f"Data format policy {policy.label} success: {success}")
        return success


