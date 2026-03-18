#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Set, Any, Generator
import os
import io
from warnings import warn
from collections import OrderedDict
import copy
import argparse
import shlex

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanProgressCallback
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.builder.mapper_datastore_multi import RequestFileMapperIndexKeys
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC
from ckanapi_harvesters.harvesters.file_formats.file_format_init import init_file_format_datastore
from ckanapi_harvesters.builder.mapper_datastore import DataSchemeConversion
from ckanapi_harvesters.builder.builder_resource import BuilderResourceABC, initial_resource_building_state
from ckanapi_harvesters.auxiliary.ckan_errors import DuplicateNameError
from ckanapi_harvesters.auxiliary.path import resolve_rel_path
from ckanapi_harvesters.builder.builder_errors import RequiredDataFrameFieldsError, GroupByError, IncompletePatchError
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, find_duplicates, datastore_id_col
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_init import init_data_cleaner

# number of rows to upload to initiate DataStore with datapusher, before explicitly specifying field data types and indexes
num_rows_patch_first_upload_partial: Union[int,None] = 50  # set to None to upload directly the whole DataFrame before the DataStore creation


default_alias_keyword:Union[str,None] = "default"  # generate default alias if an alias with this value is found in parameters


class BuilderDataStoreABC(BuilderResourceABC, ABC):
    """
    The base class for DataStore resources. A DataStore resource can be updated with multiple requests and holds metadata for fields.

    :param field_builders: Merged metadata for fields (used in requests)
    :param field_builders_user: Field metadata specified by user (if exists, metadata from CKAN is prioritary)
    :param field_builders_data_source: Field metadata which could be obtained from the builder data source
    :param primary_key: primary key to transmit to CKAN (cannot be obtained through API)
    :param indexes: indexes to transmit to CKAN (cannot be obtained through API)
    :param aliases: Resource id aliases for requests (API cannot delete existing aliases)
    :param aux_upload_fun_name: Name of the function used to edit DataFrames before uploading
    :param aux_download_fun_name: Name of the function used to edit DataFrames after downloading
    :param aux_read_fun_name: Name of the function used to read file contents (defines local_file_format as a UserFileFormat)
    :param aux_write_fun_name: Name of the function used to write file contents (defines local_file_format as a UserFileFormat)
    :param local_file_format: Class used to read/write files
    :param df_mapper: DataFrame mapper function. This object adds certain indexes and applies the upload/download functions.
    It is responsible for mapping DataStore queries to file outputs.
    :param data_cleaner_upload: Data sanitizer used to automate certain tasks and replacing invalid values (default is None)
    """

    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, options_string:str=None, base_dir:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url, options_string=options_string)
        self.field_builders: Union[Dict[str, BuilderField],None] = None
        self.field_builders_user: Union[Dict[str, BuilderField],None] = None
        self.field_builders_data_source: Union[Dict[str, BuilderField],None] = None
        # self.datastore_attributes: Union[CkanDataStoreInfo,None] = None
        # self.datastore_attributes_user: CkanDataStoreInfo = CkanDataStoreInfo()
        # self.datastore_attributes_data_source: Union[CkanDataStoreInfo,None] = None
        self.primary_key: Union[List[str],None] = None
        self.indexes: Union[List[str],None] = None
        self.aliases: Union[List[str],None] = None
        self.aux_upload_fun_name:str = ""
        self.aux_download_fun_name:str = ""
        self.aux_read_fun_name:str = ""
        self.aux_write_fun_name:str = ""
        # Functions input/outputs
        self.data_cleaner_upload: Union[CkanDataCleanerABC,None] = None
        self.reupload_on_update = False  # do not reupload on update for DataStores
        self.reupload_if_needed: bool = True
        self.reupload_needed: Union[bool,None] = None
        self.df_mapper = DataSchemeConversion()
        self.local_file_format: Union[FileFormatABC,None] = None
        self.read_line_counter:int = 0
        self.upload_start_line:int = 0

    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.field_builders = copy.deepcopy(self.field_builders)
        dest.field_builders_user = copy.deepcopy(self.field_builders_user)
        dest.field_builders_data_source = copy.deepcopy(self.field_builders_data_source)
        dest.primary_key = copy.deepcopy(self.primary_key)
        dest.indexes = copy.deepcopy(self.indexes)
        dest.aliases = copy.deepcopy(self.aliases)
        dest.aux_upload_fun_name = self.aux_upload_fun_name
        dest.aux_download_fun_name = self.aux_download_fun_name
        dest.aux_read_fun_name = self.aux_read_fun_name
        dest.aux_write_fun_name = self.aux_write_fun_name
        dest.reupload_on_update = self.reupload_on_update
        dest.reupload_if_needed = self.reupload_if_needed
        dest.reupload_needed = self.reupload_needed
        dest.df_mapper = self.df_mapper.copy()
        dest.local_file_format = self.local_file_format.copy()
        return dest

    @staticmethod
    def _setup_cli_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="DataStore resource specific options", add_help=False,
                                             epilog=
                                             "Examples: \n"
                                             "- Selecting a Data Cleaner: --data-cleaner GeoJSON \n"
                                             "- Process one file per primary key combination (first columns of the primary key, except the last one): --one-frame-per-primary-key --no-chunks")
        parser.add_argument("--data-cleaner", type=str,
                            help="Data cleaner to call before uploading data")
        parser.add_argument("--one-frame-per-primary-key",
                            help="Enabling this option makes the upload process expect one DataFrame per primary key combination (except the last field of the primary key, which could be an index in the file).\n"
                            "This option should be associated with the file format option --no-chunks to ensure a file is treated at once", action="store_true", default=False)
        parser.add_argument("--group-by", type=str,
                            help="Fields of the primary key defining the request to reconstruct a file, in --one-frame-per-primary-key mode, separated by a comma (no spaces). \n"
                                 "By default, the first columns of the primary, except the last one is used. \n"
                                 "At least one field of the primary key must be unused here. \n")
        return parser

    def _setup_cli_parser_external(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        # return FileFormatABC._setup_cli_parser(parser)
        return parser

    def print_help_cli(self, display:bool=True) -> str:
        parser = self._setup_cli_parser()
        self._setup_cli_parser_external(parser)
        if display:
            parser.print_help()
        buffer = io.StringIO()
        parser.print_help(buffer)
        return buffer.getvalue()

    def _cli_args_apply(self, args: argparse.Namespace, *, base_dir: str = None, error_not_found: bool = True) -> None:
        if args.data_cleaner is not None:
            self.data_cleaner_upload = init_data_cleaner(args.data_cleaner)
        if args.one_frame_per_primary_key is not None and args.one_frame_per_primary_key:
            self.apply_one_frame_per_primary_key(args.group_by)
        elif args.group_by is not None:
            msg = GroupByError("Argument --group-by cannot be used without option --one-frame-per-primary-key")
            warn(msg)

    def apply_one_frame_per_primary_key(self, group_by_argument:Union[str, List[str]]=None):
        """
        Enables mode --one-frame-per-primary-key
        and applies option --group-by

        In this mode, the upload process expect one DataFrame per primary key combination
        (except the last field of the primary key, which could be an index in the file).
        Upload update checks are performed using this assumption (do not read files by chunks).
        Downloads fill files according to unique combinations of the first columns of the primary key.
        """
        assert (self.primary_key is not None and len(self.primary_key) > 1)
        if group_by_argument is None:
            # default mode
            group_by_keys = self.primary_key[:-1]
            # sort_by_keys = self.primary_key[-1:]
            sort_by_keys = self.primary_key
        else:
            # custom --group-by argument
            if isinstance(group_by_argument, str):
                group_by_keys = group_by_argument.split(ckan_tags_sep)
            else:
                group_by_keys = group_by_argument
            group_by_keys =  [key.strip() for key in group_by_keys]
            extra_keys_primary = set(group_by_keys) - set(self.primary_key)
            if len(extra_keys_primary) > 0:
                raise GroupByError("--group-by argument must contain only columns of the primary key")
            elif len(group_by_keys) == len(self.primary_key):
                raise GroupByError("--group-by argument must not contain all columns of the primary key")
            # sort_by_keys = list(set(self.primary_key) - set(group_by_keys))
            sort_by_keys = self.primary_key
        self.df_mapper = RequestFileMapperIndexKeys(group_by_keys=group_by_keys, sort_by_keys=sort_by_keys)
        if self.local_file_format.allow_chunks:
            msg = "Mode --one-frame-per-primary-key is not compatible with reading files by chunks"
            warn(msg)

    def initialize_extra_options_string(self, extra_options_string:str, base_dir:str) -> None:
        self.local_file_format = init_file_format_datastore(self.resource_attributes_user.format, extra_options_string, self.aux_read_fun_name, self.aux_write_fun_name)  # default file format is CSV (user can change)

    def _merge_resource_attributes_from_file(self) -> None:
        """
        This function merges metadata which could have been extracted from a file reading function into the attributes from data source.
        Call after self.local_file_format.read_file()
        """
        resource_attributes_from_file = self.local_file_format.resource_attributes_from_file
        if resource_attributes_from_file is not None:
            if self.resource_attributes_data_source is None:
                self.resource_attributes_data_source = resource_attributes_from_file
            else:
                self.resource_attributes_data_source.update_missing(resource_attributes_from_file)
                if resource_attributes_from_file.datastore_info is not None and resource_attributes_from_file.datastore_info.fields_dict is not None:
                    if self.resource_attributes_data_source.datastore_info is None:
                        self.resource_attributes_data_source.datastore_info = resource_attributes_from_file.datastore_info
                    elif self.resource_attributes_data_source.datastore_info.fields_dict is None:
                        self.resource_attributes_data_source.datastore_info.fields_dict = resource_attributes_from_file.datastore_info.fields_dict
                    for field_name, field_info in resource_attributes_from_file.datastore_info.fields_dict.items():
                        field_builder = BuilderField._from_ckan_field(field_info)
                        if field_name in self.field_builders_data_source.keys():
                            self.field_builders_data_source[field_name].update_missing(field_builder)
                        else:
                            self.field_builders_data_source[field_name] = field_builder

    def initialize_from_options_string(self, base_dir:str, *, options_string:str=None, parser:argparse.ArgumentParser=None) -> None:
        if options_string is None:
            options_string = self.options_string
        if options_string is None:
            self.initialize_extra_options_string(None, base_dir=base_dir)
            return
        parser = self._setup_cli_parser(parser)
        args, extra_args = parser.parse_known_args(shlex.split(options_string))
        self.initialize_extra_options_string(shlex.join(extra_args), base_dir=base_dir)
        self._cli_args_apply(args)

    def init_options_from_ckan(self, ckan:CkanApi, *, base_dir:str=None) -> None:
        if self.known_resource_info is None:
            self.known_resource_info = ckan.get_resource_info_or_request(self.name, self.package_name,
                                                                         error_not_found=False, datastore_info=True)
        super().init_options_from_ckan(ckan, base_dir=base_dir)
        # self.local_file_format.chunk_size = ckan.params.default_limit_read
        if self.field_builders is not None:
            for field_builder in self.field_builders.values():
                field_builder.internal_attrs.update_from_ckan(ckan)

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row, base_dir=base_dir)
        primary_keys_string: Union[str,None] = _string_from_element(row["primary key"])
        indexes_string: Union[str,None] = _string_from_element(row["indexes"])
        aliases_string: Union[str,None] = None
        if "data cleaner" in row.keys():
            data_cleaner_string = _string_from_element(row["data cleaner"], empty_value="")
            if data_cleaner_string is not None:
                self.data_cleaner_upload = init_data_cleaner(data_cleaner_string)
        if "upload function" in row.keys():
            self.aux_upload_fun_name: str = _string_from_element(row["upload function"], empty_value="")
        if "download function" in row.keys():
            self.aux_download_fun_name: str = _string_from_element(row["download function"], empty_value="")
        if "read function" in row.keys():
            self.aux_read_fun_name: str = _string_from_element(row["read function"], empty_value="")
        if "write function" in row.keys():
            self.aux_write_fun_name: str = _string_from_element(row["write function"], empty_value="")
        if "aliases" in row.keys():
            aliases_string = _string_from_element(row["aliases"])
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
        if aliases_string is not None:
            self.aliases = aliases_string.split(ckan_tags_sep)
        self.initialize_from_options_string(base_dir=base_dir)

    @abstractmethod
    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["Primary key"] = ckan_tags_sep.join(self.primary_key) if self.primary_key else ""
        d["Indexes"] = ckan_tags_sep.join(self.indexes) if self.indexes is not None else ""
        d["Data cleaner"] = self.data_cleaner_upload.get_class_keyword() if self.data_cleaner_upload is not None else ""
        d["Upload function"] = self.aux_upload_fun_name
        d["Download function"] = self.aux_download_fun_name
        d["Read function"] = self.aux_read_fun_name
        d["Write function"] = self.aux_write_fun_name
        d["Aliases"] = ckan_tags_sep.join(self.aliases) if self.aliases is not None else ""
        return d

    def _check_field_duplicates(self):
        if self.field_builders is not None:
            duplicates = find_duplicates([field_builder.name for field_builder in self.field_builders.values()])
            if len(duplicates) > 0:
                raise DuplicateNameError("Field", duplicates)

    def _get_fields_dict(self) -> Dict[str, dict]:
        self._check_field_duplicates()
        # self._get_fields_update(ckan=None, current_df_fields=None)
        if self.field_builders_user is not None:
            fields_dict = OrderedDict([(field_builder.name, field_builder._to_dict()) for field_builder in self.field_builders_user.values()])
        else:
            fields_dict = None
        return fields_dict

    def _get_fields_info(self) -> Dict[str, CkanField]:
        self._check_field_duplicates()
        if self.field_builders_user is not None:
            builder_fields = OrderedDict([(field_builder.name, field_builder._to_ckan_field()) for field_builder in self.field_builders_user.values()])
        else:
            builder_fields = {}
        return builder_fields

    def _get_fields_df(self) -> Union[pd.DataFrame,None]:
        if self.field_builders_user is not None:
            fields_dict_list = [value for value in self._get_fields_dict().values()]
            fields_df = pd.DataFrame.from_records(fields_dict_list)
            return fields_df
        else:
            return None

    def _load_fields_df(self, fields_df: pd.DataFrame):
        fields_df.columns = fields_df.columns.map(str.lower)
        fields_df.columns = fields_df.columns.map(str.strip)
        self.field_builders_user = OrderedDict()
        for row_loc, row in fields_df.iterrows():
            field_builder = BuilderField()
            field_builder._load_from_df_row(row=row)
            self.field_builders_user[field_builder.name] = field_builder

    def _to_ckan_resource_info(self, package_id:str, check_id:bool=True) -> CkanResourceInfo:
        resource_info = super()._to_ckan_resource_info(package_id=package_id, check_id=check_id)
        resource_info.datastore_info = CkanDataStoreInfo()
        resource_info.datastore_info.resource_id = resource_info.id
        if self.field_builders is not None:
            resource_info.datastore_info.fields_dict = OrderedDict()
            for name, field_builder in self.field_builders.items():
                resource_info.datastore_info.fields_dict[name] = field_builder._to_ckan_field()
        else:
            resource_info.datastore_info.fields_dict = None
        resource_info.datastore_info.fields_id_list = [name for name, field_builder in self.field_builders.items()] if self.field_builders is not None else []
        if self.indexes is not None:
            resource_info.datastore_info.index_fields = self.indexes.copy()
        aliases = self._get_alias_list(None)
        if aliases is not None:
            resource_info.datastore_info.aliases = aliases.copy()
        return resource_info

    @abstractmethod
    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True) -> GeneralDataFrame:
        """
        Function returning the data from the indicated resources as a pandas DataFrame.
        This is the DataFrame equivalent for load_sample_data.

        :param resources_base_dir: base directory to find the resources on the local machine
        :return:
        """
        raise NotImplementedError()

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir: str, ckan:Union[CkanApi,None]=None) -> None:
        return None

    def load_sample_data(self, resources_base_dir:str) -> bytes:
        df = self.load_sample_df(resources_base_dir=resources_base_dir)
        return self.local_file_format.write_in_memory(df, fields=self._get_fields_info())

    def upsert_request_df(self, ckan: CkanApi, df_upload:pd.DataFrame, *,
                          total_lines_read:int, file_name:str,
                          method:UpsertChoice=UpsertChoice.Upsert,
                          apply_last_condition:bool=None, always_last_condition:bool=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Call to ckan datastore_upset.
        Before sending the DataFrame, a call to df_upload_alter is made.
        This method is overloaded in BuilderDataStoreMultiABC and BuilderDataStoreFolder

        :param ckan:
        :param df_upload:
        :param method:
        :return:
        """
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=True)
        df_upload_transformed = self.df_mapper.df_upload_alter(df_upload, fields=self._get_fields_info(),
                                                               total_lines_read=total_lines_read, file_name=file_name)
        ret_df = ckan.datastore_upsert(df_upload_transformed, resource_id, method=method,
                                       apply_last_condition=apply_last_condition,
                                       always_last_condition=always_last_condition,
                                       data_cleaner=self.data_cleaner_upload,
                                       progress_callback=self.progress_callback)
        return df_upload_transformed, ret_df

    def upsert_request_final(self, ckan: CkanApi, *, force:bool=False) -> None:
        """
        Final steps after the last upsert query.
        These steps are automatically done for a DataStore defined by one file.

        :param ckan:
        :param force: perform request anyways
        :return:
        """
        if force:
            resource_id = self.get_or_query_resource_id(ckan, error_not_found=True)
            ckan.datastore_upsert_last_line(resource_id=resource_id)

    def _get_alias_list(self, ckan:Union[CkanApi,None]):
        aliases = self.aliases
        if default_alias_keyword is not None:
            if ckan is not None:
                default_alias_name = ckan.datastore_default_alias(self.name, self.package_name, error_not_found=False)
            else:
                default_alias_name = CkanApi.datastore_default_alias_of_names(self.name, self.package_name)
            if aliases is not None:
                for i, alias in enumerate(aliases):
                    if alias.lower().strip() == default_alias_keyword:
                        aliases[i] = default_alias_name
        return aliases

    def _check_necessary_fields(self, current_fields: Set[str] = None, empty_datastore:bool=False, raise_error: bool = True) -> Set[str]:
        """
        Auxiliary function to list the fields which are required:
        - for df_mapper to determine the file names, associated requests, and recognize the last inserted row of a document.
        - to initialize the DataStore with the columns for the primary key and indexes

        The required fields are compared to current_fields, if provided.
        """
        if empty_datastore:
            return set()
        required_fields = self.df_mapper.get_necessary_fields()
        if self.primary_key is not None:
            required_fields = required_fields.union(set(self.primary_key))
        if self.indexes is not None:
            required_fields = required_fields.union(set(self.indexes))
        if current_fields is not None:
            missing_fields = required_fields - current_fields
            if len(missing_fields) > 0:
                msg = RequiredDataFrameFieldsError(missing_fields)
                if raise_error:
                    raise msg
                else:
                    warn(str(msg))
        return required_fields

    def _check_undocumented_fields(self, current_fields: Set[str]) -> None:
        if self.field_builders is not None:
            # list fields which are not documented
            fields_doc = set(self.field_builders.keys())
            missing_doc = current_fields - fields_doc
            extra_doc = fields_doc - current_fields
            if len(extra_doc) > 0:
                msg = f"{len(extra_doc)} extra fields were documented but absent of sample data for table {self.name}: {', '.join(extra_doc)}"
                warn(msg)
            if len(missing_doc) > 0:
                msg = f"{len(missing_doc)} fields are left documented for table {self.name}: {', '.join(missing_doc)}"
                warn(msg)
        else:
            msg = f"No field documentation was provided for table {self.name}. {len(current_fields)} fields are left documented: {', '.join(current_fields)}"
            warn(msg)

    def _get_fields_update(self, ckan: CkanApi, *, current_df_fields:Union[Set[str],None], data_cleaner_fields:Union[List[dict],None],
                           reupload:bool, override_ckan:bool) -> OrderedDict[str, CkanField]:
        """
        Merge field builders in the following order of priority:
        1. Existing metadata from CKAN (can be ignored with option override_ckan)
        2. Metadata specified by the user in the Excel worksheet
        3. Metadata found automatically from the data source (e.g. in file header or database)
        4. Metadata found automatically by the data cleaner, especially for field typing
        """
        self.field_builders = OrderedDict()
        # 1. Information specified by user in Excel, for preserving field specified order
        if self.field_builders_user is not None:
            for field_name, field_builder in self.field_builders_user.items():
                self.field_builders[field_name] = field_builder.copy()
        # 2. Current CKAN information: update missing information
        if (not override_ckan) and self.known_resource_info is not None and self.known_resource_info.datastore_info is not None and self.known_resource_info.datastore_info.fields_dict is not None:
            known_fields = self.known_resource_info.datastore_info.fields_dict
            for field_name, field_info in known_fields.items():
                if field_name in self.field_builders.keys():
                    self.field_builders[field_name].update_missing(BuilderField._from_ckan_field(field_info))
                else:
                    self.field_builders[field_name] = BuilderField._from_ckan_field(field_info)
        # 3. Metadata generated from data source: update missing information
        if self.field_builders_data_source is not None:
            for field_name, field_builder in self.field_builders_data_source.items():
                if field_name in self.field_builders.keys():
                    self.field_builders[field_name].update_missing(field_builder)
                else:
                    self.field_builders[field_name] = field_builder.copy()
        # 4. Metadata generated from Data cleaner
        if data_cleaner_fields is not None:
            for field_dict in data_cleaner_fields:
                field_builder = BuilderField._from_ckan_field(CkanField.from_ckan_dict(field_dict))
                if field_builder.name in self.field_builders.keys():
                    self.field_builders[field_builder.name].update_missing(field_builder)
                else:
                    self.field_builders[field_builder.name] = field_builder.copy()
        if self.field_builders is not None:
            if current_df_fields is not None:
                builder_fields = [field_builder._to_ckan_field() for field_builder in self.field_builders.values() if field_builder.name in current_df_fields]
            else:
                # use case: get all known fields (before data_cleaner)
                builder_fields = [field_builder._to_ckan_field() for field_builder in self.field_builders.values()]
        else:
            builder_fields = None
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=False)
        if resource_id is not None and not reupload:
                # This call merges the field builder with existing information
            update_needed, fields_update = ckan.datastore_field_patch_dict(fields_merge=data_cleaner_fields, fields_update=builder_fields,
                                                                           return_list=False, datastore_merge=not override_ckan,
                                                                           resource_id=resource_id, error_not_found=False)
        else:
            fields_update = CkanApi.datastore_field_dict(fields_merge=data_cleaner_fields, fields_update=builder_fields, return_list=False)
        return fields_update

    def _collect_indexes_from_fields(self) -> Set[str]:
        if self.field_builders is not None:
            return {field_builder.name for field_builder in self.field_builders.values() if field_builder.is_index}
        else:
            return set()

    def _get_primary_key_indexes(self, data_cleaner_index: Set[str], current_df_fields:Set[str], error_missing:bool, empty_datastore:bool=False) -> Tuple[Union[List[str],None], Union[List[str],None]]:
        # update primary keys and indexes: only if present
        if empty_datastore:
            return None, None
        primary_key = None
        if current_df_fields is None:
            primary_key = self.primary_key
        elif self.primary_key is not None:
            extra_primary_key = set(self.primary_key) - current_df_fields
            if len(extra_primary_key) == 0:
                primary_key = self.primary_key
            elif error_missing:
                raise RequiredDataFrameFieldsError(extra_primary_key)
        indexes = None
        if self.indexes is not None:
            indexes_full_set = set(self.indexes).union(self._collect_indexes_from_fields()).union(data_cleaner_index)
        else:
            indexes_full_set = self._collect_indexes_from_fields().union(data_cleaner_index)
        if primary_key is not None:
            indexes_full_set = indexes_full_set - set(primary_key)
        if len(indexes_full_set) == 0:
            indexes_full = None
        else:
            indexes_full = list(indexes_full_set)
        if current_df_fields is None:
            indexes = indexes_full
        elif indexes_full is not None:
            extra_indexes = set(indexes_full) - current_df_fields
            if len(extra_indexes) == 0:
                indexes = indexes_full
            elif error_missing:
                raise RequiredDataFrameFieldsError(extra_indexes)
        return primary_key, indexes

    def _compare_fields_to_datastore_info(self, resource_info:CkanResourceInfo, current_fields: Set[str], ckan:CkanApi) -> None:
        # compare fields with DataStore info (if present, for information)
        if resource_info.datastore_info is not None:
            fields_info = set(resource_info.datastore_info.fields_id_list)
            missing_info = current_fields - fields_info
            extra_info = fields_info - current_fields
            if len(extra_info) > 0:
                msg = f"{len(extra_info)} extra fields are in the database but absent of sample data for table {self.name}: {', '.join(extra_info)}"
                warn(msg)
            if len(missing_info) > 0 and ckan.params.verbose_request:
                msg = f"{len(missing_info)} fields are not in DataStore info because they are being added for table {self.name}: {', '.join(missing_info)}"
                print(msg)

    def _apply_data_cleaner_before_patch(self, ckan:CkanApi, df_upload: pd.DataFrame, *, reupload:bool, override_ckan:bool) -> Tuple[pd.DataFrame, List[dict], Set[str]]:
        if df_upload is not None and self.data_cleaner_upload is not None:
            fields_for_cleaner = self._get_fields_update(ckan, current_df_fields=None, data_cleaner_fields=None, reupload=reupload, override_ckan=override_ckan)
            df_upload = self.data_cleaner_upload.clean_records(df_upload, known_fields=fields_for_cleaner, inplace=True)
            data_cleaner_fields = self.data_cleaner_upload.merge_field_changes()
            data_cleaner_index = self.data_cleaner_upload.field_suggested_index
        else:
            data_cleaner_fields = None
            data_cleaner_index = set()
        return df_upload, data_cleaner_fields, data_cleaner_index

    def patch_request(self, ckan: CkanApi, package_id: str, *,
                      df_upload: pd.DataFrame=None, reupload: bool = None, override_ckan:bool=False,
                      resources_base_dir:str=None) -> CkanResourceInfo:
        self._merge_resource_attributes(override_ckan=override_ckan)
        if reupload is None: reupload = self.reupload_on_update
        if df_upload is None:
            df_upload = self.load_sample_df(resources_base_dir=resources_base_dir, upload_alter=True)
        else:
            pass  # do not alter df_upload because it should already be in the database format
        df_upload, data_cleaner_fields, data_cleaner_index = self._apply_data_cleaner_before_patch(ckan, df_upload, reupload=reupload, override_ckan=override_ckan)
        current_df_fields = set(df_upload.columns) - {datastore_id_col}  # _id field cannot be documented
        if num_rows_patch_first_upload_partial is not None:
            num_rows_patch_first_upload_partial_apply = min(num_rows_patch_first_upload_partial, ckan.params.default_limit_write)
        else:
            num_rows_patch_first_upload_partial_apply = None
        if num_rows_patch_first_upload_partial_apply is not None and len(df_upload) > num_rows_patch_first_upload_partial_apply:
            df_upload_partial = df_upload.iloc[:num_rows_patch_first_upload_partial_apply]
            df_upload_upsert = df_upload.iloc[num_rows_patch_first_upload_partial_apply:]
        else:
            df_upload_partial, df_upload_upsert = df_upload, None
        empty_datastore = df_upload is None or len(df_upload) == 0
        self._check_necessary_fields(current_df_fields, empty_datastore=empty_datastore, raise_error=True)
        self._check_undocumented_fields(current_df_fields)
        aliases = self._get_alias_list(ckan)
        primary_key, indexes = self._get_primary_key_indexes(data_cleaner_index, current_df_fields=current_df_fields,
                                                             error_missing=True, empty_datastore=empty_datastore)
        fields_update = self._get_fields_update(ckan, current_df_fields=current_df_fields,
                                                data_cleaner_fields=data_cleaner_fields, reupload=reupload, override_ckan=override_ckan)
        fields = list(fields_update.values()) if len(fields_update) > 0 else None
        resource_info = ckan.resource_create(package_id, name=self.name, format=self.resource_attributes.format,
                                             description=self.resource_attributes.description,
                                             state=initial_resource_building_state if initial_resource_building_state is not None else self.resource_attributes.state,
                                             create_default_view=self.create_default_view,
                                             cancel_if_exists=True, update_if_exists=True, reupload=reupload,
                                             datastore_create=True, records=df_upload_partial, fields=fields,
                                             primary_key=primary_key, indexes=indexes, aliases=aliases)
        resource_id = resource_info.id
        self.known_id = resource_id
        reupload = reupload or resource_info.newly_created
        self._compare_fields_to_datastore_info(resource_info, current_df_fields, ckan)
        if df_upload_upsert is not None and reupload:
            if reupload:
                ckan.datastore_upsert(df_upload_upsert, resource_id, method=UpsertChoice.Insert,
                                      always_last_condition=None, data_cleaner=self.data_cleaner_upload,
                                      progress_callback=self.progress_callback)
            else:
                # case where a reupload was needed but is not permitted by self.reupload_if_needed
                msg = f"Did not upload the remaining part of the resource {self.name}."
                raise IncompletePatchError(msg)
        return resource_info

    def download_sample_df(self, ckan: CkanApi, search_all:bool=True, download_alter:bool=True, **kwargs) -> Union[pd.DataFrame,None]:
        """
        Download the resource and return it as a DataFrame.
        This is the DataFrame equivalent for download_sample.

        :param ckan:
        :param search_all:
        :param download_alter:
        :param kwargs:
        :return:
        """
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        df_download = ckan.datastore_dump(resource_id, search_all=search_all, **kwargs)
        if download_alter:
            df_local = self.df_mapper.df_download_alter(df_download, fields=self._get_fields_info())
            return df_local
        else:
            return df_download

    def download_sample(self, ckan:CkanApi, full_download:bool=True, **kwargs) -> bytes:
        df = self.download_sample_df(ckan=ckan, search_all=full_download, **kwargs)
        return self.local_file_format.write_in_memory(df, fields=self._get_fields_info())


class BuilderResourceIgnored(BuilderDataStoreABC):
    """
    Class to maintain a line in the resource builders list but has no action and can hold field metadata.
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, file_url:str=None, options_string:str=None, base_dir:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, options_string=options_string, base_dir=base_dir)
        self.file_url: Union[str, None] = file_url

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderResourceIgnored()
        super().copy(dest=dest)
        dest.file_url = self.file_url
        return dest

    @staticmethod
    def resource_mode_str() -> str:
        return "Ignored"

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row, base_dir=base_dir)
        self.file_url: str = _string_from_element(row["file/url"], strip=True)
        self._check_mandatory_attributes()

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.file_url
        return d

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir:str, ckan:Union[CkanApi,None]=None) -> Union[str,None]:
        return None

    def load_sample_data(self, resources_base_dir:str) -> Union[bytes,None]:
        return None

    def load_sample_df(self, resources_base_dir: str, *, upload_alter: bool = True) -> None:
        return None

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[ContextErrorLevelMessage,None]:
        return None

    def patch_request(self, ckan:CkanApi, package_id:str, *,
                      reupload:bool=None, override_ckan:bool=False, resources_base_dir:str=None,
                      payload:Union[bytes, io.BufferedIOBase]=None) -> None:
        return None

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download: bool = True, force: bool = False,
                         threads: int = 1, return_data:bool=False) -> Any:
        return None

    def download_sample(self, ckan: CkanApi, full_download: bool = True, **kwargs) -> bytes:
        return bytes()

