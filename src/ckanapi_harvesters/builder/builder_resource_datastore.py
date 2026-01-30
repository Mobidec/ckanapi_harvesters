#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to initiate a DataStore.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Set, Any
import os
import io
from warnings import warn
from collections import OrderedDict
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC
from ckanapi_harvesters.harvesters.file_formats.file_format_init import init_file_format_datastore
from ckanapi_harvesters.builder.mapper_datastore import DataSchemeConversion
from ckanapi_harvesters.builder.builder_resource import BuilderResourceABC
from ckanapi_harvesters.auxiliary.ckan_errors import DuplicateNameError
from ckanapi_harvesters.auxiliary.path import resolve_rel_path
from ckanapi_harvesters.builder.builder_errors import RequiredDataFrameFieldsError, ResourceFileNotExistMessage, IncompletePatchError
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanDataStoreInfo
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, find_duplicates, datastore_id_col
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC

# number of rows to upload to initiate DataStore with datapusher, before explicitly specifying field data types and indexes
num_rows_patch_first_upload_partial: Union[int,None] = 50  # set to None to upload directly the whole DataFrame before the DataStore creation


default_alias_keyword:Union[str,None] = "default"  # generate default alias if an alias with this value is found in parameters


class BuilderDataStoreABC(BuilderResourceABC, ABC):
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.field_builders: Union[Dict[str, BuilderField],None] = None
        self.primary_key: Union[List[str],None] = None
        self.indexes: Union[List[str],None] = None
        self.aliases: Union[List[str],None] = None
        self.aux_upload_fun_name:str = ""
        self.aux_download_fun_name:str = ""
        # Functions input/outputs
        self.data_cleaner_upload: Union[CkanDataCleanerABC,None] = None
        self.reupload_on_update = False  # do not reupload on update for DataStores
        self.reupload_if_needed: bool = True
        self.reupload_needed: Union[bool,None] = None
        self.df_mapper = DataSchemeConversion()
        self.local_file_format: FileFormatABC = init_file_format_datastore(self.format)

    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.field_builders = copy.deepcopy(self.field_builders)
        dest.primary_key = copy.deepcopy(self.primary_key)
        dest.indexes = copy.deepcopy(self.indexes)
        dest.aliases = copy.deepcopy(self.aliases)
        dest.aux_upload_fun_name = self.aux_upload_fun_name
        dest.aux_download_fun_name = self.aux_download_fun_name
        dest.reupload_on_update = self.reupload_on_update
        dest.reupload_if_needed = self.reupload_if_needed
        dest.reupload_needed = self.reupload_needed
        dest.df_mapper = self.df_mapper.copy()
        dest.local_file_format = self.local_file_format.copy()
        return dest

    def _init_file_format(self):
        self.local_file_format = init_file_format_datastore(self.format)  # default file format is CSV (user can change)

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        primary_keys_string: Union[str,None] = _string_from_element(row["primary key"])
        indexes_string: Union[str,None] = _string_from_element(row["indexes"])
        aliases_string: Union[str,None] = None
        if "upload function" in row.keys():
            self.aux_upload_fun_name: str = _string_from_element(row["upload function"], empty_value="")
        if "download function" in row.keys():
            self.aux_download_fun_name: str = _string_from_element(row["download function"], empty_value="")
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
        self._init_file_format()

    @abstractmethod
    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["Primary key"] = ckan_tags_sep.join(self.primary_key) if self.primary_key else ""
        d["Indexes"] = ckan_tags_sep.join(self.indexes) if self.indexes is not None else ""
        d["Upload function"] = self.aux_upload_fun_name
        d["Download function"] = self.aux_download_fun_name
        d["Aliases"] = ckan_tags_sep.join(self.aliases) if self.aliases is not None else ""
        return d

    def init_options_from_ckan(self, ckan:CkanApi) -> None:
        super().init_options_from_ckan(ckan)
        if self.field_builders is not None:
            for field_builder in self.field_builders.values():
                field_builder.internal_attrs.update_from_ckan(ckan)

    def _check_field_duplicates(self):
        if self.field_builders is not None:
            duplicates = find_duplicates([field_builder.name for field_builder in self.field_builders.values()])
            if len(duplicates) > 0:
                raise DuplicateNameError("Field", duplicates)

    def _get_fields_dict(self) -> Dict[str, dict]:
        self._check_field_duplicates()
        if self.field_builders is not None:
            fields_dict = OrderedDict([(field_builder.name, field_builder._to_dict()) for field_builder in self.field_builders.values()])
        else:
            fields_dict = None
        return fields_dict

    def _get_fields_info(self) -> Dict[str, CkanField]:
        self._check_field_duplicates()
        if self.field_builders is not None:
            builder_fields = OrderedDict([(field_builder.name, field_builder._to_ckan_field()) for field_builder in self.field_builders.values()])
        else:
            builder_fields = {}
        return builder_fields

    def _get_fields_df(self) -> pd.DataFrame:
        fields_dict_list = [value for value in self._get_fields_dict().values()]
        fields_df = pd.DataFrame.from_records(fields_dict_list)
        return fields_df

    def _load_fields_df(self, fields_df: pd.DataFrame):
        fields_df.columns = fields_df.columns.map(str.lower)
        fields_df.columns = fields_df.columns.map(str.strip)
        self.field_builders = {}
        for index, row in fields_df.iterrows():
            field_builder = BuilderField()
            field_builder._load_from_df_row(row=row)
            self.field_builders[field_builder.name] = field_builder

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
    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True) -> pd.DataFrame:
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

    def get_sample_file_path(self, resources_base_dir: str) -> None:
        return None

    def load_sample_data(self, resources_base_dir:str) -> bytes:
        df = self.load_sample_df(resources_base_dir=resources_base_dir)
        return self.local_file_format.write_in_memory(df, fields=self._get_fields_info())

    def upsert_request_df(self, ckan: CkanApi, df_upload:pd.DataFrame,
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
        df_upload_transformed = self.df_mapper.df_upload_alter(df_upload, fields=self._get_fields_info())
        ret_df = ckan.datastore_upsert(df_upload_transformed, resource_id, method=method,
                                       apply_last_condition=apply_last_condition,
                                       always_last_condition=always_last_condition, data_cleaner=self.data_cleaner_upload)
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

    def _get_fields_update(self, ckan: CkanApi, current_fields:Union[Set[str],None], data_cleaner_fields:Union[List[dict],None],
                           reupload:bool) -> Dict[str, dict]:
        if self.field_builders is not None:
            if current_fields is not None:
                builder_fields = [field_builder._to_ckan_field() for field_builder in self.field_builders.values() if field_builder.name in current_fields]
            else:
                # use case: get all known fields (before data_cleaner)
                builder_fields = [field_builder._to_ckan_field() for field_builder in self.field_builders.values()]
        else:
            builder_fields = None
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=False)
        if resource_id is not None and not reupload:
            update_needed, fields_update = ckan.datastore_field_patch_dict(fields_merge=data_cleaner_fields, fields_update=builder_fields,
                                                                           return_list=False,
                                                                           resource_id=resource_id, error_not_found=False)
        else:
            fields_update = CkanApi.datastore_field_dict(fields_merge=data_cleaner_fields, fields_update=builder_fields, return_list=False)
        return fields_update

    def _collect_indexes_from_fields(self) -> Set[str]:
        if self.field_builders is not None:
            return {field_builder.name for field_builder in self.field_builders.values() if field_builder.is_index}
        else:
            return set()

    def _get_primary_key_indexes(self, data_cleaner_index: Set[str], current_fields:Set[str], error_missing:bool, empty_datastore:bool=False) -> Tuple[Union[List[str],None], Union[List[str],None]]:
        # update primary keys and indexes: only if present
        if empty_datastore:
            return None, None
        primary_key = None
        if current_fields is None:
            primary_key = self.primary_key
        elif self.primary_key is not None:
            extra_primary_key = set(self.primary_key) - current_fields
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
        if current_fields is None:
            indexes = indexes_full
        elif indexes_full is not None:
            extra_indexes = set(indexes_full) - current_fields
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

    def _apply_data_cleaner_before_patch(self, ckan:CkanApi, df_upload: pd.DataFrame, reupload:bool) -> Tuple[pd.DataFrame, List[dict], Set[str]]:
        if df_upload is not None and self.data_cleaner_upload is not None:
            fields_for_cleaner = self._get_fields_update(ckan, current_fields=None, data_cleaner_fields=None, reupload=reupload)
            df_upload = self.data_cleaner_upload.clean_records(df_upload, known_fields=fields_for_cleaner, inplace=True)
            data_cleaner_fields = self.data_cleaner_upload.merge_field_changes()
            data_cleaner_index = self.data_cleaner_upload.field_suggested_index
        else:
            data_cleaner_fields = None
            data_cleaner_index = set()
        return df_upload, data_cleaner_fields, data_cleaner_index

    def patch_request(self, ckan: CkanApi, package_id: str, *,
                      df_upload: pd.DataFrame=None, reupload: bool = None, resources_base_dir:str=None) -> CkanResourceInfo:
        if reupload is None: reupload = self.reupload_on_update
        if df_upload is None:
            df_upload = self.load_sample_df(resources_base_dir=resources_base_dir, upload_alter=True)
        else:
            pass  # do not alter df_upload because it should already be in the database format
        df_upload, data_cleaner_fields, data_cleaner_index = self._apply_data_cleaner_before_patch(ckan, df_upload, reupload=reupload)
        current_fields = set(df_upload.columns) - {datastore_id_col}  # _id field cannot be documented
        if num_rows_patch_first_upload_partial is not None and len(df_upload) > num_rows_patch_first_upload_partial:
            df_upload_partial = df_upload.iloc[:num_rows_patch_first_upload_partial]
            df_upload_upsert = df_upload.iloc[num_rows_patch_first_upload_partial:]
        else:
            df_upload_partial, df_upload_upsert = df_upload, None
        empty_datastore = df_upload is None or len(df_upload) == 0
        self._check_necessary_fields(current_fields, empty_datastore=empty_datastore, raise_error=True)
        self._check_undocumented_fields(current_fields)
        aliases = self._get_alias_list(ckan)
        primary_key, indexes = self._get_primary_key_indexes(data_cleaner_index, current_fields=current_fields,
                                                             error_missing=True, empty_datastore=empty_datastore)
        fields_update = self._get_fields_update(ckan, current_fields, data_cleaner_fields, reupload=reupload)
        fields = list(fields_update.values()) if len(fields_update) > 0 else None
        resource_info = ckan.resource_create(package_id, name=self.name, format=self.format, description=self.description, state=self.state,
                                             create_default_view=self.create_default_view,
                                             cancel_if_exists=True, update_if_exists=True, reupload=reupload,
                                             datastore_create=True, records=df_upload_partial, fields=fields,
                                             primary_key=primary_key, indexes=indexes, aliases=aliases)
        resource_id = resource_info.id
        self.known_id = resource_id
        reupload = reupload or resource_info.newly_created
        self._compare_fields_to_datastore_info(resource_info, current_fields, ckan)
        if df_upload_upsert is not None and reupload:
            if reupload:
                ckan.datastore_upsert(df_upload_upsert, resource_id, method=UpsertChoice.Insert,
                                      always_last_condition=None, data_cleaner=self.data_cleaner_upload, )
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


class BuilderDataStoreFile(BuilderDataStoreABC):
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, file_name:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.file_name = file_name

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderDataStoreFile()
        super().copy(dest=dest)
        dest.file_name = self.file_name
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.file_name: str = _string_from_element(row["file/url"])

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        if os.path.isfile(file_path):
            return None
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error, f"Missing file for resource {self.name}: {file_path}")

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir:str) -> str:
        return resolve_rel_path(resources_base_dir, self.file_name, field=f"File/URL of resource {self.name}")

    def load_sample_df(self, resources_base_dir:str, *, upload_alter:bool=True) -> pd.DataFrame:
        self.sample_data_source = self.get_sample_file_path(resources_base_dir)
        df_local = self.local_file_format.read_file(self.sample_data_source, fields=self._get_fields_info())
        if isinstance(df_local, pd.DataFrame):
            df_local.attrs["source"] = self.sample_data_source
        if upload_alter:
            df_upload = self.df_mapper.df_upload_alter(df_local, self.sample_data_source, fields=self._get_fields_info())
            return df_upload
        else:
            return df_local

    @staticmethod
    def resource_mode_str() -> str:
        return "DataStore from File"

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.file_name
        return d

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download:bool=True,
                         force:bool=False, threads:int=1) -> Union[pd.DataFrame,None]:
        if (not self.enable_download) and (not force):
            msg = f"Did not download resource {self.name} because download was disabled."
            warn(msg)
            return None
        if out_dir is not None:
            self.downloaded_destination = resolve_rel_path(out_dir, self.file_name, field=f"File/URL of resource {self.name}")
            if self.download_skip_existing and os.path.exists(self.downloaded_destination):
                return None
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        df_download = ckan.datastore_dump(resource_id, search_all=full_download)
        df = self.df_mapper.df_download_alter(df_download, fields=self._get_fields_info())
        if out_dir is not None:
            os.makedirs(out_dir, exist_ok=True)
            self.local_file_format.write_file(df, self.downloaded_destination, fields=self._get_fields_info())
        return df


class BuilderResourceIgnored(BuilderDataStoreABC):
    """
    Class to maintain a line in the resource builders list but has no action and can hold field metadata.
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, file_url:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
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
        super()._load_from_df_row(row=row)
        self.file_url: str = _string_from_element(row["file/url"])
        self._check_mandatory_attributes()

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.file_url
        return d

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir:str) -> Union[str,None]:
        return None

    def load_sample_data(self, resources_base_dir:str) -> Union[bytes,None]:
        return None

    def load_sample_df(self, resources_base_dir: str, *, upload_alter: bool = True) -> None:
        return None

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[ContextErrorLevelMessage,None]:
        return None

    def patch_request(self, ckan:CkanApi, package_id:str, *,
                      reupload:bool=None, resources_base_dir:str=None,
                      payload:Union[bytes, io.BufferedIOBase]=None) -> None:
        return None

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download: bool = True, force: bool = False,
                         threads: int = 1) -> Any:
        return None

    def download_sample(self, ckan: CkanApi, full_download: bool = True, **kwargs) -> bytes:
        return bytes()

