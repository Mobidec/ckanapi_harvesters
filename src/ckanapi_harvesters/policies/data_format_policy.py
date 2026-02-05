#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
from collections import OrderedDict
from typing import List, Set, Union, Tuple, Dict
from warnings import warn
import json
import os
import copy

import requests
from requests.auth import AuthBase

from ckanapi_harvesters.auxiliary.ckan_configuration import allow_policy_from_url, download_external_resource_urls
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.urls import is_valid_url
from ckanapi_harvesters.auxiliary.path import path_rel_to_dir
from ckanapi_harvesters.auxiliary.ckan_errors import ExternalUrlLockedError
from ckanapi_harvesters.policies import POLICY_FILE_FORMAT_VERSION
from ckanapi_harvesters.policies.data_format_policy_errors import (DataPolicyError, UnsupportedPolicyVersionError,
                                                                   _policy_msg, ErrorCount, ErrorLevel, UrlPolicyLockedError)
from ckanapi_harvesters.policies.data_format_policy_defs import StringMatchMode, newline_char
from ckanapi_harvesters.policies.data_format_policy_defs import ListChoiceMode, StringValueSpecification
from ckanapi_harvesters.policies.data_format_policy_abc import DataPolicyABC
from ckanapi_harvesters.policies.data_format_policy_lists import ValueListPolicy, GroupedValueListPolicy, SingleValueListPolicy
from ckanapi_harvesters.policies.data_format_policy_tag_groups import TagListPolicy, TagGroupsListPolicy
from ckanapi_harvesters.policies.data_format_policy_custom_fields import CustomFieldSpecification, CustomFieldsPolicy
from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageInfo, CkanConfigurableObjectABC



class CkanPackageDataFormatPolicy(DataPolicyABC):
    """
    Main class to define data format policy for package metadata
    """
    default_to_json_reduced_size:bool = False

    def __init__(self, label:str=None, description:str=None,
                 package_tags:TagGroupsListPolicy=None, package_custom_fields:CustomFieldsPolicy=None,
                 package_mandatory_attributes:Set[str]=None, resource_mandatory_attributes:Set[str]=None,
                 datastore_fields_mandatory_attributes:Set[str]=None, resource_format:SingleValueListPolicy=None):
        super().__init__()
        if label is None:
            label = "Policy"
        if description is None:
            description = ""
        if isinstance(package_mandatory_attributes, str):
            package_mandatory_attributes = set(ckan_tags_sep.split(package_mandatory_attributes))
        if isinstance(resource_mandatory_attributes, str):
            resource_mandatory_attributes = set(ckan_tags_sep.split(resource_mandatory_attributes))
        if isinstance(datastore_fields_mandatory_attributes, str):
            datastore_fields_mandatory_attributes = set(ckan_tags_sep.split(datastore_fields_mandatory_attributes))
        self.label: str = label
        self.description: str = description
        self.package_tags:TagGroupsListPolicy = package_tags
        self.package_custom_fields: CustomFieldsPolicy = package_custom_fields
        self.package_mandatory_attributes:Set[str] = package_mandatory_attributes
        self.resource_mandatory_attributes:Set[str] = resource_mandatory_attributes
        self.datastore_fields_mandatory_attributes:Set[str] = datastore_fields_mandatory_attributes
        self.resource_format:SingleValueListPolicy = resource_format
        self.file_format_version:Union[str,None] = None
        self.source_file: Union[str,None] = None
        self.output_score_custom_field:Union[str,None] = None
        self.output_report_custom_field:Union[str,None] = None

    def __copy__(self):
        return self.copy()

    def copy(self) -> "CkanPackageDataFormatPolicy":
        dest = CkanPackageDataFormatPolicy()
        dest.label = self.label
        dest.description = self.description
        dest.package_tags = copy.deepcopy(self.package_tags)
        dest.package_custom_fields = copy.deepcopy(self.package_custom_fields)
        dest.package_mandatory_attributes = copy.deepcopy(self.package_mandatory_attributes)
        dest.resource_mandatory_attributes = copy.deepcopy(self.resource_mandatory_attributes)
        dest.datastore_fields_mandatory_attributes = copy.deepcopy(self.datastore_fields_mandatory_attributes)
        dest.resource_format = copy.deepcopy(self.resource_format)
        dest.file_format_version = self.file_format_version
        dest.source_file = self.source_file
        dest.error_level = self.error_level
        dest.output_score_custom_field = self.output_score_custom_field
        dest.output_report_custom_field = self.output_report_custom_field
        return dest

    def to_dict(self, *, sets_as_lists:bool=True) -> dict:
        d = {"info": {"file_format_version": POLICY_FILE_FORMAT_VERSION,
                      "label": self.label,
                      "description": self.description,
                      },}
        if self.package_tags is not None:
            d["package_tags_policy"] = self.package_tags.to_dict()
        if self.package_custom_fields is not None:
            d["package_custom_fields_policy"] = self.package_custom_fields.to_dict()
        if self.package_mandatory_attributes is not None:
            set_object = self.package_mandatory_attributes
            if sets_as_lists:
                set_object = sorted(list(set_object))
            d["package_mandatory_attributes"] = set_object
        if self.resource_mandatory_attributes is not None:
            set_object = self.resource_mandatory_attributes
            if sets_as_lists:
                set_object = sorted(list(set_object))
            d["resource_mandatory_attributes"] = set_object
        if self.datastore_fields_mandatory_attributes is not None:
            set_object = self.datastore_fields_mandatory_attributes
            if sets_as_lists:
                set_object = sorted(list(set_object))
            d["datastore_fields_mandatory_attributes"] = set_object
        if self.resource_format is not None:
            d["resource_format_policy"] = self.resource_format.to_dict()
        output_custom_field_config = OrderedDict()
        output_custom_field_config["score"] = self.output_score_custom_field if self.output_score_custom_field else ""
        output_custom_field_config["report"] = self.output_report_custom_field if self.output_report_custom_field else ""
        d["output_custom_field"] = output_custom_field_config
        d.update(super().to_dict())
        return {"ckan_package_policy": d}

    @staticmethod
    def from_dict(d:dict) -> "CkanPackageDataFormatPolicy":
        obj = CkanPackageDataFormatPolicy()
        obj._load_from_dict(d)
        return obj

    def _load_from_dict(self, d:dict):
        d = d["ckan_package_policy"]
        super()._load_from_dict(d)
        self.file_format_version = d["info"]["file_format_version"]
        if not self.file_format_version == POLICY_FILE_FORMAT_VERSION:
            raise UnsupportedPolicyVersionError(self.file_format_version)
        self.label = d["info"]["label"]
        self.description = d["info"]["description"]
        # for package tags management, see also tags and vocabularies in the CKAN API documentation. Here, tags groups are the equivalent of tag vocabularies.
        self.package_tags = TagGroupsListPolicy.from_dict(d["package_tags_policy"]) if "package_tags_policy" in d.keys() else None
        self.package_custom_fields = CustomFieldsPolicy.from_dict(d["package_custom_fields_policy"]) if "package_custom_fields_policy" in d.keys() else None
        self.package_mandatory_attributes = set(d["package_mandatory_attributes"]) if "package_mandatory_attributes" in d.keys() else None
        self.resource_mandatory_attributes = set(d["resource_mandatory_attributes"]) if "resource_mandatory_attributes" in d.keys() else None
        self.datastore_fields_mandatory_attributes = set(d["datastore_fields_mandatory_attributes"]) if "datastore_fields_mandatory_attributes" in d.keys() else None
        self.resource_format = SingleValueListPolicy.from_dict(d["resource_format_policy"]) if "resource_format_policy" in d.keys() else None
        output_custom_field_config = d.get("output_custom_field")
        if output_custom_field_config:
            self.output_score_custom_field = output_custom_field_config.get("score")
            self.output_report_custom_field = output_custom_field_config.get("report")

    def to_json(self, json_file:str, reduced_size:bool=None) -> None:
        if reduced_size is None:
            reduced_size = self.default_to_json_reduced_size
        policy_dict = self.to_dict()
        with open(json_file, "w", encoding="utf-8") as json_file:
            if reduced_size:
                json.dump(policy_dict, json_file, ensure_ascii=False)
            else:
                json.dump(policy_dict, json_file, ensure_ascii=False, indent=4)

    def to_jsons(self, reduced_size:bool=None) -> str:
        if reduced_size is None:
            reduced_size = self.default_to_json_reduced_size
        policy_dict = self.to_dict()
        if reduced_size:
            # do not include spaces and line endings (not human-readable format)
            return json.dumps(policy_dict, ensure_ascii=False)
        else:
            return json.dumps(policy_dict, indent=4, ensure_ascii=False)

    @staticmethod
    def from_jsons(stream:str, *,
                   source_file:str=None, load_error:bool=True) -> Union["CkanPackageDataFormatPolicy", None]:
        try:
            policy_dict = json.loads(stream)
            obj = CkanPackageDataFormatPolicy.from_dict(policy_dict)
        except Exception as e:
            if load_error:
                raise e from e
            else:
                msg = f"Could not load policy (JSON error): {str(e)}"
                warn(msg)
                return None
        obj.source_file = source_file
        return obj

    @staticmethod
    def from_json(policy_file:str, *, base_dir:str=None,
                  headers:dict=None, proxies:dict=None, auth:Union[AuthBase, Tuple[str,str]]=None, verify:Union[bool,str,None]=None,
                  error_not_found:bool=True) -> Union["CkanPackageDataFormatPolicy",None]:
        policy_dict = None
        if is_valid_url(policy_file):
            if not allow_policy_from_url:
                raise UrlPolicyLockedError(policy_file)
            # if (not download_external_resource_urls) and (not ckan.is_url_internal(policy_file)):  # ckan: unknown
            #     raise ExternalUrlLockedError(policy_file)
            response = requests.get(policy_file, headers=headers, proxies=proxies, auth=auth, verify=verify)
            if response.status_code != 200 and error_not_found:
                raise FileNotFoundError(policy_file)
            policy_dict = json.loads(response.content.decode())
        else:
            policy_file = path_rel_to_dir(policy_file, base_dir)
            if not os.path.isfile(policy_file) and not error_not_found:
                return None
            with open(policy_file, "r") as f:
                policy_dict = json.load(f)
        obj = CkanPackageDataFormatPolicy.from_dict(policy_dict)
        obj.source_file = policy_file
        return obj

    def _enforce_attributes_list(self, value:CkanConfigurableObjectABC, spec:Set[str], *, context:str, verbose: bool, buffer:List[DataPolicyError]):
        extra_spec = spec - value.configurable_attributes
        if len(extra_spec) > 0:
            raise KeyError("These attributes do not exist for " + value.get_resource_type() + ": " + ",".join(extra_spec) + ". Allowed attributes: " + str(value.configurable_attributes))
        current_attributes = {name for name in value.configurable_attributes if getattr(value, name) is not None}
        missing_attributes = set(spec) - current_attributes
        if missing_attributes:
            msg = DataPolicyError(context, self.error_level, f"Mandatory attributes were not found: {', '.join(missing_attributes)}")
            _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
            return False
        else:
            return True

    def enforce(self, values: CkanPackageInfo, *, context:str=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        package_info = values
        success = True
        if context is None:
            context = "Package " + package_info.name
        if self.package_tags is not None:
            success &= self.package_tags.enforce(package_info.tags, context=context + " / package tags", verbose=verbose, buffer=buffer)
        if self.package_custom_fields is not None:
            success &= self.package_custom_fields.enforce(package_info.custom_fields, context=context, verbose=verbose, buffer=buffer)
        if self.package_mandatory_attributes is not None:
            success &= self._enforce_attributes_list(package_info, self.package_mandatory_attributes, context=context, verbose=verbose, buffer=buffer)
        for resource_info in package_info.package_resources.values():
            resource_context = context + " / resource " + resource_info.name
            if self.resource_format is not None:
                resource_format_context = resource_context + " / resource format"
                success &= self.resource_format.enforce(resource_info.format, context=resource_format_context, verbose=verbose, buffer=buffer)
            if self.resource_mandatory_attributes is not None:
                success &= self._enforce_attributes_list(resource_info, self.resource_mandatory_attributes, context=resource_context, verbose=verbose, buffer=buffer)
            if self.datastore_fields_mandatory_attributes is not None and resource_info.datastore_info is not None:
                for field_info in resource_info.datastore_info.fields:
                    field_context = resource_context + " / field " + field_info.name
                    success &= self._enforce_attributes_list(field_info, self.datastore_fields_mandatory_attributes, context=field_context, verbose=verbose, buffer=buffer)
        return success

    def policy_check_package(self, package_info: CkanPackageInfo, *, package_buffer:List[DataPolicyError]=None,
                             display_message:bool=True, raise_error:bool=False, ckan=None) -> bool:
        """
        Main entry-point to check the policy rules against the package.

        :param package_info: package and resources metadata
        :param package_buffer: you can specify a list object to indirectly obtain the detailed list of error messages.
        The keys of this dictionary are the package names.
        :param display_message: option to display the messages in the command line
        :param raise_error: option to raise an exception if any rule with a high error level is encountered
        :return: True if no error was encountered
        """
        if package_buffer is None:
            package_buffer: List[DataPolicyError] = []
        context = "Package " + package_info.name
        success = self.enforce(package_info, context=context, verbose=True, buffer=package_buffer)
        error_count = ErrorCount(package_buffer)
        # update package metadata
        package_update_needed = self.output_score_custom_field or self.output_report_custom_field
        if package_update_needed and package_info.custom_fields is None:
            package_info.custom_fields = OrderedDict()
        if self.output_score_custom_field:
            package_info.custom_fields[self.output_score_custom_field] = error_count.to_dict()
        if self.output_report_custom_field:
            package_info.custom_fields[self.output_report_custom_field] = '\n'.join([error_message.message for error_message in package_buffer])
        package_info.updated = package_update_needed
        if package_update_needed and ckan is not None:
            ckan.package_patch(package_info.id, custom_fields=package_info.custom_fields)
        # consistency check
        if success:
            assert(error_count.total == 0)
        else:
            assert(error_count.total > 0)
        # command-line output
        if display_message:
            if success:
                print("Package '" + package_info.name + "' passed all tests")
            else:
                print("Package '" + package_info.name + "': " + error_count.error_count_message() + ":")
                print('\n'.join([error_message.message for error_message in package_buffer]))
        # raise error after all this
        if raise_error and error_count.error > 0:
            raise DataPolicyError(context, ErrorLevel.Error, error_count.error_count_message())
        return success






