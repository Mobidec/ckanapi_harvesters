#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
import datetime
from collections import OrderedDict
from typing import List, Set, Union, Tuple
from warnings import warn
import json
import os
import copy

import requests
from requests.auth import AuthBase

from ckanapi_harvesters.auxiliary.ckan_configuration import allow_policy_from_url
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.urls import is_valid_url
from ckanapi_harvesters.auxiliary.path import path_rel_to_dir
from ckanapi_harvesters.auxiliary.ckan_auxiliary import str_is_not_empty, size_str_mb
from ckanapi_harvesters.auxiliary.ckan_errors import NoPackageSizeError
from ckanapi_harvesters.policies import POLICY_FILE_FORMAT_VERSION
from ckanapi_harvesters.policies.data_format_policy_errors import (DataPolicyError, UnsupportedPolicyVersionError,
                                                                   _policy_msg, ErrorCount, ErrorLevel, UrlPolicyLockedError)
from ckanapi_harvesters.policies.data_format_policy_defs import StringMatchMode
from ckanapi_harvesters.policies.data_format_policy_defs import ListChoiceMode, StringValueSpecification
from ckanapi_harvesters.policies.policy_report import PackagePolicyReport
from ckanapi_harvesters.policies.data_format_policy_abc import DataPolicyABC
from ckanapi_harvesters.policies.data_format_policy_lists import ValueListPolicy, GroupedValueListPolicy, SingleValueListPolicy
from ckanapi_harvesters.policies.data_format_policy_tag_groups import TagListPolicy, TagGroupsListPolicy
from ckanapi_harvesters.policies.data_format_policy_custom_fields import CustomFieldSpecification, CustomFieldsPolicy
from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageInfo, CkanConfigurableObjectABC, CkanUserInfo
from ckanapi_harvesters.policies.data_format_policy_output_config import DataFormatPolicyOutputCustomFields


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
        self.package_author_or_maintainer_level: ErrorLevel = ErrorLevel.Error
        self.resource_same_name_level:ErrorLevel = ErrorLevel.Error
        self.resource_mandatory_attributes:Set[str] = resource_mandatory_attributes
        self.datastore_fields_mandatory_attributes:Set[str] = datastore_fields_mandatory_attributes
        self.resource_format:SingleValueListPolicy = resource_format
        self.file_format_version:Union[str,None] = None
        self.source_file: Union[str,None] = None
        self.output_custom_fields = DataFormatPolicyOutputCustomFields()

    def __copy__(self):
        return self.copy()

    def copy(self) -> "CkanPackageDataFormatPolicy":
        dest = CkanPackageDataFormatPolicy()
        dest.label = self.label
        dest.description = self.description
        dest.package_tags = copy.deepcopy(self.package_tags)
        dest.package_custom_fields = copy.deepcopy(self.package_custom_fields)
        dest.package_mandatory_attributes = copy.deepcopy(self.package_mandatory_attributes)
        dest.package_author_or_maintainer_level = self.package_author_or_maintainer_level
        dest.resource_same_name_level = self.resource_same_name_level
        dest.resource_mandatory_attributes = copy.deepcopy(self.resource_mandatory_attributes)
        dest.datastore_fields_mandatory_attributes = copy.deepcopy(self.datastore_fields_mandatory_attributes)
        dest.resource_format = copy.deepcopy(self.resource_format)
        dest.file_format_version = self.file_format_version
        dest.source_file = self.source_file
        dest.error_level = self.error_level
        dest.output_custom_fields = self.output_custom_fields.copy()
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
        d["package_author_or_maintainer_error"] = str(self.package_author_or_maintainer_level)
        d["resource_same_name_level"] = str(self.resource_same_name_level)
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
        d["output_custom_field"] = self.output_custom_fields.to_dict()
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
        error_level_str = d.get("package_author_or_maintainer_error", None)
        self.package_author_or_maintainer_level = ErrorLevel.from_str(error_level_str) if error_level_str is not None else None
        self.resource_same_name_level = ErrorLevel.from_str(d.get("resource_same_name_level", str(ErrorLevel.Error)))
        self.resource_mandatory_attributes = set(d["resource_mandatory_attributes"]) if "resource_mandatory_attributes" in d.keys() else None
        self.datastore_fields_mandatory_attributes = set(d["datastore_fields_mandatory_attributes"]) if "datastore_fields_mandatory_attributes" in d.keys() else None
        self.resource_format = SingleValueListPolicy.from_dict(d["resource_format_policy"]) if "resource_format_policy" in d.keys() else None
        output_custom_field_config = d.get("output_custom_field")
        if output_custom_field_config:
            self.output_custom_fields._load_from_dict(output_custom_field_config)

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
                  error_not_found:bool=True, load_error:bool=True) -> Union["CkanPackageDataFormatPolicy",None]:
        policy_dict = None
        if is_valid_url(policy_file):
            if not allow_policy_from_url:
                raise UrlPolicyLockedError(policy_file)
            # if (not download_external_resource_urls) and (not ckan.is_url_internal(policy_file)):  # ckan: unknown - do not add as an argument to this function
            #     raise ExternalUrlLockedError(policy_file)
            response = requests.get(policy_file, headers=headers, proxies=proxies, auth=auth, verify=verify)
            if response.status_code != 200 and error_not_found:
                raise FileNotFoundError(policy_file)
            try:
                policy_dict = json.loads(response.content.decode())
            except Exception as e:
                if load_error:
                    raise e from e
                else:
                    msg = f"Could not load policy (JSON error): {str(e)}"
                    warn(msg)
                    return None
        else:
            policy_file = path_rel_to_dir(policy_file, base_dir)
            if not os.path.isfile(policy_file) and not error_not_found:
                return None
            try:
                with open(policy_file, "r") as f:
                    policy_dict = json.load(f)
            except Exception as e:
                if load_error:
                    raise e from e
                else:
                    msg = f"Could not load policy (JSON error): {str(e)}"
                    warn(msg)
                    return None
        try:
            obj = CkanPackageDataFormatPolicy.from_dict(policy_dict)
        except Exception as e:
            if load_error:
                raise e from e
            else:
                msg = f"Could not load policy (JSON error): {str(e)}"
                warn(msg)
                return None
        obj.source_file = policy_file
        return obj

    def _enforce_attributes_list(self, value:CkanConfigurableObjectABC, spec:Set[str], *, context:dict, verbose: bool, buffer:List[DataPolicyError]):
        extra_spec = spec - value.configurable_attributes
        if len(extra_spec) > 0:
            raise KeyError("These attributes do not exist for " + value.get_resource_type() + ": " + ",".join(extra_spec) + ". Allowed attributes: " + str(value.configurable_attributes))
        current_attributes = {name for name in value.configurable_attributes if getattr(value, name) is not None}
        missing_attributes = set(spec) - current_attributes
        success = len(missing_attributes) == 0
        # specialized error messages (more user-friendly)
        if "field" in context:
            if "notes" in missing_attributes:
                msg = DataPolicyError(context, self.error_level, f"Missing field description")
                _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
                missing_attributes.remove("notes")
        elif "resource" in context:
            if "description" in missing_attributes:
                msg = DataPolicyError(context, self.error_level, f"Missing resource description")
                _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
                missing_attributes.remove("description")
        else:  # if "package" in context:
            if "description" in missing_attributes:
                msg = DataPolicyError(context, self.error_level, f"Missing package description")
                _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
                missing_attributes.remove("description")
        if missing_attributes:
            msg = DataPolicyError(context, self.error_level, f"Mandatory attributes were not found: {', '.join(missing_attributes)}")
            _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
            return success
        else:
            return success

    def enforce(self, values: CkanPackageInfo, *, context:dict=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        package_info = values
        success = True
        if context is None:
            context = OrderedDict()
            context["package"] = package_info.name
        if self.package_tags is not None:
            tags_context = context.copy()
            tags_context["package_attribute"] = "tags"
            success &= self.package_tags.enforce(package_info.tags, context=tags_context, verbose=verbose, buffer=buffer)
        if self.package_custom_fields is not None:
            success &= self.package_custom_fields.enforce(package_info.custom_fields, context=context, verbose=verbose, buffer=buffer)
        if self.package_mandatory_attributes is not None:
            success &= self._enforce_attributes_list(package_info, self.package_mandatory_attributes, context=context, verbose=verbose, buffer=buffer)
        if self.package_author_or_maintainer_level is not None:
            success_author_or_maintainer = (str_is_not_empty(package_info.author) or str_is_not_empty(package_info.author_email)
                                            or str_is_not_empty(package_info.maintainer) or str_is_not_empty(package_info.maintainer_email))
            if not success_author_or_maintainer:
                msg = DataPolicyError(context, self.package_author_or_maintainer_level, f"Author/Maintainer not found")
                _policy_msg(msg, error_level=self.package_author_or_maintainer_level, buffer=buffer, verbose=verbose)
            success &= success_author_or_maintainer
        resource_name_index = OrderedDict()
        for resource_info in package_info.package_resources.values():
            if resource_info.name in resource_name_index.keys():
                resource_name_index[resource_info.name].append(resource_info.id)
            else:
                resource_name_index[resource_info.name] = [resource_info.id]
            resource_context = context.copy()
            resource_context["resource"] = resource_info.name
            if self.resource_format is not None:
                resource_format_context = resource_context.copy()
                resource_format_context["resource_attribute"] = "format"
                success &= self.resource_format.enforce(resource_info.format, context=resource_format_context, verbose=verbose, buffer=buffer)
            if self.resource_mandatory_attributes is not None:
                success &= self._enforce_attributes_list(resource_info, self.resource_mandatory_attributes, context=resource_context, verbose=verbose, buffer=buffer)
            if self.datastore_fields_mandatory_attributes is not None and resource_info.datastore_info is not None:
                for field_info in resource_info.datastore_info.fields_dict.values():
                    field_context = resource_context.copy()
                    field_context["field"] = field_info.name
                    success &= self._enforce_attributes_list(field_info, self.datastore_fields_mandatory_attributes, context=field_context, verbose=verbose, buffer=buffer)
        if any([len(resource_ids) > 1 for resource_ids in resource_name_index.values()]):
            for resource_name, resource_ids in resource_name_index.items():
                if len(resource_ids) > 1:
                    msg = DataPolicyError(context, self.resource_same_name_level, f"{len(resource_ids)} resources with same name were found for '{resource_name}' ({', '.join(resource_ids)})")
                    _policy_msg(msg, error_level=self.resource_same_name_level, buffer=buffer, verbose=verbose)
            success = False
        return success

    def policy_check_package(self, package_info: CkanPackageInfo, *, package_report:PackagePolicyReport=None,
                             display_message:bool=True, raise_error:bool=False) -> PackagePolicyReport:
        """
        Main entry-point to check the policy rules against the package.

        :param package_info: package and resources metadata
        :param package_buffer: you can specify a list object to indirectly obtain the detailed list of error messages.
            The keys of this dictionary are the package names.
        :param display_message: option to display the messages in the command line
        :param raise_error: option to raise an exception if any rule with a high error level is encountered
        :return: True if no error was encountered
        """
        package_name = package_info.name
        if package_report is None:
            package_report = PackagePolicyReport(package_name)
        context = OrderedDict()
        context["package"] = package_name
        success = self.enforce(package_info, context=context, verbose=True, buffer=package_report.messages)
        error_count = ErrorCount(package_report.messages)
        package_report.error_count = error_count
        package_report.success = success
        # consistency check
        if success:
            assert(error_count.total == 0)
        else:
            assert(error_count.total > 0)
        # command-line output
        if display_message:
            if success:
                print("Package '" + package_name + "' passed all tests")
            else:
                print("Package '" + package_name + "': " + error_count.error_count_message() + ":")
                print('\n'.join([error_message.message for error_message in package_report.messages]))
        # raise error after all this
        if raise_error and error_count.error > 0:
            raise DataPolicyError(context, ErrorLevel.Error, error_count.error_count_message())
        return package_report

    def package_update_scores(self, ckan: "CkanApi", package_info: CkanPackageInfo, package_report:PackagePolicyReport,
                              *, date_report:datetime.datetime=None, error_no_sizes:bool=False, raise_error:bool=True) -> bool:
        """
        Update the package scores on the CKAN server in package custom fields.

        :return: True if a package update is required. If ckan argument was given, the package update is applied.
        """
        package_info.updated = False
        self._package_update_policy_scores(package_info, package_report)
        self._package_update_size_report(package_info, date_report=date_report, error_no_sizes=error_no_sizes)
        package_update_needed = package_info.updated
        if package_update_needed and ckan is not None:
            try:
                ckan.package_patch(package_info.id, custom_fields=package_info.custom_fields)
            except Exception as e:
                if raise_error:
                    raise e from e
                else:
                    msg = "Could not update policy scores: " + str(e)
                    warn(msg)
        return package_update_needed

    def _package_update_policy_scores(self, package_info: CkanPackageInfo, package_report:PackagePolicyReport) -> bool:
        """
        Update the package scores on the CKAN server in package custom fields.

        :return: True if a package update is required. If ckan argument was given, the package update is applied.
        """
        package_buffer = package_report.messages
        error_count = ErrorCount(package_buffer)
        # update package metadata
        package_update_needed = self.output_custom_fields.package_score_field is not None or self.output_custom_fields.package_report_field is not None
        if package_update_needed and package_info.custom_fields is None:
            package_info.custom_fields = OrderedDict()
        package_update_needed = package_info.updated  # initial state
        if self.output_custom_fields.package_score_field is not None:
            package_score_str = error_count.error_count_message()
            if self.output_custom_fields.package_score_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[self.output_custom_fields.package_score_field] == package_score_str
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.package_score_field] = package_score_str
        if self.output_custom_fields.package_report_field is not None:
            package_report_str = "; \n".join(["- " + error_message.get_message(with_package=False) for error_message in package_buffer])
            if self.output_custom_fields.package_report_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[self.output_custom_fields.package_report_field] == package_report_str
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.package_report_field] = package_report_str
        package_info.updated = package_update_needed

    def _package_update_size_report(self, package_info: CkanPackageInfo,
                                    *, date_report:datetime.datetime=None, error_no_sizes:bool=False) -> None:
        package_size = package_info.package_size
        if package_size is None:
            if error_no_sizes:
                raise NoPackageSizeError(package_info.name)
            else:
                return
        if date_report is None:
            date_report = datetime.datetime.now()
        package_update_needed = any([field_name is not None for field_name in [
            self.output_custom_fields.package_filestore_size_field,
            self.output_custom_fields.package_external_size_field,
            self.output_custom_fields.package_datastore_size_field,
            self.output_custom_fields.package_datastore_rowcount_field]])
        if package_update_needed and package_info.custom_fields is None:
            package_info.custom_fields = OrderedDict()
        package_update_needed = package_info.updated  # initial state
        if self.output_custom_fields.report_timestamp_field is not None:
            report_timestamp = date_report.isoformat(sep='T', timespec="seconds")
            if self.output_custom_fields.report_timestamp_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[self.output_custom_fields.report_timestamp_field] == report_timestamp
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.report_timestamp_field] = report_timestamp
        if self.output_custom_fields.package_filestore_size_field is not None:
            package_size_str = size_str_mb(package_size.filestore_size_mb)
            if self.output_custom_fields.package_filestore_size_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[self.output_custom_fields.package_filestore_size_field] == package_size_str
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.package_filestore_size_field] = package_size_str
        if self.output_custom_fields.package_external_size_field is not None:
            package_size_str = size_str_mb(package_size.external_size_mb)
            if self.output_custom_fields.package_external_size_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[self.output_custom_fields.package_external_size_field] == package_size_str
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.package_external_size_field] = package_size_str
        if self.output_custom_fields.package_datastore_size_field is not None:
            package_size_str = size_str_mb(package_size.datastore_size_mb)
            if self.output_custom_fields.package_datastore_size_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[
                                                 self.output_custom_fields.package_datastore_size_field] == package_size_str
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.package_datastore_size_field] = package_size_str
        if self.output_custom_fields.package_datastore_rowcount_field is not None:
            package_rowcount_str = str(package_size.datastore_lines)
            if self.output_custom_fields.package_datastore_rowcount_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[self.output_custom_fields.package_datastore_rowcount_field] == package_rowcount_str
            else:
                package_update_needed = True
            package_info.custom_fields[self.output_custom_fields.package_datastore_rowcount_field] = package_rowcount_str
        package_info.updated = package_update_needed


from ckanapi_harvesters.ckan_api.ckan_api import CkanApi






