#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
from collections import OrderedDict
from typing import Union

from ckanapi_harvesters.auxiliary.ckan_auxiliary import empty_str_to_None

class DataFormatPolicyOutputCustomFields:
    default_score_custom_field: Union[str, None] = "Data Policy Score"
    default_report_custom_field: Union[str, None] = "Data Policy Report"
    default_package_filestore_size_custom_field: Union[str, None] = "FileStore Size"
    default_package_external_size_custom_field: Union[str, None] = "External Size"
    default_package_datastore_size_custom_field: Union[str, None] = "DataStore Size"
    default_package_datastore_rowcount_custom_field: Union[str, None] = "DataStore Rows"

    def __init__(self):
        self.package_score_field:Union[str,None] = DataFormatPolicyOutputCustomFields.default_score_custom_field
        self.package_report_field:Union[str,None] = DataFormatPolicyOutputCustomFields.default_report_custom_field
        # configured for admin report:
        self.package_filestore_size_field:Union[str,None] = DataFormatPolicyOutputCustomFields.default_package_filestore_size_custom_field
        self.package_external_size_field:Union[str,None] = DataFormatPolicyOutputCustomFields.default_package_external_size_custom_field
        self.package_datastore_size_field:Union[str,None] = DataFormatPolicyOutputCustomFields.default_package_datastore_size_custom_field
        self.package_datastore_rowcount_field:Union[str,None] = DataFormatPolicyOutputCustomFields.default_package_datastore_rowcount_custom_field

    def copy(self) -> "DataFormatPolicyOutputCustomFields":
        dest = DataFormatPolicyOutputCustomFields()
        dest.package_score_field = self.package_score_field
        dest.package_report_field = self.package_report_field
        dest.package_filestore_size_field = self.package_filestore_size_field
        dest.package_external_size_field = self.package_external_size_field
        dest.package_datastore_size_field = self.package_datastore_size_field
        dest.package_datastore_rowcount_field = self.package_datastore_rowcount_field
        return dest

    def to_dict(self) -> dict:
        output_custom_field_config = OrderedDict()
        output_custom_field_config["package_score"] = self.package_score_field if self.package_score_field else ""
        output_custom_field_config["package_report"] = self.package_report_field if self.package_report_field else ""
        output_custom_field_config["package_filestore_size"] = self.package_filestore_size_field if self.package_filestore_size_field else ""
        output_custom_field_config["package_external_size"] = self.package_external_size_field if self.package_external_size_field else ""
        output_custom_field_config["package_datastore_size"] = self.package_datastore_size_field if self.package_datastore_size_field else ""
        output_custom_field_config["package_datastore_rowcount"] = self.package_datastore_rowcount_field if self.package_datastore_rowcount_field else ""
        return output_custom_field_config

    def _load_from_dict(self, d:dict) -> None:
        if d is None:
            return
        self.package_score_field = empty_str_to_None(d.get("package_score"))
        self.package_report_field = empty_str_to_None(d.get("package_report"))
        self.package_filestore_size_field = empty_str_to_None(d.get("package_filestore_size"))
        self.package_external_size_field = empty_str_to_None(d.get("package_external_size"))
        self.package_datastore_size_field = empty_str_to_None(d.get("package_datastore_size"))
        self.package_datastore_rowcount_field = empty_str_to_None(d.get("package_datastore_rowcount"))

    def set_metadata_policy_fields(self, *, score: str = None, report: str = None):
        if score is not None:
            self.package_score_field = empty_str_to_None(score)
        if report is not None:
            self.package_report_field = empty_str_to_None(report)

    def disable_all(self):
        self.package_score_field = None
        self.package_report_field = None
        self.package_filestore_size_field = None
        self.package_external_size_field = None
        self.package_datastore_size_field = None
        self.package_datastore_rowcount_field = None

