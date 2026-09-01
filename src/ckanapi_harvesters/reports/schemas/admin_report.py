#!python3
# -*- coding: utf-8 -*-
"""
Detailed report on package resources: size, access rights and data format policy scores
"""
from dataclasses import dataclass, field
from abc import ABC
from typing import List, Union, Callable, Any
from collections import OrderedDict
import datetime
from warnings import warn

from ckanapi_harvesters.auxiliary.ckan_model import CkanVisibility, CkanState, CkanLicenseDomain, CkanCapacity
from ckanapi_harvesters.policies.data_format_policy_errors import ErrorCount, DataPolicyError


class OrderedDataClass(ABC):
    _field_order: List[str] = []

    def _value_export(self, value: Any, *, datetime_fcn:Callable[[datetime.datetime], Any] = None,
                float_round_fcn:Callable[[float], Any] = None, convert_ckan:bool=True) -> Any:

        if isinstance(value, datetime.datetime) and datetime_fcn is not None:
            value = datetime_fcn(value)
        elif isinstance(value, float) and float_round_fcn is not None:
            value = float_round_fcn(value)
        elif convert_ckan and (isinstance(value, CkanVisibility) or isinstance(value, CkanState) or isinstance(value, CkanCapacity)):
            value = str(value)
        elif convert_ckan and (isinstance(value, CkanLicenseDomain) or isinstance(value, ErrorCount)):
            value = value.to_dict()
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                value[sub_key] = self._value_export(sub_value, datetime_fcn=datetime_fcn, float_round_fcn=float_round_fcn, convert_ckan=convert_ckan)
        elif isinstance(value, list):
            for index, sub_value in enumerate(value):
                value[index] = self._value_export(sub_value, datetime_fcn=datetime_fcn, float_round_fcn=float_round_fcn, convert_ckan=convert_ckan)
        elif isinstance(value, OrderedDataClass):
            value = value.to_dict(datetime_fcn=datetime_fcn, float_round_fcn=float_round_fcn)
        return value

    def to_dict(self, *, datetime_fcn:Callable[[datetime.datetime], Any] = None,
                float_round_fcn:Callable[[float], Any] = None, convert_ckan:bool=True) -> OrderedDict:
        d = OrderedDict()
        missing_fields = set(self.__dict__.keys()) - set(self._field_order)
        # if len(missing_fields) > 0:
        #     msg = str(RuntimeError(f"Fields were not in the ordered list: {missing_fields}"))
        #     warn(msg, RuntimeWarning)
        field_list = self._field_order + list(missing_fields)
        for key in field_list:
            value = getattr(self, key)
            d[key] = self._value_export(value, datetime_fcn=datetime_fcn, float_round_fcn=float_round_fcn, convert_ckan=convert_ckan)
        return d


@dataclass
class AdminReportHeader(OrderedDataClass):
    title: str
    date: datetime.datetime | None
    timestamp: str
    ckan_url: str  # ckan url
    user: str | None
    user_sysadmin: bool | None
    package_selection: Union[List[str], str]
    public_packages: OrderedDict[str, str] | None
    _field_order = ["title", "date", "timestamp", "ckan_url", "user", "user_sysadmin", "package_selection", "public_packages"]


@dataclass
class AdminReportFooter(OrderedDataClass):
    requests_count: int
    time_elapsed_seconds: float
    _field_order = ["requests_count", "time_elapsed_seconds"]


@dataclass
class AdminReportTotalsSimple(OrderedDataClass):
    num_packages: int
    total_resource_count: int
    last_modified_metadata: datetime.datetime | None
    _field_order = ["num_packages", "total_resource_count", "last_modified_metadata"]

@dataclass
class AdminReportTotalsExtended(AdminReportTotalsSimple):
    total_filestore_size_mb: float
    total_datastore_size_mb: float
    total_external_size_mb: float
    total_datastore_lines: int
    among_resources_filestore: int
    among_resources_external: int
    among_resources_datastore: int
    last_modified_data: datetime.datetime | None
    total_policy_errors: ErrorCount
    _field_order = ["total_filestore_size_mb", "total_datastore_size_mb", "total_external_size_mb",
        "total_datastore_lines", "num_packages", "total_resource_count", "among_resources_filestore", "among_resources_external",
        "among_resources_datastore", "last_modified_data", "last_modified_metadata", "total_policy_errors"]


@dataclass
class AdminReportUserMetadata(OrderedDataClass):
    user_name: str
    fullname: str
    email: Union[str, None]
    last_active: datetime.datetime
    organizations: OrderedDict[str, CkanCapacity]
    groups: OrderedDict[str, CkanCapacity]
    _field_order = ["user_name", "fullname", "email", "last_active", "organizations", "groups"]


@dataclass
class AdminReportUsersSection(OrderedDataClass):
    sysadmins: OrderedDict[str, AdminReportUserMetadata]
    other: OrderedDict[str, AdminReportUserMetadata]
    _field_order = ["sysadmins", "other"]


@dataclass
class AdminReportGroupMetadata(OrderedDataClass):
    group_name: str
    group_title: str
    package_count: int
    users_count: int
    users: OrderedDict[str, CkanCapacity]
    _field_order = ["group_name", "group_title", "package_count", "users_count", "users"]


@dataclass
class AdminReportOrganizationMetadata(OrderedDataClass):
    organization_name: str
    organization_title: str
    package_count: int
    users_count: int
    users: OrderedDict[str, CkanCapacity]
    _field_order = ["organization_name", "organization_title", "package_count", "users_count", "users"]


@dataclass
class AdminReportResourceMetadata(OrderedDataClass):
    resource_name: str
    id: str
    page_url: str
    state: CkanState
    external_url: str
    filestore_size_mb: float
    external_size_mb: float
    datastore_size_mb: float
    datastore_active: bool
    datastore_lines: Union[int, None]
    date_modified: datetime.datetime | None
    metadata_modified: datetime.datetime | None
    datastore_aliases: Union[List[str], None]
    _field_order = ["resource_name", "id", "page_url", "state", "external_url", "filestore_size_mb",
                     "external_size_mb", "datastore_size_mb", "datastore_active", "datastore_lines", "date_modified",
                     "metadata_modified", "datastore_aliases"]


@dataclass
class AdminReportPackageSimple(OrderedDataClass):
    package_name: str
    package_title: str
    page_url: str
    state: CkanState
    organization: str
    version: str
    license: Union[str,None]
    license_domain: Union[CkanLicenseDomain, None]
    creator: Union[str, None]
    author: str
    maintainer: str
    visibility: CkanVisibility
    metadata_modified: datetime.datetime
    resource_count: int
    tags: List[str]
    extras: OrderedDict[str, str | None]
    users: Union[OrderedDict[str, CkanCapacity], str]
    groups: List[str]
    _expand_extras = True
    _field_order = ["package_name", "package_title", "page_url", "state", "organization", "version",
                     "license", "license_domain", "creator", "author", "maintainer", "metadata_modified",
                     "visibility", "metadata_modified", "resource_count", "tags", "users", "groups"]

    def to_dict(self, *, datetime_fcn:Callable[[datetime.datetime], Any] = None,
                float_round_fcn:Callable[[float], Any] = None, convert_ckan: bool = True) -> OrderedDict:
        d = super().to_dict(datetime_fcn=datetime_fcn, float_round_fcn=float_round_fcn, convert_ckan=convert_ckan)
        if self._expand_extras:
            extras = d.pop("extras")
            d.update(extras)
        d.move_to_end("users")
        d.move_to_end("groups")
        return d

@dataclass
class AdminReportPackageExtended(AdminReportPackageSimple):
    resources_modified: datetime.datetime
    resources_metadata_modified: datetime.datetime
    among_resources_filestore: int
    among_resources_external: int
    among_resources_datastore: int
    filestore_total_size_mb: float
    external_total_size_mb: float
    datastore_total_size_mb: float
    datastore_total_lines: int
    data_format_policy_scores: ErrorCount
    resources: List[AdminReportResourceMetadata]
    policy_messages: Union[List[DataPolicyError], None]
    _field_order = (["package_name", "package_title", "page_url", "state", "organization", "version",
                     "license", "license_domain", "creator", "author", "maintainer", "metadata_modified",
                     "visibility", "metadata_modified"] +  # removed resource_count, tags, users, groups from AdminReportPackageSimple._field_order
                     ["resources_modified", "resources_metadata_modified",
                      "resource_count",
                      "among_resources_filestore", "among_resources_external", "among_resources_datastore",
                      "filestore_total_size_mb", "external_total_size_mb", "datastore_total_size_mb",
                      "datastore_total_lines",
                      "data_format_policy_scores", "tags", "users", "groups",
                      "resources", "policy_messages"])

    def to_dict(self, *, datetime_fcn:Callable[[datetime.datetime], Any] = None,
                float_round_fcn:Callable[[float], Any] = None, convert_ckan: bool = True) -> OrderedDict:
        d = super().to_dict(datetime_fcn=datetime_fcn, float_round_fcn=float_round_fcn, convert_ckan=convert_ckan)
        d.move_to_end("users")
        d.move_to_end("groups")
        d.move_to_end("resources")
        d.move_to_end("policy_messages")
        return d

@dataclass
class AdminReportSchema(OrderedDataClass):
    header: AdminReportHeader
    totals: AdminReportTotalsSimple
    packages: OrderedDict[str, AdminReportPackageSimple]
    users: AdminReportUsersSection
    groups: OrderedDict[str, AdminReportGroupMetadata] | None
    organizations: OrderedDict[str, AdminReportOrganizationMetadata] | None
    footer: AdminReportFooter
    _field_order = ["header", "totals", "packages", "users", "groups", "organizations", "footer"]


if __name__ == '__main__':
    test = AdminReportFooter(requests_count=10, time_elapsed_seconds=1)
    test_dict = test.to_dict()
    print(test_dict)

