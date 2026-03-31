#!python3
# -*- coding: utf-8 -*-
"""
Detailed report on package resources: size, access rights and data format policy scores
"""
from typing import List, Union, Dict
from collections import OrderedDict
import time
import datetime
import os
from warnings import warn

from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_abc import CkanProgressCallbackABC, CkanCallbackLevel, CkanProgressUnits
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanProgressCallback
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_errors import CkanAuthorizationError
from ckanapi_harvesters.auxiliary.ckan_auxiliary import to_jsons_indent_lists_single_line
from ckanapi_harvesters.auxiliary.ckan_model import CkanVisibility, CkanUserInfo, CkanPackageInfo
from ckanapi_harvesters.policies.data_format_policy_errors import ErrorCount, DataPolicyError


def round_size(value_mb:float) -> float:
    return round(value_mb, 2)

def size_str_mb(size_mb:float) -> str:
    return f"{round_size(size_mb):.2f} MB"


class CkanAdminReport:
    def __init__(self, package_list:List[str]=None, *, cancel_if_present:bool=True,
                 package_custom_fields:List[str]=None, ckan:CkanApi=None, full_report:bool=False,
                 auto_exec:bool=True, progress_callback:CkanProgressCallbackABC=None):
        if package_custom_fields is None:
            package_custom_fields = []  # option to include specific custom fields in the report e.g. a end of license date
        if isinstance(package_list, str):
            package_list = [package_list]
        self.package_list: Union[List[str],None] = package_list
        self.cancel_if_present: bool = cancel_if_present
        self.include_package_custom_fields: List[str] = package_custom_fields
        self.include_resources_detail: bool = True
        self.include_policy_messages: bool = full_report
        self.include_group_report: bool = full_report
        self.date_format:Union[str,None] = '%d/%m/%Y %H:%M'
        self._connected_user: Union[CkanUserInfo, None] = None
        self.report_date: Union[datetime.datetime, None] = None
        self._elapsed_time_requests: Union[float,None] = None
        self._request_count: Union[int,None] = None
        self.allow_downgraded_queries:bool = False
        self.report: Union[dict,None] = None  # report output
        if auto_exec and ckan is not None:
            self.execute(ckan, progress_callback=progress_callback)

    def _date_format_str(self, date:datetime.datetime) -> str:
        if self.date_format is not None:
            return date.strftime(self.date_format)
        else:
            return date.isoformat()

    def _perform_requests(self, ckan: CkanApi, *, progress_callback:CkanProgressCallbackABC=None) -> None:
        if not self.cancel_if_present:
            ckan.purge(purge_map=True)
        start = time.time()
        self.report_date = datetime.datetime.now()
        request_count_init = ckan.debug.ckan_request_counter
        try:
            self._connected_user = ckan.query_current_user()
        except CkanAuthorizationError as e:
            if not self.allow_downgraded_queries:
                raise e from e
            self._connected_user = None
            msg = "query_current_user raised an authorization error: " + str(e)
            warn(msg)
        if self._connected_user is None:
            msg = f"It is recommended to run the report with a user with sysadmin rights. You are not currently connected."
            warn(msg)
        elif not self._connected_user.sysadmin:
            msg = f"It is recommended to run the report with a user with sysadmin rights. Current user: {self._connected_user.name}"
            warn(msg)
        if progress_callback is not None:
            progress_callback.add_context("Step 1: Map resources", level=CkanCallbackLevel.Packages)
        ckan.map_resources(self.package_list, datastore_info=True, only_missing=self.cancel_if_present,
                           progress_callback=progress_callback)
        try:
            ckan.organization_list_all(cancel_if_present=False, include_users=True)
        except CkanAuthorizationError as e:
            if not self.allow_downgraded_queries:
                raise e from e
            msg = "organization_list_all with include_users=True raised an authorization error. Organization users will not show in report: " + str(e)
            warn(msg)
            ckan.organization_list_all(cancel_if_present=False, include_users=False)
        ckan.license_list(cancel_if_present=self.cancel_if_present)
        if progress_callback is not None:
            progress_callback.add_context("Step 2: Request file sizes", level=CkanCallbackLevel.Resources)
        ckan.map_file_resource_sizes(cancel_if_present=self.cancel_if_present, progress_callback=progress_callback)
        if progress_callback is not None:
            progress_callback.add_context("Step 3: Request user access", level=CkanCallbackLevel.Packages)
        try:
            ckan.map_user_rights(cancel_if_present=self.cancel_if_present, progress_callback=progress_callback)
        except CkanAuthorizationError as e:
            if not self.allow_downgraded_queries:
                raise e from e
            msg = "map_user_rights with include_users=True raised an authorization error. Organization users will not show in report: " + str(e)
            warn(msg)
            ckan.group_list_all(include_users=False)
        if progress_callback is not None:
            progress_callback.remove_context()
        self._elapsed_time_requests = time.time() - start
        self._request_count = ckan.debug.ckan_request_counter - request_count_init
        if ckan.params.verbose_extra:
            print(f"Done requests for admin report ({self._elapsed_time_requests} seconds, {self._request_count} requests).")

    def _consolidate(self, ckan: CkanApi) -> None:
        for user_info in ckan.map.users.values():
            user_info.organizations = []
        for organization_info in ckan.map.organizations.values():
            if organization_info.user_members is not None:
                for user_id in organization_info.user_members.keys():
                    ckan.map.users[user_id].organizations.append(organization_info.name)

    def _create_report(self, ckan: CkanApi, *, progress_callback:CkanProgressCallbackABC=None) -> None:
        start = time.time()
        request_count_init = ckan.debug.ckan_request_counter
        policy_messages: Dict[str, List[DataPolicyError]] = {}
        if progress_callback is not None:
            progress_callback.add_context("Step 4: Policy check", level=CkanCallbackLevel.Packages)
        ckan.policy_check(buffer=policy_messages, progress_callback=progress_callback)

        report_header = OrderedDict([
            ("title", "Admin report on packages and resources"),
            ("date", self._date_format_str(self.report_date)),
            ("timestamp", self.report_date.isoformat(sep='T')),
            ("ckan", ckan.url),
            ("user", self._connected_user.name if self._connected_user is not None else None),
            ("user_sysadmin", self._connected_user.sysadmin if self._connected_user is not None else None),
            ("package_selection", self.package_list if self.package_list is not None else "All"),
        ])
        packages_report = {}
        total_policy_errors = ErrorCount([])
        total_filestore_size_mb = 0.
        total_external_size_mb = 0.
        total_datastore_size_mb = 0.
        total_resource_count = 0
        total_external_resource_count = 0
        total_datastore_count = 0
        total_datastore_lines = 0
        global_last_modified_resources = None
        global_last_modified_metadata = None
        num_packages = len(ckan.map.packages)
        if progress_callback is not None:
            progress_callback.add_context("Step 5: Create report per package", level=CkanCallbackLevel.Packages)
            progress_callback.start_task(num_packages, level=CkanCallbackLevel.Packages, units=CkanProgressUnits.Items)
        for i_package, (package_id, package_info) in enumerate(ckan.map.packages.items()):
            package_name = package_info.name
            package_data_format_messages = policy_messages.get(package_name, [])
            data_format_policy_scores = ErrorCount(package_data_format_messages)
            total_policy_errors += data_format_policy_scores
            resources_report = []
            last_modified_resource = None
            last_modified_resource_metadata = None
            package_resource_count = len(package_info.package_resources)
            package_external_resource_count = 0
            package_datastore_count = 0
            package_filestore_size_mb = 0.
            package_external_size_mb = 0.
            package_datastore_size_mb = 0.
            package_datastore_lines = 0
            for resource_id in package_info.package_resources.keys():
                resource_info = ckan.map.resources[resource_id]
                resource_modified = resource_info.last_modified if resource_info.last_modified is not None else resource_info.created
                internal_filestore = ckan.is_url_internal(resource_info.download_url)
                resource_report = OrderedDict([
                    ("resource_name", resource_info.name),
                    ("id", resource_id),
                    ("state", str(resource_info.state)),
                    ("external_url", resource_info.download_url if resource_info.download_url and not internal_filestore else None),
                    ("filestore_size_mb", resource_info.download_size_mb if internal_filestore else None),
                    ("external_size_mb", resource_info.download_size_mb if not internal_filestore else None),
                    ("datastore_size_mb", 0),
                    ("datastore_active", resource_info.datastore_active),
                    ("datastore_lines", None),
                    ("date_modified", self._date_format_str(resource_modified) if resource_modified is not None else None),
                    ("metadata_modified", self._date_format_str(resource_info.metadata_modified) if resource_info.metadata_modified is not None else None),
                    ("datastore_aliases", None),
                ])
                if resource_modified is not None:
                    last_modified_resource = max(last_modified_resource, resource_modified) \
                        if last_modified_resource else resource_modified
                    global_last_modified_resources = max(global_last_modified_resources, resource_modified) \
                        if global_last_modified_resources else resource_modified
                if resource_info.metadata_modified is not None:
                    last_modified_resource_metadata = max(last_modified_resource_metadata, resource_info.metadata_modified) \
                        if last_modified_resource_metadata else resource_info.metadata_modified
                    global_last_modified_metadata = max(global_last_modified_metadata, resource_modified) \
                        if global_last_modified_metadata else resource_modified
                if resource_info.download_url:
                    if internal_filestore:
                        if resource_info.download_size_mb is not None:
                            package_filestore_size_mb += resource_info.download_size_mb
                    else:
                        if resource_info.download_size_mb is not None:
                            package_external_size_mb += resource_info.download_size_mb
                        package_external_resource_count += 1
                if resource_info.datastore_info is not None:
                    datastore_size = round_size(resource_info.datastore_info.table_size_mb + resource_info.datastore_info.index_size_mb)
                    resource_report["datastore_aliases"] = resource_info.datastore_info.aliases
                    resource_report["datastore_size_mb"] = datastore_size
                    package_datastore_size_mb += datastore_size
                    resource_report["datastore_lines"] = resource_info.datastore_info.row_count
                    package_datastore_lines += resource_info.datastore_info.row_count
                    package_datastore_count += 1
                resources_report.append(resource_report)
            license_info = ckan.map.licenses[package_info.license_id] if package_info.license_id and package_info.license_id in ckan.map.licenses.keys() else None
            package_report = OrderedDict([
                ("package_title", package_info.title),
                ("state", str(package_info.state)),
                ("organization", package_info.organization_info.name if package_info.organization_info else None),
                ("version", package_info.version),
                ("license", license_info.title if license_info else None),
                ("license_domain", license_info.domain.to_dict() if license_info else None),
                ("author", package_info.author),
                ("maintainer", package_info.maintainer),
                ("metadata_modified", self._date_format_str(package_info.metadata_modified)),
                ("resources_modified", self._date_format_str(last_modified_resource) if last_modified_resource is not None else None),
                ("resources_metadata_modified", self._date_format_str(last_modified_resource_metadata) if last_modified_resource_metadata is not None else None),
                ("visibility", str(CkanVisibility.from_bool_is_private(package_info.private))),
                ("filestore_total_size_mb", round_size(package_filestore_size_mb)),
                ("external_total_size_mb", round_size(package_external_size_mb)),
                ("datastore_total_size_mb", round_size(package_datastore_size_mb)),
                ("datastore_total_lines", package_datastore_lines),
                ("resource_count", package_resource_count),
                ("among_resources_external", package_external_resource_count),
                ("among_resources_datastore", package_datastore_count),
                ("data_format_policy_scores", data_format_policy_scores.to_dict()),
                ("tags", package_info.tags),
            ])
            for custom_field in self.include_package_custom_fields:
                package_report[custom_field] = package_info.custom_fields.get(custom_field, None)
            package_report["users"] = []
            package_report["groups"] = []
            if self.include_resources_detail:
                package_report["resources"] = resources_report
            if package_info.private:
                users_dict = {ckan.map.users[user_id].name: collaboration.to_dict(ckan.map.users[user_id], ckan.map.groups, self.date_format)
                              for user_id, collaboration in package_info.user_access.items()}
                package_report["users"] = OrderedDict(sorted(users_dict.items()))
            else:
                # TODO: do all users have write access if package is Public
                package_report["users"] = "all (Public)"
            package_report["groups"] = sorted([group_info.name for group_info in package_info.groups])
            if self.include_policy_messages:
                package_report["policy_messages"] = [message.to_dict() for message in package_data_format_messages]
            total_filestore_size_mb += package_filestore_size_mb
            total_external_size_mb += package_external_size_mb
            total_datastore_size_mb += package_datastore_size_mb
            total_resource_count += package_resource_count
            total_external_resource_count += package_external_resource_count
            total_datastore_count += package_datastore_count
            total_datastore_lines += package_datastore_lines
            global_last_modified_metadata = max(global_last_modified_metadata, package_info.metadata_modified) \
                if global_last_modified_metadata else package_info.metadata_modified
            packages_report[package_name] = package_report
            self._package_update_report(ckan, package_info, package_report)
            if progress_callback is not None:
                progress_callback.update_task(i_package, num_packages, level=CkanCallbackLevel.Packages)
        packages_report = OrderedDict(sorted(packages_report.items()))
        report_totals = OrderedDict([
            ("total_filestore_size_mb", round_size(total_filestore_size_mb)),
            ("total_datastore_size_mb", round_size(total_datastore_size_mb)),
            ("total_external_size_mb", round_size(total_external_size_mb)),
            ("total_datastore_lines", total_datastore_lines),
            ("num_packages", len(packages_report)),
            ("total_resource_count", total_resource_count),
            ("among_resources_external", total_external_resource_count),
            ("among_resources_datastore", total_datastore_count),
            ("last_modified_data", self._date_format_str(global_last_modified_resources) if global_last_modified_resources else None),
            ("last_modified_metadata", self._date_format_str(global_last_modified_metadata) if global_last_modified_metadata else None),
            ("total_policy_errors", total_policy_errors.to_dict()),
        ])
        sysadmin_report = {user_info.name: OrderedDict([
            ("fullname", user_info.fullname),
            ("last_active", self._date_format_str(user_info.last_active) if user_info.last_active is not None else None),
            ("organizations", user_info.organizations),
        ]) for user_info in ckan.map.users.values() if user_info.sysadmin}
        sysadmin_report = OrderedDict(sorted(sysadmin_report.items()))
        users_report = {user_info.name: OrderedDict([
            ("fullname", user_info.fullname),
            ("last_active", self._date_format_str(user_info.last_active) if user_info.last_active is not None else None),
            ("organizations", user_info.organizations),
         ]) for user_info in ckan.map.users.values() if not user_info.sysadmin}
        users_report = OrderedDict(sorted(users_report.items()))
        groups_report = {group_info.name: OrderedDict([
            ("group_title", group_info.title),
            ("package_count", group_info.package_count),
            ("users_count", len(group_info.user_members) if group_info.user_members is not None else None),
            ("users", OrderedDict(sorted({ckan.map.users[user_id].name: str(capacity) for user_id, capacity in group_info.user_members.items()}.items())) if group_info.user_members is not None else None),
        ]) for group_info in ckan.map.groups.values()}
        groups_report = OrderedDict(sorted(groups_report.items()))
        elapsed_time_report_and_updates = time.time() - start
        report_footer = OrderedDict([
            ("requests_count", self._request_count),
            ("time_elapsed_seconds", self._elapsed_time_requests + elapsed_time_report_and_updates),
        ])
        report = OrderedDict([
            ("header", report_header),
            ("totals", report_totals),
            ("packages", packages_report),
            ("users", OrderedDict([
                ("sysadmins", sysadmin_report),
                ("other", users_report),
            ])),
        ])
        if self.include_group_report:
            report["groups"] = groups_report
        report["footer"] = report_footer
        self.report = report
        if ckan.params.verbose_extra:
            print(f"Done generating report ({elapsed_time_report_and_updates} seconds, {ckan.debug.ckan_request_counter - request_count_init} requests).")
        if progress_callback is not None:
            progress_callback.end_task(num_packages, level=CkanCallbackLevel.Packages)
            progress_callback.remove_context()

    def _package_update_report(self, ckan:CkanApi, package_info: CkanPackageInfo, package_report) -> None:
        policy = ckan.policy
        if policy is None:
            return
        package_update_needed = any([field_name is not None for field_name in [
            policy.output_custom_fields.package_filestore_size_field,
            policy.output_custom_fields.package_external_size_field,
            policy.output_custom_fields.package_datastore_size_field,
            policy.output_custom_fields.package_datastore_rowcount_field]])
        if package_update_needed and package_info.custom_fields is None:
            package_info.custom_fields = OrderedDict()
        package_update_needed = False
        if policy.output_custom_fields.report_timestamp_field is not None:
            report_timestamp = self.report_date.isoformat(sep='T')
            if policy.output_custom_fields.report_timestamp_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[policy.output_custom_fields.report_timestamp_field] == report_timestamp
            else:
                package_update_needed = True
            package_info.custom_fields[policy.output_custom_fields.report_timestamp_field] = report_timestamp
        if policy.output_custom_fields.package_filestore_size_field is not None:
            package_size_str = size_str_mb(package_report["filestore_total_size_mb"])
            if policy.output_custom_fields.package_filestore_size_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[policy.output_custom_fields.package_filestore_size_field] == package_size_str
            else:
                package_update_needed = True
            package_info.custom_fields[policy.output_custom_fields.package_filestore_size_field] = package_size_str
        if policy.output_custom_fields.package_external_size_field is not None:
            package_size_str = size_str_mb(package_report["external_total_size_mb"])
            if policy.output_custom_fields.package_external_size_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[policy.output_custom_fields.package_external_size_field] == package_size_str
            else:
                package_update_needed = True
            package_info.custom_fields[policy.output_custom_fields.package_external_size_field] = package_size_str
        if policy.output_custom_fields.package_datastore_size_field is not None:
            package_size_str = size_str_mb(package_report["datastore_total_size_mb"])
            if policy.output_custom_fields.package_datastore_size_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[
                                                 policy.output_custom_fields.package_datastore_size_field] == package_size_str
            else:
                package_update_needed = True
            package_info.custom_fields[policy.output_custom_fields.package_datastore_size_field] = package_size_str
        if policy.output_custom_fields.package_datastore_rowcount_field is not None:
            package_rowcount_str = str(package_report["datastore_total_lines"])
            if policy.output_custom_fields.package_datastore_rowcount_field in package_info.custom_fields.keys():
                package_update_needed |= not package_info.custom_fields[policy.output_custom_fields.package_datastore_rowcount_field] == package_rowcount_str
            else:
                package_update_needed = True
            package_info.custom_fields[policy.output_custom_fields.package_datastore_rowcount_field] = package_rowcount_str
        package_info.updated = package_update_needed
        if package_update_needed and ckan is not None:
            ckan.package_patch(package_info.id, custom_fields=package_info.custom_fields)

    def execute(self, ckan: CkanApi, *, progress_callback:CkanProgressCallbackABC=None) -> dict:
        if progress_callback is not None and not isinstance(progress_callback, CkanProgressCallbackABC):
            progress_callback = CkanProgressCallback(progress_callback)
        elif progress_callback is None:
            progress_callback = CkanProgressCallback()
            progress_callback.verbosity[CkanCallbackLevel.Resources] = False
            progress_callback.progress_bar_enables[CkanCallbackLevel.Resources] = True
        self._perform_requests(ckan, progress_callback=progress_callback)
        self._consolidate(ckan)
        self._create_report(ckan, progress_callback=progress_callback)
        return self.report

    def refresh_report(self, ckan: CkanApi, *, progress_callback:CkanProgressCallbackABC=None) -> dict:
        self._create_report(ckan, progress_callback=progress_callback)
        return self.report

    def to_jsons(self) -> str:
        return to_jsons_indent_lists_single_line(self.report)

    def to_json(self, file_path:str) -> None:
        with open(file_path, "w", encoding="utf8") as f:
            f.write(self.to_jsons())


if __name__ == '__main__':
    deauthenticate = False
    ckan = CkanApi()
    ckan.initialize_from_cli_args()
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=False)
    if deauthenticate:
        ckan.apikey.clear()

    package_list = None  # use this argument or no argument to make a full report
    # package_list = ["builder-example-py"]  # limit to the example package

    ckan.load_default_policy()
    ckan.params.verbose_extra = True

    report = CkanAdminReport(ckan=ckan, package_list=package_list, full_report=True, auto_exec=False)
    if deauthenticate:
        report.allow_downgraded_queries = True
    report.execute(ckan)
    print(report.to_jsons())

    self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    top_dir = os.path.abspath(os.path.join(self_dir, "..", ".."))
    tests_dir = os.path.abspath(os.path.join(top_dir, "..", "tests"))
    out_file = os.path.join(tests_dir, "admin_report.json")  # file for last report
    report.to_json(out_file)
    out_file = os.path.join(tests_dir, f"admin_report_{report.report_date.strftime('%Y%m%dT%H%M')}.json")  # keep history of reports
    report.to_json(out_file)

    print(f"Done. Saved report to {out_file}")
