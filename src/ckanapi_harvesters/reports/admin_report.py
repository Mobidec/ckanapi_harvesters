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
from ckanapi_harvesters.auxiliary.ckan_errors import CkanAuthorizationError, UnexpectedError
from ckanapi_harvesters.auxiliary.ckan_auxiliary import to_jsons_indent_lists_single_line, round_size, assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_model import CkanVisibility, CkanUserInfo
from ckanapi_harvesters.auxiliary.ckan_rules import package_expand_user_access
from ckanapi_harvesters.policies.policy_report import PackagePolicyReport
from ckanapi_harvesters.policies.data_format_policy_errors import ErrorCount
import ckanapi_harvesters.reports.schemas.admin_report as schema


class CkanAdminReport:
    def __init__(self, package_list:List[str]=None, *, cancel_if_present:bool=True,
                 package_extra_fields:List[str]=None, ckan:CkanApi=None, full_report:bool=False,
                 owner_org:str=None,
                 auto_exec:bool=True, progress_callback:CkanProgressCallbackABC=None):
        if package_extra_fields is None:
            package_extra_fields = []  # option to include specific extra custom fields in the report e.g. a end of license date
        if isinstance(package_list, str):
            package_list = [package_list]
        self.package_list: Union[List[str],None] = package_list
        self.resource_list: Union[List[str],None] = None
        self.cancel_if_present: bool = cancel_if_present
        self.include_package_extra_fields: List[str] = package_extra_fields
        self.include_resources_detail: bool = True
        self.include_policy_messages: bool = full_report
        self.include_group_report: bool = full_report
        self.include_public_packages_in_header: bool = True
        self.include_emails: bool = True
        self.enable_policy_check:bool = True
        self.expand_resources: bool = True
        self.expand_groups: bool = True   # expand group and organization members
        self.expand_public: bool = False  # add remaining users if package is public
        self.date_format:Union[str,None] = '%d/%m/%Y %H:%M'
        self._connected_user: Union[CkanUserInfo, None] = None
        self.report_date: Union[datetime.datetime, None] = None
        self._elapsed_time_requests: Union[float,None] = None
        self._request_count: Union[int,None] = None
        self.allow_downgraded_queries:bool = False
        self.owner_org :Union[str,None] = owner_org
        self.auto_update_ckan: bool = True  # update custom fiels on CKAN server, if specified
        self.report: Union[schema.AdminReportSchema,None] = None  # report output
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
        if self.expand_resources:
            ckan.map_resources(self.package_list, datastore_info=True, owner_org=self.owner_org,
                               only_missing=self.cancel_if_present, progress_callback=progress_callback)
        else:
            ckan.complete_package_list(package_list=self.package_list, owner_org=self.owner_org)
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
        self.resource_list = None
        if self.package_list is not None:
            self.resource_list = ckan.get_resource_ids_of_package_list(self.package_list)  # for info
        if self.expand_resources:
            ckan.map_file_resource_sizes(package_list=self.package_list, cancel_if_present=self.cancel_if_present, progress_callback=progress_callback)
            ckan._update_package_size_fields(self.package_list)
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
            user_info.groups = {}
            user_info.organizations = {}
        for group_info in ckan.map.groups.values():
            if group_info.user_capacities is not None:
                for user_id, user_capacity in group_info.user_capacities.items():
                    ckan.map.users[user_id].groups[group_info.name] = user_capacity
        for organization_info in ckan.map.organizations.values():
            if organization_info.user_members is not None:
                for user_id, user_capacity in organization_info.user_members.items():
                    ckan.map.users[user_id].organizations[organization_info.name] = user_capacity

    def _create_report(self, ckan: CkanApi, *, progress_callback:CkanProgressCallbackABC=None) -> None:
        start = time.time()
        request_count_init = ckan.debug.ckan_request_counter
        policy_messages: Dict[str, PackagePolicyReport] = {}
        if progress_callback is not None:
            progress_callback.add_context("Step 4: Policy check", level=CkanCallbackLevel.Packages)
        if not self.expand_resources:
            self.enable_policy_check = False
        if self.enable_policy_check:
            ckan.policy_check(buffer=policy_messages, progress_callback=progress_callback,
                              date_report=self.report_date, auto_update=self.auto_update_ckan)
        else:
            self.include_policy_messages = False

        report_header = schema.AdminReportHeader(
            title="Admin report on packages and resources",
            date=self.report_date,
            timestamp=self.report_date.isoformat(sep='T'),
            ckan_url=ckan.url,
            user=self._connected_user.name if self._connected_user is not None else None,
            user_sysadmin=self._connected_user.sysadmin if self._connected_user is not None else None,
            package_selection=self.package_list if self.package_list is not None else "All",
            public_packages=None,
        )
        packages_report = {}
        total_policy_errors = ErrorCount([])
        total_filestore_size_mb = 0.
        total_external_size_mb = 0.
        total_datastore_size_mb = 0.
        total_resource_count = 0
        total_filestore_count = 0
        total_external_resource_count = 0
        total_datastore_count = 0
        total_datastore_lines = 0
        public_packages = OrderedDict()
        global_last_modified_resources = None
        global_last_modified_metadata = None
        num_packages = len(ckan.map.packages)
        if progress_callback is not None:
            progress_callback.add_context("Step 5: Create report per package", level=CkanCallbackLevel.Packages)
            progress_callback.start_task(num_packages, level=CkanCallbackLevel.Packages, units=CkanProgressUnits.Items)
        for i_package, (package_id, package_info) in enumerate(ckan.map.packages.items()):
            package_name = package_info.name
            if self.enable_policy_check:
                package_policy_report = policy_messages.get(package_name, None)
                # data_format_policy_scores = ErrorCount(package_policy_report.messages)
                data_format_policy_scores = package_policy_report.error_count
                total_policy_errors += data_format_policy_scores
            else:
                package_policy_report = None
                data_format_policy_scores = None
            if self.expand_resources:
                package_size = package_info.package_size  # computed by _update_package_size_fields
                if package_size.date_last_modified_resource is not None:
                    global_last_modified_resources = max(global_last_modified_resources, package_size.date_last_modified_resource) \
                        if global_last_modified_resources else package_size.date_last_modified_resource
                if package_size.date_last_modified_resource_metadata is not None:
                    global_last_modified_metadata = max(global_last_modified_metadata, package_size.date_last_modified_resource_metadata) \
                        if global_last_modified_metadata else package_size.date_last_modified_resource_metadata
                resources_report = []
                for resource_id in package_info.package_resources.keys():
                    resource_info = ckan.map.resources[resource_id]
                    resource_modified = resource_info.last_modified if resource_info.last_modified is not None else resource_info.created
                    internal_filestore = ckan.is_url_internal(resource_info.download_url)
                    resource_report = schema.AdminReportResourceMetadata(
                        resource_name=resource_info.name,
                        id=resource_id,
                        page_url=ckan.get_resource_page_url(resource_id),
                        state=resource_info.state,
                        external_url=resource_info.download_url if resource_info.download_url and not internal_filestore else None,
                        filestore_size_mb=resource_info.download_size_mb if internal_filestore else None,
                        external_size_mb=resource_info.download_size_mb if not internal_filestore else None,
                        datastore_size_mb=0,
                        datastore_active=resource_info.datastore_active,
                        datastore_lines=None,
                        date_modified=resource_modified if resource_modified is not None else None,
                        metadata_modified=resource_info.metadata_modified if resource_info.metadata_modified is not None else None,
                        datastore_aliases=None,
                    )
                    if resource_info.datastore_info is not None:
                        datastore_size = round_size(resource_info.datastore_info.table_size_mb + resource_info.datastore_info.index_size_mb)
                        resource_report.datastore_aliases = resource_info.datastore_info.aliases
                        resource_report.datastore_size_mb = datastore_size
                        resource_report.datastore_lines = resource_info.datastore_info.row_count
                    resources_report.append(resource_report)
            else:
                resources_report = None
                package_size = None
            license_info = ckan.map.licenses[package_info.license_id] if package_info.license_id and package_info.license_id in ckan.map.licenses.keys() else None
            if package_info.creator_user_id is not None:
                user_info = ckan.map.users.get(package_info.creator_user_id, None)
                package_creator_name = user_info.name
            else:
                package_creator_name = None
            package_report = schema.AdminReportPackageSimple(
                package_name=package_name,
                package_title=package_info.title,
                page_url=ckan.get_package_page_url(package_name),
                state=package_info.state,
                organization=package_info.organization_info.name if package_info.organization_info else None,
                version=package_info.version,
                license=license_info.title if license_info else None,
                license_domain=license_info.domain.to_dict() if license_info else None,
                creator=package_creator_name,
                author=package_info.author,
                maintainer=package_info.maintainer,
                visibility=CkanVisibility.from_bool_is_private(package_info.private),
                metadata_modified=package_info.metadata_modified,
                resource_count=len(package_info.package_resources),
                tags=package_info.tags,
                extras=OrderedDict([(extra_field, package_info.custom_fields.get(extra_field, None)) for extra_field in self.include_package_extra_fields]),
                users=None,
                groups=None,
            )
            if self.expand_resources:
                package_report = schema.AdminReportPackageExtended(**package_report.__dict__,
                    resources_modified=package_size.date_last_modified_resource if package_size.date_last_modified_resource is not None else None,
                    resources_metadata_modified=package_size.date_last_modified_resource_metadata if package_size.date_last_modified_resource_metadata is not None else None,
                    among_resources_external=package_size.external_resource_count,
                    among_resources_filestore=package_size.filestore_count,
                    among_resources_datastore=package_size.datastore_count,
                    filestore_total_size_mb=round_size(package_size.filestore_size_mb),
                    external_total_size_mb=round_size(package_size.external_size_mb),
                    datastore_total_size_mb=round_size(package_size.datastore_size_mb),
                    datastore_total_lines=package_size.datastore_lines,
                    data_format_policy_scores=None,
                    resources=None,
                    policy_messages=None,
                )
                package_report.resource_count = package_size.resource_count
            if self.enable_policy_check and self.expand_resources:
                package_report.data_format_policy_scores = data_format_policy_scores
            if self.include_resources_detail:
                package_report.resources = resources_report
            package_info.user_access = package_expand_user_access(package_info, user_table=ckan.map.users,
                                                                   organization_table=ckan.map.organizations,
                                                                   group_table=ckan.map.groups,
                                                                   expand_groups=self.expand_groups,
                                                                   expand_public=self.expand_public,
                                                                   expand_excluded=False)
            if package_info.private or self.expand_public:
                users_dict = OrderedDict([(ckan.map.users[user_id].name, collaboration.to_dict(user_info=ckan.map.users[user_id],
                                      group_table=ckan.map.groups, organization_table=ckan.map.organizations, date_format=self.date_format))
                              for user_id, collaboration in package_info.user_access.items()])
                package_report.users = OrderedDict(sorted(users_dict.items()))
            else:
                # TODO: do all users have write access if package is Public
                package_report.users = "all (Public)"
                public_packages[package_name] = ckan.get_package_page_url(package_name)
            assert_or_raise(package_info.groups is not None, UnexpectedError("groups in ckan.map should not be None"))
            package_report.groups = sorted([group_info.name for group_info in package_info.groups])
            if self.include_policy_messages:
                package_report.policy_messages = [message.to_dict() for message in package_policy_report.messages]
            if self.expand_resources:
                total_filestore_size_mb += package_size.filestore_size_mb
                total_external_size_mb += package_size.external_size_mb
                total_datastore_size_mb += package_size.datastore_size_mb
                total_resource_count += package_size.resource_count
                total_filestore_count += package_size.filestore_count
                total_external_resource_count += package_size.external_resource_count
                total_datastore_count += package_size.datastore_count
                total_datastore_lines += package_size.datastore_lines
            global_last_modified_metadata = max(global_last_modified_metadata, package_info.metadata_modified) \
                if global_last_modified_metadata else package_info.metadata_modified
            packages_report[package_name] = package_report
            if progress_callback is not None:
                progress_callback.update_task(i_package, num_packages, level=CkanCallbackLevel.Packages)
        packages_report = OrderedDict(sorted(packages_report.items()))
        if self.expand_resources:
            report_totals = schema.AdminReportTotalsExtended(
                total_filestore_size_mb=round_size(total_filestore_size_mb),
                total_datastore_size_mb=round_size(total_datastore_size_mb),
                total_external_size_mb=round_size(total_external_size_mb),
                total_datastore_lines=total_datastore_lines,
                num_packages=len(packages_report),
                total_resource_count=total_resource_count,
                among_resources_filestore=total_filestore_count,
                among_resources_external=total_external_resource_count,
                among_resources_datastore=total_datastore_count,
                last_modified_data=global_last_modified_resources if global_last_modified_resources else None,
                last_modified_metadata=global_last_modified_metadata if global_last_modified_metadata else None,
                total_policy_errors=total_policy_errors,
            )
        else:
            report_totals = schema.AdminReportTotalsSimple(
                num_packages=len(packages_report),
                total_resource_count=total_resource_count,
                last_modified_metadata=global_last_modified_metadata if global_last_modified_metadata else None,
            )
        sysadmin_report = {user_info.name: schema.AdminReportUserMetadata(
            user_name=user_info.name,
            fullname=user_info.fullname,
            email=user_info.email if self.include_emails else None,
            last_active=user_info.last_active if user_info.last_active is not None else None,
            organizations=user_info.organizations,
            groups=user_info.groups,
        ) for user_info in ckan.map.users.values() if user_info.sysadmin}
        sysadmin_report = OrderedDict(sorted(sysadmin_report.items()))
        users_report = {user_info.name: schema.AdminReportUserMetadata(
            user_name=user_info.name,
            fullname=user_info.fullname,
            email=user_info.email if self.include_emails else None,
            last_active=user_info.last_active if user_info.last_active is not None else None,
            organizations=user_info.organizations,
            groups=user_info.groups,
         ) for user_info in ckan.map.users.values() if not user_info.sysadmin}
        users_report = OrderedDict(sorted(users_report.items()))
        groups_report = {group_info.name: schema.AdminReportGroupMetadata(
            group_name=group_info.name,
            group_title=group_info.title,
            package_count=group_info.package_count,
            users_count=len(group_info.user_capacities) if group_info.user_capacities is not None else None,
            users=OrderedDict(sorted({ckan.map.users[user_id].name: str(capacity) for user_id, capacity in group_info.user_capacities.items()}.items())) if group_info.user_capacities is not None else None,
        ) for group_info in ckan.map.groups.values()}
        groups_report = OrderedDict(sorted(groups_report.items()))
        organizations_report = {organization_info.name: schema.AdminReportOrganizationMetadata(
            organization_name=organization_info.name,
            organization_title=organization_info.title,
            package_count=len([package_metadata.package_name for package_metadata in packages_report.values() if package_metadata.organization == organization_info.name]),
            users_count=len(organization_info.user_members) if organization_info.user_members is not None else None,
            users=OrderedDict(sorted({ckan.map.users[user_id].name: str(capacity) for user_id, capacity in organization_info.user_members.items()}.items())) if organization_info.user_members is not None else None,
        ) for organization_info in ckan.map.organizations.values()}
        organizations_report = OrderedDict(sorted(organizations_report.items()))
        elapsed_time_report_and_updates = time.time() - start
        if self.include_public_packages_in_header:
            report_header.public_packages = public_packages
        report_footer = schema.AdminReportFooter(
            requests_count=self._request_count,
            time_elapsed_seconds=self._elapsed_time_requests + elapsed_time_report_and_updates,
        )
        report = schema.AdminReportSchema(
            header=report_header,
            totals=report_totals,
            packages=packages_report,
            users=schema.AdminReportUsersSection(
                sysadmins=sysadmin_report,
                other=users_report,
            ),
            groups=None,
            organizations=None,
            footer=report_footer,
        )
        if self.include_group_report:
            report.groups = groups_report
            report.organizations = organizations_report
        self.report = report
        if ckan.params.verbose_extra:
            print(f"Done generating report ({elapsed_time_report_and_updates} seconds, {ckan.debug.ckan_request_counter - request_count_init} requests).")
        if progress_callback is not None:
            progress_callback.end_task(num_packages, level=CkanCallbackLevel.Packages)
            progress_callback.remove_context()

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
        return to_jsons_indent_lists_single_line(self.report.to_dict(datetime_fcn=self._date_format_str))

    def to_json(self, file_path:str) -> None:
        with open(file_path, "w", encoding="utf8") as f:
            f.write(self.to_jsons())


if __name__ == '__main__':
    deauthenticate = False
    ckan = CkanApi()
    ckan.init_from_environ()
    ckan.initialize_from_cli_args()
    ckan.input_missing_info(input_args_if_necessary=True, input_owner_org=False)
    if deauthenticate:
        ckan.apikey.clear()

    package_list = None  # use this argument or no argument to make a full report
    package_list = ["builder-example-py"]  # limit to the example package

    ckan.load_default_policy()
    ckan.params.verbose_extra = True
    ckan.set_verbosity(True)

    report = CkanAdminReport(package_list=package_list, ckan=ckan, full_report=True, auto_exec=False,
                             package_extra_fields=["Access Terms"])
    if deauthenticate:
        report.allow_downgraded_queries = True
    # report.expand_resources = False  # temporary: disable requests to obtain detailed info on package resources
    report.execute(ckan)
    report_dict = report.report.to_dict(datetime_fcn=report._date_format_str)
    print(report.to_jsons())

    self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    top_dir = os.path.abspath(os.path.join(self_dir, "..", ".."))
    tests_dir = os.path.abspath(os.path.join(top_dir, "..", "tests"))
    out_file = os.path.join(tests_dir, "admin_report.json")  # file for last report
    report.to_json(out_file)
    out_file = os.path.join(tests_dir, f"admin_report_{report.report_date.strftime('%Y%m%dT%H%M')}.json")  # keep history of reports
    report.to_json(out_file)

    print(f"Done. Saved report to {out_file}")
