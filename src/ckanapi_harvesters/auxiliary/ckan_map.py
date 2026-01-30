#!python3
# -*- coding: utf-8 -*-
"""
Data model to represent a CKAN database architecture
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Union
from warnings import warn
import copy

from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageInfo, CkanResourceInfo, CkanState, CkanDataStoreInfo, \
    CkanOrganizationInfo, CkanLicenseInfo, CkanViewInfo, CkanGroupInfo, CkanUserInfo
from ckanapi_harvesters.auxiliary.ckan_errors import NotMappedObjectNameError, IntegrityError
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise


class CkanMapABC(ABC):
    @abstractmethod
    def purge(self):
        raise NotImplementedError()

    @abstractmethod
    def copy(self):
        raise NotImplementedError()

    @abstractmethod
    def to_dict(self) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def update_from_dict(self, data:dict) -> None:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def from_dict(d: dict) -> "CkanMap":
        raise NotImplementedError()


class CkanMap(CkanMapABC):
    """
    Class to store an image of the CKAN database architecture
    Auxiliary class of CkanApi
    """

    def __init__(self):
        self.packages:Dict[str,CkanPackageInfo] = {}    # package id -> info
        self.packages_id_index:Dict[str, str] = {}      # package name -> id
        self.packages_title_index:Dict[str, str] = {}   # package title -> id
        self.resources:Dict[str,CkanResourceInfo] = {}  # resource id -> info
        self.resource_alias_index:Dict[str,str] = {}    # resource alias -> id
        self.licenses:Dict[str,CkanLicenseInfo] = {}    # license id -> info
        self.licenses_title_index:Dict[str, str] = {}   # license title -> id
        self.organizations:Dict[str,CkanOrganizationInfo] = {} # organization id -> info
        self.organizations_id_index:Dict[str, str] = {} # organization name -> id
        self.organizations_title_index:Dict[str, str] = {} # organization title -> id
        self.users:Dict[str,CkanUserInfo] = {}          # user id -> info
        self.users_id_index:Dict[str, str] = {}         # user name -> id
        self.groups:Dict[str,CkanGroupInfo] = {}        # group id -> info
        self.groups_id_index:Dict[str, str] = {}        # group name -> id
        self.groups_title_index:Dict[str, str] = {}     # group title -> id
        self.organizations_listed_all:bool = False
        self.organizations_listed_all_users:bool = False
        self.users_listed_all:bool = False
        self.groups_listed_all:bool = False
        self._mapping_query_datastore_info = False      # default request for datastore_info during mapping operations
        self._mapping_query_resource_view_list = False  # False: do not request resource_view_list by default
        self._mapping_query_license_list = False        # False by default
        self._mapping_query_organization_info = False   # False by default

    def purge(self):
        """
        Erase known package mappings.

        :return:
        """
        self.packages:Dict[str,CkanPackageInfo] = {}    # package id -> info
        self.packages_id_index:Dict[str, str] = {}      # package name -> id
        self.packages_title_index:Dict[str, str] = {}   # package title -> id
        self.resources:Dict[str,CkanResourceInfo] = {}  # resource id -> info
        self.resource_alias_index:Dict[str,str] = {}    # resource alias -> id
        self.licenses:Dict[str,CkanLicenseInfo] = {}    # license id -> info
        self.licenses_title_index:Dict[str, str] = {}      # license title -> id
        self.organizations:Dict[str,CkanOrganizationInfo] = {} # organization id -> info
        self.organizations_id_index:Dict[str, str] = {} # organization name -> id
        self.organizations_title_index:Dict[str, str] = {} # organization title -> id
        self.organizations_listed_all = False

    def copy(self) -> "CkanMap":
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {"packages":[package.to_dict() for package in self.packages.values()],
                "licenses":[license.to_dict() for license in self.licenses.values()],
                "organizations":[organization.to_dict() for organization in self.organizations.values()],
                }

    def update_from_dict(self, data:dict) -> None:
        for package_dict in data["packages"]:
            self._update_package_info(CkanPackageInfo.from_dict(package_dict))
        for license_dict in data["licenses"]:
            self._update_license_info(CkanLicenseInfo.from_dict(license_dict))
        for org_dict in data["organizations"]:
            self._update_organization_info(CkanOrganizationInfo.from_dict(org_dict))

    @staticmethod
    def from_dict(d: dict) -> "CkanMap":
        map = CkanMap()
        map.update_from_dict(d)
        return map

    ## Resource ID Map navigation  ------------------
    def get_package_id(self, package_name:str, *, error_not_mapped:bool=True, search_title:bool=True) -> Union[str,None]:
        """
        Retrieve the package id for a given package name based on the package map.

        :param package_name: package name or id.
        :return:
        """
        if package_name is None:
            raise ValueError("package_name cannot be None")
        if package_name in self.packages.keys():
            # recognized package_id
            package_id = package_name
        elif package_name in self.packages_id_index.keys():
            package_id = self.packages_id_index[package_name]
        elif search_title and package_name in self.packages_title_index.keys():
            package_id = self.packages_title_index[package_name]
        elif error_not_mapped:
            raise NotMappedObjectNameError(f"Package {package_name} is not mapped or does not exist.")
        else:
            package_id = None
        return package_id

    def get_resource_id(self, resource_name:str, package_name:str=None, *, error_not_mapped:bool=True) -> Union[str,None]:
        """
        Retrieve the resource id for a given combination of (package name and resource name) based on the package map.

        :param resource_name: resource alias, name or id.
        :param package_name: package name or id (required if resource_name is a resource name). An integrity check is performed if given.
        :return:
        """
        if resource_name in self.resources.keys():
            # recognized resource_id
            resource_id = resource_name
        elif resource_name in self.resource_alias_index.keys():
            # found resource alias
            resource_id = self.resource_alias_index[resource_name]
        else:
            package_id = self.get_package_id(package_name, error_not_mapped=error_not_mapped)
            if package_id is None and not error_not_mapped:
                return None
            if resource_name in self.packages[package_id].resources_id_index.keys():
                resource_id = self.packages[package_id].resources_id_index[resource_name]
            elif error_not_mapped:
                raise NotMappedObjectNameError(f"Resource {resource_name} is not mapped or does not exist.")
            else:
                resource_id = None
        # sanity check
        if package_name is not None and resource_id is not None:
            resource_info = self.resources[resource_id]
            map_package_id = self.get_package_id(package_name, error_not_mapped=error_not_mapped)
            if map_package_id is not None:
                assert_or_raise(map_package_id == resource_info.package_id, IntegrityError("package_id"))
        return resource_id

    def get_organization_id(self, organization_name:str, *, error_not_mapped:bool=True, search_title:bool=True) -> Union[str,None]:
        """
        Retrieve the organization id for a given organization name based on the mapped data.

        :param organization_name: organization name, title or id.
        :return:
        """
        if organization_name is None:
            raise ValueError("organization_name cannot be None")
        if organization_name in self.organizations.keys():
            # recognized organization_id
            organization_id = organization_name
        elif organization_name in self.organizations_id_index.keys():
            organization_id = self.organizations_id_index[organization_name]
        elif search_title and organization_name in self.organizations_title_index.keys():
            organization_id = self.organizations_title_index[organization_name]
        elif error_not_mapped:
            raise NotMappedObjectNameError(f"Organization {organization_name} is not mapped or does not exist.")
        else:
            organization_id = None
        return organization_id

    def get_resource_info(self, resource_name:str, package_name:str=None, *, error_not_mapped:bool=True) -> Union[CkanResourceInfo,None]:
        """
        Retrieve the information on a given resource.

        :param resource_name: resource name or id.
        :param package_name: package name or id (required if resource_name is a resource name). An integrity check is performed if given.
        :return:
        """
        resource_id = self.get_resource_id(resource_name, package_name, error_not_mapped=error_not_mapped)
        if resource_id is not None:
            return self.resources[resource_id]
        else:
            return None

    def get_package_info(self, package_name:str, *, error_not_mapped:bool=True) -> Union[CkanPackageInfo,None]:
        """
        Retrieve the package info for a given package name based on the package map.

        :param package_name: package name or id.
        :return:
        """
        package_id = self.get_package_id(package_name, error_not_mapped=error_not_mapped)
        if package_id is not None:
            return self.packages[package_id]
        else:
            return None

    def get_organization_info(self, organization_name:str, *, error_not_mapped:bool=True) -> Union[CkanOrganizationInfo,None]:
        """
        Retrieve the organization info for a given organization name based on the mapped data.

        :param organization_name: organization name or id.
        :return:
        """
        organization_id = self.get_organization_id(organization_name, error_not_mapped=error_not_mapped)
        if organization_id is not None:
            return self.organizations[organization_id]
        else:
            return None

    def get_organization_for_owner_org(self, organization_name:str, *, error_not_mapped:bool=True) -> Union[CkanOrganizationInfo,None]:
        """
        Retrieve the organization name for a given organization name based on the mapped data.
        This is the field usually used for the owner_org argument. Calls CkanOrganizationInfo.get_owner_org

        :param organization_name: organization name or id.
        :return:
        """
        organization_info = self.get_organization_info(organization_name, error_not_mapped=error_not_mapped)
        if organization_info is not None:
            return organization_info.get_owner_org()
        else:
            return None

    def get_resource_package_id(self, resource_name:str, package_name:str=None, *, error_not_mapped:bool=True) -> Union[str,None]:
        """
        Retrieve the package id of a given resource.

        :param resource_name: resource name or id.
        :param package_name: package name or id (required if resource_name is a resource name). An integrity check is performed if given.
        :return:
        """
        resource_info = self.get_resource_info(resource_name, package_name, error_not_mapped=error_not_mapped)
        if resource_info is not None:
            return resource_info.package_id
        else:
            return None

    def get_datastore_info(self, resource_name:str, package_name:str=None, *, error_not_mapped:bool=True) -> Union[CkanDataStoreInfo,None]:
        """
        :param resource_name: resource name or id.
        :param package_name: package name or id (required if resource_name is a resource name). An integrity check is performed if given.
        :return:
        """
        resource_info = self.get_resource_info(resource_name, package_name, error_not_mapped=error_not_mapped)
        if resource_info is not None:
            if resource_info.datastore_info is not None:
                return resource_info.datastore_info
            elif error_not_mapped:
                raise NotMappedObjectNameError(f"DataStore of resource {resource_name} is not mapped or does not exist.")
            else:
                return None
        else:
            return None

    def get_datastore_len(self, resource_name:str, package_name:str=None, *, error_not_mapped:bool=True) -> Union[int,None]:
        """
        Retrieve the number of rows in a DataStore from the mapped data. This requires the map_resources to be called with the option datastore_info=True.

        :param resource_name: resource name or id.
        :param package_name: package name or id (required if resource_name is a resource name). An integrity check is performed if given.
        :return:
        """
        datastore_info = self.get_datastore_info(resource_name, package_name, error_not_mapped=error_not_mapped)
        if datastore_info is not None:
            return datastore_info.row_count
        else:
            return None

    def _update_datastore_len(self, resource_id:str, new_len:int) -> None:
        """
        Internal function to update the length of a DataStore without making a request.

        :param resource_id: resource id.
        :param new_len: value to replace
        """
        resource_info = self.resources[resource_id]
        package_id = resource_info.package_id
        self.resources[resource_id].datastore_info.row_count = new_len
        self.resources[resource_id].datastore_info.details["meta"]["count"] = new_len
        self.packages[package_id].package_resources[resource_id].datastore_info.row_count = new_len
        self.packages[package_id].package_resources[resource_id].datastore_info.details["meta"]["count"] = new_len

    def _update_datastore_info(self, datastore_info:CkanDataStoreInfo) -> None:
        """
        Internal function to update the length of a DataStore without making a request.
        """
        resource_id = datastore_info.resource_id
        if resource_id in self.resources.keys():
            resource_info = self.resources[resource_id]
            package_id = resource_info.package_id
            self.resources[resource_id].datastore_info = datastore_info
            self.packages[package_id].package_resources[resource_id].datastore_info = datastore_info
            self.packages[package_id].resources_id_index[resource_info.name] = resource_id
        if datastore_info is not None and datastore_info.aliases is not None:
            self.resource_alias_index.update({alias: resource_id for alias in datastore_info.aliases})

    def _update_resource_info(self, resource_info:Union[CkanResourceInfo, List[CkanResourceInfo]]) -> None:
        """
        Internal function to update the length of a DataStore without making a request.
        """
        if not(isinstance(resource_info, list)):
            resource_info = [resource_info]
        for res_info in resource_info:
            resource_id = res_info.id
            package_id = res_info.package_id
            res_info.index_in_package = None
            if package_id in self.packages.keys():
                self.packages[package_id].update_resource(res_info)
            self.resources[resource_id] = res_info
            if res_info.datastore_info is not None and res_info.datastore_info.aliases is not None:
                self.resource_alias_index.update({alias: res_info.id for alias in res_info.datastore_info.aliases})

    def _update_view_info(self, view_info:Union[CkanViewInfo, List[CkanViewInfo]], view_list:bool=False) -> None:
        if isinstance(view_info, CkanViewInfo):
            view_info = [view_info]
        for view_info_update in view_info:
            resource_id = view_info_update.resource_id
            self.resources[resource_id].update_view(view_info_update, view_list=view_list)

    def _update_package_info(self, package_info:Union[CkanPackageInfo, List[CkanPackageInfo]]) -> None:
        """
        Internal function to update the information of a package.

        NB: the indicator pkg_info.requested_datastore_info remains False until map_resources is called.
        """
        if not(isinstance(package_info, list)):
            package_info = [package_info]
        # already done by __init__:
        # for pkg_info in package_info:
        #     pkg_info.resources_id_index.update({resource_info.name: resource_info.id for resource_info in pkg_info.resources})
        self.packages.update({pkg_info.id: pkg_info for pkg_info in package_info})
        self.packages_id_index.update({pkg_info.name: pkg_info.id for pkg_info in package_info})
        self.packages_title_index.update({pkg_info.title: pkg_info.id for pkg_info in package_info})
        for pkg_info in package_info:
            self.resources.update({resource_info.id: resource_info for resource_info in pkg_info.package_resources.values()})
            for resource_info in pkg_info.package_resources.values():
                if resource_info.datastore_info is not None and resource_info.datastore_info.aliases is not None:
                    self.resource_alias_index.update({alias: resource_info.id for alias in resource_info.datastore_info.aliases})
        for pkg_info in package_info:
            if pkg_info.organization_info is not None:
                self._update_organization_info(pkg_info.organization_info)
            if pkg_info.groups is not None:
                self._update_group_info(pkg_info.groups)


    def get_license_id(self, license_name: str, *, error_not_mapped: bool = True) -> str:
        """
        Retrieve the ID of a license based on the mapped data.

        :param license_name: license title or id.
        :return:
        """
        if license_name is None:
            raise ValueError("license_name cannot be None")
        if license_name in self.licenses.keys():
            # recognized license_id
            license_id = license_name
        elif license_name in self.licenses_title_index.keys():
            license_id = self.licenses_title_index[license_name]
        elif error_not_mapped:
            raise NotMappedObjectNameError(f"License {license_name} is not mapped or does not exist.")
        else:
            license_id = None
        return license_id

    def get_license_info(self, license_name: str, *, error_not_mapped: bool = True) -> Union[CkanLicenseInfo,None]:
        """
        Retrieve the information on a license based on the mapped data.

        :param license_name: license title or id.
        :return:
        """
        license_id = self.get_license_id(license_name, error_not_mapped=error_not_mapped)
        if license_id is not None:
            return self.licenses[license_id]
        else:
            return None

    def _update_license_info(self, license_info: Union[CkanLicenseInfo, List[CkanLicenseInfo]]) -> None:
        """
        Internal function to update the information on a license.
        """
        if not (isinstance(license_info, list)):
            license_info = [license_info]
        self.licenses.update({license.id: license for license in license_info})
        self.licenses_title_index.update({license.title: license.id for license in license_info})

    ## Package record changes  ------------------
    def _record_package_update(self, pkg_info: CkanPackageInfo) -> None:
        package_id = pkg_info.id
        package_name = pkg_info.name
        self.packages[package_id].update(pkg_info)
        self.packages_id_index[package_name] = package_id
        self.packages_title_index[pkg_info.title] = package_id

    def _record_package_create(self, pkg_info: CkanPackageInfo) -> None:
        package_id = pkg_info.id
        package_name = pkg_info.name
        self.packages[package_id] = pkg_info
        self.packages_id_index[package_name] = package_id
        self.packages_title_index[pkg_info.title] = package_id

    def _record_package_delete_state(self, package_id: str) -> None:
        # only pass in delete state
        pkg_info = self.get_package_info(package_id, error_not_mapped=False)
        if pkg_info is not None:
            pkg_info.state = CkanState.Deleted

    def _record_package_purge_removal(self, package_id:str) -> None:
        # purge = full removal
        pkg_info = self.get_package_info(package_id, error_not_mapped=False)
        if pkg_info is None:
            return
        if package_id in self.packages.keys():
            self.packages.pop(package_id)
        if pkg_info.name in self.packages_id_index.keys():
            self.packages_id_index.pop(pkg_info.name)
            self.packages_title_index.pop(pkg_info.title)

    ## Resource record changes  ------------------
    def _record_resource_update(self, resource_info:CkanResourceInfo) -> None:
        resource_id = resource_info.id
        new_resource = resource_id not in self.resources.keys()
        self.resources[resource_id] = resource_info
        if new_resource:
            self.packages[resource_info.package_id].package_resources[resource_id] = resource_info
        self.packages[resource_info.package_id].resources_id_index[resource_info.name] = resource_id

    def _record_resource_create(self, resource_info:CkanResourceInfo) -> None:
        resource_id = resource_info.id
        new_resource = resource_id not in self.resources.keys()
        self.resources[resource_id] = resource_info
        if new_resource:
            self.packages[resource_info.package_id].package_resources[resource_id] = resource_info
        self.packages[resource_info.package_id].resources_id_index[resource_info.name] = resource_id

    def _record_resource_delete(self, resource_id:str) -> None:
        if resource_id not in self.resources.keys():
            msg = f"Resource {resource_id} not found in mapped objects"
            warn(msg)
            return
        resource_info = self.resources[resource_id]
        self.resources.pop(resource_id)
        if resource_id in self.packages[resource_info.package_id].package_resources.keys():
            self.packages[resource_info.package_id].package_resources.pop(resource_id)
        if resource_info.name in self.packages[resource_info.package_id].resources_id_index.keys():
            self.packages[resource_info.package_id].resources_id_index.pop(resource_info.name)

    def _record_datastore_delete(self, resource_id:str) -> None:
        if resource_id not in self.resources.keys():
            msg = f"DataStore {resource_id} not found in mapped objects"
            warn(msg)
            return
        resource_info = self.resources[resource_id]
        resource_info.datastore_info = None
        if resource_id in self.packages[resource_info.package_id].package_resources.keys():
            self.packages[resource_info.package_id].package_resources[resource_id].datastore_info = None

    ## Organization record changes  ------------------
    def _update_organization_info(self, organization_info:Union[CkanOrganizationInfo, List[CkanOrganizationInfo]]) -> None:
        """
        Internal function to update information on an organization.
        """
        if not(isinstance(organization_info, list)):
            organization_info = [organization_info]
        self.organizations.update({info.id: info for info in organization_info})
        self.organizations_id_index.update({info.name: info.id for info in organization_info})
        self.organizations_title_index.update({info.title: info.id for info in organization_info})

    ## Group and users record changes  ------------------
    def _update_group_info(self, group_info:Union[CkanGroupInfo, List[CkanGroupInfo]]) -> None:
        """
        Internal function to update information on a group.
        """
        if not(isinstance(group_info, list)):
            group_info = [group_info]
        self.groups.update({info.id: info for info in group_info})
        self.groups_id_index.update({info.name: info.id for info in group_info})
        self.groups_title_index.update({info.title: info.id for info in group_info})

    def _update_user_info(self, user_info:Union[CkanUserInfo, List[CkanUserInfo]]) -> None:
        """
        Internal function to update information on a group.
        """
        if not(isinstance(user_info, list)):
            user_info = [user_info]
        self.users.update({info.id: info for info in user_info})
        self.users_id_index.update({info.name: info.id for info in user_info})


