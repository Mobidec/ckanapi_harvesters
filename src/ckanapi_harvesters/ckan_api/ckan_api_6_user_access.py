#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Union
import copy

from ckanapi_harvesters.auxiliary.ckan_model import (CkanCapacity, CkanCollaboration,
                                                     CkanGroupInfo, CkanUserInfo)
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType
from ckanapi_harvesters.auxiliary.ckan_action import CkanActionNotFoundError
from ckanapi_harvesters.auxiliary.ckan_errors import (AdminFeatureLockedError, UnexpectedError)
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiManage


class CkanApiUserAccess(CkanApiManage):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements requests to modify user access rights.
    """

    def _user_list_extract_ids(self, users: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]]) -> List[str]:
        if not(isinstance(users, list)):
            users = [users]
        user_ids = [None]*len(users)
        for i, user in enumerate(users):
            if isinstance(user, CkanUserInfo):
                user_ids[i] = user.id
            else:
                user_ids[i] = self.get_user_id_or_request(user)
        return user_ids

    ## User package collaborations
    def _api_package_collaborator_create(self, package_id: str, user_id: str, capacity: Union[CkanCapacity,str],
                                         *, bypass_admin:bool=True, params:dict=None) -> CkanCollaboration:
        """
        Call to API package_collaborator_create
        Adds a user or changes its capacities on a package.

        :param user_id: user id
        :param capacity: member or editor
        """
        assert_or_raise(bypass_admin or self.params.enable_admin, AdminFeatureLockedError())
        if params is None:
            params = {}
        params["id"] = package_id
        params["user_id"] = user_id
        params["capacity"] = str(capacity)
        response = self._api_action_request(f"package_collaborator_create", method=RequestType.Post, json=params)
        if response.success:
            # update map
            collaboration = CkanCollaboration(d=response.result)
            if package_id in self.map.packages.keys() and self.map.packages[package_id].user_access is not None:
                self.map.packages[package_id].user_access[user_id] = collaboration
            return collaboration.copy()
        else:
            raise response.default_error(self)

    def package_collaborator_create(self, package_id: str, user_name: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]],
                                    capacity: Union[CkanCapacity,str],
                                    *, bypass_admin:bool=True, params:dict=None) -> List[CkanCollaboration]:
        """
        Function alias of _api_package_collaborator_create.
        Adds a user or changes its capacities on a package.

        :param user_name: user id or name (list or element)
        :param capacity: member or editor
        """
        user_ids = self._user_list_extract_ids(user_name)
        out_list = [None]*len(user_ids)
        for i, user_id in enumerate(user_ids):
            out_list[i] = self._api_package_collaborator_create(package_id=package_id, user_id=user_id, capacity=capacity,
                                                         bypass_admin=bypass_admin, params=params)
        return out_list

    def _api_package_collaborator_delete(self, package_id: str, user_id: str,
                                         *, bypass_admin:bool=True, params:dict=None) -> None:
        """
        Call to API package_collaborator_delete
        Deletes a user from the collaborator list of a package
        """
        assert_or_raise(bypass_admin or self.params.enable_admin, AdminFeatureLockedError())
        if params is None:
            params = {}
        params["id"] = package_id
        params["user_id"] = user_id
        response = self._api_action_request(f"package_collaborator_delete", method=RequestType.Post, json=params)
        if response.success:
            # update map
            if (package_id in self.map.packages.keys() and self.map.packages[package_id].user_access is not None
                    and user_id in self.map.packages[package_id].user_access.keys()):
                self.map.packages[package_id].user_access.pop(user_id)
            return response.result
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            user_info = self.user_show(user_id)  # will trigger another error if user does not exist
            raise CkanActionNotFoundError(self, "User package collaboration", response)
        else:
            raise response.default_error(self)

    def package_collaborator_delete(self, package_id: str, user_name: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]],
                                    *, bypass_admin:bool=True, collaboration_not_found_error:bool=False, params:dict=None) -> List[None]:
        # Function alias with user identification
        user_ids = self._user_list_extract_ids(user_name)
        out_list = [None]*len(user_ids)
        for i, user_id in enumerate(user_ids):
            try:
                out_list[i] = self._api_package_collaborator_delete(package_id=package_id, user_id=user_id,
                                                                    bypass_admin=bypass_admin, params=params)
            except CkanActionNotFoundError as e:
                package_info = self.get_package_info_or_request(package_id)
                if package_info.id not in self.map.packages.keys():
                    raise e from e  # it is the package which was not found
                elif collaboration_not_found_error:
                    raise e from e
        return out_list

    ## User group membership
    def _api_group_member_create(self, group_id: str, user_id: str, role: Union[CkanCapacity,str],
                                 *, bypass_admin:bool=True, params:dict=None) -> CkanCollaboration:
        """
        Call to API group_member_create
        Adds a user or changes its capacities on a group.

        :param user_id: user id
        :param role: member, editor or admin
        """
        assert_or_raise(bypass_admin or self.params.enable_admin, AdminFeatureLockedError())
        if params is None:
            params = {}
        params["id"] = group_id
        params["username"] = user_id
        params["role"] = str(role)
        response = self._api_action_request(f"group_member_create", method=RequestType.Post, json=params)
        if response.success:
            collaboration = CkanCollaboration(d=response.result)  # only for output object
            if group_id in self.map.groups.keys() and self.map.groups[group_id].user_capacities is not None:
                self.map.groups[group_id].user_dict[user_id] = self.get_user_info_or_request(user_id)
                self.map.groups[group_id].user_capacities[user_id] = role
            return collaboration
        else:
            raise response.default_error(self)

    def group_member_create(self, group_name: str, user_name: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]],
                            role: Union[CkanCapacity,str],
                            *, bypass_admin:bool=True, params:dict=None) -> List[CkanCollaboration]:
        """
        Function alias of _api_package_collaborator_create.
        Adds a user or changes its capacities on a gropu.

        :param: group name or id
        :param user_name: user id or name (list or element)
        :param role: member, editor or admin
        """
        group_info = self.get_group_info_or_request(group_name)
        group_id = group_info.id
        user_ids = self._user_list_extract_ids(user_name)
        out_list = [None]*len(user_ids)
        for i, user_id in enumerate(user_ids):
            out_list[i] = self._api_group_member_create(group_id=group_id, user_id=user_id, role=role,
                                                        bypass_admin=bypass_admin, params=params)
        return out_list

    def _api_group_member_delete(self, group_id: str, user_id: str,
                                 *, bypass_admin:bool=True, params:dict=None) -> None:
        """
        Call to API group_member_delete
        Deletes a user from the collaborator list of a group
        """
        assert_or_raise(bypass_admin or self.params.enable_admin, AdminFeatureLockedError())
        if params is None:
            params = {}
        params["id"] = group_id
        params["username"] = user_id
        response = self._api_action_request(f"group_member_delete", method=RequestType.Post, json=params)
        if response.success:
            # update map
            if (group_id in self.map.groups.keys() and self.map.groups[group_id].user_capacities is not None
                    and user_id in self.map.groups[group_id].user_capacities.keys()):
                self.map.groups[group_id].user_capacities.pop(user_id)
                self.map.groups[group_id].user_dict.pop(user_id)
            return response.result
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            # does not occur even if user was not initially member of the group
            user_info = self.user_show(user_id)  # will trigger another error if user does not exist
            raise CkanActionNotFoundError(self, "User group member", response)
        else:
            raise response.default_error(self)

    def group_member_delete(self, group_name: str, user_name: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]],
                                    *, bypass_admin:bool=True, params:dict=None) -> List[None]:
        # Function alias with user identification
        group_info = self.get_group_info_or_request(group_name)
        group_id = group_info.id
        user_ids = self._user_list_extract_ids(user_name)
        out_list = [None]*len(user_ids)
        for i, user_id in enumerate(user_ids):
            out_list[i] = self._api_group_member_delete(group_id=group_id, user_id=user_id,
                                                        bypass_admin=bypass_admin, params=params)
        return out_list

    ## User organization membership
    def _api_organization_member_create(self, organization_id: str, user_id: str, role: Union[CkanCapacity,str],
                                 *, bypass_admin:bool=True, params:dict=None) -> CkanCollaboration:
        """
        Call to API organization_member_create
        Adds a user or changes its capacities on an organization.

        :param user_id: user id
        :param role: member, editor or admin
        """
        assert_or_raise(bypass_admin or self.params.enable_admin, AdminFeatureLockedError())
        if params is None:
            params = {}
        params["id"] = organization_id
        params["username"] = user_id
        params["role"] = str(role)
        response = self._api_action_request(f"organization_member_create", method=RequestType.Post, json=params)
        if response.success:
            collaboration = CkanCollaboration(d=response.result)  # only for output object
            if organization_id in self.map.organizations.keys() and self.map.organizations[organization_id].user_members is not None:
                self.map.organizations[organization_id].user_members[user_id] = role
            return collaboration
        else:
            raise response.default_error(self)

    def organization_member_create(self, organization_name: str, user_name: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]],
                                   role: Union[CkanCapacity,str],
                                   *, bypass_admin:bool=True, params:dict=None) -> List[CkanCollaboration]:
        """
        Function alias of _api_package_collaborator_create.
        Adds a user or changes its capacities on an organization.

        :param: group name or id
        :param user_name: user id or name (list or element)
        :param role: member, editor or admin
        """
        organization_info = self.get_organization_info_or_request(organization_name)
        organization_id = organization_info.id
        user_ids = self._user_list_extract_ids(user_name)
        out_list = [None]*len(user_ids)
        for i, user_id in enumerate(user_ids):
            out_list[i] = self._api_organization_member_create(organization_id=organization_id, user_id=user_id, role=role,
                                                               bypass_admin=bypass_admin, params=params)
        return out_list

    def _api_organization_member_delete(self, organization_id: str, user_id: str,
                                        *, bypass_admin:bool=True, params:dict=None) -> None:
        """
        Call to API organization_member_delete
        Deletes a user from the collaborator list of an organization
        """
        assert_or_raise(bypass_admin or self.params.enable_admin, AdminFeatureLockedError())
        if params is None:
            params = {}
        params["id"] = organization_id
        params["username"] = user_id
        response = self._api_action_request(f"organization_member_delete", method=RequestType.Post, json=params)
        if response.success:
            # update map
            if (organization_id in self.map.organizations.keys() and self.map.organizations[organization_id].user_members is not None
                    and user_id in self.map.organizations[organization_id].user_members.keys()):
                self.map.organizations[organization_id].user_members.pop(user_id)
            return response.result
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            # does not occur even if user was not initially member of the group
            user_info = self.user_show(user_id)  # will trigger another error if user does not exist
            raise CkanActionNotFoundError(self, "User group member", response)
        else:
            raise response.default_error(self)

    def organization_member_delete(self, organization_name: str, user_name: Union[str,CkanUserInfo,List[Union[str,CkanUserInfo]]],
                                    *, bypass_admin:bool=True, params:dict=None) -> List[None]:
        # Function alias with user identification
        organization_info = self.get_organization_info_or_request(organization_name)
        organization_id = organization_info.id
        user_ids = self._user_list_extract_ids(user_name)
        out_list = [None]*len(user_ids)
        for i, user_id in enumerate(user_ids):
            out_list[i] = self._api_organization_member_delete(organization_id=organization_id, user_id=user_id,
                                                               bypass_admin=bypass_admin, params=params)
        return out_list

    ## Package group membership (using API package_patch)
    def _list_groups_extract_ids(self, groups:List[Union[dict,str,CkanGroupInfo]]) -> List[str]:
        if not(isinstance(groups, list)):
            groups = [groups]
        group_ids = copy.deepcopy(list(groups))
        for i, group_dict in enumerate(group_ids):
            if isinstance(group_dict, CkanGroupInfo):
                group_ids[i] = group_dict.id
            elif isinstance(group_dict, dict):
                if "id" not in group_dict:
                    group_info = self.map.groups_id_index[group_dict["name"]]
                    group_dict["id"] = group_info.id
                    group_dict.pop("name")
                group_ids[i] = group_dict["id"]
            else:  # str
                group_info = self.get_group_info_or_request(group_dict)
                group_ids[i] = group_info.id
        return group_ids

    def package_group_add(self, package_id:str, groups:Union[dict,str,CkanGroupInfo,List[Union[dict,str,CkanGroupInfo]]]) \
            -> None:
        if not(isinstance(groups, list)):
            groups = [groups]
        package_info = self.get_package_info_or_request(package_id)
        assert_or_raise(package_info.groups is not None, UnexpectedError("groups in ckan.map should not be None"))
        current_groups = {group_info.id for group_info in package_info.groups}
        add_group_ids = self._list_groups_extract_ids(groups)
        new_groups = current_groups.copy()
        new_groups = new_groups.union(set(add_group_ids))
        update_needed = len(new_groups - current_groups) > 0
        if update_needed:
            self._api_package_patch(package_id, groups=list(new_groups))

    def package_group_remove(self, package_id:str, groups:Union[dict,str,CkanGroupInfo,List[Union[dict,str,CkanGroupInfo]]]) \
            -> None:
        if not(isinstance(groups, list)):
            groups = [groups]
        package_info = self.get_package_info_or_request(package_id)
        remove_group_ids = self._list_groups_extract_ids(groups)
        assert_or_raise(package_info.groups is not None, UnexpectedError("groups in ckan.map should not be None"))
        current_groups = {group_info.id for group_info in package_info.groups}
        new_groups = current_groups.copy()
        new_groups = new_groups - set(remove_group_ids)
        update_needed = len(current_groups - new_groups) > 0
        if update_needed:
            self._api_package_patch(package_id, groups=list(new_groups))


