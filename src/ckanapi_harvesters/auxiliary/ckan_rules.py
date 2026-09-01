#!python3
# -*- coding: utf-8 -*-
"""
Data model to represent a CKAN database architecture
"""
from typing import Dict, Union
from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageInfo, CkanUserInfo, CkanGroupInfo, CkanCollaboration, \
    CkanOrganizationInfo, CkanCapacity


def package_expand_user_access(package_info: CkanPackageInfo, *, user_table: Dict[str, CkanUserInfo],
                               group_table: Dict[str, CkanGroupInfo], organization_table: Dict[str, CkanOrganizationInfo],
                               change_creator: bool = True, expand_groups: bool = True, expand_public: bool = True,
                               expand_excluded: bool = False) -> Union[Dict[str, CkanCollaboration], None]:
    """
    List all users having access to this package with their rights. Users are listed by their ID.
    Pre-requisites: having mapped all users, necessary groups and organizations and called ckan.map_user_rights.

    :param package_info: Package attributes
    :param user_table: Mapped user table
    :param group_table: Mapped group table
    :param organization_table: Mapped organization table
    :param change_creator: Whether to mark the user as creator if already present as a member
    :param expand_groups: Whether to expand group and organization memberships
    :param expand_public: Whether to include all remaining users if package is not private
    :param expand_excluded: Whether to include all remaining users as excluded if package is private
    """
    if package_info.collaborators is None:
        return None
    expanded_user_access = package_info.collaborators.copy()
    if package_info.creator_user_id is not None:
        user_info = user_table.get(package_info.creator_user_id, None)
        if user_info.id in expanded_user_access.keys():
            collaboration = expanded_user_access[user_info.id]
            if change_creator:
                collaboration.capacity = max(collaboration.capacity, CkanCapacity.Owner)
        else:
            expanded_user_access[user_info.id] = CkanCollaboration(capacity=CkanCapacity.Owner)
    if expand_groups:
        for package_group_info in package_info.groups:
            # obtain full group info from ckan map and add users
            group_info = group_table.get(package_group_info.id, None)
            if group_info.user_capacities is not None:
                for user_id, capacity in group_info.user_capacities.items():
                    user_info = user_table.get(user_id, None)
                    if user_info.id in expanded_user_access.keys():
                        collaboration = expanded_user_access[user_info.id]
                        if capacity > collaboration.capacity:
                            collaboration.capacity = capacity
                            collaboration.group_id = group_info.id
                    else:
                        collaboration = CkanCollaboration(capacity=capacity, group_id=package_group_info.id)
                        expanded_user_access[user_info.id] = collaboration
        # obtain full organization info from ckan map and add users
        organization_info = organization_table.get(package_info.organization_info.id, None)
        if organization_info.user_members is not None:
            for user_id, capacity in organization_info.user_members.items():
                user_info = user_table.get(user_id, None)
                if user_info.id in expanded_user_access.keys():
                    collaboration = expanded_user_access[user_info.id]
                    if capacity > collaboration.capacity:
                        collaboration.capacity = capacity
                        collaboration.organization_id = organization_info.id
                else:
                    collaboration = CkanCollaboration(capacity=capacity, organization_id=organization_info.id)
                    expanded_user_access[user_info.id] = collaboration
    if package_info.private:
        remaining_capacity = CkanCapacity.Excluded
    else:
        remaining_capacity = CkanCapacity.Public
    if (expand_public and not package_info.private) or (expand_excluded and package_info.private):
        for user_info in user_table.values():
            if user_info.name not in expanded_user_access.keys():
                expanded_user_access[user_info.id] = CkanCollaboration(capacity=remaining_capacity)
    return expanded_user_access


