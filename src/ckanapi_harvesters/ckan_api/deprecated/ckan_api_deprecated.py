#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List
import copy
from warnings import warn

from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageInfo, CkanResourceInfo, CkanViewInfo
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType, assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_action import CkanNotFoundError
from ckanapi_harvesters.auxiliary.ckan_errors import ReadOnlyError
from ckanapi_harvesters.ckan_api.ckan_api_1_map import use_ckan_owner_org_as_default

from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiManage



class CkanApiDeprecated(CkanApiManage):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements API calls which are not recommended to use.
    """

    def copy(self, new_identifier: str = None, *, dest=None):
        if dest is None:
            dest = CkanApiDeprecated()
        super().copy(new_identifier=new_identifier, dest=dest)
        return dest

    ## Not recommended mapping functions ------------------v
    def _api_package_list(self, *, params:dict=None, owner_org:str=None, limit:int=None, offset:int=None) -> List[str]:
        """
        __Not recommended__
        API call to package_list.
        :param params: typically, the request can be limited to an organization with the owner_org parameter
        :return:
        """
        msg = DeprecationWarning("Prefer using package_search rather than package_list because this API does not list private packages")
        warn(msg)
        if params is None: params = {}
        if limit is None: limit = self.params.default_limit_list
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if owner_org is None and use_ckan_owner_org_as_default:
            owner_org = self.owner_org
        if owner_org is not None:
            params["owner_org"] = owner_org
        response = self._api_action_request("package_list", method=RequestType.Get, params=params)
        if response.success:
            return response.result
        else:
            raise response.default_error(self)

    def _api_package_list_all(self, *, params:dict=None, owner_org:str=None, limit:int=None, offset:int=None) -> List[str]:
        """
        __Not recommended__
        API call to package_list until an empty list is received.
        :see: api_package_list()
        :param params:
        :return:
        """
        msg = DeprecationWarning("Prefer using package_search rather than package_list because this API does not list private packages")
        warn(msg)
        if params is None: params = {}
        responses = self._request_all_results_list(self._api_package_list, params=params, owner_org=owner_org, limit=limit, offset=offset)
        return sum(responses, [])

    package_list_all = _api_package_list_all  # function alias


    def _api_resource_search(self, query:str=None, *, order_by:str=None, limit:int=None, offset:int=None,
                             resource_name:str=None,
                             datastore_info:bool=None, resource_view_list:bool=None,
                             params:dict=None) -> List[CkanResourceInfo]:
        """
        __Not recommended__
        API call to resource_search. It is more recommended to use the package_show API because it is not possible to
        filter the resources by package name here. Moreover, it does not return information on private resources.
        :see: map_resources()
        :param query: (string or list of strings of the form {field}:{term1}) – The search criteria. See above for description.
        :param order_by: A field on the Resource model that orders the results.
        :param limit:
        :param offset:
        :param resource_name: a shortcut to add the filter "name:{resource_name}"
        :param datastore_info: an option to query the datastore info for all the resources found.
        If not provided, the last value for this option used with map_resources will be used.
        :param resource_view_list: an option to query the resource views list for all the resources found.
        If not provided, the last value for this option used with map_resources will be used.
        :param params: additional parameters to pass to resource_search
        :return:
        """
        msg = DeprecationWarning("Prefer using package_search rather than resource_search because resource_search cannot filter per package")
        warn(msg)
        if datastore_info is None:
            datastore_info = self.map._mapping_query_datastore_info
        if resource_view_list is None:
            resource_view_list = self.map._mapping_query_resource_view_list
        if params is None: params = {}
        if limit is None: limit = self.params.default_limit_list
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if query is None:
            query = []
        elif isinstance(query, str):
            query = [query]
        if resource_name is not None:
            query.append(f"name:{resource_name}")
        if query is not None:
            params["query"] = query
        if order_by is not None:
            params["order_by"] = order_by
        response = self._api_action_request("resource_search", method=RequestType.Get, params=params)
        if response.success:
            resource_info_list = [CkanResourceInfo(e) for e in response.result["results"]]
            for resource_info in resource_info_list:
                self._enrich_resource_info(resource_info, datastore_info=datastore_info,
                                           resource_view_list=resource_view_list)
            self.map._update_resource_info(resource_info_list)
            return copy.deepcopy(resource_info_list)
        else:
            raise response.default_error(self)

    def _api_resource_search_all(self, query: str = None, *, order_by: str = None, limit: int = None, offset: int = None,
                                 resource_name: str = None,
                                 datastore_info: bool = None, resource_view_list: bool = None,
                                 params: dict = None) -> List[CkanResourceInfo]:
        """
        __Not recommended__
        API call to resource_search until an empty list is received. It is more recommended to use the package_show API because it is not possible to
        filter the resources by package name here. Moreover, it does not return information on private resources.
        :see: map_resources()
        :see: _api_resource_search()
        :param query: (string or list of strings of the form {field}:{term1}) – The search criteria. See above for description.
        :param order_by: A field on the Resource model that orders the results.
        :param limit: maximum number of results to return.
        :param offset: the offset in the complete result for where the set of returned datasets should begin.
        :param resource_name: a shortcut to add the filter "name:{resource_name}"
        :param datastore_info: an option to query the datastore info for all the resources found.
        If not provided, the last value for this option used with map_resources will be used.
        :param resource_view_list: an option to query the resource views list for all the resources found.
        If not provided, the last value for this option used with map_resources will be used.
        :param params: additional parameters to pass to resource_search
        :return:
        """
        msg = DeprecationWarning("Prefer using package_search rather than resource_search because resource_search cannot filter per package")
        warn(msg)
        if params is None: params = {}
        responses = self._request_all_results_list(self._api_resource_search, params=params, limit=limit, offset=offset,
                                                   query=query, order_by=order_by,
                                                   resource_name=resource_name,
                                                   datastore_info=datastore_info, resource_view_list=resource_view_list)
        return sum(responses, [])

    resource_search_all = _api_resource_search_all  # function alias


    def _api_group_package_show(self, group_name: str, *, params:dict=None, owner_org:str=None,
                                include_private:bool=True, include_drafts:bool=False, sort:str=None,
                                limit:int=None, offset:int=None) -> List[CkanPackageInfo]:
        """
        __Not recommended__
        API call to group_package_show. Return the datasets (packages) of a group.
        :param group_name: group name or id
        :param owner_org: ability to filter packages by owner_org
        :param include_private: if True, private datasets will be included in the results. Only private datasets from the user’s organizations will be returned and sysadmins will be returned all private datasets. Optional, the default is False in the API
        :param include_drafts:  if True, draft datasets will be included in the results. A user will only be returned their own draft datasets, and a sysadmin will be returned all draft datasets. Optional, the default is False.
        :param sort: sorting of the search results. Optional. Default: 'score desc, metadata_modified desc'. As per the solr documentation, this is a comma-separated string of field names and sort-orderings.
        :param limit: maximum number of results to return. Translatees to the API rows argument.
        :param offset: the offset in the complete result for where the set of returned datasets should begin. Translatees to the API start argument.
        :param params: other parameters to pass to package_search
        :return:
        """
        msg = DeprecationWarning("Prefer using package_search rather than group_package_show knowing the name of the package because this API does not list private packages")
        warn(msg)
        if params is None: params = {}
        params["id"] = group_name
        if limit is None: limit = self.params.default_limit_list
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if owner_org is None and use_ckan_owner_org_as_default:
            owner_org = self.owner_org
        if owner_org is not None:
            owner_org_info = self.get_organization_info_or_request(owner_org)
            owner_org = owner_org_info.id
            params["owner_org"] = owner_org
        if sort is not None:
            params["sort"] = sort
        if include_private is not None:
            params["include_private"] = include_private
        if include_drafts is not None:
            params["include_drafts"] = include_drafts
        response = self._api_action_request("group_package_show", method=RequestType.Get, params=params)
        if response.success:
            package_info_list = [CkanPackageInfo(e) for e in response.result]
            self.map._update_package_info(package_info_list)
            return package_info_list
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "Group", response)
        else:
            raise response.default_error(self)

    def _api_group_package_show_all(self, group_name: str, *, params:dict=None, owner_org:str=None,
                                    include_private:bool=True, include_drafts:bool=False, sort:str=None,
                                    limit:int=None, offset:int=None) -> List[CkanPackageInfo]:
        """
        __Not recommended__
        API call to group_package_show until an empty list is received.
        :see: _api_group_package_show()
        :param group_name: group name or id
        :param owner_org: ability to filter packages by owner_org
        :param include_private: if True, private datasets will be included in the results. Only private datasets from the user’s organizations will be returned and sysadmins will be returned all private datasets. Optional, the default is False in the API
        :param include_drafts:  if True, draft datasets will be included in the results. A user will only be returned their own draft datasets, and a sysadmin will be returned all draft datasets. Optional, the default is False.
        :param sort: sorting of the search results. Optional. Default: 'score desc, metadata_modified desc'. As per the solr documentation, this is a comma-separated string of field names and sort-orderings.
        :param limit: maximum number of results to return. Translatees to the API rows argument.
        :param offset: the offset in the complete result for where the set of returned datasets should begin. Translatees to the API start argument.
        :param params: other parameters to pass to API
        :return:
        """
        msg = DeprecationWarning("Prefer using package_search rather than group_package_show knowing the name of the package because this API does not list private packages")
        warn(msg)
        if params is None: params = {}
        responses = self._request_all_results_list(self._api_group_package_show, params=params, limit=limit, offset=offset,
                                                   group_name=group_name, owner_org=owner_org,
                                                   include_private=include_private, include_drafts=include_drafts)
        return sum(responses, [])

    group_package_show_all = _api_group_package_show_all  # function alias

    ## resource view
    def _api_resource_create_default_resource_views(self, resource_id:str, *, create_datastore_views:bool=None,
                                                    params:dict=None) -> List[CkanViewInfo]:
        """
        API call to resource_create_default_resource_views
        :param resource_id: resource id
        :param create_datastore_views: whether to create views that rely on data being on the DataStore (optional, API defaults to False)
        :param params:
        :return:
        """
        msg = DeprecationWarning("Prefer using resource_view_create rather than resource_create_default_resource_views")
        warn(msg)
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        resource_info = self.resource_show(resource_id)
        resource_dict = resource_info.details
        if create_datastore_views is None:
            create_datastore_views = self.resource_is_datastore(resource_id)
        params["resource"] = resource_dict
        params["create_datastore_views"] = create_datastore_views
        response = self._api_action_request(f"resource_create_default_resource_views", method=RequestType.Post, json=params)
        if response.success:
            view_info_list = [CkanViewInfo(view_dict) for view_dict in response.result]
            self.map._update_view_info(view_info_list)
            return copy.deepcopy(view_info_list)
        else:
            raise response.default_error(self)

    resource_create_default_resource_views = _api_resource_create_default_resource_views  # function alias


