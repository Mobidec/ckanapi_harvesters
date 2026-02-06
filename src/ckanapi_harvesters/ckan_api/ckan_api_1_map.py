#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Dict, Callable, Union, Any, Generator, Sequence, Tuple, Collection
from collections import OrderedDict
import time
import copy
from warnings import warn
import argparse

from ckanapi_harvesters.auxiliary.ckan_model import (CkanPackageInfo, CkanLicenseInfo, CkanDataStoreInfo, CkanResourceInfo,
                                                     CkanOrganizationInfo, CkanViewInfo, CkanField, CkanUserInfo,
                                                     CkanGroupInfo, CkanCollaboration, CkanCapacity)
from ckanapi_harvesters.auxiliary.urls import urlsep, url_join
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_action import CkanActionResponse, CkanActionError, CkanNotFoundError
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.ckan_api.ckan_api_params import CkanApiParamsBasic
from ckanapi_harvesters.ckan_api.ckan_api_0_base import CkanApiBase, use_ckan_owner_org_as_default
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_2_geom import CkanDataCleanerUploadGeom


## Main class ------------------
class CkanApiMap(CkanApiBase):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements the resource mapping capabilities to obtain resource ids necessary for the requests.
    """

    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org:str=None, params:CkanApiParamsBasic=None,
                 map:CkanMap=None,
                 identifier=None):
        """
        CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.

        :param url: url of the CKAN server
        :param proxies: proxies to use for requests
        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        :param owner_org: name of the organization to limit package_search (optional)
        :param params: other connection/behavior parameters
        :param map: map of known resources
        :param identifier: identifier of the ckan client
        """
        super().__init__(url=url, proxies=proxies, apikey=apikey, apikey_file=apikey_file,
                         owner_org=owner_org, params=params, identifier=identifier)
        if map is None:
            map = CkanMap()
        self.map: CkanMap = map

    def copy(self, new_identifier:str=None, *, dest=None):
        """
        Returns a copy of the current instance.
        Useful to use an initialized ckan object in a multithreaded context. Each thread would have its own copy.
        It is recommended to purge the last response before doing a copy (with purge_map=False)
        """
        if dest is None:
            dest = CkanApiMap()
        super().copy(dest=dest)
        dest.map = self.map.copy()
        return dest

    def purge(self, purge_map:bool=False) -> None:
        """
        Erase temporary data stored in this object

        :param purge_map: whether to purge the map created with map_resources
        """
        super().purge()
        if purge_map:
            if self.params.verbose_request:
                print("CKAN purge map")
            self.map.purge()

    def set_owner_org(self, owner_org:str, *, error_not_found:bool=True) -> None:
        """
        Set the default owner organization.

        :param owner_org: owner organization name, title or id.
        :return:
        """
        if owner_org is None:
            self.owner_org = None
        else:
            organization_info = self.get_organization_info_or_request(owner_org, error_not_found=error_not_found)
            self.owner_org = organization_info.get_owner_org() if organization_info is not None else None

    def _setup_cli_ckan_parser(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        parser = super()._setup_cli_ckan_parser(parser=parser)
        parser.add_argument("--owner-org", type=str,
                            help="CKAN Owner Organization")
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None, error_not_found:bool=True,
                             default_proxies:dict=None, proxy_headers:dict=None) -> None:
        super()._cli_ckan_args_apply(args=args, base_dir=base_dir, error_not_found=error_not_found,
                                     default_proxies=default_proxies, proxy_headers=proxy_headers)
        if args.owner_org is not None:
            self.set_owner_org(args.owner_org)
        print(args)

    def input_missing_info(self, *, base_dir:str=None, input_args:bool=False, input_args_if_necessary:bool=False,
                           input_apikey:bool=True, error_not_found:bool=True, input_owner_org:bool=False):
        """
        Ask user information in the console window.

        :param input_owner_org: option to ask for the owner organization.
        :return:
        """
        super().input_missing_info(base_dir=base_dir, input_args=input_args,
                                   input_args_if_necessary=input_args_if_necessary,
                                   input_apikey=input_apikey, error_not_found=error_not_found)
        if self.owner_org is None and input_owner_org:
            owner_org = input("Please enter owner organization name or title: ")
            self.set_owner_org(owner_org)

    ## Resource mapping  ------------------
    def _enrich_resource_info(self, resource_info:CkanResourceInfo, *,
                              datastore_info:bool=False, resource_view_list:bool=False) -> None:
        """
        Perform additional optional queries to add more information on a resource.

        :param resource_info:
        :param datastore_info: option to query datastore_info
        :param resource_view_list: option to query resource_view_list
        :return:
        """
        resource_id = resource_info.id
        resource_name = resource_info.name
        if datastore_info:
            try:
                db_info = self.datastore_info(resource_id, display_request_not_found=False)
                resource_info.datastore_info = db_info
                resource_info.datastore_info_error = None
            except Exception as e:
                resource_info.datastore_info = None
                resource_info.datastore_info_error = {"error": str(e)}
        else:
            resource_info.datastore_info = None
        if resource_view_list:
            resource_info.update_view(self.resource_view_list(resource_id), view_list=True)
        else:
            resource_info.views = None

    def set_default_map_mode(self, datastore_info:bool=None, resource_view_list:bool=None,
                             organization_info:bool=None, license_list:bool=None) -> None:
        """
        Set up the optional queries orchestrated by the map_resources function

        :param datastore_info:
        :param resource_view_list:
        :param organization_info:
        :param license_list:
        :return:
        """
        if datastore_info is None:
            datastore_info = self.map._mapping_query_datastore_info
        if resource_view_list is None:
            resource_view_list = self.map._mapping_query_resource_view_list
        if license_list is None:
            license_list = self.map._mapping_query_license_list
        if organization_info is None:
            organization_info = self.map._mapping_query_organization_info
        self.map._mapping_query_datastore_info = datastore_info
        self.map._mapping_query_resource_view_list = resource_view_list
        self.map._mapping_query_license_list = license_list
        self.map._mapping_query_organization_info = organization_info

    def complete_package_list(self, package_list:Union[str, List[str]]=None,
                              *, owner_org:str=None, params:dict=None) -> List[str]:
        """
        This function can list all packages of a CKAN server, for an organization or keeps the list as is.
        It is an auxiliary function to initialize a package_list argument
        """
        if package_list is None:
            package_info_list = self.package_search_all(owner_org=owner_org, params=params)
            package_list = [e.id for e in package_info_list]
        if isinstance(package_list, str):
            package_list = [package_list]
        return package_list

    def map_resources(self, package_list:Union[str, List[str]]=None, *, params:dict=None,
                      datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None, license_list:bool=None,
                      only_missing:bool=True, error_not_found:bool=True,
                      owner_org:str=None) -> CkanMap:
        """
        Map the resources of a given package to obtain resource ids associated with the package name and resources within the pacakge.
        NB: Packages were previously referred to as DataSets in previous CKAN implementations.
        A same name can be shared between multiple resources within a package. The first occurrence is used as a reference
        and a warning is issued in this case.

        :param package_list: list of packages to request (optional, by default, the result of package_search is used)
        :param params: optional parameters to pass to API calls (not recommended)
        :param datastore_info: option to enable the request of api_datastore_info. This will return information about
        the DataStore fields, aliases and row count. It is required to enable search of a DataStore by alias.
        :param resource_view_list: option to enable the request of view_list API for each resource
        :param organization_info: option to enable the request of organization_list API before any other request
        :param license_list: option to enable the request of license_list API
        :param only_missing: option to disable the request of already mapped packages
        :param error_not_found: option to ignore the packages which were not found by the API (do not raise an error)
        :param owner_org: option to filter packages of a given organization (only if package_search is used)
        :return:
        """
        start = time.time()
        self.set_default_map_mode(datastore_info=datastore_info, resource_view_list=resource_view_list,
                                  organization_info=organization_info, license_list=license_list)
        datastore_info = self.map._mapping_query_datastore_info
        resource_view_list = self.map._mapping_query_resource_view_list
        license_list = self.map._mapping_query_license_list
        organization_info = self.map._mapping_query_organization_info

        if organization_info:
            if owner_org is None:
                self.organization_list_all(cancel_if_present=True)
            else:
                self.get_organization_info_or_request(owner_org)

        package_list = self.complete_package_list(package_list=package_list, owner_org=owner_org, params=params)

        for name in package_list:
            pkg_info = self.map.get_package_info(name, error_not_mapped=False)
            if ((not only_missing) or pkg_info is None
                    or (datastore_info and not pkg_info.requested_datastore_info)):
                try:
                    pkg_info = self.package_show(name, params=params)
                except CkanNotFoundError as e:
                    if error_not_found:
                        raise e from e  # rethrow
                    else:
                        continue
                package_id = pkg_info.id
                package_name = pkg_info.name
                pkg_info.resources_id_index = {}  # reset
                for j, resource_info in enumerate(pkg_info.package_resources.values()):
                    resource_id = resource_info.id
                    resource_name = resource_info.name
                    resource_info.index_in_package = j
                    self._enrich_resource_info(resource_info, datastore_info=datastore_info, resource_view_list=resource_view_list)
                    if resource_name not in pkg_info.resources_id_index.keys():
                        pkg_info.resources_id_index[resource_name] = resource_id
                        pkg_info.resources_id_index_counts[resource_name] = 1
                    else:
                        pkg_info.resources_id_index_counts[resource_name] += 1
                        msg = "Two or more resources with same name {} were found during mapping.".format(resource_name)
                        warn(msg)
                pkg_info.requested_datastore_info = datastore_info
                if pkg_info.organization_info is not None:
                    self.map._update_organization_info(pkg_info.organization_info)
                self.map.packages_id_index[package_name] = package_id
                self.map.packages[package_id] = pkg_info
                self.map.resources.update({resource_info.id: resource_info for resource_info in pkg_info.package_resources.values()})
        if license_list:
            self._api_license_list(params=params)
        current = time.time()
        if self.params.verbose_multi_requests:
            print(f"{self.identifier} Resources mapped in {current - start} seconds")
        return self.map.copy()

    def remap_resources(self, *, params=None, purge:bool=True,
                        datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None, license_list:bool=None):
        """
        Perform a new request on previously mapped packages.

        :param params:
        :param purge: option to reset the map before remapping.
        :param datastore_info: enforce the request of api_datastore_info
        :param resource_view_list: enforce the request of view_list API for each resource
        :param license_list: enforce the request of license_list API
        :return:
        """
        package_list = list(self.map.packages_id_index.keys())
        if purge:
            self.map.purge()
        return self.map_resources(package_list, params=params,
                                  datastore_info=datastore_info, resource_view_list=resource_view_list,
                                  organization_info=organization_info, license_list=license_list)

    def get_resource_id_or_request(self, resource_name:str, package_name:str, *,
                                     request_missing:bool=True, error_not_mapped:bool=False,
                                     error_not_found:bool=True) -> Union[str,None]:
        resource_id = self.map.get_resource_id(resource_name, package_name, error_not_mapped=error_not_mapped)
        if resource_id is None and request_missing:
            if package_name is not None:
                self.map_resources(package_name)
                resource_id = self.map.get_resource_id(resource_name, package_name, error_not_mapped=error_not_mapped)
            else:
                try:
                    resource_info = self.resource_show(resource_id)
                    resource_id = resource_info.id
                except CkanNotFoundError as e:
                    if error_not_found:
                        raise e from e
                    else:
                        resource_id = None
        return resource_id

    def get_resource_info_or_request(self, resource_name:str, package_name:str=None, *,
                                     request_missing:bool=True, error_not_mapped:bool=False,
                                     error_not_found:bool=True) -> Union[CkanResourceInfo,None]:
        resource_id = self.get_resource_id_or_request(resource_name, package_name, error_not_mapped=error_not_mapped,
                                                      request_missing=request_missing, error_not_found=error_not_found)
        if resource_id is None:
            return None
        return self.get_resource_info_or_request_of_id(resource_id, request_missing=request_missing,
                                                       error_not_mapped=error_not_mapped, error_not_found=error_not_found)

    def get_resource_info_or_request_of_id(self, resource_id:str, *,
                                           request_missing:bool=True, error_not_mapped:bool=False,
                                           error_not_found:bool=True) -> Union[CkanResourceInfo,None]:
        """
        Get information on a resource if present in the map or perform request.
        Recommended: self.map.get_resource_info() rather than this for this usage because resource information is returned
        when calling package_info during the mapping process.

        :param resource_id: resource id
        :param request_missing: confirm to perform the request if the information is missing
        :param error_not_mapped: raise error if the resource is not mapped
        :return:
        """
        resource_info = self.map.get_resource_info(resource_id, error_not_mapped=error_not_mapped)
        if resource_info is not None:
            return resource_info
        elif request_missing:
            try:
                return self.resource_show(resource_id)
            except CkanNotFoundError as e:
                if error_not_found:
                    raise e from e
                else:
                    return None
        else:
            return None

    def get_datastore_info_or_request(self, resource_name:str, package_name:str=None, *,
                                      request_missing:bool=True, error_not_mapped:bool=False,
                                      error_not_found:bool=True) -> Union[CkanDataStoreInfo,None]:
        """
        Get information on a DataStore if present in the map or perform request.

        :param resource_name: resource name or id
        :param package_name: package name or id (required if the resource name is provided)
        :param request_missing: confirm to perform the request if the information is missing
        :param error_not_mapped: raise error if the resource is not mapped
        :return:
        """
        resource_id = self.map.get_resource_id(resource_name, package_name, error_not_mapped=error_not_mapped)
        if resource_id is None and request_missing and package_name is not None:
            self.map_resources(package_name, error_not_found=error_not_found)
            resource_id = self.map.get_resource_id(resource_name, package_name, error_not_mapped=error_not_mapped)
        if resource_id is not None:
            return self.get_datastore_info_or_request_of_id(resource_id, request_missing=request_missing, error_not_found=error_not_found)
        else:
            return None  # resource not mapped

    def get_datastore_info_or_request_of_id(self, resource_id: str, *,
                                            request_missing: bool = True, error_not_mapped: bool = False,
                                            error_not_found: bool = True) -> Union[CkanDataStoreInfo, None]:
        """
        Get information on a DataStore if present in the map or perform request.

        :param resource_id: resource id
        :param request_missing: confirm to perform the request if the information is missing
        :param error_not_mapped: raise error if the resource is not mapped
        :return:
        """
        datastore_info = self.map.get_datastore_info(resource_id, error_not_mapped=False)
        if datastore_info is not None:
            return datastore_info
        elif request_missing:
            try:
                return self.datastore_info(resource_id)
            except CkanNotFoundError as e:
                if error_not_found:
                    raise e from e
                else:
                    return None
        else:
            return None

    def get_datastore_fields_or_request(self, resource_id:str, *,
                                        request_missing:bool=True, error_not_mapped:bool=False,
                                        error_not_found:bool=True, return_list:bool=False) -> Union[List[dict], OrderedDict[str,CkanField],None]:
        datastore_info = self.get_datastore_info_or_request_of_id(resource_id, error_not_mapped=error_not_mapped,
                                                            request_missing=request_missing, error_not_found=error_not_found)
        if datastore_info is not None and datastore_info.fields_dict is not None:
            if not return_list:
                return datastore_info.fields_dict
            else:
                return [field_info.to_ckan_dict() for field_info in datastore_info.fields_dict.values()]
        else:
            return None

    def get_resource_view_list_or_request(self, resource_id:str, error_not_found:bool=True) -> Union[List[CkanViewInfo],None]:
        """
        Returns either the resource view list which was already received or emits a new query for this information.

        :param resource_id:
        :param error_not_found:
        :return:
        """
        resource_info = self.get_resource_info_or_request_of_id(resource_id, error_not_found=error_not_found)
        if resource_info is None:
            return None
        elif not resource_info.view_is_full_list:
            resource_info.update_view(self.resource_view_list(resource_id))
        return list(resource_info.views.values())

    def get_package_info_or_request(self, package_name:str, *,
                                    request_missing:bool=True, error_not_mapped:bool=False, error_not_found:bool=True,
                                    datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None,
                                    license_list:bool=None,) -> Union[CkanPackageInfo,None]:
        """
        Get information on a Package if present in the map or perform request.

        :param package_name: package name or id
        :param request_missing: confirm to perform the request if the information is missing
        :param error_not_mapped: raise error if the resource is not mapped
        :return:
        """
        package_info = self.map.get_package_info(package_name, error_not_mapped=error_not_mapped)
        if package_info is not None:
            return package_info
        elif request_missing:
            self.map_resources(package_name, error_not_found=error_not_found,
                               datastore_info=datastore_info, resource_view_list=resource_view_list,
                               organization_info=organization_info, license_list=license_list)  # request DataStore information if parameterized for
            return self.map.get_package_info(package_name, error_not_mapped=error_not_mapped)
        else:
            return None

    def get_organization_info_or_request(self, organization_name:str, *,
                                         request_missing:bool=True, error_not_mapped:bool=False,
                                         error_not_found:bool=True) -> Union[CkanOrganizationInfo,None]:
        """
        Get information on a Package if present in the map or perform request.

        :param organization_name: organization name or id
        :param request_missing: confirm to perform the request if the information is missing
        :param error_not_mapped: raise error if the resource is not mapped
        :return:
        """
        organization_info = self.map.get_organization_info(organization_name, error_not_mapped=error_not_mapped)
        if organization_info is not None:
            return organization_info
        elif request_missing:
            try:
                return self.organization_show(organization_name)
            except CkanNotFoundError as e:
                if error_not_found:
                    raise e from e
                else:
                    return None
        else:
            return None

    ## API calls needed to make the map and auxiliary API functions  ------------------
    def _api_package_search(self, *, params:dict=None, owner_org:str=None, filter:dict=None, q:str=None,
                            include_private:bool=True, include_drafts:bool=False, sort:str=None,
                            facet:bool=False, limit:int=None, offset:int=None) -> List[CkanPackageInfo]:
        """
        API call to package_search.

        :param owner_org: ability to filter packages by owner_org
        :param filter: dict of filters to apply, which translate to the API fq argument
        fq documentation: any filter queries to apply. Note: +site_id:{ckan_site_id} is added to this string prior to the query being executed.
        :param q: the solr query. Optional. Default is '*:*'
        :param include_private: if True, private datasets will be included in the results. Only private datasets from the user’s organizations will be returned and sysadmins will be returned all private datasets. Optional, the default is False in the API
        :param include_drafts:  if True, draft datasets will be included in the results. A user will only be returned their own draft datasets, and a sysadmin will be returned all draft datasets. Optional, the default is False.
        :param sort: sorting of the search results. Optional. Default: 'score desc, metadata_modified desc'. As per the solr documentation, this is a comma-separated string of field names and sort-orderings.
        :param facet:  whether to enable faceted results. Default: True in API.
        :param limit: maximum number of results to return. Translatees to the API rows argument.
        :param offset: the offset in the complete result for where the set of returned datasets should begin. Translatees to the API start argument.
        :param params: other parameters to pass to package_search
        :return:
        """
        if params is None: params = {}
        if limit is None: limit = self.params.default_limit_list
        if limit is not None:
            params["rows"] = limit
        if offset is not None:
            params["start"] = offset
        if owner_org is None and use_ckan_owner_org_as_default:
            owner_org = self.owner_org
        if owner_org is not None:
            owner_org_info = self.get_organization_info_or_request(owner_org)
            owner_org = owner_org_info.id
            if filter is None: filter = {}
            filter["owner_org"] = owner_org
        if q is not None:
            params["q"] = q
        if filter is not None:
            params["fq"] = '+'.join([f"{key}:{value}" for key, value in filter.items()])
        if sort is not None:
            params["sort"] = sort
        if facet is not None:
            params["facet"] = facet  # what are facets?
        if include_private is not None:
            params["include_private"] = include_private
        if include_drafts is not None:
            params["include_drafts"] = include_drafts
        response = self._api_action_request("package_search", method=RequestType.Get, params=params)
        if response.dry_run:
            return []
        elif response.success:
            package_info_list = [CkanPackageInfo(e) for e in response.result["results"]]
            self.map._update_package_info(package_info_list)
            return package_info_list
        else:
            raise response.default_error(self)

    def _api_package_search_all(self, *, params:dict=None, owner_org:str=None, filter:dict=None, q:str=None,
                                include_private:bool=True, include_drafts:bool=False, sort:str=None,
                                facet:bool=False, limit:int=None, offset:int=None, search_all:bool=True) -> List[CkanPackageInfo]:
        """
        API call to package_search until an empty list is received.

        :see: _api_package_search()
        :param owner_org: ability to filter packages by owner_org
        :param filter: dict of filters to apply, which translate to the API fq argument
        fq documentation: any filter queries to apply. Note: +site_id:{ckan_site_id} is added to this string prior to the query being executed.
        :param q: the solr query. Optional. Default is '*:*'
        :param include_private: if True, private datasets will be included in the results. Only private datasets from the user’s organizations will be returned and sysadmins will be returned all private datasets. Optional, the default is False in the API
        :param include_drafts:  if True, draft datasets will be included in the results. A user will only be returned their own draft datasets, and a sysadmin will be returned all draft datasets. Optional, the default is False.
        :param sort: sorting of the search results. Optional. Default: 'score desc, metadata_modified desc'. As per the solr documentation, this is a comma-separated string of field names and sort-orderings.
        :param facet:  whether to enable faceted results. Default: True in API.
        :param limit: maximum number of results to return. Translatees to the API rows argument.
        :param offset: the offset in the complete result for where the set of returned datasets should begin. Translatees to the API start argument.
        :param params: other parameters to pass to package_search
        :return:
        """
        if params is None: params = {}
        responses = self._request_all_results_list(self._api_package_search, params=params, limit=limit, offset=offset,
                                                   owner_org=owner_org, filter=filter, q=q, sort=sort, facet=facet,
                                                   include_private=include_private, include_drafts=include_drafts,
                                                   search_all=search_all)
        return sum(responses, [])

    def package_search_all(self, *, params:dict=None, owner_org:str=None, filter:dict=None, q:str=None,
                                include_private:bool=True, include_drafts:bool=False, sort:str=None,
                                facet:bool=False, limit:int=None, offset:int=None, search_all:bool=True) -> List[CkanPackageInfo]:
        # function alias
        return self._api_package_search_all(params=params, owner_org=owner_org, filter=filter, q=q,
                                            include_private=include_private, include_drafts=include_drafts, sort=sort,
                                            facet=facet, limit=limit, offset=offset, search_all=search_all)


    def _api_package_show(self, package_id, *, params:dict=None) -> CkanPackageInfo:
        """
        API call to package_show. Returns the information on the package and the resources contained in the package.
        Not recommended for outer use because this method does not return information about the DataStores. Prefer the map_resources method.

        :see: map_resources()
        :param package_id: package id.
        :param params: See API documentation.
        :return:
        """
        if params is None: params = {}
        params["id"] = package_id
        response = self._api_action_request("package_show", method=RequestType.Get, params=params)
        if response.success:
            package_info = CkanPackageInfo(response.result)
            # update map
            self.map._update_package_info(package_info)
            return package_info.copy()
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "Package", response)
        else:
            raise response.default_error(self)

    def package_show(self, package_id, *, params:dict=None) -> CkanPackageInfo:
        # function alias
        return self._api_package_show(package_id=package_id, params=params)

    def _api_resource_show(self, resource_id, *, params:dict=None) -> CkanResourceInfo:
        """
        API call to resource_show. Returns the metadata on a resource.

        :param resource_id: resource id.
        :param params: See API documentation.
        :return:
        """
        if params is None: params = {}
        params["id"] = resource_id
        response = self._api_action_request("resource_show", method=RequestType.Get, params=params)
        if response.success:
            return CkanResourceInfo(response.result)
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "Resource", response)
        else:
            raise response.default_error(self)

    def resource_show(self, resource_id, *, params:dict=None) -> CkanResourceInfo:
        # function alias
        return self._api_resource_show(resource_id=resource_id, params=params)

    def _api_datastore_info(self, resource_id:str, *, params:dict=None, display_request_not_found:bool=True) -> CkanDataStoreInfo:
        """
        API call to datastore_info. Returns the information on the DataStore. Used to know the number of rows in a DataStore.

        :param resource_id: resource id.
        :param params: N/A
        :param display_request_not_found: whether to display the request in the command window, in case of a CkanNotFoundError.
        This option is recommended if you are testing whether the resource has a DataStore or not.
        :return:
        """
        if params is None: params = {}
        params["id"] = resource_id
        response = self._api_action_request("datastore_info", method=RequestType.Post, json=params)
        if response.success:
            datastore_info = CkanDataStoreInfo(response.result)
            self.map._update_datastore_info(datastore_info)
            return datastore_info.copy()
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "DataStore", response, display_request=display_request_not_found)
        else:
            raise response.default_error(self)

    def datastore_info(self, resource_id:str, *, params:dict=None, display_request_not_found:bool=True) -> CkanDataStoreInfo:
        # function alias
        return self._api_datastore_info(resource_id=resource_id, params=params, display_request_not_found=display_request_not_found)

    def _api_resource_view_list(self, resource_id:str, *, params:dict=None) -> List[CkanViewInfo]:
        """
        API call to resource_view_list.

        :param params: typically, the request can be limited to an organization with the owner_org parameter
        :return:
        """
        if params is None:
            params = {}
        params["id"] = resource_id
        response = self._api_action_request("resource_view_list", method=RequestType.Get, params=params)
        if response.success:
            view_info_list = [CkanViewInfo(view_dict) for view_dict in response.result]
            self.map._update_view_info(view_info_list, view_list=True)
            return copy.deepcopy(view_info_list)
        else:
            raise response.default_error(self)

    def resource_view_list(self, resource_id:str, *, params:dict=None) -> List[CkanViewInfo]:
        # function alias
        return self._api_resource_view_list(resource_id=resource_id, params=params)

    def _api_organization_show(self, id:str, *, params:dict=None) -> CkanOrganizationInfo:
        """
        API call to organization_show.

        :param id: organization id or name.
        :param params: typically, the request can be limited to an organization with the owner_org parameter
        :return:
        """
        if params is None: params = {}
        if id is not None:
            params["id"] = id
        response = self._api_action_request("organization_show", method=RequestType.Get, params=params)
        if response.success:
            organization_info = CkanOrganizationInfo(response.result)
            # update map
            self.map._update_organization_info(organization_info)
            return organization_info.copy()
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "Organization", response)
        else:
            raise response.default_error(self)

    def organization_show(self, id:str, *, params:dict=None) -> CkanOrganizationInfo:
        # function alias
        return self._api_organization_show(id=id, params=params)

    def _api_organization_list(self, *, params:dict=None, all_fields:bool=True,
                               include_users:bool=False,
                               limit:int=None, offset:int=None) -> Union[List[CkanOrganizationInfo], List[str]]:
        """
        API call to organization_list.

        :param params: typically, the request can be limited to an organization with the owner_org parameter
        :param all_fields: whether to return full information or only the organization names in a list
        :return:
        """
        if params is None: params = {}
        if limit is None: limit = self.params.default_limit_list
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        params["all_fields"] = all_fields
        params["include_users"] = include_users
        response = self._api_action_request("organization_list", method=RequestType.Get, params=params)
        if response.success:
            if all_fields:
                # returns a list of dicts
                organization_list = [CkanOrganizationInfo(e) for e in response.result]
                # update map
                self.map._update_organization_info(organization_list)
                return copy.deepcopy(organization_list)
            else:
                # returns a list of organization names
                return response.result
        else:
            raise response.default_error(self)

    def _api_organization_list_all(self, *, params:dict=None, all_fields:bool=True,
                                   include_users:bool=False,
                                   limit:int=None, offset:int=None) -> Union[List[CkanOrganizationInfo], List[str]]:
        """
        API call to organization_list until an empty list is received.

        :see: _api_organization_list()
        :param params:
        :return:
        """
        if params is None: params = {}
        responses = self._request_all_results_list(self._api_organization_list, params=params, limit=limit, offset=offset,
                                                   all_fields=all_fields, include_users=include_users)
        self.map.organizations_listed_all = True
        self.map.organizations_listed_all_users = include_users
        return sum(responses, [])

    def organization_list_all(self, *, cancel_if_present:bool=False, params:dict=None,
                              all_fields:bool=True, include_users:bool=False,
                              limit:int=None, offset:int=None) -> Union[List[CkanOrganizationInfo], List[str]]:
        """
        API call to license_list.
        The call can be canceled if the list is already present (not recommended, rather use get_organization_info_or_request).

        :param params:
        :param cancel_if_present: option to cancel when list is already present.
        :return:
        """
        if self.map.organizations_listed_all and cancel_if_present \
                and self.map.organizations_listed_all_users == include_users:
            return list(self.map.organizations.values())
        else:
            return self._api_organization_list_all(params=params, all_fields=all_fields, include_users=include_users, limit=limit, offset=offset)

    def _api_license_list(self, *, params:dict=None) -> List[CkanLicenseInfo]:
        """
        API call to license_list.

        :param params:
        :return:
        """
        if params is None: params = {}
        response = self._api_action_request(f"license_list", method=RequestType.Post, json=params)
        if response.success:
            license_list = [CkanLicenseInfo(license_dict) for license_dict in response.result]
            # update map:
            self.map._update_license_info(license_list)
            return copy.deepcopy(license_list)
        else:
            raise response.default_error(self)

    def license_list(self, *, cancel_if_present:bool=True, params:dict=None) -> List[CkanLicenseInfo]:
        """
        API call to license_list. The call can be canceled if the list is already present.

        :param params:
        :param cancel_if_present: option to cancel when list is already present.
        :return:
        """
        if len(self.map.licenses) > 0 and cancel_if_present:
            return list(self.map.licenses.values())
        else:
            return self._api_license_list(params=params)

    def resource_is_datastore(self, resource_id:str) -> bool:
        """
        Basic test to know whether a resource is DataStore.

        :param resource_id:
        :return:
        """
        try:
            datastore_info = self.datastore_info(resource_id, display_request_not_found=False)
        except CkanNotFoundError as e:
            return False
        return True

    def get_package_page_url(self, package_name:str, *, error_not_found:bool=True) -> str:
        """
        Get URL of package presentation page in CKAN (landing page).

        :param package_name:
        :param error_not_found:
        :return:
        """
        self._error_empty_url()
        package_info = self.get_package_info_or_request(package_name, error_not_found=error_not_found)
        if package_info is not None:
            url = url_join(self.url, "dataset" + urlsep + package_info.name)
        else:
            url = None
        return url

    def get_resource_page_url(self, resource_name:str, package_name:str=None,
                              *, error_not_mapped:bool=True) -> str:
        """
        Get URL of resource presentation page in CKAN (landing page).

        :param package_name:
        :return:
        """
        self._error_empty_url()
        resource_info = self.map.get_resource_info(resource_name, package_name=package_name, error_not_mapped=error_not_mapped)
        if resource_info is not None:
            package_info = self.map.get_package_info(resource_info.package_id)
            url = url_join(self.url, "dataset" + urlsep + package_info.name + urlsep + "resource" + urlsep + resource_info.id)
        else:
            url = None
        return url

    def test_ckan_connection(self, raise_error:bool=False) -> bool:
        """
        Test if the CKAN URL aims to a CKAN server by testing the package_search API.
        This does not check authentication.
        """
        try:
            self.package_search_all(limit=1, search_all=False)
        except CkanActionError as e:
            if e.status_code == 220:
                if raise_error:
                    raise e from e
                else:
                    return False
            else:
                raise e from e
        return True

    def _api_user_show(self, *, params:dict=None) -> Union[CkanUserInfo,None]:
        """
        API call to user_show. With no params, returns the name of the current user logged in.

        :return: dict with information on the current user
        """
        if params is None: params = {}
        response = self._api_action_request("user_show", method=RequestType.Get, params=params)
        if response.success:
            user_info = CkanUserInfo(response.result)
            self.map._update_user_info(user_info)
            return user_info.copy()
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "User", response)
        else:
            raise response.default_error(self)

    def query_current_user(self, *, verbose:bool=None, error_not_found:bool=False) -> Union[CkanUserInfo,None]:
        if verbose is None:
            verbose = self.params.verbose_extra
        try:
            user_info = self._api_user_show()
        except CkanNotFoundError as e:
            if error_not_found:
                raise e from e
            else:
                user_info = None
        if verbose:
            if user_info is not None:
                print("Authenticated as " + user_info.name)
            else:
                print("User not authenticated")
        return user_info

    def test_ckan_login(self, *, raise_error:bool=False, verbose:bool=None,
                        empty_key_connected:bool=False) -> bool:
        """
        Test if your login leads to a user account.

        :param raise_error: option to raise an error if no account was detected
        :param verbose: option to display username in console
        :param empty_key_connected: option to ignore the test if the API key is empty
        """
        user_info = self.query_current_user(verbose=verbose, error_not_found=raise_error and not empty_key_connected)
        if user_info is None:
            if self.apikey.is_empty():
                if raise_error and not empty_key_connected:
                    raise ConnectionError("No API key loaded")
                return empty_key_connected
            else:
                if raise_error:
                    raise ConnectionError("The current API key did not authenticate a user")
                return False
        else:
            return True


    ## List users and groups
    def _api_user_list(self, *, q:str=None, email:str=None,
                       params:dict=None) -> List[CkanUserInfo]:
        """
        API call to user_list.

        :param params:
        :return:
        """
        if params is None: params = {}
        if q is not None:
            params["q"] = q
        if email is not None:
            params["email"] = email
        response = self._api_action_request(f"user_list", method=RequestType.Post, json=params)
        if response.success:
            user_list = [CkanUserInfo(user_dict) for user_dict in response.result]
            # update map:
            self.map._update_user_info(user_list)
            return copy.deepcopy(user_list)
        else:
            raise response.default_error(self)

    def user_list(self, *, cancel_if_present:bool=False, q:str=None, email:str=None,
                  params:dict=None) -> List[CkanUserInfo]:
        """
        API call to user_list. The call can be canceled if the list is already present.

        :param params:
        :param cancel_if_present: option to cancel when list is already present.
        :return:
        """
        if q is None and email is None and params is None:
            if self.map.users_listed_all > 0 and cancel_if_present:
                return list(self.map.users.values())
            else:
                result = self._api_user_list(params=params)
                self.map.users_listed_all = True
                return result
        else:
            return self._api_user_list(q=q, email=email, params=params)

    def _api_package_collaborator_list(self, package_id:str, *, params:dict=None,
                                       cancel_if_present:bool=False) -> Dict[str,CkanCollaboration]:
        """
        API call to package_collaborator_list.

        :param params:
        :return:
        """
        if cancel_if_present:
            package_info = self.get_package_info_or_request(package_id)
            if package_info.collaborators is not None:
                return package_info.collaborators
        if params is None: params = {}
        params["id"] = package_id
        response = self._api_action_request(f"package_collaborator_list", method=RequestType.Post, json=params)
        if response.success:
            package_info = self.get_package_info_or_request(package_id)
            package_info.collaborators = {}
            for collaborator_dict in response.result:
                assert (collaborator_dict["package_id"] == package_id)
                package_info.collaborators[collaborator_dict["user_id"]] = CkanCollaboration(d=collaborator_dict)
            return package_info.collaborators
        else:
            raise response.default_error(self)

    def package_collaborator_list(self, package_id:str, *, params:dict=None,
                                  cancel_if_present:bool=False) -> Dict[str,CkanCollaboration]:
        return self._api_package_collaborator_list(package_id=package_id, params=params,
                                                   cancel_if_present=cancel_if_present)

    def _api_group_list(self, *, limit:int=None, offset:int=0, groups:List[str]=None,
                        all_fields:bool=True, include_users:bool=True,
                        params:dict=None) -> Union[List[CkanGroupInfo], List[str]]:
        """
        API call to group_list.

        :param params:
        :return:
        """
        if params is None: params = {}
        if limit is None:
            limit = self.params.default_limit_list
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if groups is not None:
            params["groups"] = groups
        params["include_users"] = include_users
        all_fields = all_fields or include_users
        params["all_fields"] = all_fields
        response = self._api_action_request(f"group_list", method=RequestType.Post, json=params)
        if response.success:
            if all_fields:
                group_list = [CkanGroupInfo(group_dict) for group_dict in response.result]
                # update map:
                if include_users:
                    for group_info in group_list:
                        user_list = [CkanUserInfo(user_dict) for user_dict in group_info.details["users"]]
                        self.map._update_user_info(user_list)
                        group_info.user_members = {user_info.id: CkanCapacity.from_str(user_dict["capacity"]) for user_info, user_dict in zip(user_list, group_info.details["users"])}
                self.map._update_group_info(group_list)
                return copy.deepcopy(group_list)
            else:
                return response.result  # list of names
        else:
            raise response.default_error(self)

    def group_list(self, *, limit:int=None, offset:int=0, groups:List[str]=None,
                        all_fields:bool=True, include_users:bool=True,
                        params:dict=None) -> List[CkanGroupInfo]:
        # function alias
        return self._api_group_list(groups=groups, all_fields=all_fields, include_users=include_users,
                                    limit=limit, offset=offset, params=params)

    def _api_group_list_all(self, *, all_fields:bool=True, include_users:bool=True, params:dict=None,
                            limit:int=None, offset:int=None) -> Union[List[CkanUserInfo], List[str]]:
        """
        API call to group_list until an empty list is received.

        :see: _api_group_list()
        :param params:
        :return:
        """
        if params is None: params = {}
        responses = self._request_all_results_list(self._api_group_list, params=params, limit=limit, offset=offset,
                                                   all_fields=all_fields, include_users=include_users)
        self.map.groups_listed_all = True
        return sum(responses, [])

    def group_list_all(self, *, all_fields:bool=True, include_users:bool=True,
                       cancel_if_present:bool=False, params:dict=None,
                       limit:int=None, offset:int=None) -> Union[List[CkanGroupInfo], List[str]]:
        """
        API call to group_list.
        The call can be canceled if the list is already present (not recommended, rather use get_organization_info_or_request).

        :param params:
        :param cancel_if_present: option to cancel when list is already present.
        :return:
        """
        if self.map.groups_listed_all and cancel_if_present:
            return list(self.map.groups.values())
        else:
            return self._api_group_list_all(params=params, all_fields=all_fields, include_users=include_users, limit=limit, offset=offset)

    def map_user_rights(self, cancel_if_present:bool=True):
        """
        Map user and group access rights to the packages currently mapped by CKAN
        :return:
        """
        self.group_list_all(cancel_if_present=cancel_if_present)
        self.user_list(cancel_if_present=cancel_if_present)
        for package_id, package_info in self.map.packages.items():
            self.package_collaborator_list(package_id, cancel_if_present=cancel_if_present)
            # merge collaborators with groups of the package
            package_info.user_access = package_info.collaborators.copy()
            for group in package_info.groups:
                group_info = self.map.groups[group.id]
                for user_id, user_capacity in group_info.user_members.items():
                    if user_id not in package_info.user_access:
                        package_info.user_access[user_id] = CkanCollaboration(user_capacity, None, group_id=group.id)
        return self.map
