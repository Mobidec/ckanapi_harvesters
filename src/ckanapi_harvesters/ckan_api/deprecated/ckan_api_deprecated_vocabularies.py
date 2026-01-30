#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Dict, Union
import copy
from warnings import warn

from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType, assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_errors import MandatoryAttributeError
from ckanapi_harvesters.auxiliary.ckan_vocabulary_deprecated import CkanTagVocabularyInfo, CkanVocabularyMap
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.policies.data_format_policy_tag_groups import TagListPolicy

from ckanapi_harvesters.ckan_api.deprecated.ckan_api_deprecated import CkanApiDeprecated


class CkanApiVocabulariesDeprecated(CkanApiDeprecated):
    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None,
                 apikey:str=None, apikey_file:str=None,
                 owner_org:str=None,
                 policy:CkanPackageDataFormatPolicy=None, policy_file:str=None,
                 identifier=None):
        """
        CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.

        :param url: url of the CKAN server
        :param proxies: proxies to use for requests
        :param ckan_headers: headers to use for requests, only to the CKAN server
        :param http_headers: headers to use for requests, for all requests, including external requests and to the CKAN server
        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        :param policy: data format policy to use with policy_check function
        :param policy_file: path to a JSON file containing the data format policy to use with policy_check function
        :param owner_org: name of the organization to limit package_search (optional)
        """
        msg = DeprecationWarning("Vocabularies are used to define custom fields which accept specific values and require to implement an IDatasetForm extension. This is not covered in this package.")
        warn(msg)
        super().__init__(url=url, proxies=proxies, apikey=apikey, apikey_file=apikey_file,
                         ckan_headers=ckan_headers, http_headers=http_headers,
                         owner_org=owner_org, policy=policy, policy_file=policy_file, identifier=identifier)
        self.map_vocabulary: CkanVocabularyMap = CkanVocabularyMap()

    def copy(self, new_identifier: str = None, *, dest=None):
        if dest is None:
            dest = CkanApiVocabulariesDeprecated()
        super().copy(new_identifier=new_identifier, dest=dest)
        dest.map_vocabulary = self.map_vocabulary.copy()
        return dest

    def set_default_map_mode(self, datastore_info:bool=None, resource_view_list:bool=None,
                             organization_info:bool=None, license_list:bool=None,
                             load_policy:bool=None, vocabulary_list:bool=None) -> None:
        super().set_default_map_mode(datastore_info=datastore_info, resource_view_list=resource_view_list,
                                     organization_info=organization_info, license_list=license_list,
                                     load_policy=load_policy)
        if vocabulary_list is None:
            vocabulary_list = self.map_vocabulary._mapping_query_vocabulary_list
        self.map_vocabulary._mapping_query_vocabulary_list = vocabulary_list

    def map_resources(self, package_list:Union[str, List[str]]=None, *, params:dict=None,
                      datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None, license_list:bool=None,
                      only_missing:bool=True, error_not_found:bool=True,
                      owner_org:str=None, load_policy:bool=None, vocabulary_list:bool=None) -> CkanMap:
        # overload including a call to load the default data format policy
        self.set_default_map_mode(vocabulary_list=vocabulary_list)
        map = super().map_resources(package_list=package_list, params=params, datastore_info=datastore_info,
                              resource_view_list=resource_view_list, organization_info=organization_info,
                              license_list=license_list, only_missing=only_missing, error_not_found=error_not_found,
                              owner_org=owner_org, load_policy=load_policy)
        vocabulary_list = self.map_vocabulary._mapping_query_vocabulary_list
        if vocabulary_list:
            self.vocabulary_list(cancel_if_present=True)
        return map


    ## Vocabulary management (requires sysadmin rights) --------------
    def _api_vocabulary_list(self, *, params:dict=None) -> List[CkanTagVocabularyInfo]:
        """
        API call to vocabulary_list.

        :return: a list of vocabulary info objects
        """
        msg = DeprecationWarning("Vocabulary functions did not work when tested")
        warn(msg)
        response = self._api_action_request(f"vocabulary_list", method=RequestType.Post, json=params)
        if response.success:
            vocabulary_list = [CkanTagVocabularyInfo(vocabulary_dict) for vocabulary_dict in response.result]
            self.map_vocabulary._update_vocabulary_info(vocabulary_list, vocabularies_listed=True)  # update map
            return copy.deepcopy(vocabulary_list)
        else:
            raise response.default_error(self)

    def vocabulary_list(self, cancel_if_present:bool=True) -> List[CkanTagVocabularyInfo]:
        if self.map_vocabulary.vocabularies_listed and cancel_if_present:
            return list(self.map_vocabulary.vocabularies.values())
        else:
            return self._api_vocabulary_list()

    def _api_vocabulary_create(self, vocabulary_name: str, tags_list_dict: List[Dict[str, str]], *, params:dict=None) -> CkanTagVocabularyInfo:
        """
        API call to vocabulary_create.

        :return: a
        """
        msg = DeprecationWarning("Vocabulary functions did not work when tested")
        warn(msg)
        if params is None: params = {}
        params["name"] = vocabulary_name
        params["tags"] = tags_list_dict
        response = self._api_action_request(f"vocabulary_create", method=RequestType.Post, json=params)
        if response.success:
            vocabulary_info = CkanTagVocabularyInfo(response.result)
            self.map_vocabulary._update_vocabulary_info(vocabulary_info)
            return copy.deepcopy(vocabulary_info)
        else:
            raise response.default_error(self)

    def _api_vocabulary_update(self, vocabulary_id: str, tags_list_dict: List[Dict[str, str]], *, params:dict=None) -> CkanTagVocabularyInfo:
        """
        API call to vocabulary_update.

        :return: a
        """
        msg = DeprecationWarning("Vocabulary functions did not work when tested")
        warn(msg)
        if params is None: params = {}
        params["id"] = vocabulary_id
        params["tags"] = tags_list_dict
        response = self._api_action_request(f"vocabulary_update", method=RequestType.Post, json=params)
        if response.success:
            vocabulary_info = CkanTagVocabularyInfo(response.result)
            self.map_vocabulary._update_vocabulary_info(vocabulary_info)
            return copy.deepcopy(vocabulary_info)
        else:
            raise response.default_error(self)

    def vocabulary_update(self, vocabulary_name: str, tags_list_dict: List[Dict[str, str]]):
        vocabulary_id = self.map_vocabulary.get_vocabulary_id(vocabulary_name, error_not_mapped=False)
        if vocabulary_id is None:
            self._api_vocabulary_create(vocabulary_name=vocabulary_name, tags_list_dict=tags_list_dict)
        else:
            self._api_vocabulary_update(vocabulary_id, tags_list_dict=tags_list_dict)

    def _api_vocabulary_delete(self, vocabulary_id: str, *, params:dict=None) -> bool:
        """
        API call to vocabulary_delete.

        :return: True if success
        """
        msg = DeprecationWarning("Vocabulary functions did not work when tested")
        warn(msg)
        if params is None: params = {}
        params["id"] = vocabulary_id
        response = self._api_action_request(f"vocabulary_delete", method=RequestType.Post, json=params)
        if response.success:
            return True
        else:
            raise response.default_error(self)

    def vocabulary_delete(self, vocabulary_id: str) -> bool:
        return self._api_vocabulary_delete(vocabulary_id)

    def vocabularies_clear(self):
        self.vocabulary_list(cancel_if_present=True)
        vocabulary_ids = list(self.map_vocabulary.vocabularies.keys())
        for vocabulary_id in vocabulary_ids:
            self._api_vocabulary_delete(vocabulary_id)

    def initiate_vocabularies_from_policy(self, policy:CkanPackageDataFormatPolicy, *, remove_others:bool=False):
        vocabulary_policy = policy.package_tags
        vocabulary_list: TagListPolicy
        vocabulary_names = {vocabulary_list.group_name for vocabulary_list in vocabulary_policy.value_group_specs}
        if remove_others:
            self.vocabulary_list(cancel_if_present=True)
            current_vocabularies = set(self.map_vocabulary.vocabulary_id_index.keys())
            extra_vocabularies = current_vocabularies - vocabulary_names
            for vocabulary_name in extra_vocabularies:
                vocabulary_id = self.map_vocabulary.vocabulary_id_index[vocabulary_name]
                self._api_vocabulary_delete(vocabulary_id)
        for vocabulary_list in vocabulary_policy.value_group_specs:
            vocabulary_name = vocabulary_list.group_name
            tags_list_dict = vocabulary_list.get_tags_list_dict()
            assert_or_raise(vocabulary_name is not None, MandatoryAttributeError("Tag vocabulary", "vocabulary_name"))
            self.vocabulary_update(vocabulary_name=vocabulary_name, tags_list_dict=tags_list_dict)
