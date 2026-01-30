#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Union, Tuple, Dict
from collections import OrderedDict
import copy
import re
from warnings import warn
import argparse
import io
import hashlib

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.auxiliary.ckan_auxiliary import json_encode_params
from ckanapi_harvesters.auxiliary.ckan_configuration import default_ckan_has_postgis, default_ckan_target_epsg
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageInfo, CkanResourceInfo, CkanViewInfo, CkanField
from ckanapi_harvesters.auxiliary.ckan_model import CkanState
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise, ckan_package_name_re, datastore_id_col
from ckanapi_harvesters.auxiliary.ckan_auxiliary import dict_recursive_update
from ckanapi_harvesters.auxiliary.ckan_auxiliary import upload_prepare_requests_files_arg, RequestType
from ckanapi_harvesters.auxiliary.ckan_action import CkanNotFoundError
from ckanapi_harvesters.auxiliary.ckan_errors import (ReadOnlyError, AdminFeatureLockedError, NoDefaultView,
                                                      InvalidParameterError, CkanMandatoryArgumentError,
                                                      IntegrityError)
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.ckan_api.ckan_api_1_map import use_ckan_owner_org_as_default

from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.ckan_api.ckan_api_4_readwrite import CkanApiReadWriteParams
from ckanapi_harvesters.ckan_api.ckan_api_4_readwrite import CkanApiReadWrite


default_alias_package_resource_sep:str = "."
ckan_table_name_max_len:int = 63  # this comes from a PostgreSQL length limitation and applies to DataStore aliases
alias_name_max_len:Union[int,None] = ckan_table_name_max_len
# if name exceeds max length, a hash of the full name is used so this hash should be unchanged if the resource is re-created:
default_alias_hash_replace:bool = True  # True: replace with full hash / False: only the exceeding characters are replaced, with the following parameters:
default_alias_hash_len:int = 6
default_alias_hash_sep:str = ":"

table_name_subs_re = '[^\w-]|^(?=\d)'

def clean_table_name(variable_name: str) -> str:
    """
    Replace unwanted characters and spaces to generate a table name similar to a table name
    """
    return re.sub(table_name_subs_re,'_', variable_name)


class CkanApiManageParams(CkanApiReadWriteParams):
    default_enable_admin: bool = False  # False: disable advanced admin operations by default such as resource/package deletion
    default_alias_enforce: bool = True  # if True, always add the default alias when calling datastore_create

    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None):
        super().__init__(proxies=proxies, ckan_headers=ckan_headers, http_headers=http_headers)
        self.enable_admin: bool = self.default_enable_admin

    def copy(self, new_identifier:str=None, *, dest=None):
        if dest is None:
            dest = CkanApiManageParams()
        super().copy(dest=dest)
        dest.enable_admin = self.enable_admin
        return dest

    def _setup_cli_ckan_parser__params(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        # overload adding support to trigger admin mode
        parser = super()._setup_cli_ckan_parser__params(parser=parser)
        parser.add_argument("--admin", action="store_true",
                            help="Option to enable admin mode")
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None,
                             error_not_found:bool=True, default_proxies:dict=None, proxy_headers:dict=None,
                             proxies:dict=None, headers:dict=None) -> None:
        # overload adding support to trigger admin mode
        super()._cli_ckan_args_apply(args=args, base_dir=base_dir, error_not_found=error_not_found,
                                     default_proxies=default_proxies, proxy_headers=proxy_headers)
        if args.admin:
            self.enable_admin = args.admin


class CkanApiExtendedParams(CkanApiManageParams):
    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None):
        super().__init__(proxies=proxies, ckan_headers=ckan_headers, http_headers=http_headers)
        self.ckan_has_postgis: bool = default_ckan_has_postgis
        self.ckan_default_target_epsg: Union[int,None] = default_ckan_target_epsg

    def copy(self, new_identifier:str=None, *, dest=None):
        if dest is None:
            dest = CkanApiExtendedParams()
        super().copy(dest=dest)
        dest.ckan_has_postgis = self.ckan_has_postgis
        dest.ckan_default_target_epsg = self.ckan_default_target_epsg
        return dest

    def _setup_cli_ckan_parser__params(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        # overload adding support to change extended parameters
        parser = super()._setup_cli_ckan_parser__params(parser=parser)
        parser.add_argument("--ckan-postgis", action="store_true",
                            help="Option to notify that CKAN is compatible with PostGIS")  # default=default_ckan_has_postgis
        parser.add_argument("--ckan-epsg", type=int,
                            help="Default EPSG for CKAN", default=default_ckan_target_epsg)
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None,
                             error_not_found:bool=True, default_proxies:dict=None, proxy_headers:dict=None,
                             proxies:dict=None, headers:dict=None) -> None:
        # overload adding support to trigger admin mode
        super()._cli_ckan_args_apply(args=args, base_dir=base_dir, error_not_found=error_not_found,
                                     default_proxies=default_proxies, proxy_headers=proxy_headers)
        if args.ckan_postgis:
            self.ckan_has_postgis = args.ckan_postgis
        if args.ckan_epsg:
            self.ckan_default_target_epsg = args.ckan_epsg


class CkanApiManage(CkanApiReadWrite):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements more advanced requests to manage packages, resources and DataStores on the CKAN server.
    """

    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org:str=None, params:CkanApiExtendedParams=None,
                 map:CkanMap=None, policy: CkanPackageDataFormatPolicy = None, policy_file:str=None,
                 data_cleaner_upload:CkanDataCleanerABC=None,
                 identifier=None):
        """
        CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.

        :param url: url of the CKAN server
        :param proxies: proxies to use for requests
        :param apikey: way to provide the API key directly (optional)
        :param apikey_file: path to a file containing a valid API key in the first line of text (optional)
        :param policy: data format policy to use with policy_check function
        :param policy_file: path to a JSON file containing the data format policy to use with policy_check function
        :param owner_org: name of the organization to limit package_search (optional)
        :param params: other connection/behavior parameters
        :param map: map of known resources
        :param policy: data format policy to be used with the policy_check function.
        :param policy_file: path to a JSON file containing the data format policy to load.
        :param data_cleaner_upload: data cleaner object to use before uploading to a CKAN DataStore.
        :param identifier: identifier of the ckan client
        """
        super().__init__(url=url, proxies=proxies, apikey=apikey, apikey_file=apikey_file,
                         owner_org=owner_org, map=map, policy=policy, policy_file=policy_file,
                         data_cleaner_upload=data_cleaner_upload, identifier=identifier)
        if params is None:
            params = CkanApiExtendedParams()
        if proxies is not None:
            params.proxies = proxies
        self.params: CkanApiExtendedParams = params

    def copy(self, new_identifier: str = None, *, dest=None):
        if dest is None:
            dest = CkanApiManage()
        super().copy(new_identifier=new_identifier, dest=dest)
        return dest

    def full_unlock(self, unlock:bool=True,
                    *, no_ca:bool=None, external_url_resource_download:bool=None) -> None:
        """
        Function to unlock full capabilities of the CKAN API

        :param unlock:
        :return:
        """
        super().full_unlock(unlock, no_ca=no_ca, external_url_resource_download=external_url_resource_download)
        self.params.enable_admin = unlock

    def _setup_cli_ckan_parser(self, parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        # overload adding support to load a policy from a file
        parser = super()._setup_cli_ckan_parser(parser=parser)
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace, *, base_dir:str=None,
                             error_not_found:bool=True, default_proxies:dict=None, proxy_headers:dict=None,
                             proxies:dict=None, headers:dict=None) -> None:
        # overload adding support to load a policy from a file
        super()._cli_ckan_args_apply(args=args, base_dir=base_dir, error_not_found=error_not_found,
                                     default_proxies=default_proxies, proxy_headers=proxy_headers)

    ## Field modification ------------------
    @staticmethod
    def datastore_field_dict(fields:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None,
                             fields_merge:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None,
                             fields_update:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None, *,
                             fields_type_override:Dict[str,str]=None, fields_description:Dict[str,str]=None,
                             fields_label:Dict[str,str]=None, return_list:bool=False) \
            -> Union[Dict[str, CkanField], List[dict]]:
        """
        Initialization of the `fields` parameter for datastore_create.
        Only parts used by this package are present.
        To complete the field's dictionnaries, refer to datastore_field_patch_dict.

        :param fields: first source of field information, usually the fields from the DataStore
        :param fields_merge: second source. Values from this dictionary will overwrite fields
        :param fields_update: third source. Values from this dictionary will be prioritary over all values.
        :param fields_type_override:
        :param fields_description:
        :param fields_label:
        :param return_list:
        :return: dict if return_list is False, list if return_list is True.
        You can easily transform the dict to a list with the following code:
        ```python
        fields = list(fields_update.values())
        ```
        """
        fields_updated: OrderedDict[str, CkanField]
        if fields is None:
            fields_updated = OrderedDict()
        elif isinstance(fields, list):
            fields_updated = OrderedDict()
            for field_info in fields:
                if isinstance(field_info, CkanField):
                    fields_updated[field_info.name] = field_info
                elif isinstance(field_info, dict):
                    fields_updated[field_info["id"]] = CkanField.from_ckan_dict(field_info)
                else:
                    raise TypeError(f"Field type {type(field_info)} not supported.")
        elif isinstance(fields, dict):
            fields_updated = OrderedDict()
            for field_name, field_info in fields.items():
                if isinstance(field_info, CkanField):
                    fields_updated[field_info.name] = field_info
                    assert_or_raise(field_name == field_info.name, IntegrityError(f"Field {field_name} does not match its id ({field_info.name})"))
                elif isinstance(field_info, dict):
                    fields_updated[field_info["id"]] = CkanField.from_ckan_dict(field_info)
                    assert_or_raise(field_name == field_info["id"], IntegrityError(f"Field {field_name} does not match its id ({field_info['id']})"))
                else:
                    raise TypeError(f"Field type {type(field_info)} not supported for {field_name}.")
        else:
            raise TypeError(f"Field type {type(fields)} not supported.")
        if fields_merge is None:
            pass
        elif isinstance(fields_merge, list):
            for field_info in fields_merge:
                if isinstance(field_info, CkanField):
                    fields_updated[field_info.name] = field_info
                elif isinstance(field_info, dict):
                    fields_updated[field_info["id"]] = CkanField.from_ckan_dict(field_info)
                else:
                    raise TypeError(f"Field type {type(field_info)} not supported.")
        elif isinstance(fields_merge, dict):
            for field_name, field_info in fields_merge.items():
                if isinstance(field_info, CkanField):
                    fields_updated[field_info.name] = field_info
                    assert_or_raise(field_name == field_info.name, IntegrityError(f"Field {field_name} does not match its id ({field_info.name})"))
                elif isinstance(field_info, dict):
                    fields_updated[field_info["id"]] = CkanField.from_ckan_dict(field_info)
                    assert_or_raise(field_name == field_info["id"], IntegrityError(f"Field {field_name} does not match its id ({field_info['id']})"))
                else:
                    raise TypeError(f"Field type {type(field_info)} not supported for {field_name}.")
        else:
            raise TypeError(f"Field type {type(fields_merge)} not supported.")
        if fields_update is None:
            pass
        elif isinstance(fields_update, list):
            for field_info in fields_update:
                if isinstance(field_info, CkanField):
                    fields_updated[field_info.name] = field_info
                elif isinstance(field_info, dict):
                    fields_updated[field_info["id"]] = CkanField.from_ckan_dict(field_info)
                else:
                    raise TypeError(f"Field type {type(field_info)} not supported.")
        elif isinstance(fields_update, dict):
            for field_name, field_info in fields_update.items():
                if isinstance(field_info, CkanField):
                    fields_updated[field_info.name] = field_info
                    assert_or_raise(field_name == field_info.name, IntegrityError(f"Field {field_name} does not match its id ({field_info.name})"))
                elif isinstance(field_info, dict):
                    fields_updated[field_info["id"]] = CkanField.from_ckan_dict(field_info)
                    assert_or_raise(field_name == field_info["id"], IntegrityError(f"Field {field_name} does not match its id ({field_info['id']})"))
                else:
                    raise TypeError(f"Field type {type(field_info)} not supported for {field_name}.")
        else:
            raise TypeError(f"Field type {type(fields_update)} not supported.")
        if fields_type_override is not None:
            for field_name, field_type in fields_type_override.items():
                if field_name in fields_updated.keys():
                    fields_updated[field_name].type_override = field_type
                else:
                    fields_updated[field_name] = CkanField(field_name, field_type)
            # fields_updated = dict_recursive_update(fields_updated, {field_id: {"type": str(value), "info": {"type_override": str(value)}, "schema": {"native_type": str(value)}} for field_id, value in fields_type_override.items()})
        if fields_description is not None:
            for field_name, description in fields_description.items():
                if field_name not in fields_updated.keys():
                    fields_updated[field_name] = CkanField(field_name, None)
                fields_updated[field_name].notes = description
            # fields_updated = dict_recursive_update(fields_updated, {field_id: {"info": {"notes": value}} for field_id, value in fields_description.items()})
        if fields_label is not None:
            for field_name, label in fields_description.items():
                if field_name not in fields_updated.keys():
                    fields_updated[field_name] = CkanField(field_name, None)
                fields_updated[field_name].label = label
            # fields_updated = dict_recursive_update(fields_updated, {field_id: {"info": {"label": value}} for field_id, value in fields_label.items()})
        for field_id, field_dict in fields_updated.items():
            field_dict.name = field_id
        if return_list:
            return [field_info.to_ckan_dict() for field_info in fields_updated.values()]
        else:
            return fields_updated

    def datastore_field_patch_dict(self, fields_merge:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None,
                                   fields_update:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None, *,
                                   fields_type_override:Dict[str,str]=None, fields_description:Dict[str,str]=None,
                                   fields_label:Dict[str,str]=None, return_list:bool=False,
                                   datastore_merge:bool=True, resource_id:str=None, error_not_found:bool=True) \
            -> Tuple[Union[bool,None], Union[Dict[str, CkanField], List[dict]]]:
        """
        Calls datastore_field_dict and merges attributes with those found in datastore_info if datastore_merge=True.

        :param fields_update:
        :param fields_type_override:
        :param fields_description:
        :param fields_label:
        :param return_list:
        :param datastore_merge:
        :param resource_id: required if datastore_merge=True
        :return:
        """
        fields_update: Dict[str, CkanField] = CkanApiManage.datastore_field_dict(fields=None, fields_merge=fields_merge, fields_update=fields_update,
                                               fields_type_override=fields_type_override, fields_description=fields_description,
                                               fields_label=fields_label, return_list=False)
        if datastore_merge:
            if error_not_found:
                assert(resource_id is not None)
            datastore_info = self.get_datastore_info_or_request_of_id(resource_id, error_not_found=error_not_found)
            if datastore_info is not None:
                fields_base = copy.deepcopy(datastore_info.fields_dict)
                if len(fields_base) == 0:
                    msg = f"No fields found for {resource_id}"
                    warn(msg)
                fields_new = copy.deepcopy(fields_base)
                update_needed = False
                for field_name, field_info in fields_update.items():
                    if field_name not in fields_base.keys():
                        fields_new[field_name] = field_info
                        update_needed = True
                    else:
                        fields_new[field_name] = fields_base[field_name].merge(field_info)
                        update_needed |= not fields_new[field_name] == fields_base[field_name]
            else:
                fields_new = fields_update
                update_needed = False
            if return_list:
                return update_needed, [field_info.to_ckan_dict() for field_info in fields_new.values()]
            else:
                return update_needed, fields_new
        else:
            if return_list:
                return None, [field_info.to_ckan_dict() for field_info in fields_update.values()]
            else:
                return None, fields_update

    def datastore_field_patch(self, resource_id:str, fields_merge:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None,
                              fields_update:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None, *,
                              only_if_needed:bool=False, fields:Union[List[Union[CkanField,dict]], OrderedDict[str,Union[CkanField,dict]]]=None,
                              fields_type_override:Dict[str,str]=None, field_description:Dict[str,str]=None,
                              fields_label:Dict[str,str]=None) -> Tuple[bool, List[dict], Union[dict,bool,None]]:
        """
        Function helper call to API datastore_create in order to update the parameters of some fields. The initial field
        configuration is taken from the mapped information or requested.
        Typically, this could be used to enforce a data type on a field. In this case, it is required to resubmit the
        resource data with the API resource_patch.
        The field_update argument would be e.g. field_update={"id": {"info": {"type_override": "text"}}}
        This is equivalent to the option field_type_override={"id": "text"}

        __NB__: it is not possible to rename a field after creation through the API. To do this, the change must be done in the database.

        :param resource_id: resource id
        :param fields_update: dictionary of field id and properties to change. The update of the property dictionary is
        recursive, ensuring only the fields appearing in the update are changed.
        This field can be overridden by the values given in field_type_override, field_description, or field_label.
        :param fields_type_override: argument to simplify the edition of the info.type_override value for each field id.
        :param field_description: argument to simplify the edition of the info.notes value for each field id
        :param fields_label: argument to simplify the edition of the info.label value for each field id
        :param only_if_needed: Cancels the request if the changes do not affect the current configuration
        :return: a tuple (update_needed, fields_new, update_dict)
        """
        update_needed, fields_update = self.datastore_field_patch_dict(fields_merge=fields_merge, fields_update=fields_update,
                                                                       fields_type_override=fields_type_override,
                                                                       fields_description=field_description, fields_label=fields_label,
                                                                       datastore_merge=True, resource_id=resource_id, return_list=True)
        if update_needed or not only_if_needed:
            return update_needed, fields_update, self.datastore_create(resource_id, fields=fields_update)
        else:
            return update_needed, fields_update, None


    ## Data deletions ------------------
    def _api_datastore_delete(self, resource_id:str, *, params:dict=None,
                             force:bool=None) -> dict:
        """
        Function to delete rows an api_datastore using api_datastore_upsert.
        If no filter is given, the whole database will be erased.
        This function is private and should not be called directly.

        :param resource_id:
        :param params:
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        if force is None: force = self.params.default_force
        params["resource_id"] = resource_id
        params["force"] = force
        response = self._api_action_request(f"datastore_delete", method=RequestType.Post, json=params)
        if response.success:
            return response.result
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            resource_info = self.resource_show(resource_id)  # will trigger another error if resource does not exist
            raise CkanNotFoundError(self, "DataStore", response)
        else:
            raise response.default_error(self)

    def datastore_delete_rows(self, resource_id:str, filters:dict, *, params:dict=None,
                              force:bool=None, calculate_record_count:bool=True) -> dict:
        """
        Function to delete certain rows a DataStore using _api_datastore_delete.
        The filters are mandatory here.
        If not given, the whole database would be erased. Prefer using datastore_clear for this usage.

        :see: _api_datastore_delete()
        :param resource_id:
        :param filters:
        :param params:
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :param calculate_record_count:
        :return:
        """
        if params is None: params = {}
        if force is None: force = self.params.default_force
        params["filters"] = filters
        params["calculate_record_count"] = calculate_record_count
        assert_or_raise(len(filters) > 0, InvalidParameterError("filters"))
        return self._api_datastore_delete(resource_id, params=params, force=force)

    def datastore_clear(self, resource_id:str, *, error_not_found:bool=True, params:dict=None,
                        force:bool=None, bypass_admin:bool=False) -> Union[dict,None]:
        """
        Function to clear data in a DataStore using _api_datastore_delete. Requires enable_admin=True.
        This implementation adds the option error_not_found. If set to False, no error is raised if the resource is found by the datastore is not.

        :see: _api_datastore_delete()
        :param resource_id:
        :param error_not_found: if False, does not raise an exception if the resource exists but there is not datastore
        :param params:
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :param bypass_admin: option to bypass check of enable_admin
        :return:
        """
        if not bypass_admin:
            assert_or_raise(self.params.enable_admin, AdminFeatureLockedError())
        if params is None: params = {}
        if force is None: force = self.params.default_force
        try:
            result = self._api_datastore_delete(resource_id, params=params, force=force)
            self.map._record_datastore_delete(resource_id)
            return result
        except CkanNotFoundError as e:
            if not error_not_found and e.object_type == "DataStore":
                msg = f"Tried to delete DataStore of existing resource_id {resource_id} but there is no DataStore."
                if self.params.verbose_request:
                    print(msg)
            else:
                raise e from e

    def _api_resource_delete(self, resource_id:str, *, params:dict=None,
                             force:bool=None, bypass_admin:bool=False) -> dict:
        """
        Function to delete a resource. This fully removes the resource, definitively. Requires enable_admin=True.

        :param resource_id:
        :param params:
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :return:
        """
        if not bypass_admin:
            assert_or_raise(self.params.enable_admin, AdminFeatureLockedError())
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        if force is None: force = self.params.default_force
        params["id"] = resource_id
        # params["force"] = force
        response = self._api_action_request(f"resource_delete", method=RequestType.Post, json=params)
        if response.success:
            # update map
            self.map._record_resource_delete(resource_id)
            return response.result
        else:
            raise response.default_error(self)

    def resource_delete(self, resource_id:str, *, params:dict=None,
                        force:bool=None, bypass_admin:bool=False) -> dict:
        # function alias
        return self._api_resource_delete(resource_id, params=params, force=force, bypass_admin=bypass_admin)


    ## Datastore creation ------------------
    @staticmethod
    def default_resource_view(resource_format:str) -> Tuple[str,str]:
        """
        Definition of the default resource view based on the resource format.

        :param resource_format:
        :return:
        """
        if resource_format is None:
            resource_format = "unknown"
        resource_format = resource_format.lower()
        if resource_format == "csv":
            title = "Table"
            view_type = "recline_view"  # Data Explorer
        elif resource_format in {"json", "txt", "py"}:
            title = "Text"
            view_type = "text_view"
        elif resource_format in {"png", "svg"}:
            title = "Image"
            view_type = "image_view"
        else:
            title = None
            view_type = None
        return title, view_type

    def _api_resource_view_create(self, resource_id:str, title:Union[str,List[str]]=None, *,
                                  view_type:Union[str,List[str]]=None, params:dict=None) -> List[CkanViewInfo]:
        """
        API call to resource_view_create.

        title and view_type must have same length if specified as lists.

        :param resource_id:  resource id
        :param title: Title of the resource
        :param view_type: Type of view, typically recline_view for Data Explorer
        :param params:
        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        if title is None or view_type is None:
            raise ValueError("title and view_type must be specified together")
        if isinstance(view_type, str):
            view_type = [view_type]
        if isinstance(title, str):
            title = [title]
        assert(len(title) == len(view_type))
        params["resource_id"] = resource_id
        view_info_list = []
        for title_selected, view_type_selected in zip(title, view_type):
            params["title"] = title_selected
            params["view_type"] = view_type_selected
            response = self._api_action_request(f"resource_view_create", method=RequestType.Post, json=params)
            if response.success:
                view_info = CkanViewInfo(response.result)
                self.map._update_view_info(view_info)
                view_info_list.append(view_info.copy())
            else:
                raise response.default_error(self)
        return view_info_list

    def resource_view_create(self, resource_id:str, title:Union[str,List[str]]=None, *,
                              view_type:Union[str,List[str]]=None, params:dict=None,
                              error_no_default_view_type:bool=False, cancel_if_exists:bool=True) -> List[CkanViewInfo]:
        """
        Encapsulation of the API resource_view_create. If no resource view is provided to create (None),
        the function looks up the default view defined in default_resource_view.
        This function also looks at the existing views and cancels the creation of those which have the same title.
        If provided as a list, title and view_type must have same length.

        :param resource_id:
        :param title:
        :param view_type:
        :param params:
        :param error_no_default_view_type:
        :param cancel_if_exists: option to cancel an existing view if it exists (based on the title)
        :return:
        """
        if title is None and view_type is None:
            resource_info = self.get_resource_info_or_request_of_id(resource_id)
            resource_format = resource_info.format
            title, view_type = self.default_resource_view(resource_format)
            if title is None:
                title = []
                view_type = []
                msg = NoDefaultView(resource_format)
                if error_no_default_view_type:
                    raise(msg)
                else:
                    warn(str(msg))
                    return []
        if isinstance(view_type, str):
            view_type = [view_type]
        if isinstance(title, str):
            title = [title]
        assert(len(title) == len(view_type))
        if cancel_if_exists:
            resource_views_by_title = {view_info.title: view_info for view_info in self.get_resource_view_list_or_request(resource_id)}
            i_rm = []
            for i, view_title in enumerate(title):
                if view_title in resource_views_by_title.keys():
                    i_rm.append(i)
            for i in reversed(i_rm):
                title.pop(i)
                view_type.pop(i)
        if len(title) > 0:
            return self._api_resource_view_create(resource_id, title, view_type=view_type, params=params)
        else:
            return []

    def _api_resource_create(self, package_id:str, name:str, *, format:str=None, description:str=None,
                             state:CkanState=None,
                             df:pd.DataFrame=None, file_path:str=None, url:str=None, files=None,
                             payload:Union[bytes, io.BufferedIOBase]=None, payload_name:str=None,
                             params:dict=None) -> CkanResourceInfo:
        """
        API call to resource_create.

        :see: _api_resource_patch
        :see: resource_create
        :param package_id:
        :param name:
        :param format:
        :param url: url of the resource to replace resource
        :param params: additional parameters such as resource_type can be set

        For file uploads, the following parameters are taken, by order of priority:
        See upload_prepare_requests_files_arg for an example of formatting.

        :param files: files pass through argument to the requests.post function. Use to send other data formats.
        :param payload: bytes to upload as a file
        :param payload_name: name of the payload to use (associated with the payload argument) - this determines the format recognized in CKAN viewers.
        :param file_path: path of the file to transmit (binary and text files are supported here)
        :param df: pandas DataFrame to replace resource

        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        params["package_id"] = package_id
        params["name"] = name
        if format is not None:
            params["format"] = format
        if state is not None:
            params["state"] = str(state)
        if description is not None:
            params["description"] = description
        files = upload_prepare_requests_files_arg(files=files, file_path=file_path, df=df, payload=payload, payload_name=payload_name)
        if url is not None:
            params["url"] = url
            assert(files is None)
        if files is not None:
            response = self._api_action_request(f"resource_create", method=RequestType.Post,
                                                files=files, data=params)
        else:
            response = self._api_action_request(f"resource_create", method=RequestType.Post, json=params)
        if response.success:
            # update map
            resource_info = CkanResourceInfo(response.result)
            self.map._record_resource_create(resource_info)
            return resource_info.copy()
        else:
            raise response.default_error(self)

    def datastore_default_alias(self, resource_name:str, package_name:str, *,
                                query_names:bool=True, error_not_found:bool=True) -> str:
        if query_names:
            package_info = self.get_package_info_or_request(package_name)
            resource_info = self.get_resource_info_or_request(resource_name, package_name, error_not_found=error_not_found)
            if resource_info is not None:
                return CkanApiManage.datastore_default_alias_of_info(resource_info, package_info)
        return CkanApiManage.datastore_default_alias_of_names(resource_name, package_name)

    @staticmethod
    def datastore_default_alias_of_info(resource_info:CkanResourceInfo, package_info:CkanPackageInfo) -> str:
        package_name= package_info.name
        resource_name = resource_info.name
        return CkanApiManage.datastore_default_alias_of_names(resource_name, package_name)

    @staticmethod
    def datastore_default_alias_of_names(resource_name:str, package_name:str) -> str:
        resource_varname = clean_table_name(resource_name.lower().strip())
        alias_name = package_name + default_alias_package_resource_sep + resource_varname
        if alias_name_max_len is not None and len(alias_name) > alias_name_max_len:
            alias_hash = hashlib.sha1(alias_name.encode("utf-8")).hexdigest()
            if default_alias_hash_replace:
                return "alias" + default_alias_hash_sep + alias_hash
            else:
                alias_name_truncated = alias_name[:alias_name_max_len-default_alias_hash_len-len(default_alias_hash_sep)] + default_alias_hash_sep + alias_hash[:default_alias_hash_len]
                return alias_name_truncated
        else:
            return alias_name

    def resource_create(self, package_id:str, name:str, *, format:str=None, description:str=None, state:CkanState=None,
                        params:dict=None,
                        url:str=None,
                        files=None, file_path:str=None, df:pd.DataFrame=None,
                        payload:Union[bytes, io.BufferedIOBase]=None, payload_name:str=None,
                        cancel_if_exists:bool=True, update_if_exists:bool=False, reupload:bool=False, create_default_view:bool=True, auto_submit:bool=False,
                        datastore_create:bool=False, records:Union[dict, List[dict], pd.DataFrame]=None, fields:List[dict]=None,
                            primary_key: Union[str, List[str]] = None, indexes: Union[str, List[str]] = None,
                            aliases: Union[str, List[str]] = None, data_cleaner:CkanDataCleanerABC=None) -> CkanResourceInfo:
        """
        Proxy to API call resource_create verifying if a resource with the same name already exists and adding the default view.

        :param package_id:
        :param name:
        :param format:
        :param params:
        :param cancel_if_exists: check if a resource with the same name already exists in the package on CKAN server
        If a resource with the same name already exists, the info for this resource is returned
        :param update_if_exists: If a resource with the same name already exists (and cancel_if_exists=True), a call to resource_patch is performed.
        :param reupload: re-upload the resource if a resource with the same name already exists and cancel_if_exists=True and update_if_exists=True
        :param create_default_view:

        For file uploads, the following parameters are taken, by order of priority:
        See upload_prepare_requests_files_arg for an example of formatting.

        :param files: files pass through argument to the requests.post function. Use to send other data formats.
        :param payload: bytes to upload as a file
        :param payload_name: name of the payload to use (associated with the payload argument) - this determines the format recognized in CKAN viewers.
        :param file_path: path of the file to transmit (binary and text files are supported here)
        :param df: pandas DataFrame to replace resource

        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        has_file_data = (files is not None or df is not None or file_path is not None or payload is not None)
        has_records = records is not None
        delete_previous_datastore = (has_file_data or has_records) and reupload and datastore_create
        if name is None or name == "":
            raise CkanMandatoryArgumentError("resource_create", "name")
        if df is not None and datastore_id_col in df.columns:
            msg = f"You cannot initiate a resource with an {datastore_id_col} column if you are creating a DataStore. You risk to create a conflict error."
            warn(msg)
        if cancel_if_exists:
            self.map_resources(package_id, only_missing=True)
            if name in self.map.packages[package_id].resources_id_index.keys():
                resource_info = self.map.get_resource_info(name, package_id)
                resource_id = resource_info.id
                resource_info.newly_created = False
                resource_info.newly_updated = False
                delete_previous_datastore = delete_previous_datastore # and self.resource_is_datastore(resource_id)
                if update_if_exists:
                    if has_file_data and not reupload:
                        # cancel reupload if not enabled
                        has_file_data = False
                        df = None
                        file_path = None
                        files = None
                        payload = None
                    if has_records and not reupload:
                        records = None
                        has_records = False
                    if delete_previous_datastore:
                        # if there already was a datastore, clear it
                        self.datastore_clear(resource_id, error_not_found=False, bypass_admin=True)
                    resource_info.update(self.resource_patch(resource_id, name=name, format=format, state=state,
                                                             description=description, url=url,
                                                             df=df, file_path=file_path, files=files,
                                                             payload=payload, payload_name=payload_name))
                    if create_default_view:
                        view_info_list = self.resource_view_create(resource_info.id)
                        resource_info.update_view(view_info_list)
                    if has_file_data:
                        resource_info.newly_updated = True
                        if auto_submit:
                            self.datastore_submit(resource_info.id)
                    if datastore_create or delete_previous_datastore:
                        info = self.datastore_create(resource_info.id, records=records, fields=fields, primary_key=primary_key,
                                                     indexes=indexes, aliases=aliases, delete_previous=False, data_cleaner=data_cleaner)
                    return resource_info
                else:
                    return resource_info
        # here: the resource does not exist => create a new one
        resource_info = self._api_resource_create(package_id, name, format=format, description=description, state=state,
                                                  url=url,
                                                  files=files, file_path=file_path, df=df,
                                                  payload=payload, payload_name=payload_name, params=params)
        resource_info.newly_created = True
        resource_info.newly_updated = False
        if create_default_view:
            view_info_list = self.resource_view_create(resource_info.id)
            resource_info.update_view(view_info_list)
        if auto_submit and has_file_data:
            self.datastore_submit(resource_info.id)
        if datastore_create:
            info = self.datastore_create(resource_info.id, records=records, fields=fields, primary_key=primary_key,
                                         indexes=indexes, aliases=aliases, delete_previous=False, data_cleaner=data_cleaner)
        return resource_info

    def _api_datastore_create(self, resource_id:str, *, records:Union[dict, List[dict], pd.DataFrame]=None,
                             fields:List[Union[dict, CkanField]]=None,
                             primary_key:Union[str, List[str]]=None, indexes:Union[str, List[str]]=None,
                             aliases: Union[str, List[str]]=None,
                             params:dict=None,force:bool=None) -> dict:
        """
        API call to datastore_create.
        This endpoint also supports altering tables, aliases and indexes and bulk insertion.

        :param resource_id: resource id
        :param records:
        :param fields:
        :param primary_key:
        :param indexes:
        :param params:
        :param force:
        :return:
        """
        # assert_or_raise(self.enable_admin, AdvancedFeatureLockedError())
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        if force is None: force = self.params.default_force
        params["resource_id"] = resource_id
        params["force"] = force
        if primary_key is not None:
            if isinstance(primary_key, str):
                primary_key = primary_key.split(ckan_tags_sep)
            params["primary_key"] = primary_key
        if indexes is not None:
            if isinstance(indexes, str):
                indexes = indexes.split(ckan_tags_sep)
            params["indexes"] = indexes
        if aliases is not None:
            if isinstance(aliases, str):
                aliases = aliases.split(ckan_tags_sep)
            params["aliases"] = aliases
        if records is not None:
            if isinstance(records, pd.DataFrame):
                params["records"] = records.to_dict(orient='records')
            else:
                params["records"] = records
        if fields is not None:
            # list of dicts
            fields_list_dict = [field_info.to_ckan_dict() if isinstance(field_info, CkanField) else field_info for field_info in fields]
            params["fields"] = fields_list_dict
        data_payload, json_headers = json_encode_params(params)
        response = self._api_action_request(f"datastore_create", method=RequestType.Post,
                                            data=data_payload, headers=json_headers)
        # response = self._api_action_request(f"datastore_create", method=RequestType.Post, json=params)
        if response.success:
            return response.result
        else:
            raise response.default_error(self)

    def datastore_create(self, resource_id:str, *, delete_previous:bool=False, bypass_admin:bool=False,
                         records:Union[dict, List[dict], pd.DataFrame]=None,
                         fields:List[Union[dict,CkanField]]=None,
                         primary_key:Union[str, List[str]]=None, indexes:Union[str, List[str]]=None,
                         aliases: Union[str, List[str]]=None,
                         params:dict=None,force:bool=None, data_cleaner:CkanDataCleanerABC=None) -> dict:
        """
        Encapsulation of the datastore_create API call.
        This function can optionally clear the DataStore before creating it.

        :param resource_id:
        :param delete_previous: option to delete the previous datastore, if exists (default:False)
        :param records:
        :param fields:
        :param primary_key:
        :param indexes:
        :param params:
        :param force:
        :return:
        """
        if delete_previous:
            self.datastore_clear(resource_id, error_not_found=False, bypass_admin=bypass_admin)
        if data_cleaner is None:
            data_cleaner = self.data_cleaner_upload
        if data_cleaner is not None:
            if not delete_previous:
                fields_for_cleaner_dict = CkanApiManage.datastore_field_dict(fields=fields)
                fields_for_cleaner = OrderedDict([(field_name, CkanField.from_ckan_dict(field_dict)) for field_name, field_dict in fields_for_cleaner_dict.items()])
            else:
                fields_for_cleaner = None
            records = data_cleaner.clean_records(records, known_fields=fields_for_cleaner, inplace=True)
            if len(records) > 0:
                if primary_key is None:
                    primary_key = data_cleaner.field_suggested_primary_key
                if indexes is None:
                    indexes = list(data_cleaner.field_suggested_index)
                fields = data_cleaner.merge_field_changes(fields)
        if self.params.default_alias_enforce:
            resource_info = self.get_resource_info_or_request_of_id(resource_id)
            package_info = self.get_package_info_or_request(resource_info.package_id)
            if aliases is None and not delete_previous:
                datastore_info = self.get_datastore_info_or_request_of_id(resource_id, error_not_found=False)
                if datastore_info is not None:
                    aliases = datastore_info.aliases  # when aliases argument is None, the aliases are not modified => keep existing aliases from server
            default_alias_name = self.datastore_default_alias_of_info(resource_info, package_info)
            if aliases is None:
                aliases = [default_alias_name]
            else:
                aliases.append(default_alias_name)
            aliases = list(set(aliases))  # keep unique values
        return self._api_datastore_create(resource_id, records=records, fields=fields,
                                          primary_key=primary_key, indexes=indexes, aliases=aliases,
                                          params=params, force=force)


    ## Package creation/deletion/edit ------------------
    def _api_package_patch(self, package_id: str, package_name:str=None, private:bool=None, *, title:str=None, notes:str=None, owner_org:str=None,
                           state:Union[CkanState,str]=None, license_id:str=None, tags:List[str]=None, tags_list_dict:List[Dict[str, str]]=None,
                           url:str=None, version:str=None, custom_fields:dict=None,
                           author:str=None, author_email:str=None, maintainer:str=None, maintainer_email:str=None,
                           params:dict=None) -> CkanPackageInfo:
        """
        API call to package_patch. Use to change the properties of a package.
        This method is preferred to package_update which requires to resend the full package configuration.
        (API doc for package_update: It is recommended to call ckanapi_harvesters.logic.action.get.package_show(),
        make the desired changes to the result, and then call package_update() with it.)

        :param package_id:
        :param package_name:
        :param private:
        :param title:
        :param notes:
        :param owner_org:
        :param state:
        :param license_id:
        :param params:
        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        params["id"] = package_id
        params["private"] = private
        if owner_org is None and use_ckan_owner_org_as_default:
            owner_org = self.owner_org
        if owner_org is not None:
            params["owner_org"] = owner_org
        if package_name is not None:
            assert (2 <= len(package_name) <= 100 and re.match(ckan_package_name_re, package_name))
            params["name"] = package_name
        if title is not None:
            params["title"] = title
        if notes is not None:
            params["notes"] = notes
        if url is not None:
            params["url"] = url
        if version is not None:
            params["version"] = version
        if tags is not None:
            if tags_list_dict is None:
                tags_list_dict = []
            for tag in tags:
                tags_list_dict.append({"name": tag})
        if tags_list_dict is not None:
            params["tags"] = tags_list_dict
        if custom_fields is not None:
            params["extras"] = [{"key": key, "value": value if value is not None else ""} for key, value in custom_fields.items()]
        if author is not None:
            params["author"] = author
        if author_email is not None:
            params["author_email"] = author_email
        if maintainer is not None:
            params["maintainer"] = maintainer
        if maintainer_email is not None:
            params["maintainer_email"] = maintainer_email
        if state is not None:
            if isinstance(state, str):
                params["state"] = state
            else:
                params["state"] = str(state)
        if license_id is not None:
            params["license_id"] = license_id
        response = self._api_action_request(f"package_patch", method=RequestType.Post, json=params)
        if response.success:
            # update map
            pkg_info = CkanPackageInfo(response.result)
            self.map._record_package_update(pkg_info)
            return pkg_info.copy()
        else:
            raise response.default_error(self)

    def package_patch(self, package_id: str, package_name:str=None, private:bool=None, *, title:str=None, notes:str=None, owner_org:str=None,
                           state:Union[CkanState,str]=None, license_id:str=None, tags:List[str]=None, tags_list_dict:List[Dict[str, str]]=None,
                           url:str=None, version:str=None, custom_fields:dict=None,
                           author:str=None, author_email:str=None, maintainer:str=None, maintainer_email:str=None,
                           params:dict=None) -> CkanPackageInfo:
        # function alias
        return self._api_package_patch(package_id=package_id, package_name=package_name, private=private,
                                       title=title, notes=notes, owner_org=owner_org, state=state,
                                       license_id=license_id, tags=tags, tags_list_dict=tags_list_dict, url=url, version=version,
                                       custom_fields=custom_fields, author=author, author_email=author_email,
                                       maintainer=maintainer, maintainer_email=maintainer_email,
                                       params=params)

    def package_state_change(self, package_id:str, state:CkanState) -> CkanPackageInfo:
        """
        Change package state using the package_patch API.

        :param package_id:
        :param state:
        :return:
        """
        return self.package_patch(package_id, state=state)

    def _api_package_create(self, name:str, private:bool, *, title:str=None, notes:str=None, owner_org:str=None,
                            state: Union[CkanState, str] = None, license_id: str = None, tags: List[str] = None, tags_list_dict:List[Dict[str, str]]=None,
                            url: str = None, version: str = None, custom_fields: dict = None,
                            author: str = None, author_email: str = None,
                            maintainer: str = None, maintainer_email: str = None,
                            params:dict=None) -> CkanPackageInfo:
        """
        API call to package_create.

        :param name:
        :param private:
        :param title:
        :param notes:
        :param owner_org:
        :param state:
        :param license_id:
        :param tags:
        :param params:
        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        assert(2 <= len(name) <= 100 and re.match(ckan_package_name_re, name))
        params["name"] = name
        params["private"] = private
        if owner_org is None and use_ckan_owner_org_as_default:
            owner_org = self.owner_org
        if owner_org is not None:
            params["owner_org"] = owner_org
        if title is not None:
            params["title"] = title
        if notes is not None:
            params["notes"] = notes
        if url is not None:
            params["url"] = url
        if version is not None:
            params["version"] = version
        if tags is not None:
            if tags_list_dict is None:
                tags_list_dict = []
            for tag in tags:
                tags_list_dict.append({"name": tag})
        if tags_list_dict is not None:
            params["tags"] = tags_list_dict
        if custom_fields is not None:
            params["extras"] = [{"key": key, "value": value if value is not None else ""} for key, value in custom_fields.items()]
        if author is not None:
            params["author"] = author
        if author_email is not None:
            params["author_email"] = author_email
        if maintainer is not None:
            params["maintainer"] = maintainer
        if maintainer_email is not None:
            params["maintainer_email"] = maintainer_email
        if state is not None:
            if isinstance(state, str):
                params["state"] = state
            else:
                params["state"] = str(state)
        if license_id is not None:
            params["license_id"] = license_id
        response = self._api_action_request(f"package_create", method=RequestType.Post, json=params)
        if response.success:
            # update map
            pkg_info = CkanPackageInfo(response.result)
            self.map._record_package_create(pkg_info)
            return pkg_info.copy()
        else:
            raise response.default_error(self)

    def package_create(self, package_name:str, private:bool=True, *, title:str=None, notes:str=None, owner_org:str=None,
                       state: Union[CkanState, str] = None, license_id: str = None, tags: List[str] = None, tags_list_dict:List[Dict[str, str]]=None,
                       url: str = None, version: str = None, custom_fields: dict = None,
                       author: str = None, author_email: str = None,
                       maintainer: str = None, maintainer_email: str = None,
                       params:dict=None, cancel_if_exists:bool=True, update_if_exists=True) -> CkanPackageInfo:
        """
        Helper function to create a new package. This first checks if the package already exists.

        :see: _api_package_create()
        :param package_name:
        :param private:
        :param title:
        :param notes:
        :param owner_org:
        :param license_id:
        :param state:
        :param params:
        :param cancel_if_exists:
        :param update_if_exists:
        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if package_name is None or package_name == "":
            raise CkanMandatoryArgumentError("package_create", "package_name")
        self.map_resources(package_name, only_missing=True, error_not_found=False)
        pkg_info = self.map.get_package_info(package_name, error_not_mapped=False)
        if pkg_info is not None and cancel_if_exists:
            if update_if_exists:
                pkg_info = self.package_patch(pkg_info.id, package_name, private=private, title=title, notes=notes,
                                              owner_org=owner_org, state=state, license_id=license_id, tags=tags,
                                              tags_list_dict=tags_list_dict,
                                              url=url, version=version, custom_fields=custom_fields,
                                              author=author, author_email=author_email,
                                              maintainer=maintainer, maintainer_email=maintainer_email,
                                              params=params)
            pkg_info.newly_created = False
            return pkg_info
        else:
            pkg_info = self._api_package_create(package_name, private, title=title, notes=notes,
                                                owner_org=owner_org, state=state, license_id=license_id, tags=tags,
                                                tags_list_dict=tags_list_dict,
                                                url=url, version=version, custom_fields=custom_fields,
                                                author=author, author_email=author_email,
                                                maintainer=maintainer, maintainer_email=maintainer_email,
                                                params=params)
            pkg_info.newly_created = True
            return pkg_info

    def _api_package_delete(self, package_id:str,
                            *, params:dict=None) -> dict:
        """
        API call to package_delete.
        This marks the package as deleted and does not remove data.

        :param package_id:
        :param params:
        :return:
        """
        assert_or_raise(self.params.enable_admin, AdminFeatureLockedError())
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        params["id"] = package_id
        response = self._api_action_request(f"package_delete", method=RequestType.Post, json=params)
        if response.success:
            # update map
            self.map._record_package_delete_state(package_id)
            return response.result
        else:
            raise response.default_error(self)

    def _api_package_resource_reorder(self, package_id:str, resource_ids: List[str],
                            *, params:dict=None) -> dict:
        """
        API call to package_resource_reorder. Reorders resources within a package.
        Reorder resources against datasets. If only partial resource ids are supplied then these are assumed to be first and the other resources will stay in their original order.

        :param package_id: the id or name of the package to update
        :param resource_ids: a list of resource ids in the order needed
        :param params:
        :return:
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        params["id"] = package_id
        params["order"] = resource_ids
        response = self._api_action_request(f"package_resource_reorder", method=RequestType.Post, json=params)
        if response.success:
            return response.result
        else:
            raise response.default_error(self)

    package_resource_reorder = _api_package_resource_reorder

    def _api_dataset_purge(self, package_id:str,
                           *, params:dict=None) -> dict:
        """
        API call to dataset_purge.
        This fully removes the package.
        This action is not reversible.
        It requires an admin account.

        :param package_id:
        :param params:
        :return:
        """
        assert_or_raise(self.params.enable_admin, AdminFeatureLockedError())
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        params["id"] = package_id
        response = self._api_action_request(f"dataset_purge", method=RequestType.Post, json=params)
        if response.success:
            # update map
            self.map._record_package_purge_removal(package_id)
            return response.result
        else:
            raise response.default_error(self)

    def package_delete_resources(self, package_name:str, *, bypass_admin:bool=False):
        """
        Definitively delete all resources associated with the package.

        :param package_name:
        :return:
        """
        package_info = self.map.get_package_info(package_name)
        resource_ids = [resource_info.id for resource_info in package_info.package_resources.values()]
        for resource_id in resource_ids:
            self.resource_delete(resource_id, bypass_admin=bypass_admin)

    def package_delete(self, package_id:str, definitive_delete:bool=False, *, params:dict=None) -> dict:
        """
        Alias function for package removal. Either calls API package_delete to simply mark for deletion or dataset_purge
        to definitively delete the package.

        :param package_id:
        :param definitive_delete: True: calls dataset_purge (action not reversible), False: calls API package_delete.
        :param params:
        :return:
        """
        if definitive_delete:
            return self._api_dataset_purge(package_id, params=params)
        else:
            return self._api_package_delete(package_id, params=params)


