#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Tuple, Generator, Any, Union, OrderedDict
import io
import json
import re
from warnings import warn
from urllib.parse import urlencode

import numpy as np
import requests
from requests.auth import AuthBase
import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.list_records import ListRecords, records_to_df
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanAliasInfo
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_auxiliary import bytes_to_megabytes
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _reassign_limit_argument
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise, CkanIdFieldTreatment
from ckanapi_harvesters.auxiliary.ckan_auxiliary import datastore_id_col
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType
from ckanapi_harvesters.auxiliary.ckan_model import CkanPackageSizeInfo
from ckanapi_harvesters.auxiliary.urls import url_join, urlsep
from ckanapi_harvesters.auxiliary.ckan_action import (CkanActionResponse, CkanActionNotFoundError, CkanSqlCapabilityError,
                                                      CkanSqlLimitOffsetError)
from ckanapi_harvesters.auxiliary.ckan_errors import (IntegrityError, CkanServerError, CkanArgumentError, SearchAllNoCountsError,
                                                      DataStoreNotFoundError, RequestError, MissingDataStoreInfoError)
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanProgressCallbackABC, CkanCallbackLevel, CkanProgressUnits
from ckanapi_harvesters.ckan_api.ckan_api_params import CkanApiParamsBasic
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.ckan_api.ckan_api_0_base import ckan_request_proxy_default_auth_if_ckan
from ckanapi_harvesters.ckan_api.ckan_api_1_map import CkanApiMap

df_download_read_csv_kwargs = dict(keep_default_na=False)

ckan_dtype_mapper = {
    "text": "str",
    "numeric": "float",
    "timestamp": "datetime64",
    "int": "int",
    "name": "str",
    "oid": "str",  # to confirm
    "bool": "object",  # enable None values but if they are present, booleans are converted to str...
    "json": "object",
}

class CkanApiReadOnlyParams(CkanApiParamsBasic):
    map_all_aliases:bool = True
    default_df_download_id_field_treatment: CkanIdFieldTreatment = CkanIdFieldTreatment.SetIndex
    apply_default_limit_to_sql_when_search_all:bool = True

    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None):
        super().__init__(proxies=proxies, ckan_headers=ckan_headers, http_headers=http_headers)
        self.df_download_id_field_treatment: CkanIdFieldTreatment = self.default_df_download_id_field_treatment

    def copy(self, new_identifier:str=None, *, dest=None):
        if dest is None:
            dest = CkanApiReadOnlyParams()
        super().copy(dest=dest)
        dest.df_download_id_field_treatment = self.df_download_id_field_treatment
        return dest


## Main class ------------------
class CkanApiReadOnly(CkanApiMap):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements requests to read data from the CKAN server resources / DataStores.
    """

    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org:str=None, params:CkanApiReadOnlyParams=None,
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
                         owner_org=owner_org, map=map, identifier=identifier)
        if params is None:
            params = CkanApiReadOnlyParams()
        if proxies is not None:
            params.proxies = proxies
        self.params: CkanApiReadOnlyParams = params

    def _rx_records_df_clean(self, df: pd.DataFrame) -> None:
        """
        Auxiliary function for cleaning dataframe from DataStore requests

        :param df:
        :return:
        """
        if len(df) > 0 and datastore_id_col in df.columns:
            if self.params.df_download_id_field_treatment == CkanIdFieldTreatment.SetIndex:
                # use _id column as new index
                df.set_index(datastore_id_col, drop=False, inplace=True)
                assert(df.index.is_unique)  # verify integrity
            elif self.params.df_download_id_field_treatment == CkanIdFieldTreatment.Remove:
                # remove "_id" column
                df.pop(datastore_id_col)

    @staticmethod
    def read_fields_type_dict(fields_list_dict: List[dict]) -> OrderedDict:
        return OrderedDict([(field_dict["id"], field_dict["type"]) for field_dict in fields_list_dict])

    @staticmethod
    def read_fields_df_args(fields_type_dict: OrderedDict) -> dict:
        if fields_type_dict is None:
            return {}
        # fields_dtype_dict = fields_type_dict.copy()
        # for key, ckan_type in fields_type_dict.items():
        #     if ckan_type in ckan_dtype_mapper:
        #         fields_dtype_dict[key] = ckan_dtype_mapper[ckan_type]
        #     else:
        #         fields_dtype_dict[key] = "object"
        # return dict(names=list(fields_dtype_dict.keys()), dtype=fields_dtype_dict)
        return dict(names=list(fields_type_dict.keys()))

    @staticmethod
    def from_dict_df_args(fields_type_dict: OrderedDict) -> dict:
        df_args_dict = CkanApiReadOnly.read_fields_df_args(fields_type_dict)
        df_args_dict.pop("names")
        return df_args_dict

    @staticmethod
    def _get_default_bom_option_read(bom:bool=None, format:str=None, search_method:bool=False,
                                     apply_defaults:bool=True) -> Union[bool, None]:
        """
        API datastore_dump includes an option to return the BOM (Byte Order Mark) for requests in CSV/TSV format.
        The BOM helps text-processing tools and applications determine the encoding of the file e.g. to distinguish
        between UTF-8 and UTF-16.

        .. note :: To correctly handle BOM characters in pandas.read_csv, you should specify encoding=utf-8-sig parameter.
            This is taken into account in the decoding function.
        """
        if search_method:
            if bom is not None:
                msg = "Argument bom is not used with API datastore_search and will be ignored"
                warn(msg)
            return None  # argument bom not used with API datastore_search
        elif bom is not None or not apply_defaults:
            return bom
        elif format is not None:
            return format.lower() in {"csv", "tsv"}  # equivalent to format.lower() not in {"json", "xml"}
        else:
            return True  # default format is CSV

    @staticmethod
    def _get_default_format_read(format:str=None, search_method:bool=False, return_df:bool=True) -> Union[str, None]:
        """
        Configure default format for best interpretation when reading results
        """
        if search_method:
            # format used for datastore_search
            if return_df and format is None: format = "csv"
        else:
            # format used for DataStore dump
            if return_df and format is None: format = "csv"
        return format

    ## Data queries ------------------
    ### Dump method ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    def _datastore_dump_params(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                               sort:str=None, limit_per_request:int=None, offset:int=None, format:str=None, bom:bool=None,
                               params:dict=None, default_limit_offset:bool=True):
        if params is None:
            params = {}
        if offset is None and default_limit_offset:
            offset = 0
        if offset is not None:
            params["offset"] = offset
        if limit_per_request is None and default_limit_offset:
            limit_per_request = self.params.default_limit_read_per_request
        if limit_per_request is not None:
            params["limit"] = limit_per_request
        if filters is not None:
            if isinstance(filters, str):
                # not recommended
                params["filters"] = filters
            else:
                params["filters"] = json.dumps(filters)
        if q is not None:
            params["q"] = q
        if fields is not None:
            params["fields"] = fields
        if sort is not None:
            params["sort"] = sort
        if format is not None:
            format = format.lower()
            params["format"] = format
        if bom is None and format is not None:
            bom = CkanApiReadOnly._get_default_bom_option_read(bom=bom, format=format, search_method=False,
                                                               apply_defaults=default_limit_offset)
        if bom is not None:
            params["bom"] = bom
        # params["bom"] = True  # useful?
        return params

    def _get_datastore_dump_url(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                               sort:str=None, limit_per_request:int=None, offset:int=None, format:str=None, bom:bool=None,
                               params:dict=None, default_limit_offset:bool=False):
        params = self._datastore_dump_params(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                             sort=sort, limit_per_request=limit_per_request, offset=offset,
                                             format=format, bom=bom, params=params,
                                             default_limit_offset=default_limit_offset)
        url = url_join(self.url, f"datastore/dump/{resource_id}")
        if len(params) > 0:
            url = f"{url}?{urlencode(params)}"
        return url

    # NB: dump methods are not exposed to the user by default. Only datastore_search and resource_download methods are exposed.
    def _api_datastore_dump_raw(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                sort:str=None, limit_per_request:int=None, offset:int=0, format:str=None, bom:bool=None, params:dict=None,
                                compute_len:bool=False) -> requests.Response:
        """
        URL call to datastore/dump URL. Dumps successive lines in the DataStore.

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param format: The return format in the returned response (default=csv, tsv, json, xml) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :return: raw response
        """
        if compute_len:
            raise SearchAllNoCountsError("datastore_search", f"format={format}")
        params = self._datastore_dump_params(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                             sort=sort, limit_per_request=limit_per_request, offset=offset,
                                             format=format, bom=bom, params=params)
        response = self._ckan_url_request(f"datastore/dump/{resource_id}", method=RequestType.Get, params=params)
        if response.status_code == 200:
            return response
        elif response.status_code == 404 and "DataStore resource not found" in response.text:
            raise DataStoreNotFoundError(resource_id, response.content.decode())
        else:
            raise CkanServerError(self, response, response.content.decode())

    def _api_datastore_dump_df(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                               sort:str=None, limit_per_request:int=None, offset:int=0, format:str=None, bom:bool=None, params:dict=None) -> pd.DataFrame:
        """
        Convert output of _api_datastore_dump_raw to pandas DataFrame.
        """
        response = self._api_datastore_dump_raw(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                                sort=sort, limit_per_request=limit_per_request, offset=offset, format=format, bom=bom,
                                                params=params, compute_len=False)
        if format is not None:
            format = format.lower()
        bom = CkanApiReadOnly._get_default_bom_option_read(bom=bom, format=format, search_method=False)
        assert_or_raise(bom is not None, RuntimeError())
        with io.StringIO(response.content.decode()) as stream:
            read_csv_argument_bom = dict(encoding="utf-8-sig") if bom else {}  # dict(encoding="utf-8")
            if format is None or format == "csv":
                response_df = pd.read_csv(stream, **df_download_read_csv_kwargs, **read_csv_argument_bom)
            elif format == "tsv":
                response_df = pd.read_csv(stream, sep="\t", **df_download_read_csv_kwargs, **read_csv_argument_bom)  # not tested
            elif format == "json":
                response_dict = json.load(stream)
                fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response_dict["fields"])
                df_args = CkanApiReadOnly.read_fields_df_args(fields_type_dict)
                response_df = records_to_df(response_dict["records"], df_args)
                response_df.attrs["fields"] = fields_type_dict
            elif format == "xml":
                response_df = pd.read_xml(stream, parser="etree") # , xpath=".//row")  # partially tested  # otherwise, necessitates the installation of parser lxml
            else:
                raise NotImplementedError()
        self._rx_records_df_clean(response_df)
        return response_df

    def _api_datastore_dump_all(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                sort:str=None, limit_per_request:int=None, offset:int=0, format:str=None, bom:bool=None,
                                total_limit:int=None, requests_limit:int=None, params:dict=None, search_all:bool=True, return_df:bool=True,
                                progress_callback:CkanProgressCallbackABC=None) \
            -> Union[pd.DataFrame, requests.Response]:
        """
        Successive calls to _api_datastore_dump_df until an empty list is received.

        :see: _api_datastore_dump()
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param format: The return format in the returned response (default=csv, tsv, json, xml) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            return self._request_all_results_df(api_fun=self._api_datastore_dump_df, params=params,
                                                limit_per_request=limit_per_request, offset=offset,
                                                total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                search_all=search_all, resource_id=resource_id,
                                                filters=filters, q=q, fields=fields, sort=sort, format=format, bom=bom)
        elif search_all:
            # cannot determine the number of records received if the response is not parsed with pandas in this mode
            # at least, the total number of rows should be known
            # concatenation of results requires parsing of the result
            # => this mode is useless => raise error
            raise SearchAllNoCountsError("datastore_dump")
        else:
            response = self._api_datastore_dump_raw(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                                    sort=sort, limit_per_request=limit_per_request, offset=offset, format=format, bom=bom,
                                                    params=params, compute_len=search_all)
            return response

    def _api_datastore_dump_all_page_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                               sort:str=None, limit_per_request:int=None, offset:int=0,
                                               total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                               format:str=None, bom:bool=None,
                                               params:dict=None, search_all:bool=True, return_df:bool=True) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[requests.Response, Any, None]]:
        """
        Successive calls to _api_datastore_dump until an empty list is received.
        Generator implementation which yields one DataFrame per request.

        :see: _api_datastore_dump()
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param format: The return format in the returned response (default=csv, tsv, json, xml) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            return self._request_all_results_page_generator(api_fun=self._api_datastore_dump_df, params=params,
                                                            limit_per_request=limit_per_request, offset=offset,
                                                            total_limit=total_limit, requests_limit=requests_limit,
                                                            progress_callback=progress_callback,
                                                            search_all=search_all, resource_id=resource_id,
                                                            filters=filters, q=q, fields=fields, sort=sort, format=format, bom=bom)
        else:
            return self._request_all_results_page_generator(api_fun=self._api_datastore_dump_raw, params=params,
                                                            limit_per_request=limit_per_request, offset=offset,
                                                            total_limit=total_limit, requests_limit=requests_limit,
                                                            progress_callback=progress_callback,
                                                            search_all=search_all, resource_id=resource_id,
                                                            filters=filters, q=q, fields=fields, sort=sort, format=format, bom=bom,
                                                            compute_len=search_all)


    ### Search method ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    def _datastore_search_params(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                 distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=None, format:str=None,
                                 params:dict=None, default_limit_offset:bool=True) -> dict:
        if params is None:
            params = {}
        if offset is None and default_limit_offset:
            offset = 0
        if offset is not None:
            params["offset"] = offset
        if limit_per_request is None and default_limit_offset:
            limit_per_request = self.params.default_limit_read_per_request
        if limit_per_request is not None:
            params["limit"] = limit_per_request
        params["resource_id"] = resource_id
        if filters is not None:
            if isinstance(filters, str):
                # not recommended
                params["filters"] = filters
            else:
                params["filters"] = json.dumps(filters)
        if q is not None:
            params["q"] = q
        if fields is not None:
            params["fields"] = fields
        if distinct is not None:
            params["distinct"] = distinct
        if sort is not None:
            params["sort"] = sort
        if format is not None:
            format = format.lower()
            params["records_format"] = format
        return params

    def _get_datastore_search_url(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                 distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=None, format:str=None,
                                 params:dict=None, default_limit_offset:bool=False):
        params = self._datastore_search_params(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                               distinct=distinct, sort=sort, limit_per_request=limit_per_request, offset=offset,
                                               format=format, params=params,
                                               default_limit_offset=default_limit_offset)
        base = self._get_api_url("action")
        url = base + urlsep + "datastore_search"
        if len(params) > 0:
            url = f"{url}?{urlencode(params)}"
        return url

    def _api_datastore_search_raw(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                  distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0, format:str=None,
                                  params:dict=None, compute_len:bool=False) -> CkanActionResponse:
        """
        API call to datastore_search. Performs queries on the DataStore.

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param format: The return format in the returned response (default=objects, csv, tsv, lists) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :return:
        """
        params = self._datastore_search_params(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                               distinct=distinct, sort=sort, limit_per_request=limit_per_request, offset=offset,
                                               format=format, params=params)
        response = self._api_action_request(f"datastore_search", method=RequestType.Get, params=params)
        if response.success:
            if response.dry_run:
                response.records = []
                return response
            elif format is None or format in ["objects", "lists"]:
                response.records = response.result["records"]
                response.len = len(response.records)
                response.total_len = response.result["total"]
            elif compute_len:
                raise SearchAllNoCountsError("datastore_search", f"format={format}")
            return response
        elif response.status_code == 404 and response.error_message["__type"] == "Not Found Error":
            raise DataStoreNotFoundError(resource_id, response.error_message)
        else:
            raise response.default_error(self)

    def _api_datastore_search_df(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                 distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0, format:str=None,
                                 params:dict=None, compute_len:bool=True) -> pd.DataFrame:
        """
        Convert output of _api_datastore_search_raw to pandas DataFrame.
        """
        response = self._api_datastore_search_raw(resource_id=resource_id, filters=filters, q=q, fields=fields, format=format,
                                                  distinct=distinct, sort=sort, limit_per_request=limit_per_request, offset=offset,
                                                  params=params, compute_len=False)
        if response.dry_run:
            return pd.DataFrame()
        if format is not None:
            format = format.lower()
        fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
        if format is None or format == "objects":
            df_args_dict = CkanApiReadOnly.from_dict_df_args(fields_type_dict)
            response_df = pd.DataFrame.from_dict(response.records, **df_args_dict)
        else:
            df_args = CkanApiReadOnly.read_fields_df_args(fields_type_dict)
            if format == "lists":
                response_df = records_to_df(response.records, df_args)
            else:
                with io.StringIO(response.result["records"]) as stream:
                    if format == "csv":
                        response_df = pd.read_csv(stream, **df_args, **df_download_read_csv_kwargs)
                    elif format == "tsv":
                        response_df = pd.read_csv(stream, sep='\t', **df_args, **df_download_read_csv_kwargs)
                    else:
                        raise NotImplementedError()
        self._rx_records_df_clean(response_df)
        response.result.pop("records")
        response_df.attrs["result"] = response.result
        response_df.attrs["fields"] = fields_type_dict
        response_df.attrs["total"] = response.result["total"]
        response_df.attrs["total_was_estimated"] = response.result["total_was_estimated"]
        response_df.attrs["limit"] = response.result["limit"]
        return response_df

    def _api_datastore_search_all(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                  distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0, format:str=None,
                                  total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                  search_all:bool=True, params:dict=None, return_df:bool=True, compute_len:bool=False) \
            -> Union[pd.DataFrame, ListRecords, Any]:
        """
        Successive calls to _api_datastore_search_df until an empty list is received.

        :see: _api_datastore_search()
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param format: The return format in the returned response (default=objects, csv, tsv, lists) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            df = self._request_all_results_df(api_fun=self._api_datastore_search_df, params=params, limit_per_request=limit_per_request, offset=offset,
                                              total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                              search_all=search_all, resource_id=resource_id,
                                              filters=filters, q=q, fields=fields, distinct=distinct, sort=sort, format=format)
            if "fields" in df.attrs.keys():
                df.attrs["fields"] = df.attrs["fields"][0]
            if "total" in df.attrs.keys():
                assert_or_raise(np.all(np.array(df.attrs["total"]) == df.attrs["total"][0]), IntegrityError("total field varied across requests"))
                df.attrs["total"] = df.attrs["total"][0]
            return df
        else:
            responses = self._request_all_results_list(api_fun=self._api_datastore_search_raw, params=params, limit_per_request=limit_per_request, offset=offset,
                                            total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                            search_all=search_all, resource_id=resource_id,
                                            filters=filters, q=q, fields=fields, distinct=distinct, sort=sort, format=format, compute_len=compute_len)
            # aggregate results, depending on the format
            if self.params.dry_run:
                return []
            if format is not None:
                format = format.lower()
            if len(responses) > 0:
                response = responses[0]
                fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                df_args = CkanApiReadOnly.read_fields_df_args(fields_type_dict)
            else:
                fields_type_dict = None
                df_args = {}
            if format is None or format == "objects":
                return ListRecords(sum([response.records for response in responses], []))
            else:
                if format == "lists":
                    return sum([response.records for response in responses], [])
                else:
                    if total_limit is not None:
                        msg = "Cannot apply total_limit in raw format"
                        warn(msg)
                    return "\n".join([response.records for response in responses])

    def _api_datastore_search_all_page_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                                 distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0,
                                                 format:str=None, search_all:bool=True,
                                                 total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                                 params:dict=None, return_df:bool=True) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None]]:
        """
        Successive calls to _api_datastore_search_df until an empty list is received.
        Generator implementation which yields one DataFrame per request.

        :see: _api_datastore_search()
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param format: The return format in the returned response (default=objects, csv, tsv, lists) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            return self._request_all_results_page_generator(api_fun=self._api_datastore_search_df, params=params, limit_per_request=limit_per_request, offset=offset,
                                                            total_limit=total_limit, requests_limit=requests_limit,
                                                            progress_callback=progress_callback,
                                                            search_all=search_all, resource_id=resource_id,
                                                            filters=filters, q=q, fields=fields, distinct=distinct, sort=sort, format=format, compute_len=True)
        else:
            return self._request_all_results_page_generator(api_fun=self._api_datastore_search_raw, params=params,
                                                            limit_per_request=limit_per_request, offset=offset, search_all=search_all,
                                                            total_limit=total_limit, requests_limit=requests_limit,
                                                            progress_callback=progress_callback,
                                                            resource_id=resource_id, filters=filters, q=q,
                                                            fields=fields, distinct=distinct, sort=sort,
                                                            format=format, compute_len=search_all)


    ### search_sql method ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    @staticmethod
    def _datastore_search_sql_apply_default_limit(search_all:bool) -> bool:
        if search_all:
            return CkanApiReadOnlyParams.apply_default_limit_to_sql_when_search_all
        else:
            return False  # do not apply default limits when using datastore_search_sql in mode search_all=False => user can include a LIMIT statement in his query

    def _api_datastore_search_sql_raw(self, sql:str, *, params:dict=None, limit_per_request:int=None, offset:int=None) -> CkanActionResponse:
        """
        API call to datastore_search_sql. Performs SQL queries on the DataStore. These queries can be more complex than
        with datastore_search. The DataStores are referenced by their resource_id, surrounded by quotes. The field names
        are referred by their name in upper case, surrounded by quotes.
        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        .. note : The limit_per_request and offset parameters modify the SQL query if these statements are not already part of the query
            (otherwise an error is raised).

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param params: N/A
        :return:
        """
        if params is None:
            params = {}
        if limit_per_request is None and CkanApiReadOnly._datastore_search_sql_apply_default_limit(search_all=False) and not re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
            limit_per_request = self.params.default_limit_read_per_request
        if limit_per_request is not None:
            if re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
                raise CkanSqlLimitOffsetError("SQL LIMIT statement is already specified in query. Consider using option search_all=False.")
            else:
                sql = sql + f' LIMIT {limit_per_request}'
        # if offset is None:
        #     offset = 0
        if offset is not None:
            if re.search(r'\bOFFSET\b', sql, re.IGNORECASE):
                raise CkanSqlLimitOffsetError("SQL OFFSET statement is already specified in query. Consider using option search_all=False.'")
            else:
                sql = sql + f' OFFSET {offset}'
        params["sql"] = sql
        response = self._api_action_request(f"datastore_search_sql", method=RequestType.Post, params=params)
        if response.success:
            if response.dry_run:
                response.records = []
                return response
            else:
                response.records = response.result["records"]
                response.len = len(response.records)
                # total_len is not available
            return response
        elif response.status_code == 400 and response.success_json_loads and response.response.text == '"Bad request - Action name not known: datastore_search_sql"':
            raise CkanSqlCapabilityError(self, response)
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanActionNotFoundError(self, "SQL", response)
        else:
            raise response.default_error(self)

    def _api_datastore_search_sql_df(self, sql:str, *, params:dict=None, limit_per_request:int=None, offset:int=None) -> pd.DataFrame:
        """
        Convert output of _api_datastore_search_sql_raw to pandas DataFrame.
        """
        response = self._api_datastore_search_sql_raw(sql=sql, params=params, limit_per_request=limit_per_request, offset=offset)
        fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
        df_args_dict = CkanApiReadOnly.from_dict_df_args(fields_type_dict)
        response_df = pd.DataFrame.from_dict(response.records, **df_args_dict)
        response.result.pop("records")
        response_df.attrs["result"] = response.result
        response_df.attrs["fields"] = fields_type_dict
        # response_df.attrs["total"] = response.result["total"]
        # response_df.attrs["total_was_estimated"] = response.result["total_was_estimated"]
        # response_df.attrs["limit"] = response.result["limit"]
        self._rx_records_df_clean(response_df)
        return response_df

    def _api_datastore_search_sql_all(self, sql:str, *, params:dict=None,
                                      search_all:bool=True, limit_per_request:int=None, offset:int=None,
                                      total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                      return_df:bool=True) \
            -> Union[pd.DataFrame, ListRecords]:
        """
        Successive calls to _api_datastore_search_sql until an empty list is received.

        :see: _api_datastore_search_sql()
        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param params: N/A
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            df = self._request_all_results_df(api_fun=self._api_datastore_search_sql_df, params=params,
                                              limit_per_request=limit_per_request, offset=offset,
                                              default_limit_per_request=CkanApiReadOnly._datastore_search_sql_apply_default_limit(search_all=search_all),
                                              total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                              search_all=search_all, sql=sql)
            if "fields" in df.attrs.keys():
                df.attrs["fields"] = df.attrs["fields"][0]
            # if "total" in df.attrs.keys():
            #     assert_or_raise(np.all(np.array(df.attrs["total"]) == df.attrs["total"][0]), IntegrityError("total field varied across requests"))
            #     df.attrs["total"] = df.attrs["total"][0]
            return df
        else:
            responses = self._request_all_results_list(api_fun=self._api_datastore_search_sql_raw, params=params,
                                                       limit_per_request=limit_per_request, offset=offset, requests_limit=requests_limit,
                                                       total_limit=total_limit, default_limit_per_request=CkanApiReadOnly._datastore_search_sql_apply_default_limit(search_all=search_all),
                                                       progress_callback=progress_callback,
                                                       search_all=search_all, sql=sql)
            # TODO: test
            if len(responses) > 0:
                response = responses[0]
                fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
            else:
                fields_type_dict = None
            return ListRecords(sum([response.records for response in responses], []))

    def _api_datastore_search_sql_all_page_generator(self, sql:str, *, params:dict=None,
                                                     search_all:bool=True, limit_per_request:int=None, offset:int=0,
                                                     total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                                     return_df:bool=True) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None]]:
        """
        Successive calls to _api_datastore_search_sql until an empty list is received.
        Generator implementation which yields one DataFrame per request.

        :see: _api_datastore_search_sql()
        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit_per_request: Limit the number of records per request. This parameter applies if there is no LIMIT statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param offset: Offset in the returned records. This parameter applies if there is no OFFSET statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param params: N/A
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            return self._request_all_results_page_generator(api_fun=self._api_datastore_search_sql_df, params=params,
                                                            limit_per_request=limit_per_request, offset=offset, search_all=search_all,
                                                            default_limit_per_request=CkanApiReadOnly._datastore_search_sql_apply_default_limit(search_all=search_all),
                                                            total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                            sql=sql)
        else:
            return self._request_all_results_page_generator(api_fun=self._api_datastore_search_sql_raw, params=params,
                                                            limit_per_request=limit_per_request, offset=offset, search_all=search_all,
                                                            default_limit_per_request=CkanApiReadOnly._datastore_search_sql_apply_default_limit(search_all=search_all),
                                                            total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                            sql=sql)


    ## Function aliases to limit the entry-points for the user  -------------------------------------------------------
    def get_datastore_search_url(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                 distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=None, format:str=None, bom:bool=None,
                                 params:dict=None, default_limit_offset:bool=False, search_method:bool=True):
        """
        Obtain the datastore search URL used for the datastore_search query
        """
        return_df = False
        format = CkanApiReadOnly._get_default_format_read(format=format, search_method=search_method,
                                                          return_df=return_df)
        bom = CkanApiReadOnly._get_default_bom_option_read(bom=bom, format=format, search_method=search_method,
                                                           apply_defaults=default_limit_offset)
        if search_method:
            assert_or_raise(bom is None, CkanArgumentError("datastore_search", "bom"))
            return self._get_datastore_search_url(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                                  distinct=distinct, sort=sort, limit_per_request=limit_per_request, offset=offset,
                                                  format=format, params=params, default_limit_offset=default_limit_offset)
        else:
            assert_or_raise(distinct is None, CkanArgumentError("DataStore dump", "distinct"))
            return self._get_datastore_dump_url(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                                sort=sort, limit_per_request=limit_per_request, offset=offset, bom=bom,
                                                format=format, params=params, default_limit_offset=default_limit_offset)

    def datastore_search(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                         distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0,
                         total_limit:int=None, requests_limit:int=None, search_all:bool=False,
                         search_method:bool=True, params:dict=None, limit:int=None,
                         progress_callback:CkanProgressCallbackABC=None,
                         format:str=None, bom:bool=None, return_df:bool=True) \
            -> Union[pd.DataFrame, ListRecords, Any, List[CkanActionResponse]]:
        """
        Preferred entry-point for a DataStore read request.
        Uses the API datastore_search

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param limit: previously limit_per_request, now stands for total_limit. This parameter is deprecated and will be removed in a future release.
        :param progress_callback: Progress callback function
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :return:
        """
        if limit is not None:
            locals_update = _reassign_limit_argument(limit, total_limit=total_limit, limit_per_request=limit_per_request)
            total_limit = locals_update["total_limit"]
            limit_per_request = locals_update["limit_per_request"]
        format = CkanApiReadOnly._get_default_format_read(format=format, search_method=search_method,
                                                          return_df=return_df)
        bom = CkanApiReadOnly._get_default_bom_option_read(bom=bom, format=format, search_method=search_method)
        if search_method:
            assert_or_raise(bom is None, CkanArgumentError("datastore_search", "bom"))
            return self._api_datastore_search_all(resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort,
                                                  limit_per_request=limit_per_request, offset=offset, total_limit=total_limit, requests_limit=requests_limit,
                                                  progress_callback=progress_callback,
                                                  format=format, params=params, search_all=search_all, return_df=return_df)
        else:
            assert_or_raise(distinct is None, CkanArgumentError("DataStore dump", "distinct"))
            return self._api_datastore_dump_all(resource_id, filters=filters, q=q, fields=fields, sort=sort,
                                                limit_per_request=limit_per_request, offset=offset, requests_limit=requests_limit, total_limit=total_limit,
                                                progress_callback=progress_callback,
                                                format=format, bom=bom, params=params, search_all=search_all, return_df=return_df)

    def datastore_search_page_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                        distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0,
                                        total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None, params:dict=None,
                                        search_all:bool=True, search_method:bool=True, format:str=None, bom:bool=None, return_df:bool=True, limit:int=None) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None], Generator[requests.Response, Any, None]]:
        """
        Preferred entry-point for a DataStore read request.
        Uses the API datastore_search

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param limit: previously limit_per_request, now stands for total_limit. This parameter is deprecated and will be removed in a future release.
        :param progress_callback: Progress callback function
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :param return_df: Return pandas DataFrame (True) or dict (False)
        :return:
        """
        if limit is not None:
            locals_update = _reassign_limit_argument(limit, total_limit=total_limit, limit_per_request=limit_per_request)
            total_limit = locals_update["total_limit"]
            limit_per_request = locals_update["limit_per_request"]
        format = CkanApiReadOnly._get_default_format_read(format=format, search_method=search_method,
                                                          return_df=return_df)
        bom = CkanApiReadOnly._get_default_bom_option_read(bom=bom, format=format, search_method=search_method)
        if search_method:
            assert_or_raise(bom is None, CkanArgumentError("datastore_search", "bom"))
            return self._api_datastore_search_all_page_generator(resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort,
                                                                 limit_per_request=limit_per_request, offset=offset, total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                                 format=format, params=params, search_all=search_all, return_df=return_df)
        else:
            assert_or_raise(distinct is None, CkanArgumentError("DataStore dump", "distinct"))
            return self._api_datastore_dump_all_page_generator(resource_id, filters=filters, q=q, fields=fields, sort=sort,
                                                               limit_per_request=limit_per_request, offset=offset, total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                               format=format, bom=bom, params=params, search_all=search_all, return_df=return_df)

    def datastore_search_cursor(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                distinct:bool=None, sort:str=None, limit_per_request:int=None, offset:int=0,
                                total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None, params:dict=None,
                                search_all:bool=True, search_method:bool=True, format:str=None, bom:bool=None, return_df:bool=False, limit:int=None) \
            -> Generator[Union[pd.Series, dict, list, str], Any, None]:
        """
        Cursor on rows of datastore_search

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit_per_request: Limit the number of records per request
        :param offset: Offset in the returned records
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param limit: previously limit_per_request, now stands for total_limit. This parameter is deprecated and will be removed in a future release.
        :param progress_callback: Progress callback function
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :param return_df: Return pandas Series (True) or dict (False)
        :param format: Format of the data requested through the API. This does not change the output if return_df is True.
        :return:
        """
        limit_reached = False
        if total_limit is not None and total_limit <= 0:
            limit_reached = True
            return
        row_index = 0
        page_generator = self.datastore_search_page_generator(resource_id, filters=filters, q=q, fields=fields,
                                                              distinct=distinct, sort=sort, limit_per_request=limit_per_request, offset=offset,
                                                              total_limit=total_limit, requests_limit=requests_limit,
                                                              progress_callback=progress_callback, params=params, limit=limit,
                                                              search_all=search_all, search_method=search_method, format=format, bom=bom, return_df=return_df)
        if return_df:
            df: pd.DataFrame
            row: pd.Series
            for df in page_generator:
                for row_loc, row in df.iterrows():
                    yield row
                    row_index += 1
                    if total_limit is not None and row_index >= total_limit:  # double-check from page generator
                        limit_reached = True
                        return
        elif search_method:
            response: CkanActionResponse
            # response.result: list
            if format is not None:
                format = format.lower()
            if format is None or format == "objects":
                for response in page_generator:
                    fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                    for element in response.records:
                        yield element
                        row_index += 1
                        if total_limit is not None and row_index >= total_limit:  # double-check from page generator
                            limit_reached = True
                            return
            else:
                for response in page_generator:
                    fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                    for element in response.records:
                        yield element
                        row_index += 1
                        if total_limit is not None and row_index >= total_limit:  # double-check from page generator
                            limit_reached = True
                            return
        else:
            raise TypeError("dumping datastore without parsing with a DataFrame does not return an iterable object")

    def datastore_search_sql(self, sql:str, *, params:dict=None, search_all:bool=False,
                             limit_per_request:int=None, offset:int=None, total_limit:int=None, requests_limit:int=None,
                             progress_callback:CkanProgressCallbackABC=None, return_df:bool=True, limit:int=None) \
            -> Union[pd.DataFrame, Tuple[ListRecords, dict]]:
        """
        Preferred entry-point for a DataStore SQL request.
        :see: _api_datastore_search_sql()
        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit_per_request: Limit the number of records per request. This parameter applies if there is no LIMIT statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param offset: Offset in the returned records. This parameter applies if there is no OFFSET statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param limit: previously limit_per_request, now stands for total_limit. This parameter is deprecated and will be removed in a future release.
        :param progress_callback: Progress callback function
        :param params: N/A
        :param search_all: Option to renew the request until there are no more records.
        :param return_df: Return pandas DataFrame (True) or dict (False)
        :return:
        """
        if limit is not None:
            locals_update = _reassign_limit_argument(limit, total_limit=total_limit, limit_per_request=limit_per_request)
            total_limit = locals_update["total_limit"]
            limit_per_request = locals_update["limit_per_request"]
        return self._api_datastore_search_sql_all(sql, params=params, limit_per_request=limit_per_request, offset=offset,
                                                  total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                  search_all=search_all, return_df=return_df)

    def datastore_search_sql_page_generator(self, sql:str, *, params:dict=None, search_all:bool=True,
                                            limit_per_request:int=None, offset:int=None,
                                            total_limit:int=None, requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                            return_df:bool=True, limit:int=None) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None]]:
        """
        Preferred entry-point for a DataStore SQL request.
        :see: _api_datastore_search_sql()

        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit_per_request: Limit the number of records per request. This parameter applies if there is no LIMIT statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param offset: Offset in the returned records. This parameter applies if there is no OFFSET statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param limit: previously limit_per_request, now stands for total_limit. This parameter is deprecated and will be removed in a future release.
        :param progress_callback: Progress callback function
        :param params: N/A
        :param search_all: Option to renew the request until there are no more records.
        :param return_df: Return pandas DataFrame (True) or dict (False)
        :return:
        """
        if limit is not None:
            locals_update = _reassign_limit_argument(limit, total_limit=total_limit, limit_per_request=limit_per_request)
            total_limit = locals_update["total_limit"]
            limit_per_request = locals_update["limit_per_request"]
        return self._api_datastore_search_sql_all_page_generator(sql, params=params, limit_per_request=limit_per_request, offset=offset,
                                                                 total_limit=total_limit, requests_limit=requests_limit, progress_callback=progress_callback,
                                                                 search_all=search_all, return_df=return_df)

    def datastore_search_sql_cursor(self, sql:str, *, params:dict=None, search_all:bool=True,
                                    limit_per_request:int=None, offset:int=None, total_limit:int=None,
                                    requests_limit:int=None, progress_callback:CkanProgressCallbackABC=None,
                                    return_df:bool=False, limit:int=None) \
            -> Generator[Union[pd.Series,dict], Any, None]:
        """
        Preferred entry-point for a DataStore SQL request, to iterate over records.
        :see: _api_datastore_search_sql()

        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit_per_request: Limit the number of records per request. This parameter applies if there is no LIMIT statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param offset: Offset in the returned records. This parameter applies if there is no OFFSET statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param total_limit: Strictly limit the number of records to return, counting from the initial offset
        :param requests_limit: Limit the number of requests
        :param limit: previously limit_per_request, now stands for total_limit. This parameter is deprecated and will be removed in a future release.
        :param progress_callback: Progress callback function
        :param params: N/A
        :param search_all: Option to renew the request until there are no more records.
        :param return_df: Return pandas Series (True) or dict (False)
        :return:
        """
        limit_reached = False
        if total_limit is not None and total_limit <= 0:
            limit_reached = True
            return
        row_index = 0
        page_generator = self.datastore_search_sql_page_generator(sql, params=params, search_all=search_all,
                                                                  limit_per_request=limit_per_request, offset=offset,
                                                                  total_limit=total_limit, requests_limit=requests_limit, limit=limit,
                                                                  progress_callback=progress_callback,
                                                                  return_df=return_df)
        if return_df:
            df: pd.DataFrame
            row: pd.Series
            for df in page_generator:
                for row_loc, row in df.iterrows():
                    yield row
                    row_index += 1
                    if total_limit is not None and row_index >= total_limit:  # double-check from page generator
                        limit_reached = True
                        return
        else:
            response: CkanActionResponse
            # response.result: list
            element: Any
            for response in page_generator:
                fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                for element in response.records:
                    yield element
                    row_index += 1
                    if total_limit is not None and row_index >= total_limit:  # double-check from page generator
                        limit_reached = True
                        return

    def datastore_search_sql_find_one(self, sql:str, *, params:dict=None,
                                      offset:int=0, return_df:bool=True) -> Union[pd.DataFrame, Tuple[ListRecords, dict]]:
        """
        First element of an SQL request

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param offset: Offset in the returned records. This parameter applies if there is no OFFSET statement
            in the sql query. Incompatible usage raises a CkanSqlLimitOffsetError.
        :param params: N/A
        :param return_df: Return pandas Series (True) or dict (False)
        """
        df_row = self.datastore_search_sql(sql, limit_per_request=1, total_limit=1, search_all=False, offset=offset,
                                           params=params, return_df=return_df)
        return df_row

    def datastore_search_sql_fields_type_dict(self, sql:str, *, params:dict=None) -> OrderedDict:
        document, fields_dict = self.datastore_search_sql_find_one(sql, offset=0, params=params, return_df=False)
        return fields_dict

    def datastore_search_find_one(self, resource_id:str, *, filters:dict=None, q:str=None, distinct:bool=None,
                                  fields:List[str]=None, offset:int=0, return_df:bool=True) \
            -> Union[pd.DataFrame, ListRecords, Any, List[CkanActionResponse]]:
        """
        Request first result of a query

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param offset: Offset in the returned records
        :param return_df: Return pandas Series (True) or dict (False)
        :return:
        """
        # resource_info = self.get_resource_info_or_request(resource_id)
        # return resource_info.datastore_info.row_count
        df_row = self.datastore_search(resource_id, limit_per_request=1, total_limit=1, search_all=False, filters=filters, q=q,
                                       distinct=distinct, fields=fields, offset=offset, return_df=return_df)
        return df_row

    def datastore_search_fields_type_dict(self, resource_id:str, *,
                                          filters:dict=None, q:str=None, distinct:bool=None, fields:List[str]=None,
                                          request_missing:bool=True, error_not_mapped:bool=False,
                                          error_not_found:bool=True) -> OrderedDict:
        if fields is None:
            # if no field restriction was provided, refer to the fields of the DataStore
            fields_list = self.get_datastore_fields_or_request_of_id(resource_id, return_list=True,
                                                                     request_missing=request_missing,
                                                                     error_not_mapped=error_not_mapped,
                                                                     error_not_found=error_not_found)
            return CkanApiReadOnly.read_fields_type_dict(fields_list)
        else:
            document, fields_dict = self.datastore_search_find_one(resource_id, filters=filters, q=q, distinct=distinct,
                                                                   fields=fields, return_df=False)
            return fields_dict

    def datastore_search_row_count(self, resource_id:str, *, filters:dict=None, q:str=None, distinct:bool=None,
                                   fields:List[str]=None) -> int:
        """
        Request the number of rows in a DataStore

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :return:
        """
        df_row = self.datastore_search_find_one(resource_id, filters=filters, q=q, distinct=distinct,
                                                fields=fields, return_df=True)
        return df_row.attrs["total"]

    def test_sql_capabilities(self, *, raise_error:bool=False) -> bool:
        """
        Test the availability of the API datastore_search_sql

        :return:
        """
        try:
            self.api_help_show("datastore_search_sql", print_output=False)
            return True
        except CkanActionNotFoundError:
            if raise_error:
                raise CkanSqlCapabilityError(self, CkanActionResponse(requests.Response()))
            return False


    ## Resource download by direct link (FileStore) -----------------------------------------------
    def get_resource_download_url(self, resource_id:str, package_name:str=None):
        resource_info = self.get_resource_info_or_request(resource_id, package_name=package_name)
        return resource_info.download_url

    def resource_download(self, resource_id:str, *, method:str=None,
                          proxies:dict=None, headers:dict=None, auth: Union[AuthBase, Tuple[str,str]]=None,
                          verify:Union[bool,str,None]=None, stream:bool=False) \
            -> Tuple[CkanResourceInfo, Union[requests.Response,None]]:
        """
        Uses the link provided in resource_show to download a resource.

        :param resource_id: resource id
        :return:
        """
        resource_info = self.get_resource_info_or_request(resource_id)
        url = resource_info.download_url
        if len(url) == 0:
            return resource_info, None
        response = self.download_url_proxy(url, method=method, auth_if_ckan=ckan_request_proxy_default_auth_if_ckan,
                                           proxies=proxies, headers=headers, auth=auth, verify=verify, stream=stream)
        content_length = response.headers.get("content-length", None)
        if content_length is not None:
            resource_info.download_size_mb = bytes_to_megabytes(int(content_length))
        return resource_info, response

    def resource_download_test_head(self, resource_id:str, *, raise_error:bool=False,
                                    proxies:dict=None, headers:dict=None, auth: Union[AuthBase, Tuple[str,str]]=None, verify:Union[bool,str,None]=None) \
            -> Union[None,ContextErrorLevelMessage]:
        """
        This sends a HEAD request to the resource download url using the CKAN connexion parameters via resource_download.
        The resource is not downloaded but the headers indicate if the url is valid.

        :return: None if successful
        """
        resource_info = self.get_resource_info_or_request_of_id(resource_id)
        try:
            _, response = self.resource_download(resource_id, method="HEAD", proxies=proxies, headers=headers, auth=auth, verify=verify)
        except Exception as e:
            if raise_error:
                raise e from e
            return ContextErrorLevelMessage(f"Resource from URL {resource_info.name}", ErrorLevel.Error, f"Failed to query download url for resource id {resource_id}: {str(e)}")
        if response.ok and response.status_code == 200:
            return None
        else:
            if raise_error:
                raise RequestError(f"Failed to query download url for resource id {resource_id}: status {response.status_code} {response.reason}")
            return ContextErrorLevelMessage(f"Resource from URL {resource_info.name}", ErrorLevel.Error, f"Failed to query download url for resource id {resource_id}: status {response.status_code} {response.reason}")

    def resource_download_df(self, resource_id:str, *, method:str=None,
                          proxies:dict=None, headers:dict=None, auth: Union[AuthBase, Tuple[str,str]]=None, verify:Union[bool,str,None]=None) \
            -> Tuple[CkanResourceInfo, Union[pd.DataFrame,None]]:
        """
        Uses the link provided in resource_show to download a resource and interprets it as a DataFrame.

        :param resource_id: resource id
        :return:
        """
        resource_info, response = self.resource_download(resource_id, method=method, proxies=proxies, headers=headers, auth=auth, verify=verify)
        if response is None:
            return resource_info, None
        with io.StringIO(response.content.decode()) as stream:
            df = pd.read_csv(stream, **df_download_read_csv_kwargs)
        self._rx_records_df_clean(df)
        return resource_info, df

    def map_file_resource_sizes(self, resource_list:List[str]=None,
                                *, package_list:List[str]=None,
                                cancel_if_present:bool=True, progress_callback:CkanProgressCallbackABC=None) -> None:
        num_resources = len(self.map.resources)
        if progress_callback is not None:
            progress_callback.start_task(num_resources, level=CkanCallbackLevel.Resources, units=CkanProgressUnits.Items)
        if package_list is not None:
            if resource_list is None:
                resource_list = []
            resource_list = list(set(resource_list).union(set(self.get_resource_ids_of_package_list(package_list))))
        if resource_list is None:
            resource_info_dict = self.map.resources
        else:
            resource_info_dict =  {resource_id: self.get_resource_info_or_request_of_id(resource_id) for resource_id in resource_list}
        for i_resource, (resource_id, resource_info) in enumerate(resource_info_dict.items()):
            if progress_callback is not None:
                progress_callback.update_task(i_resource, num_resources, level=CkanCallbackLevel.Resources)
            if resource_info.download_url:
                if not (cancel_if_present and resource_info.download_size_mb is not None):
                    try:
                        _, response = self.resource_download(resource_id, method="HEAD")
                        content_length_str = response.headers.get("content-length", None)
                    except Exception as e:
                        msg = f"Failed to query download url for resource id {resource_id}: {str(e)}"
                        warn(msg)
                        content_length_str = None
                    if content_length_str is not None:
                        content_length = int(content_length_str)  # raise error if not found or bad format
                        resource_info.download_size_mb = bytes_to_megabytes(content_length)
                    else:
                        resource_info.download_size_mb = None
        if progress_callback is not None:
            progress_callback.end_task(num_resources, level=CkanCallbackLevel.Resources)

    def _update_package_size_fields(self, package_list:List[str]=None) -> None:
        if package_list is None:
            package_list = list(self.map.packages.keys())
        for package_name in package_list:
            package_info = self.get_package_info_or_request(package_name)
            if package_info is not None:
                if package_info.package_size is None:
                    package_info.package_size = CkanPackageSizeInfo()
                package_size = package_info.package_size
                package_size.reset()
                package_size.resource_count = len(package_info.package_resources)
                for resource_id in package_info.package_resources.keys():
                    resource_info = self.map.get_resource_info(resource_id)
                    if not resource_info.datastore_queried():
                        raise MissingDataStoreInfoError(resource_info.id)
                    resource_modified = resource_info.last_modified if resource_info.last_modified is not None else resource_info.created
                    internal_filestore = self.is_url_internal(resource_info.download_url)
                    if resource_modified is not None:
                        package_size.date_last_modified_resource = max(package_size.date_last_modified_resource, resource_modified) \
                            if package_size.date_last_modified_resource else resource_modified
                    if resource_info.metadata_modified is not None:
                        package_size.date_last_modified_resource_metadata = max(package_size.date_last_modified_resource_metadata,
                                                                                resource_info.metadata_modified) \
                            if package_size.date_last_modified_resource_metadata else resource_info.metadata_modified
                    if resource_info.download_url:
                        if internal_filestore:
                            if resource_info.download_size_mb is not None:
                                package_size.filestore_size_mb += resource_info.download_size_mb
                        else:
                            if resource_info.download_size_mb is not None:
                                package_size.external_size_mb += resource_info.download_size_mb
                            package_size.external_resource_count += 1
                        package_size.filestore_count += 1
                    if resource_info.datastore_info is not None:
                        datastore_size = resource_info.datastore_info.table_size_mb + resource_info.datastore_info.index_size_mb
                        package_size.datastore_size_mb += datastore_size
                        package_size.datastore_lines += resource_info.datastore_info.row_count
                        package_size.datastore_count += 1

    ## Mapping of resource aliases from table
    def list_datastore_aliases(self) -> List[CkanAliasInfo]:
        alias_resource_id = "_table_metadata"  # resource name of table containing CKAN aliases
        alias_list_dict = self.datastore_search(alias_resource_id, search_all=True, return_df=False, format="objects", search_method=True)
        alias_list = [CkanAliasInfo(alias_dict) for alias_dict in alias_list_dict]
        for alias_info in alias_list:
            if alias_info.alias_of is not None:
                self.map.resource_alias_index[alias_info.name] = alias_info.alias_of
        return alias_list

    def map_resources(self, package_list:Union[str, List[str]]=None, *, params:dict=None,
                      datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None, license_list:bool=None,
                      only_missing:bool=True, error_not_found:bool=True,
                      owner_org:str=None, progress_callback:CkanProgressCallbackABC=None) -> CkanMap:
        # overload including a call to list all aliases
        if len(self.map.resource_alias_index) == 0 and self.params.map_all_aliases:
            self.list_datastore_aliases()
        map = super().map_resources(package_list=package_list, params=params, datastore_info=datastore_info,
                              resource_view_list=resource_view_list, organization_info=organization_info,
                              license_list=license_list, only_missing=only_missing, error_not_found=error_not_found,
                              owner_org=owner_org, progress_callback=progress_callback)
        return map

