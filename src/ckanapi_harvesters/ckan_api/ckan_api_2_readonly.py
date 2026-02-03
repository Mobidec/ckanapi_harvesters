#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Dict, Tuple, Generator, Any, Union, OrderedDict
import io
import json
from warnings import warn

import numpy as np
import requests
from requests.auth import AuthBase
import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.list_records import ListRecords, records_to_df
from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo, CkanAliasInfo, CkanField
from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_auxiliary import bytes_to_megabytes
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise, CkanIdFieldTreatment
from ckanapi_harvesters.auxiliary.ckan_auxiliary import datastore_id_col
from ckanapi_harvesters.auxiliary.ckan_auxiliary import RequestType
from ckanapi_harvesters.auxiliary.ckan_action import CkanActionResponse, CkanNotFoundError, CkanSqlCapabilityError
from ckanapi_harvesters.auxiliary.ckan_errors import (IntegrityError, CkanServerError, CkanArgumentError, SearchAllNoCountsError,
                                                      DataStoreNotFoundError, RequestError)
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
                df.set_index(datastore_id_col, drop=False, inplace=True, verify_integrity=True)
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

    ## Data queries ------------------
    ### Dump method ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    # NB: dump methods are not exposed to the user by default. Only datastore_search and resource_download methods are exposed.
    def _api_datastore_dump_raw(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                sort:str=None, limit:int=None, offset:int=0, format:str=None, bom:bool=None, params:dict=None,
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
        if params is None:
            params = {}
        if offset is None:
            offset = 0
        params["offset"] = offset
        if limit is None:
            limit = self.params.default_limit_read
        if limit is not None:
            params["limit"] = limit
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
            bom = format not in {"json", "xml"}
        if bom is not None:
            params["bom"] = bom
        # params["bom"] = True  # useful?
        response = self._url_request(f"datastore/dump/{resource_id}", method=RequestType.Get, params=params)
        if response.status_code == 200:
            return response
        elif response.status_code == 404 and "DataStore resource not found" in response.text:
            raise DataStoreNotFoundError(resource_id, response.content.decode())
        else:
            raise CkanServerError(self, response, response.content.decode())

    def _api_datastore_dump_df(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                               sort:str=None, limit:int=None, offset:int=0, format:str=None, bom:bool=None, params:dict=None) -> pd.DataFrame:
        """
        Convert output of _api_datastore_dump_raw to pandas DataFrame.
        """
        response = self._api_datastore_dump_raw(resource_id=resource_id, filters=filters, q=q, fields=fields,
                                                sort=sort, limit=limit, offset=offset, format=format, bom=bom,
                                                params=params, compute_len=False)
        if format is not None:
            format = format.lower()
        buffer = io.StringIO(response.content.decode())
        if format is None or format == "csv":
            response_df = pd.read_csv(buffer, **df_download_read_csv_kwargs)
        elif format == "tsv":
            response_df = pd.read_csv(buffer, sep="\t", **df_download_read_csv_kwargs)  # not tested
        elif format == "json":
            response_dict = json.load(buffer)
            fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response_dict["fields"])
            df_args = CkanApiReadOnly.read_fields_df_args(fields_type_dict)
            response_df = records_to_df(response_dict["records"], df_args)
            response_df.attrs["fields"] = fields_type_dict
        elif format == "xml":
            response_df = pd.read_xml(buffer, parser="etree") # , xpath=".//row")  # partially tested  # otherwise, necessitates the installation of parser lxml
        else:
            raise NotImplementedError()
        self._rx_records_df_clean(response_df)
        return response_df

    def _api_datastore_dump_all(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                sort:str=None, limit:int=None, offset:int=0, format:str=None, bom:bool=None,
                                params:dict=None, search_all:bool=True, return_df:bool=True) \
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
            return self._request_all_results_df(api_fun=self._api_datastore_dump_df, params=params, limit=limit, offset=offset,
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
                                                    sort=sort, limit=limit, offset=offset, format=format, bom=bom,
                                                    params=params, compute_len=search_all)
            return response

    def _api_datastore_dump_all_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                          sort:str=None, limit:int=None, offset:int=0, format:str=None, bom:bool=None,
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
            return self._request_all_results_generator(api_fun=self._api_datastore_dump_df, params=params, limit=limit, offset=offset,
                                                       search_all=search_all, resource_id=resource_id,
                                                       filters=filters, q=q, fields=fields, sort=sort, format=format, bom=bom)
        else:
            return self._request_all_results_generator(api_fun=self._api_datastore_dump_raw, params=params, limit=limit, offset=offset,
                                                       search_all=search_all, resource_id=resource_id,
                                                       filters=filters, q=q, fields=fields, sort=sort, format=format, bom=bom,
                                                       compute_len=search_all)


    ### Search method ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    def _api_datastore_search_raw(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                  distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, format:str=None,
                                  params:dict=None, compute_len:bool=False) -> CkanActionResponse:
        """
        API call to datastore_search. Performs queries on the DataStore.

        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param format: The return format in the returned response (default=objects, csv, tsv, lists) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :return:
        """
        if params is None:
            params = {}
        if offset is None:
            offset = 0
        params["offset"] = offset
        if limit is None:
            limit = self.params.default_limit_read
        if limit is not None:
            params["limit"] = limit
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
        response = self._api_action_request(f"datastore_search", method=RequestType.Get, params=params)
        if response.success:
            if response.dry_run:
                return response
            elif format is None or format in ["objects", "lists"]:
                response.len = len(response.result["records"])
            elif compute_len:
                raise SearchAllNoCountsError("datastore_search", f"format={format}")
            return response
        elif response.status_code == 404 and response.error_message["__type"] == "Not Found Error":
            raise DataStoreNotFoundError(resource_id, response.error_message)
        else:
            raise response.default_error(self)

    def _api_datastore_search_df(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                 distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, format:str=None,
                                 params:dict=None, compute_len:bool=True) -> pd.DataFrame:
        """
        Convert output of _api_datastore_search_raw to pandas DataFrame.
        """
        response = self._api_datastore_search_raw(resource_id=resource_id, filters=filters, q=q, fields=fields, format=format,
                                                  distinct=distinct, sort=sort, limit=limit, offset=offset,
                                                  params=params, compute_len=False)
        if response.dry_run:
            return pd.DataFrame()
        if format is not None:
            format = format.lower()
        fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
        if format is None or format == "objects":
            df_args_dict = CkanApiReadOnly.from_dict_df_args(fields_type_dict)
            response_df = pd.DataFrame.from_dict(response.result["records"], **df_args_dict)
        else:
            df_args = CkanApiReadOnly.read_fields_df_args(fields_type_dict)
            if format == "lists":
                response_df = records_to_df(response.result["records"], df_args)
            else:
                buffer = io.StringIO(response.result["records"])
                if format == "csv":
                    response_df = pd.read_csv(buffer, **df_args, **df_download_read_csv_kwargs)
                elif format == "tsv":
                    response_df = pd.read_csv(buffer, sep='\t', **df_args, **df_download_read_csv_kwargs)
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
                                  distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, format:str=None,
                                  search_all:bool=True, params:dict=None, return_df:bool=True, compute_len:bool=False) \
            -> Union[pd.DataFrame, Tuple[ListRecords, OrderedDict], Any]:
        """
        Successive calls to _api_datastore_search_df until an empty list is received.

        :see: _api_datastore_search()
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param format: The return format in the returned response (default=objects, csv, tsv, lists) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            df = self._request_all_results_df(api_fun=self._api_datastore_search_df, params=params, limit=limit, offset=offset,
                                              search_all=search_all, resource_id=resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort, format=format)
            if "fields" in df.attrs.keys():
                df.attrs["fields"] = df.attrs["fields"][0]
            if "total" in df.attrs.keys():
                assert_or_raise(np.all(np.array(df.attrs["total"]) == df.attrs["total"][0]), IntegrityError("total field varied in the responses"))
                df.attrs["total"] = df.attrs["total"][0]
            return df
        else:
            responses = self._request_all_results_list(api_fun=self._api_datastore_search_raw, params=params, limit=limit, offset=offset,
                                            search_all=search_all, resource_id=resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort, format=format, compute_len=compute_len)
            # aggregate results, depending on the format
            if self.params.dry_run:
                return [], {}
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
                return ListRecords(sum([response.result["records"] for response in responses], [])), fields_type_dict
            else:
                if format == "lists":
                    return sum([response.result["records"] for response in responses], []), fields_type_dict
                else:
                    return "\n".join([response.result["records"] for response in responses]), fields_type_dict

    def _api_datastore_search_all_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                            distinct:bool=None, sort:str=None, limit:int=None, offset:int=0,
                                            format:str=None, search_all:bool=True, params:dict=None, return_df:bool=True) \
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
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param format: The return format in the returned response (default=objects, csv, tsv, lists) (optional)
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            return self._request_all_results_generator(api_fun=self._api_datastore_search_df, params=params, limit=limit, offset=offset,
                                                       search_all=search_all, resource_id=resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort, format=format, compute_len=True)
        else:
            return self._request_all_results_generator(api_fun=self._api_datastore_search_raw, params=params,
                                                       limit=limit, offset=offset, search_all=search_all,
                                                       resource_id=resource_id, filters=filters, q=q,
                                                       fields=fields, distinct=distinct, sort=sort,
                                                       format=format, compute_len=search_all)


    ### search_sql method ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    def _api_datastore_search_sql_raw(self, sql:str, *, params:dict=None, limit:int=None, offset:int=0) -> CkanActionResponse:
        """
        API call to datastore_search_sql. Performs SQL queries on the DataStore. These queries can be more complex than
        with datastore_search. The DataStores are referenced by their resource_id, surrounded by quotes. The field names
        are referred by their name in upper case, surrounded by quotes.
        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: N/A
        :return:
        """
        if params is None:
            params = {}
        params["sql"] = sql
        if offset is None:
            offset = 0
        params["offset"] = offset
        if limit is None:
            limit = self.params.default_limit_read
        if limit is not None:
            params["limit"] = limit
        response = self._api_action_request(f"datastore_search_sql", method=RequestType.Post, params=params)
        if response.success:
            if response.dry_run:
                return response
            else:
                response.len = len(response.result["records"])
            return response
        elif response.status_code == 400 and response.success_json_loads and response.response.text == '"Bad request - Action name not known: datastore_search_sql"':
            raise CkanSqlCapabilityError(self, response)
        elif response.status_code == 404 and response.success_json_loads and response.error_message["__type"] == "Not Found Error":
            raise CkanNotFoundError(self, "SQL", response)
        else:
            raise response.default_error(self)

    def _api_datastore_search_sql_df(self, sql:str, *, params:dict=None, limit:int=None, offset:int=0) -> pd.DataFrame:
        """
        Convert output of _api_datastore_search_sql_raw to pandas DataFrame.
        """
        response = self._api_datastore_search_sql_raw(sql=sql, params=params, limit=limit, offset=offset)
        fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
        df_args_dict = CkanApiReadOnly.from_dict_df_args(fields_type_dict)
        response_df = pd.DataFrame.from_dict(response.result["records"], **df_args_dict)
        response.result.pop("records")
        response_df.attrs["result"] = response.result
        response_df.attrs["fields"] = fields_type_dict
        # response_df.attrs["total"] = response.result["total"]
        # response_df.attrs["total_was_estimated"] = response.result["total_was_estimated"]
        response_df.attrs["limit"] = response.result["limit"]
        self._rx_records_df_clean(response_df)
        return response_df

    def _api_datastore_search_sql_all(self, sql:str, *, params:dict=None,
                                      search_all:bool=True, limit:int=None, offset:int=0, return_df:bool=True) \
            -> Union[pd.DataFrame, Tuple[ListRecords, dict]]:
        """
        Successive calls to _api_datastore_search_sql until an empty list is received.

        :see: _api_datastore_search_sql()
        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: N/A
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            df = self._request_all_results_df(api_fun=self._api_datastore_search_sql_df, params=params,
                                              limit=limit, offset=offset, search_all=search_all, sql=sql)
            if "fields" in df.attrs.keys():
                df.attrs["fields"] = df.attrs["fields"][0]
            # if "total" in df.attrs.keys():
            #     assert_or_raise(np.all(np.array(df.attrs["total"]) == df.attrs["total"][0]), IntegrityError("total field varied in the responses"))
            #     df.attrs["total"] = df.attrs["total"][0]
            return df
        else:
            responses = self._request_all_results_list(api_fun=self._api_datastore_search_sql_raw, params=params,
                                                       limit=limit, offset=offset, search_all=search_all, sql=sql)
            # TODO: test
            if len(responses) > 0:
                response = responses[0]
                fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
            else:
                fields_type_dict = None
            return ListRecords(sum([response.result["records"] for response in responses], [])), fields_type_dict

    def _api_datastore_search_sql_all_generator(self, sql:str, *, params:dict=None,
                                                search_all:bool=True, limit:int=None, offset:int=0, return_df:bool=True) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None]]:
        """
        Successive calls to _api_datastore_search_sql until an empty list is received.
        Generator implementation which yields one DataFrame per request.

        :see: _api_datastore_search_sql()
        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: N/A
        :param search_all: if False, only the first request is operated
        :return:
        """
        if return_df:
            return self._request_all_results_generator(api_fun=self._api_datastore_search_sql_df, params=params,
                                                       limit=limit, offset=offset, search_all=search_all, sql=sql)
        else:
            return self._request_all_results_generator(api_fun=self._api_datastore_search_sql_raw, params=params,
                                                       limit=limit, offset=offset, search_all=search_all, sql=sql)


    ## Function aliases to limit the entry-points for the user  -------------------------------------------------------
    def datastore_search(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                         distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, params:dict=None,
                         search_all:bool=False, search_method:bool=True, format:str=None, return_df:bool=True) \
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
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :return:
        """
        if search_method:
            if return_df and format is None: format = "csv"
            return self._api_datastore_search_all(resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort,
                                                  limit=limit, offset=offset, format=format, params=params, search_all=search_all, return_df=return_df)
        else:
            assert_or_raise(distinct is None, CkanArgumentError("DataStore dump", "distinct"))
            if return_df and format is None: format, bom = "csv", True
            return self._api_datastore_dump_all(resource_id, filters=filters, q=q, fields=fields, sort=sort,
                                                limit=limit, offset=offset, format=format, bom=bom, params=params, search_all=search_all, return_df=return_df)

    def datastore_dump(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                         distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, params:dict=None,
                         search_all:bool=True, search_method:bool=True, format:str=None, return_df:bool=True) \
            -> Union[pd.DataFrame, ListRecords, Any, List[CkanActionResponse]]:
        """
        Alias of datastore_search with search_all=True by default.
        Uses the API datastore_search

        :see: datastore_search()
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :return:
        """
        return self.datastore_search(resource_id, filters=filters, q=q, fields=fields,
                                     distinct=distinct, sort=sort, limit=limit, offset=offset, params=params,
                                     search_all=search_all, search_method=search_method, format=format, return_df=return_df)

    def datastore_search_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                   distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, params:dict=None,
                                   search_all:bool=False, search_method:bool=True, format:str=None, return_df:bool=True) \
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
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :return:
        """
        if search_method:
            if return_df and format is None: format = "csv"
            return self._api_datastore_search_all_generator(resource_id, filters=filters, q=q, fields=fields, distinct=distinct, sort=sort,
                                                               limit=limit, offset=offset, format=format, params=params, search_all=search_all, return_df=return_df)
        else:
            assert_or_raise(distinct is None, CkanArgumentError("DataStore dump", "distinct"))
            if return_df and format is None: format, bom = "csv", True
            return self._api_datastore_dump_all_generator(resource_id, filters=filters, q=q, fields=fields, sort=sort,
                                                          limit=limit, offset=offset, format=format, bom=bom, params=params, search_all=search_all, return_df=return_df)

    def datastore_search_cursor(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                   distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, params:dict=None,
                                   search_all:bool=False, search_method:bool=True, format:str=None, return_df:bool=False) \
            -> Generator[Union[pd.Series, Tuple[dict,dict], Tuple[list,dict], Tuple[str,dict]], Any, None]:
        """
        Cursor on rows
        """
        generator = self.datastore_search_generator(resource_id, filters=filters, q=q, fields=fields,
                                                    distinct=distinct, sort=sort, limit=limit, offset=offset, params=params,
                                                    search_all=search_all, search_method=search_method, format=format, return_df=return_df)
        if return_df:
            df: pd.DataFrame
            row: pd.Series
            for df in generator:
                for index, row in df.iterrows():
                    yield row
        elif search_method:
            response: CkanActionResponse
            # response.result: list
            if format is not None:
                format = format.lower()
            if format is None or format == "objects":
                for response in generator:
                    fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                    for element in response.result["records"]:
                        yield element, fields_type_dict
            else:
                for response in generator:
                    fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                    for element in response.result["records"]:
                        yield element, fields_type_dict
        else:
            raise TypeError("dumping datastore without parsing with a DataFrame does not return an iterable object")

    def datastore_dump_generator(self, resource_id:str, *, filters:dict=None, q:str=None, fields:List[str]=None,
                                   distinct:bool=None, sort:str=None, limit:int=None, offset:int=0, params:dict=None,
                                   search_all:bool=True, search_method:bool=True, format:str=None, return_df:bool=True) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None]]:
        """
        Function alias to datastore_search_generator with search_all=True by default.
        Uses the API datastore_search

        :see: datastore_search_generator
        :param resource_id: resource id.
        :param filters: The base argument to filter values in a table (optional)
        :param q: Full text query (optional)
        :param fields: The base argument to filter columns (optional)
        :param distinct: return only distinct rows (optional, default: false) e.g. to return distinct ids: fields="id", distinct=True
        :param sort: Argument to sort results e.g. sort="index, quantity desc"  or  sort="index asc"
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: Additional parameters such as filters, q, sort and fields can be given. See DataStore API documentation.
        :param search_all: Option to renew the request until there are no more records.
        :param search_method: API method selection (True=datastore_search, False=datastore_dump)
        :return:
        """
        return self.datastore_search_generator(resource_id, filters=filters, q=q, fields=fields,
                                               distinct=distinct, sort=sort, limit=limit, offset=offset, params=params,
                                               search_all=search_all, search_method=search_method, format=format, return_df=return_df)

    def datastore_search_sql(self, sql:str, *, params:dict=None, search_all:bool=False,
                             limit:int=None, offset:int=0, return_df:bool=True) -> Union[pd.DataFrame, Tuple[ListRecords, dict]]:
        """
        Preferred entry-point for a DataStore SQL request.
        :see: _api_datastore_search_sql()
        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: N/A
        :param search_all: Option to renew the request until there are no more records.
        :return:
        """
        return self._api_datastore_search_sql_all(sql, params=params, limit=limit, offset=offset, search_all=search_all, return_df=return_df)

    def datastore_search_sql_generator(self, sql:str, *, params:dict=None, search_all:bool=False,
                                       limit:int=None, offset:int=0, return_df:bool=True) \
            -> Union[Generator[pd.DataFrame, Any, None], Generator[CkanActionResponse, Any, None]]:
        """
        Preferred entry-point for a DataStore SQL request.
        :see: _api_datastore_search_sql()

        __NB__: This action is not available when ckanapi_harvesters.datastore.sqlsearch.enabled is set to false

        :param sql: SQL query e.g. f'SELECT * IN "{resource_id}" WHERE "USER_ID" < 0'
        :param limit: Limit the number of records to return
        :param offset: Offset in the returned records
        :param params: N/A
        :param search_all: Option to renew the request until there are no more records.
        :return:
        """
        return self._api_datastore_search_sql_all_generator(sql, params=params, limit=limit, offset=offset, search_all=search_all, return_df=return_df)

    def datastore_search_sql_cursor(self, sql:str, *, params:dict=None, search_all:bool=False,
                                    limit:int=None, offset:int=0, return_df:bool=False) \
            -> Generator[Union[pd.Series,Tuple[dict,dict]], Any, None]:
        generator = self.datastore_search_sql_generator(sql, params=params, search_all=search_all,
                                                        limit=limit, offset=offset, return_df=return_df)
        if return_df:
            df: pd.DataFrame
            row: pd.Series
            for df in generator:
                for index, row in df.iterrows():
                    yield row
        else:
            response: CkanActionResponse
            # response.result: list
            element: Any
            for response in generator:
                fields_type_dict = CkanApiReadOnly.read_fields_type_dict(response.result["fields"])
                for element in response.result["records"]:
                    yield element, fields_type_dict

    def datastore_search_sql_find_one(self, sql:str, *, params:dict=None,
                                      offset:int=0, return_df:bool=True) -> Union[pd.DataFrame, Tuple[ListRecords, dict]]:
        df_row = self.datastore_search_sql(sql, limit=1, search_all=False, offset=offset, params=params, return_df=return_df)
        return df_row

    def datastore_search_sql_fields_type_dict(self, sql:str, *, params:dict=None) -> OrderedDict:
        document, fields_dict = self.datastore_search_sql_find_one(sql, offset=0, params=params, return_df=False)
        return fields_dict

    def datastore_search_sql_row_count(self, sql:str, *, params:dict=None) -> int:
        df_row = self.datastore_search_sql_find_one(sql, offset=0, params=params, return_df=True)
        return df_row.attrs["total"]

    def datastore_search_find_one(self, resource_id:str, *, filters:dict=None, q:str=None, distinct:bool=None,
                                  fields:List[str]=None, offset:int=0, return_df:bool=True) \
            -> Union[pd.DataFrame, ListRecords, Any, List[CkanActionResponse]]:
        """
        Request one result for a query

        :param resource_id: resource id
        :return:
        """
        # resource_info = self.get_resource_info_or_request(resource_id)
        # return resource_info.datastore_info.row_count
        df_row = self.datastore_search(resource_id, limit=1, search_all=False, filters=filters, q=q, distinct=distinct,
                                       fields=fields, offset=offset, return_df=return_df)
        return df_row

    def datastore_search_fields_type_dict(self, resource_id:str, *,
                                          filters:dict=None, q:str=None, distinct:bool=None, fields:List[str]=None,
                                          request_missing:bool=True, error_not_mapped:bool=False,
                                          error_not_found:bool=True) -> OrderedDict:
        if fields is None:
            # if no field restriction was provided, refer to the fields of the DataStore
            fields_list = self.get_datastore_fields_or_request(resource_id, return_list=True,
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

        :param resource_id: resource id
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
        except CkanNotFoundError:
            if raise_error:
                raise CkanSqlCapabilityError(self, CkanActionResponse(requests.Response()))
            return False


    ## Resource download by direct link (FileStore) -----------------------------------------------
    def resource_download(self, resource_id:str, *, method:str=None,
                          proxies:dict=None, headers:dict=None, auth: Union[AuthBase, Tuple[str,str]]=None, verify:Union[bool,str,None]=None) \
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
                                           proxies=proxies, headers=headers, auth=auth, verify=verify)
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
        buffer = io.StringIO(response.content.decode())
        df = pd.read_csv(buffer, **df_download_read_csv_kwargs)
        self._rx_records_df_clean(df)
        return resource_info, df

    def map_file_resource_sizes(self, cancel_if_present:bool=True) -> None:
        for resource_id, resource_info in self.map.resources.items():
            if resource_info.download_url:
                if not (cancel_if_present and resource_info.download_size_mb is not None):
                    _, response = self.resource_download(resource_id, method="HEAD")
                    content_length = int(response.headers.get("content-length", None))  # raise error if not found or bad format
                    resource_info.download_size_mb = bytes_to_megabytes(content_length)


    ## Mapping of resource aliases from table
    def list_datastore_aliases(self) -> List[CkanAliasInfo]:
        alias_resource_id = "_table_metadata"  # resource name of table containing CKAN aliases
        alias_list_dict, _ = self.datastore_search(alias_resource_id, search_all=True, return_df=False, format="objects", search_method=True)
        alias_list = [CkanAliasInfo(alias_dict) for alias_dict in alias_list_dict]
        for alias_info in alias_list:
            if alias_info.alias_of is not None:
                self.map.resource_alias_index[alias_info.name] = alias_info.alias_of
        return alias_list

    def map_resources(self, package_list:Union[str, List[str]]=None, *, params:dict=None,
                      datastore_info:bool=None, resource_view_list:bool=None, organization_info:bool=None, license_list:bool=None,
                      only_missing:bool=True, error_not_found:bool=True,
                      owner_org:str=None) -> CkanMap:
        # overload including a call to list all aliases
        if len(self.map.resource_alias_index) == 0 and self.params.map_all_aliases:
            self.list_datastore_aliases()
        map = super().map_resources(package_list=package_list, params=params, datastore_info=datastore_info,
                              resource_view_list=resource_view_list, organization_info=organization_info,
                              license_list=license_list, only_missing=only_missing, error_not_found=error_not_found,
                              owner_org=owner_org)
        return map

