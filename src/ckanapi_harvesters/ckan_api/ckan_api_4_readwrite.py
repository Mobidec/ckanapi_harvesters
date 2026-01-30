#!python3
# -*- coding: utf-8 -*-
"""

"""
from typing import List, Union, Tuple
import time
from warnings import warn
import io

import pandas as pd

from ckanapi_harvesters.auxiliary.proxy_config import ProxyConfig
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice, CkanState
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_action import CkanActionResponse
# from ckanapi_harvesters.auxiliary.list_records import records_to_df
from ckanapi_harvesters.auxiliary.ckan_auxiliary import upload_prepare_requests_files_arg, RequestType, json_encode_params
from ckanapi_harvesters.auxiliary.ckan_errors import (ReadOnlyError, IntegrityError, MaxRequestsCountError,
                                                      UnexpectedError, InvalidParameterError, DataStoreNotFoundError)
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy

from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.ckan_api.ckan_api_3_policy import CkanApiPolicyParams
from ckanapi_harvesters.ckan_api.ckan_api_3_policy import CkanApiPolicy

from ckanapi_harvesters.auxiliary.ckan_map import CkanMap
from ckanapi_harvesters.auxiliary.ckan_api_key import CkanApiKey
from ckanapi_harvesters.auxiliary.ckan_auxiliary import df_upload_to_csv_kwargs




class CkanApiReadWriteParams(CkanApiPolicyParams):
    # not read-only by default
    default_readonly:bool = False

    def __init__(self, *, proxies:Union[str,dict,ProxyConfig]=None,
                 ckan_headers:dict=None, http_headers:dict=None):
        super().__init__(proxies=proxies, ckan_headers=ckan_headers, http_headers=http_headers)
        self.default_limit_write: Union[int,None] = self.default_limit_read  # limit the number of entries per upsert (used as default value)
        self.default_force: bool = True  # set to True to edit a read-only resource
        self.read_only: bool = self.default_readonly
        self.submit_delay: float = 2.0      # delay between requests when running datapusher_submit
        self.submit_timeout: float = 90.0   # maximum wait time after datapusher_submit

    def copy(self, new_identifier:str=None, *, dest=None):
        if dest is None:
            dest = CkanApiReadWriteParams()
        super().copy(dest=dest)
        dest.default_limit_write = self.default_limit_write
        dest.default_force = self.default_force
        dest.read_only = self.read_only
        dest.submit_delay = self.submit_delay
        dest.submit_timeout = self.submit_timeout
        return dest


class CkanApiReadWrite(CkanApiPolicy):
    """
    CKAN Database API interface to CKAN server with helper functions using pandas DataFrames.
    This class implements requests to write data to the CKAN server resources / DataStores.
    """

    def __init__(self, url:str=None, *, proxies:Union[str,dict,ProxyConfig]=None,
                 apikey:Union[str,CkanApiKey]=None, apikey_file:str=None,
                 owner_org: str = None, params:CkanApiPolicyParams=None,
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
                         owner_org=owner_org, map=map, policy=policy, policy_file=policy_file, identifier=identifier)
        self.data_cleaner_upload: Union[CkanDataCleanerABC, None] = data_cleaner_upload
        if params is None:
            params = CkanApiReadWriteParams()
        if proxies is not None:
            params.proxies = proxies
        self.params: CkanApiReadWriteParams = params

    def copy(self, new_identifier: str = None, *, dest=None):
        if dest is None:
            dest = CkanApiReadWrite()
        super().copy(new_identifier=new_identifier, dest=dest)
        dest.data_cleaner_upload = self.data_cleaner_upload.copy() if self.data_cleaner_upload is not None else None
        return dest

    def full_unlock(self, unlock:bool=True,
                    *, no_ca:bool=None, external_url_resource_download:bool=None) -> None:
        """
        Function to unlock full capabilities of the CKAN API

        :param unlock:
        :return:
        """
        super().full_unlock(unlock, no_ca=no_ca, external_url_resource_download=external_url_resource_download)
        self.params.default_force = unlock
        self.params.read_only = not unlock

    def set_limits(self, limit_read:Union[int,None], limit_write:int=None) -> None:
        """
        Set default query limits. If only one argument is provided, it applies to both limits.

        :param limit_read: default limit for read requests
        :param limit_write: default limit for upsert (write) requests
        :return:
        """
        super().set_limits(limit_read)
        if limit_write is not None:
            self.params.default_limit_write = limit_write
        else:
            self.params.default_limit_write = limit_read

    def set_submit_timeout(self, submit_timeout:float, submit_delay:float=None) -> None:
        """
        Set timeout for the datastore_wait method. This is called after datastore_submit.

        :param submit_timeout: timeout after which a TimeoutError is raised
        :param submit_delay: delay between requests to peer on DataStore initialization
        :return:
        """
        self.params.submit_timeout = submit_timeout
        if submit_delay is not None:
            self.params.submit_delay = submit_delay

    ## DataStore insertions ------------------
    def _api_datastore_upsert_raw(self, records:Union[dict, List[dict], pd.DataFrame], resource_id:str, *,
                                  method:Union[UpsertChoice,str], params:dict=None, force:bool=None, dry_run:bool=False,
                                  last_insertion:bool=True) -> CkanActionResponse:
        """
        API call to api_datastore_upsert.

        :param records: records, preferably in a pandas DataFrame - they will be converted to a list of dictionaries.
        :param resource_id: destination resource id
        :param method: see UpsertChoice (insert, update or upsert)
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :param params: additional parameters
        :param dry_run: set to True to abort transaction instead of committing, e.g. to check for validation or type errors
        :param last_insertion: trigger for calculate_record_count
        (doc: updates the stored count of records, used to optimize datastore_search in combination with the
        total_estimation_threshold parameter. If doing a series of requests to change a resource, you only need to set
        this to True on the last request.)
        :return: the inserted records as a pandas DataFrame, from the server response
        """
        assert_or_raise(not self.params.read_only, ReadOnlyError())
        if params is None: params = {}
        if force is None: force = self.params.default_force
        if method is not None: method = str(method)
        params["resource_id"] = resource_id
        params["force"] = force
        params["dry_run"] = dry_run
        #  doc calculate_record_count: updates the stored count of records, used to optimize datastore_search in combination with the
        # total_estimation_threshold parameter. If doing a series of requests to change a resource, you only need to set
        # this to True on the last request.
        params["calculate_record_count"] = last_insertion
        # params["force_indexing"] = last_insertion
        N = self.map.get_datastore_len(resource_id, error_not_mapped=False)
        has_datastore_info = N is not None
        format = None  # API does not support csv or other formats
        mode_df = True
        if records is not None:
            method = method.lower()
            params["method"] = method
            if isinstance(records, dict):
                records = pd.DataFrame.from_dict(records)
            elif isinstance(records, list):
                # records = records_to_df(records)
                mode_df = False
            else:
                assert(isinstance(records, pd.DataFrame))
            n_upsert = len(records)
            if not mode_df:
                params["records"] = records
                format = None
            elif format is None or format == "objects":
                params["records"] = records.to_dict(orient='records')
            else:
                # dead code
                fields_id_list = records.columns.tolist()
                params["fields"] = fields_id_list  # [{"id": id} for id in fields_id_list]
                params["records_format"] = format
                if format == "csv":
                    params["records"] = records.to_csv(index=False, header=False, **df_upload_to_csv_kwargs)
                elif format == "lists":
                    params["records"] = records.values.tolist()
                else:
                    raise NotImplementedError()
        else:
            # possibility to call with None records and method to trigger row counts etc.
            # this request may not be useful
            assert(method is None)
            n_upsert = 0
        # json encode here in the case there are NaN values, not supported by the requests encoder
        data_payload, json_headers = json_encode_params(params)
        response = self._api_action_request(f"datastore_upsert", method=RequestType.Post,
                                            data=data_payload, headers=json_headers)
        # response = self._api_action_request(f"datastore_upsert", method=RequestType.Post,
        #                                     json=params)
        if response.success:
            if method is not None:
                n_return = len(response.result["records"])
                if has_datastore_info and not dry_run and method == "insert":
                    # in modes other than insert could be updated rather than inserted
                    self.map._update_datastore_len(resource_id, N + n_return)
                assert_or_raise(n_return == n_upsert, IntegrityError("Returned dataframe does not match number of requested rows"))
            return response
        else:
            raise response.default_error(self)

    def _api_datastore_upsert(self, records:Union[dict, List[dict], pd.DataFrame], resource_id:str, *,
                              method:Union[UpsertChoice,str], params:dict=None, force:bool=None, dry_run:bool=False,
                              last_insertion:bool=True, return_df:bool=None) -> Union[pd.DataFrame, List[dict], dict]:
        mode_df = True
        if isinstance(records, list):
            mode_df = False
        if return_df is None:
            return_df = mode_df
        response = self._api_datastore_upsert_raw(records=records, resource_id=resource_id, method=method,
                                                  params=params, force=force, dry_run=dry_run,
                                                  last_insertion=last_insertion)
        if method is not None:
            if return_df:
                response_df = pd.DataFrame.from_dict(response.result["records"])
                self._rx_records_df_clean(response_df)
                return response_df
            else:
                return response.result["records"]
        else:
            return response.result

    def datastore_upsert_last_line(self, resource_id:str):
        """
        Apply last line treatments to a resource.
        """
        return self._api_datastore_upsert(None, resource_id=resource_id, method=None, last_insertion=True)

    def datastore_upsert(self, records:Union[dict, List[dict], pd.DataFrame], resource_id:str, *,
                         dry_run:bool=False, limit:int=None, offset:int=0, force:bool=None,
                         method:Union[UpsertChoice,str]=UpsertChoice.Upsert, apply_last_condition:bool=True,
                         always_last_condition:bool=None, return_df:bool=None,
                         data_cleaner:CkanDataCleanerABC=None, params:dict=None) -> Union[pd.DataFrame, List[dict]]:
        """
        Encapsulation of _api_datastore_upsert to cut the requests to a limited number of rows.

        :see: _api_datastore_upsert()
        :param records: records, preferably in a pandas DataFrame - they will be converted to a list of dictionaries.
        :param resource_id: destination resource id
        :param method: by default, set to Upsert
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :param limit: number of records per transaction
        :param offset: number of records to skip - use to restart the transfer
        :param params: additional parameters
        :param dry_run: set to True to abort transaction instead of committing, e.g. to check for validation or type errors
        :param apply_last_condition: if True, the last upsert request applies the last insert operations (calculate_record_count and force_indexing).
        :param always_last_condition: if True, each request applies the last insert operations - default is False
        :return: the inserted records as a pandas DataFrame, from the server response
        """
        method_str = str(method)
        if apply_last_condition is None:
            apply_last_condition = True
        if always_last_condition is None:
            always_last_condition = False
        mode_df = True
        assert(records is not None)
        if isinstance(records, dict):
            records = pd.DataFrame.from_dict(records)
        elif isinstance(records, list):
            # records = records_to_df(records)
            mode_df = False
        else:
            assert(isinstance(records, pd.DataFrame))
        if data_cleaner is None:
            data_cleaner = self.data_cleaner_upload
        if data_cleaner is not None:
            datastore_info = self.get_datastore_info_or_request_of_id(resource_id=resource_id, error_not_found=True)
            records = data_cleaner.clean_records(records, known_fields=datastore_info.fields_dict, inplace=True)
            data_cleaner.apply_new_fields_request(self, resource_id=resource_id)
        if return_df is None:
            return_df = mode_df
        if limit is None: limit = self.params.default_limit_write
        if limit is None:
            # direct API call with one request
            if self.params.store_last_response_debug_info:
                self.debug.multi_requests_last_successful_offset = offset
            return self._api_datastore_upsert(records, return_df=return_df,
                                              method=method_str, dry_run=dry_run, resource_id=resource_id,
                                              force=force, params=params,
                                              last_insertion=apply_last_condition or always_last_condition)
        assert_or_raise(limit > 0, InvalidParameterError("limit"))
        n = len(records)
        if self.params.store_last_response_debug_info:
            self.debug.multi_requests_last_successful_offset = offset
        requests_count = 0
        last_insertion = True
        df, returned_rows = None, None
        if return_df:
            df = None
        else:
            returned_rows = []
        start = time.time()
        current = start
        timeout = False
        n_cum = 0
        while offset < n and requests_count < self.params.max_requests_count and not timeout:
            last_insertion = offset+limit >= n
            i_end_add = min(n, offset+limit)
            n_add = i_end_add-1 - offset + 1
            if self.params.verbose_multi_requests:
                print(f"{self.identifier} Multi-requests upsert {requests_count} to add {n_add} records ...")
            if mode_df:
                df_upsert = records.iloc[offset:i_end_add]
            else:
                df_upsert = records[offset:i_end_add]
            df_add = self._api_datastore_upsert(df_upsert, return_df=return_df,
                                                method=method_str, dry_run=dry_run, resource_id=resource_id,
                                                force=force, params=params,
                                                last_insertion=(last_insertion and apply_last_condition) or always_last_condition)
            n_cum += len(df_add)
            assert_or_raise(len(df_add) == n_add, IntegrityError("Second check on response len failed in datastore_upsert"))  # consistency check, in double of _api_datastore_upsert
            if self.params.store_last_response_debug_info:
                self.debug.multi_requests_last_successful_offset = offset
            if return_df:
                if df is None:
                    # 1st execution: pandas cannot concatenate with an empty DataFrame => use None as indicator
                    assert(df_add is not None)
                    df = df_add
                else:
                    df = pd.concat([df, df_add], ignore_index=True)
            else:
                returned_rows = returned_rows + df_add
            if self.params.multi_requests_time_between_requests > 0 and not last_insertion:
                time.sleep(self.params.multi_requests_time_between_requests)
            if not last_insertion:
                assert_or_raise(n_add == limit, IntegrityError("datastore_upsert implementation is wrong"))
            offset += limit
            requests_count += 1
            current = time.time()
            timeout = current - start > self.params.multi_requests_timeout
        if return_df:
            if df is None:
                df = pd.DataFrame()  # always return a DataFrame object and not None
            df.attrs["requests_count"] = requests_count
            df.attrs["elapsed_time"] = current - start
            df.attrs["offset"] = offset
        if self.params.verbose_multi_requests:
            print(f"{self.identifier} Multi-requests upsert done to add {n_cum} records done in {requests_count} requests and {round(current - start, 2)} seconds.")
        if timeout:
            raise TimeoutError()
        if requests_count >= self.params.max_requests_count:
            raise MaxRequestsCountError()
        assert_or_raise(last_insertion, UnexpectedError("last_insertion should be True at last iteration"))
        if mode_df:
            return df
        else:
            return returned_rows

    def datastore_insert(self, records:Union[dict, List[dict], pd.DataFrame], resource_id:str, *,
                         dry_run:bool=False, limit:int=None, offset:int=0, apply_last_condition:bool=True,
                         always_last_condition:bool=None,
                         data_cleaner:CkanDataCleanerABC=None, force:bool=None, params:dict=None) -> pd.DataFrame:
        """
        Alias function to insert data in a DataStore using datastore_upsert.

        :see: _api_datastore_upsert()
        :param records: records, preferably in a pandas DataFrame - they will be converted to a list of dictionaries.
        :param resource_id: destination resource id
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :param params: additional parameters
        :param dry_run: set to True to abort transaction instead of committing, e.g. to check for validation or type errors
        :return: the inserted records as a pandas DataFrame, from the server response
        """
        return self.datastore_upsert(records, resource_id, dry_run=dry_run, limit=limit, offset=offset,
                                     method=UpsertChoice.Insert, apply_last_condition=apply_last_condition,
                                     always_last_condition=always_last_condition, data_cleaner=data_cleaner,
                                     force=force, params=params)

    def datastore_update(self, records:Union[dict, List[dict], pd.DataFrame], resource_id:str, *,
                         dry_run:bool=False, limit:int=None, offset:int=0, apply_last_condition:bool=True,
                         always_last_condition:bool=None,
                         data_cleaner:CkanDataCleanerABC=None, force:bool=None, params:dict=None) -> pd.DataFrame:
        """
        Alias function to update data in a DataStore using datastore_upsert.
        The update is performed based on the DataStore primary keys

        :see: _api_datastore_upsert()
        :param records: records, preferably in a pandas DataFrame - they will be converted to a list of dictionaries.
        :param resource_id: destination resource id
        :param force: set to True to edit a read-only resource. If not provided, this is overridden by self.default_force
        :param params: additional parameters
        :param dry_run: set to True to abort transaction instead of committing, e.g. to check for validation or type errors
        :return: the inserted records as a pandas DataFrame, from the server response
        """
        return self.datastore_upsert(records, resource_id, dry_run=dry_run, limit=limit, offset=offset,
                                     method=UpsertChoice.Update, apply_last_condition=apply_last_condition,
                                     always_last_condition=always_last_condition, data_cleaner=data_cleaner,
                                     force=force, params=params)


    ## Resource updates ------------------
    def _api_resource_patch(self, resource_id:str, *, name:str=None, format:str=None, description:str=None, title:str=None,
                            state:CkanState=None,
                            df:pd.DataFrame=None, file_path:str=None, url:str=None, files=None,
                            payload: Union[bytes, io.BufferedIOBase] = None, payload_name: str = None,
                            params:dict=None) -> CkanResourceInfo:
        """
        Call to resource_patch API. This call can be used to change the resource parameters via params (cf. API documentation)
        or to reupload the resource file into FileStore.
        The latter action replaces the current resource. If it is a DataStore, it is reset to the new contents of the file.
        The file can be transmitted either as an url, a file path or a pandas DataFrame.
        The files argument can pass through these arguments to the requests.post function.
        A call to datapusher_submit() could be required to take immediately into account the newly downloaded file.

        :see: _api_resource_create
        :see: resource_create
        :param resource_id: resource id
        :param url: url of the resource to replace resource
        :param params: parameters such as name, format, resource_type can be changed

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
        params["id"] = resource_id
        if description is not None:
            params["description"] = description
        if title is not None:
            params["title"] = title
        if name is not None:
            params["name"] = name
        if format is not None:
            params["format"] = format
        if state is not None:
            params["state"] = str(state)
        files = upload_prepare_requests_files_arg(files=files, file_path=file_path, df=df, payload=payload, payload_name=payload_name)
        if url is not None:
            params["url"] = url
            params["clear_upload"] = True
            assert(files is None)
        if files is not None:
            response = self._api_action_request(f"resource_patch", method=RequestType.Post,
                                                files=files, data=params)
        else:
            response = self._api_action_request(f"resource_patch", method=RequestType.Post, json=params)
        if response.success:
            resource_info = CkanResourceInfo(response.result)
            self.map._record_resource_update(resource_info)
            return resource_info
        else:
            raise response.default_error(self)

    def resource_patch(self, resource_id:str, *, name:str=None, format:str=None, description:str=None, title:str=None,
                       state:CkanState=None,
                       df:pd.DataFrame=None, file_path:str=None, url:str=None, files=None,
                       payload: Union[bytes, io.BufferedIOBase] = None, payload_name: str = None,
                       params:dict=None) -> CkanResourceInfo:
        # function alias
        return self._api_resource_patch(resource_id, name=name, format=format, description=description, state=state,
                                        title=title, df=df, file_path=file_path, url=url, files=files,
                                        payload=payload, payload_name=payload_name, params=params)

    ### DataPusher submit ------------------
    def _api_datapusher_submit(self, resource_id: str, *, params: dict = None) -> bool:
        """
        Call to API action datapusher_submit. This triggers the normally asynchronous DataPusher service for a given resource.

        :param resource_id: resource id
        :param params:
        :return:
        """
        if params is None: params = {}
        params["resource_id"] = resource_id
        response = self._api_action_request(f"datapusher_submit", method=RequestType.Post, json=params)
        if response.success:
            return response.result
        else:
            raise response.default_error(self)

    def datastore_wait(self, resource_id: str, *,
                       apply_delay:bool=True, error_timeout:bool=True) -> Tuple[int, float]:
        """
        Wait until a DataStore has at least one row.
        The delay between requests to peer on the presence of the DataStore is given by the class attribute submit_delay.
        If the loop exceeds submit_timeout, an exception is raised.

        :param resource_id:
        :param apply_delay:
        :param error_timeout: option to raise an exception in case of timeout
        :return:
        """
        if self.params.submit_delay <= 0 or not apply_delay:
            return 0, 0.0
        # resource_info = self.resource_show(resource_id)
        # init_timestamp = resource_info.last_modified
        # current_timestamp = init_timestamp
        if self.params.verbose_request:
            print(f"Waiting for data treatments on DataStore {resource_id}...")
        start = time.time()
        current = start
        timeout = False
        counter = 0
        df = pd.DataFrame()  # empty DataFrame
        while not timeout and df.empty:  # current_timestamp <= init_timestamp:
            time.sleep(self.params.submit_delay)
            # resource_info = self.resource_show(resource_id)
            # current_timestamp = resource_info.last_modified
            try:
                df = self.datastore_search(resource_id, limit=1, search_all=False, search_method=True)
            except DataStoreNotFoundError:
                pass
            current = time.time()
            timeout = (current - start) > self.params.submit_timeout
            counter += 1
        if timeout:
            if error_timeout:
                raise TimeoutError("datastore_wait")
            else:
                msg = str(TimeoutError("datastore_wait"))
                warn(msg)
        if self.params.verbose_request:
            print(f"Resource updated after {current - start} seconds ({counter} iterations)")
        return counter, current - start

    def datastore_submit(self, resource_id: str,
                          *, apply_delay:bool=True, error_timeout:bool=True,
                          params: dict = None) -> bool:
        """
        Submit file to re-initiate DataStore, using the preferred method.
        Current method is datapusher_submit.
        This encapsulation includes a call to datastore_wait.

        :param resource_id:
        :param apply_delay: Keep true to wait until the datastore is ready (a datastore_search query is performed as a test)
        :param params:
        :return:
        """
        result = self._api_datapusher_submit(resource_id, params=params)
        self.datastore_wait(resource_id, apply_delay=apply_delay, error_timeout=error_timeout)
        return result

    # def datapusher_submit_insert(self, resource_id: str, *, params: dict = None) -> dict:
    #     # idea: modify datapusher such as it would upsert data instead of replacing the entire datastore
    #     raise NotImplementedError()
    #     if params is None: params = {}
    #     params["insert"] = True
    #     return self._api_datapusher_submit(resource_id, params)








