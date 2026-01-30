#!python3
# -*- coding: utf-8 -*-
"""
Code to initiate a DataStore defined by a large number of files to concatenate into one table
"""
from concurrent.futures import ThreadPoolExecutor
import threading
from threading import current_thread
from abc import ABC, abstractmethod
from typing import Dict, List, Callable, Any, Tuple, Generator, Union, Set
from warnings import warn
import copy

import pandas as pd

from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreABC
from ckanapi_harvesters.builder.builder_aux import positive_end_index
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice, CkanResourceInfo
from ckanapi_harvesters.auxiliary.ckan_auxiliary import datastore_id_col
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.builder.mapper_datastore_multi import RequestFileMapperABC, default_file_mapper_from_primary_key
from ckanapi_harvesters.builder.builder_resource_multi_file import BuilderMultiABC, default_progress_callback

# apply last_condition for each upsert request when in a multi-threaded upload on a same DataStore:
datastore_multi_threaded_always_last_condition:bool = True
# when there are multiple files, apply last insertion commands after each document? True: after each csv file, False: only at the end
datastore_multi_apply_last_condition_intermediary:bool = False


class BuilderDataStoreMultiABC(BuilderDataStoreABC, BuilderMultiABC, ABC):
    """
    generic class to manage large DataStore, divided into files/parts
    This abstract class is intended to be overloaded in order to be used to generate data from the workspace, without using CSV files
    """

    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, dirname:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        # Functions inputs/outputs
        self.df_mapper: RequestFileMapperABC = default_file_mapper_from_primary_key(self.primary_key)
        self.reupload_if_needed = False  # do not reupload if needed because this could cause data loss (the upload function only uploads the first table)
        self.upsert_method: UpsertChoice = UpsertChoice.Upsert
        # BuilderMultiABC:
        self.stop_event = threading.Event()
        self.thread_ckan: Dict[str, CkanApi] = {}
        self.progress_callback: Union[Callable[[int, int, Any], None], None] = default_progress_callback
        self.progress_callback_kwargs: dict = {}
        self.enable_multi_threaded_upload:bool = True
        self.enable_multi_threaded_download:bool = True

    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.reupload_if_needed = self.reupload_if_needed
        # BuilderMultiABC:
        dest.progress_callback = self.progress_callback
        dest.progress_callback_kwargs = copy.deepcopy(self.progress_callback_kwargs)
        dest.enable_multi_threaded_upload = self.enable_multi_threaded_upload
        dest.enable_multi_threaded_download = self.enable_multi_threaded_download
        # do not copy stop_event
        return dest

    ## upload ---------
    @abstractmethod
    def get_local_df_generator(self, resources_base_dir:str) -> Generator[pd.DataFrame, None, None]:
        """
        Returns an iterator over the parts of the upload, loaded as DataFrames (not recommended in a multi-threaded context).
        """
        raise NotImplementedError()

    @abstractmethod
    def load_local_df(self, file: Any, **kwargs) -> pd.DataFrame:
        """
        Load the DataFrame pointed by the upload part "file"
        """
        raise NotImplementedError()

    # do not change default argument apply_last_condition=True
    # def upsert_request_df(self, ckan: CkanApi, df_upload:pd.DataFrame,
    #                       method:UpsertChoice=UpsertChoice.Upsert,
    #                       apply_last_condition:bool=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    #     # calls super method, with apply_last_condition defaulting to datastore_multi_apply_last_condition_intermediary
    #     if apply_last_condition is None:
    #         apply_last_condition = True  # datastore_multi_apply_last_condition_intermediary
    #     return super().upsert_request_df(ckan=ckan, df_upload=df_upload, method=method,
    #                                      apply_last_condition=apply_last_condition)

    def _get_primary_key_indexes(self, data_cleaner_index: Set[str], current_fields:Set[str], error_missing:bool, empty_datastore:bool=False) -> Tuple[Union[List[str],None], Union[List[str],None]]:
        primary_key, indexes = super()._get_primary_key_indexes(data_cleaner_index, current_fields, error_missing, empty_datastore)
        # it is highly recommended to specify a primary key: warning if not defined
        if primary_key is None:
            msg = f"It is highly recommended to specify the primary key for a DataStore defined from a directory to ensure no duplicate values are upserted to the database. Resource: {self.name}"
            warn(msg)
        else:
            ultra_required_fields = set(primary_key)
            missing_fields = ultra_required_fields
            if current_fields is not None:
                missing_fields -= current_fields
            if len(missing_fields) > 0:
                msg = f"The primary key {self.primary_key} is set for resource {self.name} but it is not present in the sample data."
                warn(msg)
        if primary_key is None or len(primary_key) == 0:
            self.upsert_method = UpsertChoice.Insert  # do not use upsert
        return primary_key, indexes

    def upsert_request_final(self, ckan: CkanApi, *, force:bool=False) -> None:
        """
        Final steps after the last upsert query.
        This call is mandatory at the end of all requests if the user called upsert_request_df for a multi-part DataStore manually.

        :param ckan:
        :param force: perform request anyways
        :return:
        """
        force = force or not datastore_multi_apply_last_condition_intermediary
        return super().upsert_request_final(ckan, force=force)

    def upload_request_final(self, ckan: CkanApi, *, force:bool=False) -> None:
        return self.upsert_request_final(ckan=ckan, force=force)

    def upsert_request_df_no_return(self, ckan: CkanApi, df_upload:pd.DataFrame,
                                    method:UpsertChoice=UpsertChoice.Upsert,
                                    apply_last_condition:bool=None, always_last_condition:bool=None) -> None:
        """
        Calls upsert_request_df but does not return anything

        :return:
        """
        self.upsert_request_df(ckan=ckan, df_upload=df_upload, method=method,
                               apply_last_condition=apply_last_condition, always_last_condition=always_last_condition)
        return None

    def _unit_upload_apply(self, *, ckan: CkanApi, file: str,
                           index: int, start_index: int, end_index: int, total: int,
                           method: UpsertChoice, **kwargs) -> None:
        if index == 0 and self.upsert_method == UpsertChoice.Insert:
            return  # do not reupload the first document, which was used for the initialization of the dataset
        if start_index <= index and index < end_index:
            df_upload_local = self.load_local_df(file, **kwargs)
            self._call_progress_callback(index, total, info=df_upload_local,
                                         context=f"{ckan.identifier} single-thread upload")
            self.upsert_request_df_no_return(ckan=ckan, df_upload=df_upload_local, method=method,
                                             apply_last_condition=datastore_multi_apply_last_condition_intermediary)
        else:
            # self._call_progress_callback(index, total, info=df_upload_local, context=f"{ckan.identifier} single-thread skip")
            pass

    def upload_request_full(self, ckan:CkanApi, resources_base_dir:str, *,
                            method:UpsertChoice=None,
                            threads:int=1, external_stop_event=None,
                            only_missing:bool=False,
                            start_index:int=0, end_index:int=None, **kwargs) -> None:
        self.df_mapper.upsert_only_missing_rows = only_missing
        if method is None:
            if self.primary_key is None or len(self.primary_key) == 0:
                self.upsert_method = UpsertChoice.Insert  # do not use upsert if there is no primary key
            method = self.upsert_method
        super().upload_request_full(ckan=ckan, resources_base_dir=resources_base_dir,
                                    threads=threads, external_stop_event=external_stop_event,
                                    start_index=start_index, end_index=end_index,
                                    method=method, **kwargs)
        # if threads < 0:
        #     # cancel large uploads in this case
        #     return None
        # elif threads is None or threads > 1:
        #     return self.upload_request_full_multi_threaded(resources_base_dir=resources_base_dir, ckan=ckan, method=method,
        #                                                    threads=threads, external_stop_event=external_stop_event,
        #                                                    start_index=start_index, end_index=end_index)
        # else:
        #     self.init_local_files_list(resources_base_dir=resources_base_dir, cancel_if_present=True)
        #     if ckan.verbose_extra:
        #         print(f"Launching single-threaded upload of multi-file resource {self.name}")
        #     total = self.get_local_file_len()
        #     end_index = positive_end_index(end_index, total)
        #     for index, file in enumerate(self.get_local_file_generator(resources_base_dir=resources_base_dir)):
        #         if external_stop_event is not None and external_stop_event.is_set():
        #             print(f"{ckan.identifier} Interrupted")
        #             return
        #         self._unit_upload_apply(ckan=ckan, file=file,
        #                                 index=index, start_index=start_index, end_index=end_index, total=total,
        #                                 method=method)
        #     self._call_progress_callback(total, total, context=f"{ckan.identifier} single-thread upload")
        #     # at last, apply final actions:
        #     self.upload_request_final(ckan, force=not datastore_multi_apply_last_condition_intermediary)

    # def upsert_request_file_graceful(self, ckan: CkanApi, file: Any, index:int,
    #                                  method: UpsertChoice = UpsertChoice.Upsert, external_stop_event=None,
    #                                  start_index:int=0, end_index:int=None) -> None:
    #     """
    #     Calls upsert_request_df_clear with checks specific to multi-threading.
    #
    #     :return:
    #     """
    #     # ckan.session_reset()
    #     # ckan.identifier = current_thread().name
    #     ckan = self.thread_ckan[current_thread().name]
    #     total = self.get_local_file_len()
    #     end_index = positive_end_index(end_index, total)
    #     if self.stop_event.is_set():
    #         return
    #     if external_stop_event is not None and external_stop_event.is_set():
    #         print(f"{ckan.identifier} Interrupted")
    #         return
    #     try:
    #         self._unit_upload_apply(ckan=ckan, file=file,
    #                                 index=index, start_index=start_index, end_index=end_index, total=total,
    #                                 method=method)
    #     except Exception as e:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print(f"Stopping all threads because an exception occurred in thread: {e}")
    #         raise e from e

    # def upload_request_full_multi_threaded(self, ckan: CkanApi, resources_base_dir: str, threads: int = None,
    #                                        method: UpsertChoice = UpsertChoice.Upsert, external_stop_event=None,
    #                                        start_index:int=0, end_index:int=None, **kwargs):
    #     """
    #     Multi-threaded implementation of upload_request_full, using ThreadPoolExecutor.
    #     """
    #     self.init_local_files_list(resources_base_dir=resources_base_dir, cancel_if_present=True)
    #     resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=True)  # prepare CKAN object for multi-threading: perform mapping requests if necessary
    #     self._prepare_for_multithreading(ckan)
    #     try:
    #         with ThreadPoolExecutor(max_workers=threads, initializer=self._init_thread, initargs=(ckan,)) as executor:
    #             if ckan.verbose_extra:
    #                 print(f"Launching multi-threaded upload of multi-file resource {self.name}")
    #             futures = [executor.submit(self.upsert_request_file_graceful, ckan=ckan, file=file, method=method, index=index,
    #                                        start_index=start_index, end_index=end_index, external_stop_event=external_stop_event)
    #                        for index, file in enumerate(self.get_local_file_generator(resources_base_dir=resources_base_dir))]
    #             for future in futures:
    #                 future.result()  # This will propagate the exception
    #         total = self.get_local_file_len()
    #         self._call_progress_callback(total, total, context=f"{ckan.identifier} multi-thread upload")
    #     except Exception as e:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print(f"Stopping all threads because an exception occurred: {e}")
    #         raise e from e
    #     finally:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print("End of multi-threaded upload...")
    #     # at last, apply final actions:
    #     self.upload_request_final(ckan, force=not datastore_multi_apply_last_condition_intermediary)


    ## download -------
    def download_request_df(self, ckan: CkanApi, file_query:dict) -> Union[pd.DataFrame,None]:
        """
        Download the DataFrame with the file_query arguments
        """
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        df_download = self.df_mapper.download_file_query(ckan=ckan, resource_id=resource_id, file_query=file_query)
        df = self.df_mapper.df_download_alter(df_download, file_query=file_query, fields=self._get_fields_info())
        return df

    def _unit_download_apply(self, ckan:CkanApi, file_query_item:Any, out_dir:str,
                           index:int, start_index:int, end_index:int, total:int) -> Any:
        if start_index <= index and index < end_index:
            self._call_progress_callback(index, total, info=file_query_item,
                                         context=f"{ckan.identifier} single-thread download")
            self.download_file_query_item(ckan=ckan, out_dir=out_dir, file_query_item=file_query_item)
        else:
            pass
            # self._call_progress_callback(index, total, info=file_query_item, context=f"{ckan.identifier} single-thread skip")

    def download_request_full(self, ckan: CkanApi, out_dir: str, threads:int=1, external_stop_event=None,
                              start_index:int=0, end_index:int=None, force:bool=False) -> None:
        return super().download_request_full(ckan=ckan, out_dir=out_dir,
                                             threads=threads, external_stop_event=external_stop_event,
                                             start_index=start_index, end_index=end_index, force=force)
        # if (not self.enable_download) and (not force):
        #     msg = f"Did not download resource {self.name} because download was disabled."
        #     warn(msg)
        #     return None
        # if threads < 0:
        #     # do not download large datasets in this case
        #     return None
        # elif threads is None or threads > 1:
        #     return self.download_request_full_multi_threaded(ckan=ckan, out_dir=out_dir,
        #                                                      threads=threads, external_stop_event=external_stop_event,
        #                                                      start_index=start_index, end_index=end_index)
        # else:
        #     self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True)
        #     if ckan.verbose_extra:
        #         print(f"Launching single-threaded download of multi-file resource {self.name}")
        #     total = self.get_file_query_len()
        #     end_index = positive_end_index(end_index, total)
        #     for index, file_query_item in enumerate(self.get_file_query_generator()):
        #         if external_stop_event is not None and external_stop_event.is_set():
        #             print(f"{ckan.identifier} Interrupted")
        #             return
        #         self._unit_download_apply(ckan=ckan, file_query_item=file_query_item,
        #                                   index=index, start_index=start_index, end_index=end_index, total=total)
        #     self._call_progress_callback(total, total, context=f"{ckan.identifier} single-thread download")

    def download_request_generator(self, ckan: CkanApi, out_dir: str) -> Generator[Tuple[Any, pd.DataFrame], Any, None]:
        """
        Iterator on file_queries.
        """
        self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True)
        for file_query_item in self.get_file_query_generator():
            yield self.download_file_query_item(ckan=ckan, out_dir=out_dir, file_query_item=file_query_item)

    # def download_file_query_item_graceful(self, ckan: CkanApi, out_dir: str, file_query_item: Any, index:int,
    #                                       external_stop_event=None, start_index:int=0, end_index:int=None) -> None:
    #     """
    #     Implementation of download_file_query_item with checks for a multi-threaded download.
    #     """
    #     # ckan.session_reset()
    #     # ckan.identifier = current_thread().name
    #     ckan = self.thread_ckan[current_thread().name]
    #     total = self.get_file_query_len()
    #     end_index = positive_end_index(end_index, total)
    #     if self.stop_event.is_set():
    #         return
    #     if external_stop_event is not None and external_stop_event.is_set():
    #         print(f"{ckan.identifier} Interrupted")
    #         return
    #     try:
    #         # self._unit_download_apply(ckan=ckan, file_query_item=file_query_item,
    #         #                           index=index, start_index=start_index, end_index=end_index, total=total)
    #     except Exception as e:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print(f"Stopping all threads because an exception occurred in thread: {e}")
    #         raise e from e

    # def download_request_full_multi_threaded(self, ckan: CkanApi, out_dir: str,
    #                                          threads: int = None, external_stop_event=None,
    #                                          start_index:int=0, end_index:int=-1) -> None:
    #     """
    #     Multi-threaded implementation of download_request_full using ThreadPoolExecutor.
    #     """
    #     self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True)
    #     self._prepare_for_multithreading(ckan)
    #     try:
    #         with ThreadPoolExecutor(max_workers=threads, initializer=self._init_thread, initargs=(ckan,)) as executor:
    #             if ckan.verbose_extra:
    #                 print(f"Launching multi-threaded download of multi-file resource {self.name}")
    #             futures = [executor.submit(self.download_file_query_item_graceful, ckan=ckan, out_dir=out_dir, file_query_item=file_query_item,
    #                                        index=index, external_stop_event=external_stop_event, start_index=start_index, end_index=end_index)
    #                        for index, file_query_item in enumerate(self.get_file_query_generator())]
    #             for future in futures:
    #                 future.result()  # This will propagate the exception
    #         total = self.get_file_query_len()
    #         self._call_progress_callback(total, total, context=f"multi-thread download")
    #     except Exception as e:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print(f"Stopping all threads because an exception occurred: {e}")
    #         raise e from e
    #     finally:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print("End of multi-threaded download...")

    def download_sample_df(self, ckan: CkanApi, search_all:bool=False, **kwargs) -> pd.DataFrame:
        # alias with search_all=False by default
        return super().download_sample_df(ckan=ckan, search_all=search_all, **kwargs)

    def download_sample(self, ckan:CkanApi, full_download:bool=False, **kwargs) -> bytes:
        # alias with full_download=False by default
        return super().download_sample(ckan=ckan, full_download=full_download, **kwargs)


