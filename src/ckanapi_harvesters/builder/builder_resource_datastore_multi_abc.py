#!python3
# -*- coding: utf-8 -*-
"""
Code to initiate a DataStore defined by a large number of files to concatenate into one table
"""
import threading
from threading import Semaphore
from abc import ABC
from typing import Dict, List, Any, Tuple, Generator, Union, Set, Collection
from warnings import warn
from collections import OrderedDict

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanCallbackLevel
from ckanapi_harvesters.builder.builder_resource_datastore import BuilderDataStoreABC
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.ckan_configuration import datastore_default_upload_index_col_name, datastore_default_source_file_col_name
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.builder.mapper_datastore_multi import RequestFileMapperABC, default_file_mapper_from_primary_key
from ckanapi_harvesters.builder.builder_resource_multi_abc import FileChunkDataFrame
from ckanapi_harvesters.builder.builder_resource_multi_file import BuilderMultiABC
from ckanapi_harvesters.builder.builder_field import BuilderField

# apply last_condition for each upsert request when in a multi-threaded upload on a same DataStore:
datastore_multi_threaded_always_last_condition:bool = True
# when there are multiple files, apply last insertion commands after each document? True: after each csv file, False: only at the end
datastore_multi_apply_last_condition_intermediary:bool = False


class BuilderDataStoreMultiABC(BuilderDataStoreABC, BuilderMultiABC, ABC):
    """
    generic class to manage large DataStore, divided into files/parts
    This abstract class is intended to be overloaded in order to be used to generate data from the workspace, without using CSV files
    """

    def __init__(self, *, parent, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, options_string:str=None, base_dir:str=None):
        super().__init__(parent=parent, name=name, format=format, description=description, resource_id=resource_id,
                         download_url=download_url, options_string=options_string, base_dir=base_dir)
        # Functions inputs/outputs
        self.df_mapper: RequestFileMapperABC
        self.setup_default_file_mapper()
        self.reupload_if_needed = False  # do not reupload if needed because this could cause data loss (the upload function only uploads the first table)
        self.upsert_method: UpsertChoice = UpsertChoice.Upsert
        # BuilderMultiABC:
        self.stop_event = threading.Event()
        self.thread_ckan: Dict[str, CkanApi] = {}
        self.enable_multi_threaded_upload:bool = True
        self.enable_multi_threaded_download:bool = True
        self.file_semaphore = Semaphore()
        self.process_level:CkanCallbackLevel = CkanCallbackLevel.ResourceChunks  # resource builder - can be changed to 3 for a file of a multi-file resource

    def copy(self, *, dest=None, parent=None):
        super().copy(dest=dest, parent=parent)
        dest.reupload_if_needed = self.reupload_if_needed
        # BuilderMultiABC:
        dest.progress_callback = self.progress_callback.copy()
        dest.enable_multi_threaded_upload = self.enable_multi_threaded_upload
        dest.enable_multi_threaded_download = self.enable_multi_threaded_download
        # do not copy stop_event
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None) -> None:
        super()._load_from_df_row(row=row, base_dir=base_dir)
        self.setup_default_file_mapper()

    def _update_metadata(self, ckan: CkanApi, *, base_dir:str=None) -> None:
        """
        In certain implementations, the resource & field metadata can be derived from the data source.
        Normally, the metadata is defined by the user in an Excel worksheet. When a description is left empty,
        the value left on the CKAN server is left unchanged.
        The objective here is to propose values that override the Excel worksheet when the description
        is empty on the CKAN side (still leave CKAN values unchanged, if present).

        :param ckan: CkanApi instance
        :param override_ckan: when True, override the values from the CKAN server, if present
        """
        super()._update_metadata(ckan=ckan, base_dir=base_dir)
        if self.primary_key is not None and len(self.primary_key) == 1 and self.primary_key[0] == datastore_default_upload_index_col_name:
            if self.field_builders_user is None:
                self.field_builders_user = OrderedDict()
            if datastore_default_upload_index_col_name not in self.field_builders_user.keys():
                self.field_builders_user[datastore_default_upload_index_col_name] = BuilderField(name=datastore_default_upload_index_col_name, type_override="int8")
            field_builder = self.field_builders_user[datastore_default_upload_index_col_name]
            known_field = None
            if self.known_resource_info is not None and self.known_resource_info.datastore_info is not None and self.known_resource_info.datastore_info.fields_dict is not None:
                if datastore_default_upload_index_col_name in self.known_resource_info.datastore_info.fields_dict.keys():
                    known_field = self.known_resource_info.datastore_info.fields_dict[datastore_default_upload_index_col_name]
            if known_field is None or known_field.notes is None:
                field_builder.description = "Index of the line in the upload process"

    def setup_default_file_mapper(self, *, primary_key:List[str]=None, file_query_list:Collection[Tuple[str, dict]]=None) -> None:
        """
        This function enables the user to define the primary key and initializes the default file mapper.

        :param primary_key: manually specify the primary key
        :return:
        """
        df_mapper_mem = self.df_mapper
        if primary_key is None:
            if self.primary_key_user is not None:
                self.primary_key = self.primary_key_user
            elif self.primary_key_data_source is not None:
                self.primary_key = self.primary_key_data_source
        else:
            self.primary_key = primary_key
        if (self.primary_key is None or len(self.primary_key) == 0) and self.column_enable_upload_index:
            self.primary_key = [datastore_default_upload_index_col_name]
        self.df_mapper = default_file_mapper_from_primary_key(self.primary_key, file_query_list)
        if self.column_enable_source_file:
            self.df_mapper.source_file_column = datastore_default_source_file_col_name
        if file_query_list is not None:
            self.downloaded_file_query_list = file_query_list
        # preserve upload/download functions
        self.df_mapper.df_upload_fun = df_mapper_mem.df_upload_fun
        self.df_mapper.df_download_fun = df_mapper_mem.df_download_fun

    ## upload ---------
    # do not change default argument apply_last_condition=True
    # def upsert_request_df(self, ckan: CkanApi, df_upload:pd.DataFrame,
    #                       method:UpsertChoice=UpsertChoice.Upsert,
    #                       apply_last_condition:bool=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    #     # calls super method, with apply_last_condition defaulting to datastore_multi_apply_last_condition_intermediary
    #     if apply_last_condition is None:
    #         apply_last_condition = True  # datastore_multi_apply_last_condition_intermediary
    #     return super().upsert_request_df(ckan=ckan, df_upload=df_upload, method=method,
    #                                      apply_last_condition=apply_last_condition)

    def _get_primary_key_indexes(self, data_cleaner_index: Set[str], current_df_fields:Set[str], error_missing:bool, empty_datastore:bool=False) -> Tuple[Union[List[str],None], Union[List[str],None]]:
        primary_key, indexes = super()._get_primary_key_indexes(data_cleaner_index, current_df_fields, error_missing, empty_datastore)
        # it is highly recommended to specify a primary key: warning if not defined
        if primary_key is None:
            msg = f"It is highly recommended to specify the primary key for a DataStore defined from a directory to ensure no duplicate values are upserted to the database. Resource: {self.name}"
            warn(msg)
        else:
            ultra_required_fields = set(primary_key)
            missing_fields = ultra_required_fields
            if current_df_fields is not None:
                missing_fields -= current_df_fields
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
        super().upload_request_final(ckan, force=force)
        return self.upsert_request_final(ckan=ckan, force=force)

    def upsert_request_df_no_return(self, ckan: CkanApi, df_upload:pd.DataFrame, *,
                                    total_lines_read:int, file_name:str,
                                    method:UpsertChoice=UpsertChoice.Upsert,
                                    apply_last_condition:bool=None, always_last_condition:bool=None) -> None:
        """
        Calls upsert_request_df but does not return anything

        :return:
        """
        self.upsert_request_df(ckan=ckan, df_upload=df_upload, total_lines_read=total_lines_read, file_name=file_name,
                               method=method,
                               apply_last_condition=apply_last_condition, always_last_condition=always_last_condition)
        return None

    def _unit_upload_apply(self, *, ckan: CkanApi, file_chunk: FileChunkDataFrame,
                           upload_alter:bool=True, overall_chunk_index: int, file_count: int, start_index: int, end_index: int,
                           method: UpsertChoice, **kwargs) -> None:
        file_index = file_chunk.file_index
        if file_index == 0 and file_chunk.chunk_index == 0 and self.upsert_method == UpsertChoice.Insert:
            return  # do not reupload the first document, which was used for the initialization of the dataset
        process_upsert = start_index <= file_index and file_index < end_index and file_chunk.read_line_counter - len(file_chunk.df) >= self.upload_start_line
        self.progress_callback.update_task(self.get_local_file_offset(file_chunk), self.get_local_file_total_size(),
                                           info=file_chunk, level=self.process_level,
                                           file_index=file_index, file_count=file_count,
                                           lines_chunk=len(file_chunk.df), total_lines_read=file_chunk.read_line_counter,
                                           canceled_request=not process_upsert,
                                           context=f"{ckan.identifier} upload")
        if process_upsert:
            # if upload_alter:
            #     file_chunk.df = self.df_mapper.df_upload_alter(file_chunk.df, self.sample_data_source, fields=self._get_fields_info(), **kwargs)
            # alter function is applied in upsert_request_df
            self.upsert_request_df_no_return(ckan=ckan, df_upload=file_chunk.df, method=method,
                                             total_lines_read=file_chunk.read_line_counter, file_name=file_chunk.file_path,
                                             apply_last_condition=datastore_multi_apply_last_condition_intermediary)
        else:
            # self._call_progress_callback(index, total, info=df_upload_local, context=f"{ckan.identifier} single-thread skip")
            pass

    def get_datastore_len(self, ckan:CkanApi) -> int:
        datastore_info = ckan.get_datastore_info_or_request(self.name, self.package_name, error_not_found=True)
        return datastore_info.row_count

    def upload_request_full(self, ckan:CkanApi, resources_base_dir:str, *,
                            method:UpsertChoice=None,
                            threads:int=1, external_stop_event=None,
                            allow_chunks:bool=True,
                            only_missing:bool=False, from_line_count:bool=False,
                            start_index:int=0, end_index:int=None,
                            inhibit_datastore_patch_indexes:bool=False, **kwargs) -> None:
        self.df_mapper.upsert_only_missing_rows = only_missing
        if from_line_count:
            self.upload_start_line = self.get_datastore_len(ckan)
            if self.upload_start_line is None:
                self.upload_start_line = 0
            if ckan.params.verbose_extra:
                print(f"Transfer will start from line {self.upload_start_line}")
        if method is None:
            if self.primary_key is None or len(self.primary_key) == 0:
                self.upsert_method = UpsertChoice.Insert  # do not use upsert if there is no primary key
            method = self.upsert_method
        super().upload_request_full(ckan=ckan, resources_base_dir=resources_base_dir,
                                    threads=threads, external_stop_event=external_stop_event,
                                    allow_chunks=allow_chunks,
                                    start_index=start_index, end_index=end_index,
                                    method=method, inhibit_datastore_patch_indexes=inhibit_datastore_patch_indexes, **kwargs)
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
    def download_file_query_generator(self, ckan: CkanApi, file_query:dict) -> Generator[pd.DataFrame, Any, None]:
        """
        Download the DataFrame with the file_query arguments
        """
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        download_generator = self.df_mapper.download_file_query(ckan=ckan, resource_id=resource_id, file_query=file_query,
                                                                progress_callback=self.progress_callback)
        for df_download in download_generator:
            df = self.df_mapper.df_download_alter(df_download, file_query=file_query, fields=self._get_fields_info())
            yield df

    def _unit_download_apply(self, ckan:CkanApi, file_query_item:Any, out_dir:str,
                            index:int, start_index:int, end_index:int, total:int,
                            **kwargs) -> Any:
        if start_index <= index and index < end_index:
            self.progress_callback.update_task(index, total, info=file_query_item, level=self.process_level,
                                               file_index=index, file_count=total,
                                               total_lines_read=self.read_line_counter,
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

    def download_resource_df(self, ckan: CkanApi, search_all:bool=False, **kwargs) -> pd.DataFrame:
        # alias with search_all=False by default
        return super().download_resource_df(ckan=ckan, search_all=search_all, **kwargs)

    def download_resource_bytes(self, ckan:CkanApi, full_download:bool=False, **kwargs) -> bytes:
        # alias with full_download=False by default
        return super().download_resource_bytes(ckan=ckan, full_download=full_download, **kwargs)


