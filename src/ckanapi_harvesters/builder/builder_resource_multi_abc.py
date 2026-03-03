#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements the basic resources. See builder_datastore for specific functions to initiate datastores.
"""
from concurrent.futures import ThreadPoolExecutor
import threading
from threading import current_thread
from typing import Any, Generator, Union, Callable, Set, List, Dict, Tuple
from abc import ABC, abstractmethod
import io
import os
import glob
import fnmatch
from warnings import warn
import copy

import pandas as pd
import requests

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice, CkanResourceInfo
from ckanapi_harvesters.builder.builder_aux import positive_end_index
from ckanapi_harvesters.auxiliary.list_records import GeneralDataFrame
from ckanapi_harvesters.builder.builder_errors import ResourceFileNotExistMessage
from ckanapi_harvesters.builder.builder_field import BuilderField
from ckanapi_harvesters.builder.builder_resource import BuilderResourceABC

multi_file_exclude_other_files:bool = True


def default_progress_callback(position:int, total:int, info:Any=None, *, context:str=None,
                              file_index:int=None, file_count:int=None, end_message:bool=False, **kwargs) -> None:
    if context is None:
        context = ""
    if position == total and end_message:
        # info is None
        print(f"{context} Finished {file_index}/{file_count} (100%)")
    elif info is None:
        print(f"{context} Request {file_index}/{file_count} ({position/total*100.0:.2f}%)")
    else:
        if isinstance(info, str):
            info_str = info
        elif isinstance(info, pd.DataFrame):
            if "source" in info.attrs.keys():
                info_str = str(info.attrs["source"])
            else:
                info_str = "<DataFrame>"
        elif isinstance(info, list):
            info_str = "<records>"
        else:
            info_str = str(info)
        print(f"{context} Request {file_index}/{file_count} ({position/total*100.0:.2f}%): " + info_str)


class FileChunkDataFrame:
    """
    Class to hold a chunk of a DataFrame of a file (only for DataStores), with the file name, index and an indication of the position in the file
    """
    def __init__(self, df: Union[GeneralDataFrame,None], file_path: str, file_index: int, chunk_index: int, file_position: int) -> None:
        """
        :param df: the data of the file chunk (leave None if not loaded)
        :param file_path: the path to the source
        :param file_index: the index of the file in the list
        :param chunk_index: counter of chunks read within the file
        :param file_position: the position within the file itself (approximation)
        """
        self.df: Union[GeneralDataFrame,None] = df
        self.file_path: str = file_path
        self.file_index: int = file_index
        self.chunk_index: int = chunk_index
        self.file_position: int = file_position
        self.is_first_chunk: bool = chunk_index == 0  # boolean indicating if the chunk is the first chunk of the file or not


class BuilderMultiABC(ABC):
    def __init__(self):
        self.progress_callback: Union[Callable[[int, int, Any], None], None] = default_progress_callback
        self.progress_callback_kwargs: dict = {}
        self.stop_event = threading.Event()
        self.thread_ckan: Dict[str, CkanApi] = {}
        self.enable_multi_threaded_upload:bool = True
        self.enable_multi_threaded_download:bool = True
        # from Resource (for code validation)
        self.name:str = ""
        self.enable_download:bool = True

    def copy(self, *, dest=None):
        dest.progress_callback = self.progress_callback
        dest.progress_callback_kwargs = copy.deepcopy(self.progress_callback_kwargs)
        dest.enable_multi_threaded_upload = self.enable_multi_threaded_upload
        dest.enable_multi_threaded_download = self.enable_multi_threaded_download
        # do not copy stop_event
        return dest

    def _call_progress_callback(self, position:int, total:int, *, info:Any=None, context:str=None,
                                file_index:int=0, file_count:int=None, end_message: bool=False) -> None:
        if self.progress_callback is not None:
            if end_message:
                position = total
                file_index = file_count
            self.progress_callback(position, total, info=info, context=context,
                                   file_index=file_index, file_count=file_count, end_message=end_message,
                                   **self.progress_callback_kwargs)

    def _prepare_for_multithreading(self, ckan: CkanApi):
        self.stop_event.clear()
        self.thread_ckan.clear()

    def _init_thread(self, ckan: CkanApi):
        thread_name = current_thread().name
        ckan_thread = ckan.copy(new_identifier=thread_name)
        ckan_thread.prepare_for_multithreading(True)  # prepare CKAN object for multi-threading
        self.thread_ckan[thread_name] = ckan_thread

    def _terminate_thread(self):
        for ckan in self.thread_ckan.values():
            ckan.disconnect()
        self.thread_ckan.clear()


    ## upload -----------------------------------------------------------------
    @abstractmethod
    def init_local_files_list(self, resources_base_dir:str, cancel_if_present:bool=True, **kwargs) -> List[str]:
        """
        Behavior to list parts of an upload.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_local_file_offset(self, file_chunk: FileChunkDataFrame) -> int:
        """
        Get the position of the current data in the overall upload.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_local_file_total_size(self) -> int:
        """
        Get the overall size of the upload, normally in bytes or line count.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_local_file_count(self) -> int:
        """
        Get the number of parts of the upload.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_local_df_chunk_generator(self, resources_base_dir:str, **kwargs) -> Generator[FileChunkDataFrame, None, None]:
        """
        Returns an iterator over the data to upload and a position in the current file.
        """
        raise NotImplementedError()

    def upload_request_final(self, ckan:CkanApi, *, force:bool=False) -> None:
        return self.upsert_request_final(ckan=ckan, force=force)

    def _unit_upload_apply(self, *, ckan:CkanApi, file_chunk: FileChunkDataFrame,
                           upload_alter:bool=True, overall_chunk_index:int, file_count: int, start_index:int, end_index:int, **kwargs) -> Any:
        """
        Unitary function deciding whether to perform upload and making the steps for the upload.
        """
        raise NotImplementedError()

    def upload_request_full(self, ckan:CkanApi, resources_base_dir:str, *,
                            threads:int=1, external_stop_event=None,
                            start_index:int=0, end_index:int=None, **kwargs) -> None:
        """
        Perform all the upload requests.

        :param ckan:
        :param resources_base_dir:
        :param threads:
        :param external_stop_event:
        :param only_missing:
        :param start_index:
        :param end_index:
        :return:
        """
        if threads < 0:
            # cancel large uploads in this case
            return None
        elif (threads is None or threads > 1) and self.enable_multi_threaded_upload:
            return self.upload_request_full_multi_threaded(ckan=ckan, resources_base_dir=resources_base_dir,
                                                           threads=threads, external_stop_event=external_stop_event,
                                                           start_index=start_index, end_index=end_index, **kwargs)
        else:
            self.init_local_files_list(resources_base_dir=resources_base_dir, cancel_if_present=True, **kwargs)
            if ckan.params.verbose_extra:
                print(f"Launching single-threaded upload of multi-file resource {self.name}")
            total = self.get_local_file_count()
            end_index = positive_end_index(end_index, total)
            for overall_chunk_index, file_chunk in enumerate(self.get_local_df_chunk_generator(resources_base_dir=resources_base_dir, **kwargs)):
                if external_stop_event is not None and external_stop_event.is_set():
                    print(f"{ckan.identifier} Interrupted")
                    return
                self._unit_upload_apply(ckan=ckan, file_chunk=file_chunk, overall_chunk_index=overall_chunk_index,
                                        start_index=start_index, end_index=end_index, file_count=total, **kwargs)
            self._call_progress_callback(self.get_local_file_total_size(), self.get_local_file_total_size(), file_count=total,
                                         context=f"{ckan.identifier} single-thread upload", end_message=True)
            # at last, apply final actions:
            self.upload_request_final(ckan)

    def upload_request_graceful(self, ckan:CkanApi, file_chunk: FileChunkDataFrame, *, overall_chunk_index:int,
                                external_stop_event=None,
                                start_index:int=0, end_index:int=None, **kwargs) -> None:
        """
        Calls upload_file with checks specific to multi-threading.

        :return:
        """
        # ckan.session_reset()
        # ckan.identifier = current_thread().name
        ckan = self.thread_ckan[current_thread().name]
        total = self.get_local_file_count()
        end_index = positive_end_index(end_index, total)
        if self.stop_event.is_set():
            return
        if external_stop_event is not None and external_stop_event.is_set():
            print(f"{ckan.identifier} Interrupted")
            return
        try:
            self._unit_upload_apply(ckan=ckan, file_chunk=file_chunk, overall_chunk_index=overall_chunk_index,
                                    start_index=start_index, end_index=end_index, file_count=total, **kwargs)
        except Exception as e:
            self.stop_event.set()  # Ensure all threads stop
            if ckan.params.verbose_extra:
                print(f"Stopping all threads because an exception occurred in thread: {e}")
            raise e from e

    def upload_request_full_multi_threaded(self, ckan:CkanApi, resources_base_dir:str,
                            threads:int=1, external_stop_event=None,
                            start_index:int=0, end_index:int=None, **kwargs):
        """
        Multi-threaded implementation of upload_request_full, using ThreadPoolExecutor.
        """
        self.init_local_files_list(resources_base_dir=resources_base_dir, cancel_if_present=True, **kwargs)
        self._prepare_for_multithreading(ckan)
        try:
            with ThreadPoolExecutor(max_workers=threads, initializer=self._init_thread, initargs=(ckan,)) as executor:
                if ckan.params.verbose_extra:
                    print(f"Launching multi-threaded upload of multi-file resource {self.name}")
                futures = [executor.submit(self.upload_request_graceful, ckan=ckan, file_chunk=file_chunk, overall_chunk_index=overall_chunk_index,
                                           start_index=start_index, end_index=end_index, external_stop_event=external_stop_event,
                                           **kwargs)
                           for overall_chunk_index, file_chunk in enumerate(self.get_local_df_chunk_generator(resources_base_dir=resources_base_dir, **kwargs))]
                for future in futures:
                    future.result()  # This will propagate the exception
            self._call_progress_callback(self.get_local_file_total_size(), self.get_local_file_total_size(), file_count=self.get_local_file_count(),
                                         context=f"{ckan.identifier} multi-thread upload", end_message=True)
        except Exception as e:
            self.stop_event.set()  # Ensure all threads stop
            if ckan.params.verbose_extra:
                print(f"Stopping all threads because an exception occurred: {e}")
            raise e from e
        finally:
            # self.stop_event.set()  # Ensure all threads stop
            if ckan.params.verbose_extra:
                print("End of multi-threaded upload...")
        # at last, apply final actions:
        self._terminate_thread()
        self.upload_request_final(ckan)

    ## download -------------------------------------------------------------
    @abstractmethod
    def init_download_file_query_list(self, ckan: CkanApi, out_dir: str, cancel_if_present: bool = True, **kwargs) -> List[Any]:
        """
        Determine the list of queries to download to reconstruct the uploaded parts.
        By default, the unique combinations of the first columns of the primary key are used.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_file_query_generator(self) -> Generator[Any, Any, None]:
        """
        Returns an iterator on all the file_queries.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_file_query_count(self) -> int:
        """
        Returns the total number of file_queries.
        """
        raise NotImplementedError()

    @abstractmethod
    def download_file_query_item(self, ckan: CkanApi, out_dir: str, file_query_item: Any) -> Any:
        """
        Download the file_query item with the its arguments
        """
        raise NotImplementedError()

    @abstractmethod
    def download_request_generator(self, ckan: CkanApi, out_dir: str) -> Generator[Any, Any, None]:
        """
        Generator to apply treatments after each request (single-threaded).

        :param ckan:
        :param out_dir:
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def _unit_download_apply(self, ckan:CkanApi, file_query_item:Any, out_dir:str,
                           index:int, start_index:int, end_index:int, total:int, **kwargs) -> Any:
        """
        Unitary function deciding whether to perform download and making the steps for the request.
        """
        raise NotImplementedError()

    def download_request_full(self, ckan: CkanApi, out_dir: str, threads:int=1, external_stop_event=None,
                              start_index:int=0, end_index:int=None, force:bool=False, **kwargs) -> None:

        if (not self.enable_download) and (not force):
            msg = f"Did not download resource {self.name} because download was disabled."
            warn(msg)
            return None
        if threads < 0:
            # do not download large datasets in this case
            return None
        elif (threads is None or threads > 1) and self.enable_multi_threaded_download:
            return self.download_request_full_multi_threaded(ckan=ckan, out_dir=out_dir,
                                                             threads=threads, external_stop_event=external_stop_event,
                                                             start_index=start_index, end_index=end_index, **kwargs)
        else:
            self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True, **kwargs)
            if ckan.params.verbose_extra:
                print(f"Launching single-threaded download of multi-file resource {self.name}")
            total = self.get_file_query_count()
            end_index = positive_end_index(end_index, total)
            for index, file_query_item in enumerate(self.get_file_query_generator()):
                if external_stop_event is not None and external_stop_event.is_set():
                    print(f"{ckan.identifier} Interrupted")
                    return
                self._unit_download_apply(ckan=ckan, file_query_item=file_query_item, out_dir=out_dir,
                                          index=index, start_index=start_index, end_index=end_index, total=total, **kwargs)
            self._call_progress_callback(total, total, context=f"{ckan.identifier} single-thread download", end_message=True)

    def download_file_query_item_graceful(self, ckan: CkanApi, out_dir: str, file_query_item: Any, index:int,
                                          external_stop_event=None, start_index:int=0, end_index:int=None, **kwargs) -> None:
        """
        Implementation of download_file_query_item with checks for a multi-threaded download.
        """
        # ckan.session_reset()
        # ckan.identifier = current_thread().name
        ckan = self.thread_ckan[current_thread().name]
        total = self.get_file_query_count()
        end_index = positive_end_index(end_index, total)
        if self.stop_event.is_set():
            return
        if external_stop_event is not None and external_stop_event.is_set():
            print(f"{ckan.identifier} Interrupted")
            return
        try:
            self._unit_download_apply(ckan=ckan, file_query_item=file_query_item, out_dir=out_dir,
                                      index=index, start_index=start_index, end_index=end_index, total=total, **kwargs)
        except Exception as e:
            self.stop_event.set()  # Ensure all threads stop
            if ckan.params.verbose_extra:
                print(f"Stopping all threads because an exception occurred in thread: {e}")
            raise e from e

    def download_request_full_multi_threaded(self, ckan: CkanApi, out_dir: str,
                                             threads: int = None, external_stop_event=None,
                                             start_index:int=0, end_index:int=-1, **kwargs) -> None:
        """
        Multi-threaded implementation of download_request_full using ThreadPoolExecutor.
        """
        self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True, **kwargs)
        self._prepare_for_multithreading(ckan)
        try:
            with ThreadPoolExecutor(max_workers=threads, initializer=self._init_thread, initargs=(ckan,)) as executor:
                if ckan.params.verbose_extra:
                    print(f"Launching multi-threaded download of multi-file resource {self.name}")
                futures = [executor.submit(self.download_file_query_item_graceful, ckan=ckan, out_dir=out_dir, file_query_item=file_chunk,
                                           index=index, external_stop_event=external_stop_event, start_index=start_index, end_index=end_index, **kwargs)
                           for index, file_chunk in enumerate(self.get_file_query_generator())]
                for future in futures:
                    future.result()  # This will propagate the exception
            total = self.get_file_query_count()
            self._call_progress_callback(total, total, context=f"multi-thread download", end_message=True)
        except Exception as e:
            self.stop_event.set()  # Ensure all threads stop
            if ckan.params.verbose_extra:
                print(f"Stopping all threads because an exception occurred: {e}")
            raise e from e
        finally:
            # self.stop_event.set()  # Ensure all threads stop
            if ckan.params.verbose_extra:
                print("End of multi-threaded download...")
        # at last, apply final actions:
        self._terminate_thread()
