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
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo
from ckanapi_harvesters.auxiliary.path import resolve_rel_path, glob_rm_glob, glob_name
from ckanapi_harvesters.builder.builder_aux import positive_end_index
from ckanapi_harvesters.builder.builder_errors import ResourceFileNotExistMessage
from ckanapi_harvesters.builder.builder_resource_multi_abc import BuilderMultiABC
from ckanapi_harvesters.builder.builder_resource import BuilderResourceABC

multi_file_exclude_other_files:bool = True


def default_progress_callback(index:int, total:int, info:Any, *, context:str=None, **kwargs) -> None:
    if context is None:
        context = ""
    if index == total:
        # info is None
        print(f"{context} Finished {index}/{total} (100%)")
    elif info is None:
        print(f"{context} Request {index}/{total} ({index/total*100.0:.2f}%)")
    else:
        if isinstance(info, str):
            info_str = info
        elif isinstance(info, pd.DataFrame):
            if "source" in info.attrs.keys():
                info_str = str(info.attrs["source"])
            else:
                info_str = "<DataFrame>"
        else:
            info_str = str(info)
        print(f"{context} Request {index}/{total} ({index/total*100.0:.2f}%): " + info_str)


class BuilderMultiFile(BuilderResourceABC, BuilderMultiABC):
    """
    Class to manage a set of files to upload as separate resources
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, dir_name:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.dir_name: str = dir_name
        self.local_file_list_base_dir: str = ""
        self.local_file_list: Union[List[str], None] = None
        self.excluded_files: Set[str] = set()
        self.remote_resource_names: Union[List[str], None] = None
        self.excluded_resource_names: Set[str] = set()
        # BuilderMultiABC:
        self.stop_event = threading.Event()
        self.thread_ckan: Dict[str, CkanApi] = {}
        self.progress_callback: Union[Callable[[int, int, Any], None], None] = default_progress_callback
        self.progress_callback_kwargs: dict = {}
        self.enable_multi_threaded_upload:bool = True
        self.enable_multi_threaded_download:bool = True

    @staticmethod
    def resource_mode_str() -> str:
        return "MultiFile"

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderMultiFile()
        super().copy(dest=dest)
        dest.dir_name = self.dir_name
        # BuilderMultiABC:
        dest.progress_callback = self.progress_callback
        dest.progress_callback_kwargs = copy.deepcopy(self.progress_callback_kwargs)
        dest.enable_multi_threaded_upload = self.enable_multi_threaded_upload
        dest.enable_multi_threaded_download = self.enable_multi_threaded_download
        # do not copy stop_event
        return dest

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.dir_name = _string_from_element(row["file/url"], empty_value="")

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.dir_name
        return d

    def get_or_query_resource_id(self, ckan: CkanApi, cancel_if_present:bool=True, error_not_found:bool=True) -> Union[None,str]:
        return None


    ## upload --------------------------------------------------------------------
    def patch_request(self, ckan: CkanApi, package_id: str, *, reupload: bool = None, resources_base_dir:str=None,
                      payload:Union[bytes, io.BufferedIOBase]=None) -> Union[None, CkanResourceInfo]:
        return None

    def upload_request_final(self, ckan:CkanApi, *, force:bool=False) -> None:
        return None

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir:str, file_index:int=0) -> Union[str,None]:
        self.list_local_files(resources_base_dir=resources_base_dir)
        return self.local_file_list[file_index]

    def load_sample_data(self, resources_base_dir:str, file_index:int=0) -> Union[bytes,None]:
        file_path:str = self.get_sample_file_path(resources_base_dir, file_index=file_index)
        with open(file_path, "rb") as f:
            return f.read()

    def list_local_files(self, resources_base_dir:str, cancel_if_present:bool=True,
                         excluded_files:Set[str]=None) -> Union[List[str],None]:
        """
        List files corresponding to the multi-file resource configuration and are not used in mono-resources

        :param resources_base_dir:
        :param cancel_if_present:
        :param excluded_files: files from mono-resources
        :return:
        """
        if excluded_files is None:
            excluded_files = set()
        if (cancel_if_present and self.local_file_list is not None
                and self.local_file_list_base_dir == resources_base_dir
                and self.excluded_files == excluded_files):
            return self.local_file_list
        dir_search_path = resolve_rel_path(resources_base_dir, self.dir_name, field=f"File/URL of resource {self.name}")
        search_query = dir_search_path
        file_set = set(glob.glob(search_query))
        file_set = file_set - excluded_files
        file_list = list(file_set)
        file_list.sort()
        self.local_file_list = file_list
        self.local_file_list_base_dir = resources_base_dir
        self.excluded_files = excluded_files
        return file_list

    def init_local_files_list(self, resources_base_dir:str, cancel_if_present:bool=True, excluded_files:Set[str]=None, **kwargs) -> List[str]:
        return self.list_local_files(resources_base_dir=resources_base_dir, cancel_if_present=cancel_if_present,
                                     excluded_files=excluded_files)

    def get_local_file_len(self) -> int:
        if self.local_file_list is None:
            raise RuntimeError("You must call list_local_files first")
        return len(self.local_file_list)

    def get_local_file_generator(self, resources_base_dir:str, excluded_files:Set[str]=None, **kwargs) -> Generator[str, None, None]:
        self.list_local_files(resources_base_dir=resources_base_dir, excluded_files=excluded_files)
        for file_name in self.local_file_list:
            yield file_name

    def upload_file_checks(self, *, resources_base_dir: str = None, ckan: CkanApi = None, excluded_files:Set[str]=None, **kwargs) \
            -> Union[None, ContextErrorLevelMessage]:
        if os.path.isdir(resolve_rel_path(resources_base_dir, glob_rm_glob(self.dir_name), field=f"File/URL of resource {self.name}")):
            if len(self.list_local_files(resources_base_dir=resources_base_dir, excluded_files=excluded_files)) > 0:
                return None
            else:
                return ResourceFileNotExistMessage(self.name, ErrorLevel.Error,
                    f"Empty resource directory for multi-file resource {self.name}: {os.path.join(resources_base_dir, self.dir_name)}")
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error,
                f"Missing directory for multi-file resource {self.name}: {os.path.join(resources_base_dir, self.dir_name)}")

    def upload_file(self, ckan:CkanApi, package_id:str, file_path:str, *,
                    reupload:bool=False, cancel_if_present:bool=True) -> CkanResourceInfo:
        """
        Upload a file, using its name as resource name
        """
        _, resource_name = os.path.split(file_path)
        resource_info = ckan.map.get_resource_info(resource_name, package_name=package_id, error_not_mapped=False)
        if resource_info is not None and cancel_if_present and not reupload:
            resource_info.newly_created = False
            resource_info.newly_updated = False
            return resource_info
        return ckan.resource_create(package_id, resource_name, format=self.format, description=self.description,
                                    state=self.state, file_path=file_path, reupload=reupload, cancel_if_exists=True, update_if_exists=True,
                                    create_default_view=True, auto_submit=False)

    def _unit_upload_apply(self, *, ckan:CkanApi, file:str,
                           index:int, start_index:int, end_index:int, total:int,
                           package_id:str, reupload:bool, only_missing:bool, excluded_files:Set[str]) -> None:
        # For each file, this function initiates its own FileStore.
        file_path = file
        _, file_name = os.path.split(file_path)
        if start_index <= index and index < end_index and file_path not in excluded_files:
            self._call_progress_callback(index, total, info=file_path,
                                         context=f"{ckan.identifier} single-thread upload")
            self.upload_file(ckan=ckan, package_id=package_id, file_path=file_path,
                             reupload=reupload, cancel_if_present=only_missing)
        else:
            # self._call_progress_callback(index, total, info=df_upload_local, context=f"{ckan.identifier} single-thread skip")
            pass

    def upload_request_full(self, ckan:CkanApi, resources_base_dir:str, *,
                            threads:int=1, external_stop_event=None,
                            start_index:int=0, end_index:int=None,
                            reupload:bool=False, only_missing:bool=False, excluded_files:Set[str]=None) -> None:
        if excluded_files is None:
            excluded_files = set()
        package_id = self.get_or_query_package_id(ckan)
        super().upload_request_full(ckan=ckan, resources_base_dir=resources_base_dir, threads=threads,
                                    external_stop_event=external_stop_event, start_index=start_index, end_index=end_index,
                                    reupload=reupload, only_missing=only_missing,
                                    package_id=package_id, excluded_files=excluded_files)
        # if threads < 0:
        #     # cancel large uploads in this case
        #     return None
        # elif threads is None or threads > 1:
        #     return self.upload_request_full_multi_threaded(ckan=ckan, resources_base_dir=resources_base_dir,
        #                                                    threads=threads, external_stop_event=external_stop_event,
        #                                                    start_index=start_index, end_index=end_index,
        #                                                    reupload=reupload, only_missing=only_missing,
        #                                                    excluded_files=excluded_files)
        # else:
        #     self.init_local_files_list(resources_base_dir=resources_base_dir, cancel_if_present=True, excluded_files=excluded_files)
        #     package_id = self.get_or_query_package_id(ckan)
        #     if ckan.verbose_extra:
        #         print(f"Launching single-threaded upload of multi-file resource {self.name}")
        #     total = self.get_local_file_len()
        #     end_index = positive_end_index(end_index, total)
        #     for index, file_path in enumerate(self.get_local_file_generator(resources_base_dir=resources_base_dir, excluded_files=excluded_files)):
        #         if external_stop_event is not None and external_stop_event.is_set():
        #             print(f"{ckan.identifier} Interrupted")
        #             return
        #         self._unit_upload_apply(ckan, file=file_path, package_id=package_id,
        #                                 reupload=reupload, only_missing=only_missing,
        #                                 index=index, start_index=start_index, end_index=end_index, total=total,
        #                                 excluded_files=excluded_files)
        #     self._call_progress_callback(total, total, context=f"{ckan.identifier} single-thread upload")
        #     # at last, apply final actions:
        #     self.upload_request_final(ckan)

    # def upload_request_graceful(self, ckan:CkanApi, file_path: str, *, index:int, package_id:str,
    #                             external_stop_event=None,
    #                             start_index:int=0, end_index:int=None,
    #                             reupload:bool=False, only_missing:bool=False, excluded_files:Set[str]=None) -> None:
    #     """
    #     Calls upload_file with checks specific to multi-threading.
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
    #         self._unit_upload_apply(ckan, file=file_path, package_id=package_id,
    #                                 reupload=reupload, only_missing=only_missing,
    #                                 index=index, start_index=start_index, end_index=end_index, total=total,
    #                                 excluded_files=excluded_files)
    #     except Exception as e:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print(f"Stopping all threads because an exception occurred in thread: {e}")
    #         raise e from e

    # def upload_request_full_multi_threaded(self, ckan:CkanApi, resources_base_dir:str,
    #                         threads:int=1, external_stop_event=None,
    #                         start_index:int=0, end_index:int=None,
    #                         reupload:bool=False, only_missing:bool=False, excluded_files:Set[str]=None):
    #     """
    #     Multi-threaded implementation of upload_request_full, using ThreadPoolExecutor.
    #     """
    #     self.init_local_files_list(resources_base_dir=resources_base_dir, cancel_if_present=True, excluded_files=excluded_files)
    #     package_id = self.get_or_query_package_id(ckan)
    #     self._prepare_for_multithreading(ckan)
    #     try:
    #         with ThreadPoolExecutor(max_workers=threads, initializer=self._init_thread, initargs=(ckan,)) as executor:
    #             if ckan.verbose_extra:
    #                 print(f"Launching multi-threaded upload of multi-file resource {self.name}")
    #             futures = [executor.submit(self.upload_request_graceful, ckan=ckan, file_path=file_path, index=index, package_id=package_id,
    #                                        start_index=start_index, end_index=end_index, external_stop_event=external_stop_event,
    #                                        excluded_files=excluded_files, reupload=reupload, only_missing=only_missing)
    #                        for index, file_path in enumerate(self.get_local_file_generator(resources_base_dir=resources_base_dir, excluded_files=excluded_files))]
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
    #     self.upload_request_final(ckan)


    ## download ------------------------------------------------
    def list_remote_resources(self, ckan:CkanApi, *, excluded_resource_names:Set[str]=None,
                              cancel_if_present: bool = True) -> List[str]:
        """
        Defines the list of resources to download that correspond to the definition and are not used in mono-resources.

        :param ckan:
        :param excluded_resource_names: resource names of mono-resources
        :param cancel_if_present:
        :return:
        """
        if cancel_if_present and self.remote_resource_names is not None and self.excluded_resource_names == excluded_resource_names:
            return self.remote_resource_names
        if excluded_resource_names is None:
            excluded_resource_names = set()
        package_info = ckan.get_package_info_or_request(self.package_name)
        resource_names = set(package_info.resources_id_index.keys())
        # resource_name_glob = self.name
        resource_name_glob = glob_name(self.dir_name)
        filtered_resource_names = set(fnmatch.filter(resource_names, resource_name_glob))  # apply name as wildcard filter
        filtered_resource_names = filtered_resource_names - excluded_resource_names
        self.remote_resource_names = sorted(list(filtered_resource_names))
        self.excluded_resource_names = excluded_resource_names
        return self.remote_resource_names

    def list_remote_resource_ids(self, ckan:CkanApi, *, excluded_resource_names:Set[str]=None,
                              cancel_if_present: bool = True) -> List[str]:
        resource_names = self.list_remote_resources(ckan, excluded_resource_names=excluded_resource_names,
                                                    cancel_if_present=cancel_if_present)
        resource_ids = [ckan.map.get_resource_id(resource_name, package_name=self.package_name) for resource_name in resource_names]
        return resource_ids

    def init_download_file_query_list(self, ckan: CkanApi, out_dir: str=None,
                                      cancel_if_present: bool = True,
                                      excluded_resource_names:Set[str]=None, **kwargs) -> List[str]:
        if out_dir is not None:
            dir_tables = resolve_rel_path(out_dir, glob_rm_glob(self.dir_name, default_rec_dir=self.name), field=f"File/URL of resource {self.name}")
            os.makedirs(dir_tables, exist_ok=True)
        return self.list_remote_resources(ckan, cancel_if_present=cancel_if_present, excluded_resource_names=excluded_resource_names)

    def get_file_query_generator(self) -> Generator[str, Any, None]:
        for resource_name in self.remote_resource_names:
            yield resource_name

    def get_file_query_len(self) -> int:
        if self.remote_resource_names is None:
            raise RuntimeError("init_download_file_query_list must be called first")
        return len(self.remote_resource_names)

    def download_file_query_item(self, ckan: CkanApi, out_dir: str, file_query_item: str) \
            -> Tuple[Union[str,None], Union[requests.Response,None]]:
        resource_name = file_query_item
        file_out = None
        if out_dir is not None:
            file_out = resolve_rel_path(out_dir, glob_rm_glob(self.dir_name, default_rec_dir=self.name), resource_name, field=f"File/URL of resource {self.name}")
            if self.download_skip_existing and os.path.exists(file_out):
                if ckan.params.verbose_extra:
                    print(f"Skipping existing file {file_out}")
                return file_out, None
        resource_id = ckan.map.get_resource_id(resource_name, package_name=self.package_name)
        resource_info, response = ckan.resource_download(resource_id)
        if out_dir is not None:
            with open(file_out, 'wb') as f:
                f.write(response.content)
        else:
            file_out = None
        return file_out, response

    def download_request_generator(self, ckan: CkanApi, out_dir: str,
                                   excluded_resource_names:Set[str]=None) -> Generator[Tuple[Union[str,None], Union[requests.Response,None]], Any, None]:
        self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True,
                                           excluded_resource_names=excluded_resource_names)
        for file_query_item in self.get_file_query_generator():
            yield self.download_file_query_item(ckan=ckan, out_dir=out_dir, file_query_item=file_query_item)

    def _unit_download_apply(self, ckan:CkanApi, file_query_item:Any, out_dir:str,
                           index:int, start_index:int, end_index:int, total:int, excluded_resource_names:Set[str]) -> Any:
        if start_index <= index and index < end_index and file_query_item not in excluded_resource_names:
            self._call_progress_callback(index, total, info=file_query_item,
                                         context=f"{ckan.identifier} single-thread download")
            self.download_file_query_item(ckan=ckan, out_dir=out_dir, file_query_item=file_query_item)
        else:
            pass
            # self._call_progress_callback(index, total, info=file_query_item, context=f"{ckan.identifier} single-thread skip")

    def download_request_full(self, ckan: CkanApi, out_dir: str, threads:int=1, external_stop_event=None,
                              start_index:int=0, end_index:int=None, force:bool=False,
                              excluded_resource_names:Set[str]=None) -> None:
        return super().download_request_full(ckan=ckan, out_dir=out_dir, threads=threads,
                                             external_stop_event=external_stop_event,
                                             start_index=start_index, end_index=end_index, force=force,
                                             excluded_resource_names=excluded_resource_names)
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
        #                                                      start_index=start_index, end_index=end_index,
        #                                                      excluded_resource_names=excluded_resource_names)
        # else:
        #     self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True,
        #                                        excluded_resource_names=excluded_resource_names)
        #     if ckan.verbose_extra:
        #         print(f"Launching single-threaded download of multi-file resource {self.name}")
        #     total = self.get_file_query_len()
        #     end_index = positive_end_index(end_index, total)
        #     for index, file_query_item in enumerate(self.get_file_query_generator()):
        #         if external_stop_event is not None and external_stop_event.is_set():
        #             print(f"{ckan.identifier} Interrupted")
        #             return
        #         self._unit_download_apply(ckan=ckan, file_query_item=file_query_item, out_dir=out_dir,
        #                                   index=index, start_index=start_index, end_index=end_index, total=total,
        #                                   excluded_resource_names=excluded_resource_names)
        #     self._call_progress_callback(total, total, context=f"{ckan.identifier} single-thread download")

    # def download_file_query_item_graceful(self, ckan: CkanApi, out_dir: str, resource_name: str, index:int,
    #                                       external_stop_event=None, start_index:int=0, end_index:int=None,
    #                                       excluded_resource_names:Set[str]=None) -> None:
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
    #         self._unit_download_apply(ckan=ckan, file_query_item=file_query_item, out_dir=out_dir,
    #                                   index=index, start_index=start_index, end_index=end_index, total=total,
    #                                   excluded_resource_names=excluded_resource_names)
    #     except Exception as e:
    #         self.stop_event.set()  # Ensure all threads stop
    #         if ckan.verbose_extra:
    #             print(f"Stopping all threads because an exception occurred in thread: {e}")
    #         raise e from e

    # def download_request_full_multi_threaded(self, ckan: CkanApi, out_dir: str,
    #                                          threads: int = None, external_stop_event=None,
    #                                          start_index:int=0, end_index:int=-1,
    #                                          excluded_resource_names:Set[str]=None) -> None:
    #     """
    #     Multi-threaded implementation of download_request_full using ThreadPoolExecutor.
    #     """
    #     self.init_download_file_query_list(ckan=ckan, out_dir=out_dir, cancel_if_present=True, excluded_resource_names=excluded_resource_names)
    #     self._prepare_for_multithreading(ckan)
    #     try:
    #         with ThreadPoolExecutor(max_workers=threads, initializer=self._init_thread, initargs=(ckan,)) as executor:
    #             if ckan.verbose_extra:
    #                 print(f"Launching multi-threaded download of multi-file resource {self.name}")
    #             futures = [executor.submit(self.download_file_query_item_graceful, ckan=ckan, out_dir=out_dir, resource_name=resource_name,
    #                                        index=index, external_stop_event=external_stop_event, start_index=start_index, end_index=end_index,
    #                                        excluded_resource_names=excluded_resource_names)
    #                        for index, resource_name in enumerate(self.get_file_query_generator())]
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

    def download_sample(self, ckan:CkanApi, full_download:bool=True, **kwargs) -> Union[bytes, None]:
        return None

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download:bool=True, threads:int=1,
                         force:bool=False, excluded_resource_names:Set[str]=None, **kwargs) -> None:
        if full_download:
            return self.download_request_full(ckan=ckan, out_dir=out_dir, threads=threads, force=force,
                                              excluded_resource_names=excluded_resource_names, **kwargs)

    def resource_info_request(self, ckan:CkanApi, error_not_found:bool=True) -> Union[CkanResourceInfo, None]:
        return None  # there are multiple resource ids => do not return info
    def _to_ckan_resource_info(self, package_id:str, check_id:bool=True) -> CkanResourceInfo:
        return None
