#!python3
# -*- coding: utf-8 -*-
"""
Progress callback function definition
"""
from typing import Any, Union, Callable, Set
from enum import IntEnum
import copy

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_auxiliary import FileChunkDataFrame


class CkanCallbackLevel(IntEnum):
    Packages = 0
    Resources = 1
    ResourceChunks = 2
    MultiFileResource = 3
    Request = 4


def default_progress_callback(position:int, total:int, info:Any=None, *, context:str=None,
                              file_index:int=None, file_count:int=None,
                              lines_chunk:int=None, total_lines_read:int=None,
                              canceled_upload: bool=False, end_message: bool=False, level:CkanCallbackLevel=None,
                              **kwargs) -> None:
    if level == CkanCallbackLevel.Packages:
        print(f"Package {position}/{total}")
    elif level == CkanCallbackLevel.Resources:
        if end_message:
            print(f"Done all {total} resources")
        else:
            print(f"Resource {position}/{total}")
    elif level == CkanCallbackLevel.MultiFileResource:
        print(f"Multi-file resource / file {position}/{total}")
    elif level == CkanCallbackLevel.ResourceChunks:
        if context is None:
            context = ""
        if position == total and end_message:
            # info is None
            print(f"{context} Finished {file_index}/{file_count} (100%) - {total_lines_read} lines read")
        elif canceled_upload:
            print(f"{context} Canceled {file_index}/{file_count} ({position/total*100.0:.2f}%) - {total_lines_read} lines read")
        elif info is None:
            print(f"{context} Request {file_index}/{file_count} ({position/total*100.0:.2f}%) - {total_lines_read} lines read")
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
            elif isinstance(info, FileChunkDataFrame):
                if isinstance(info.df, pd.DataFrame):
                    if "source" in info.df.attrs.keys():
                        info_str = str(info.df.attrs["source"])
                    else:
                        info_str = "<DataFrame>"
                elif isinstance(info.df, list):
                    info_str = "<records>"
                else:
                    info_str = str(info.df)
            else:
                info_str = str(info)
            print(f"{context} Request {file_index}/{file_count} ({position/total*100.0:.2f}%) - {total_lines_read} lines read: " + info_str)
    elif level == CkanCallbackLevel.Request:
        print(f"Multi-line request {position}/{total} ({position/total*100.0:.2f}%)")


class CkanProgressCallback:
    def __init__(self, callback_fun:Union[Callable,"CkanProgressCallback"]=None):
        self.progress_callback_fun: Union[Callable[[int, int, Any], None], None] = None
        self.progress_callback_kwargs: dict = {}
        self.verbosity:dict[CkanCallbackLevel,bool] = {level: True for level in CkanCallbackLevel}
        # self.verbosity[CkanCallbackLevel.Request] = False
        if isinstance(callback_fun, CkanProgressCallback):
            callback_fun.copy(dest=self)
            return
        else:
            if callback_fun is None:
                callback_fun = default_progress_callback
            self.progress_callback_fun = callback_fun

    def copy(self, *, dest=None):
        dest = CkanProgressCallback()
        dest.progress_callback_fun = self.progress_callback_fun
        dest.progress_callback_kwargs = copy.deepcopy(self.progress_callback_kwargs)
        dest.verbosity = self.verbosity.copy()
        return dest

    def __copy__(self):
        return self.copy()

    def call(self, position:int, total:int, *, info:Any=None, context:str=None,
             file_index:int=0, file_count:int=None, lines_chunk:int=None, total_lines_read:int=None,
             canceled_request: bool=False, end_message: bool=False, level:CkanCallbackLevel=0,
             **kwargs) -> None:
        """
        Progress callback function. Use to implement a progress indication for the user.

        :param position: the position within the resource (usually, in bytes or line count)
        :param total: the total size of the resource
        :param info: an object from which more information can be extracted, typically, the DataFrame itself, with an indication of the data origin.
        :param context: the context of the call (ckan instance, upload/download, single/multi-threaded)
        :param file_index: the index of the file in the list
        :param file_count: the number of files in the list
        :param lines_chunk: the number of lines in the chunk currently being processed
        :param total_lines_read: the total number of lines read, including the current chunk
        :param canceled_request: this callback is also called when a line is ignored
        :param end_message: boolean indicating of the work in progress
        :param level: the level of the progress callback
        """
        if self.progress_callback_fun is not None:
            if not self.verbosity[level]:
                return
            if end_message:
                position = total
                file_index = file_count
            self.progress_callback_fun(position, total, info=info, context=context,
                                       file_index=file_index, file_count=file_count,
                                       lines_chunk=lines_chunk, total_lines_read=total_lines_read,
                                       canceled_upload=canceled_request, end_message=end_message,
                                       level=level, **self.progress_callback_kwargs, **kwargs)
