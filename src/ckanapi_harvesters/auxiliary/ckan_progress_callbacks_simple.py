#!python3
# -*- coding: utf-8 -*-
"""
Progress callback function definition
"""
from typing import Any, Union, Callable
from threading import Semaphore

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_auxiliary import FileChunkDataFrame
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_abc import (CkanCallbackLevel, CkanProgressBarType,
                                                                      CkanProgressCallbackABC)


def default_progress_callback(position:int, total:int, info:Any=None, *, context:str=None,
                              file_index:int=None, file_count:int=None,
                              lines_chunk:int=None, total_lines_read:int=None,
                              canceled_upload: bool=False, end_message: bool=False,
                              level:CkanCallbackLevel=None, start_time: float=None,
                              last_position:int=None, last_progress_position:int=None,
                              **kwargs) -> Union[str,None]:
    if total is not None and total > 0:
        progress_pct_str = "{0:.2f}".format(position/total*100.0)
    else:
        progress_pct_str = None
    msg = None
    if level == CkanCallbackLevel.Packages:
        msg = f"Package {position}/{total}"
    elif level == CkanCallbackLevel.Resources:
        if end_message:
            msg = f"Done all {total} resources"
        else:
            msg = f"Resource {position}/{total}"
    elif level == CkanCallbackLevel.MultiFileResource:
        msg = f"Multi-file resource / file {position}/{total}"
    elif level == CkanCallbackLevel.ResourceChunks:
        if context is None:
            context = ""
        if position == total and end_message:
            # info is None
            msg = f"{context} Finished {file_index}/{file_count} (100%) - {total_lines_read} lines read"
        elif canceled_upload:
            msg = f"{context} Canceled {file_index}/{file_count} ({progress_pct_str}%) - {total_lines_read} lines read"
        elif info is None:
            msg = f"{context} Request {file_index}/{file_count} ({progress_pct_str}%) - {total_lines_read} lines read"
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
            msg = f"{context} Request {file_index}/{file_count} ({progress_pct_str}%) - {total_lines_read} lines read: " + info_str
    elif level == CkanCallbackLevel.Requests:
        msg = f"Multi-line request {position}/{total} ({progress_pct_str}%)"
        if context is not None:
            msg = msg + f" ({context})"
    return msg


class CkanProgressCallbackSimple(CkanProgressCallbackABC):
    def __init__(self, callback_fun:Union[Callable,"CkanProgressCallbackSimple"]=None,
                 *, progress_bar_type:CkanProgressBarType=None):
        if progress_bar_type is None:
            progress_bar_type = self.default_progress_bar_type
        if callback_fun is None:
            callback_fun = default_progress_callback
        super().__init__(callback_fun, progress_bar_type=progress_bar_type)
        self.simple_semaphore = Semaphore()

    def copy(self, *, dest=None):
        if dest is None:
            dest = CkanProgressCallbackSimple()
        return super().copy(dest=dest)

    def __copy__(self):
        return self.copy()

    def task_progress(self, position:int, total:int, *, info:Any=None, context:str=None,
        file_index:int=0, file_count:int=None, lines_chunk:int=None, total_lines_read:int=None,
        canceled_request: bool=False, end_message: bool=False, level:CkanCallbackLevel=None,
        **kwargs) -> Union[str,None]:
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
        self.simple_semaphore.acquire()
        if level is not None:
            last_position = self.last_progress_position[level]
            last_file_index = self.last_progress_file_index[level]
            self.last_progress_position[level] = position
            self.last_progress_file_index[level] = file_index
            if not self.verbosity[level]:
                self.simple_semaphore.release()
                return None
        if self.progress_callback_fun is not None:
            if end_message:
                position = total
                file_index = file_count
            # division by zero error prevention:
            if total == 0: total = max(1, position)
            if file_count is not None and file_count == 0: file_count = max(file_index, 1)
            # timing of the current task
            if level is not None:
                task_start_time = self.start_time[level]
                # if task_start_time is None or position == 0 or end_message:
                #     task_start_time = time.time()
                #     self.start_time[level] = task_start_time
            else:
                task_start_time = None
            # call user-defined function
            print_msg = self.progress_callback_fun(position, total, info=info, context=context,
                                              file_index=file_index, file_count=file_count,
                                              lines_chunk=lines_chunk, total_lines_read=total_lines_read,
                                              canceled_upload=canceled_request, end_message=end_message,
                                              level=level, start_time=task_start_time,
                                              last_position=last_position, last_file_index=last_file_index,
                                              **self.progress_callback_kwargs, **kwargs)
            self.simple_semaphore.release()
            if print_msg is not None:
                print(print_msg)
            return print_msg
        else:
            self.simple_semaphore.release()
