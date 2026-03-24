#!python3
# -*- coding: utf-8 -*-
"""
Progress callback function interface
"""
from typing import Any, Union, Callable, Dict
from abc import ABC, abstractmethod
from enum import IntEnum
import copy
import time
from warnings import warn


class CkanCallbackLevel(IntEnum):
    Packages = 0
    Resources = 1
    ResourceChunks = 2
    MultiFileResource = 3
    Requests = 4


class CkanProgressBarType(IntEnum):
    NoBar = 0
    TqdmAuto = 1
    TqdmConsole = 2
    TqdmJupyter = 3


class CkanProgressCallbackABC(ABC):
    default_progress_bar_type = CkanProgressBarType.NoBar

    def __init__(self, callback_fun:Union[Callable,"CkanProgressCallbackABC"]=None,
                 *, progress_bar_type:CkanProgressBarType = None):
        if progress_bar_type is None:
            progress_bar_type = self.default_progress_bar_type
        self.progress_callback_fun: Union[Callable[[int, int, Any], None], None] = None
        self.progress_callback_kwargs: dict = {}
        self.verbosity:dict[CkanCallbackLevel,bool] = {level: True for level in CkanCallbackLevel}
        # self.verbosity[CkanCallbackLevel.Request] = False
        self.progress_bar_enables:dict[CkanCallbackLevel,bool] = {level: False for level in CkanCallbackLevel}
        self._progress_bar_type: CkanProgressBarType = CkanProgressBarType.NoBar
        self.progress_bar_type: CkanProgressBarType = progress_bar_type
        # state variables
        self.creation_time = time.time()
        self.start_time:Dict[CkanCallbackLevel,Union[float,None]] = {level: None for level in CkanCallbackLevel}
        self.progress_bars: Dict[CkanCallbackLevel, Any] = {level: None for level in CkanCallbackLevel}
        self.last_progress_position:dict[CkanCallbackLevel,int] = {level: 0 for level in CkanCallbackLevel}
        self.last_progress_file_index:dict[CkanCallbackLevel,int] = {level: 0 for level in CkanCallbackLevel}
        if isinstance(callback_fun, CkanProgressCallbackABC):
            callback_fun.copy(dest=self)
        else:
            self.progress_callback_fun = callback_fun

    @property
    def progress_bar_type(self) -> CkanProgressBarType:
        return self._progress_bar_type
    @progress_bar_type.setter
    def progress_bar_type(self, value: CkanProgressBarType):
        if not value == CkanProgressBarType.NoBar:
            msg = f"Cannot use progress bar type {value.name} for this implementation"
            warn(msg)
        self._progress_bar_type = value

    @abstractmethod
    def copy(self, *, dest=None):
        dest.progress_callback_fun = self.progress_callback_fun
        dest.progress_callback_kwargs = copy.deepcopy(self.progress_callback_kwargs)
        dest.verbosity = self.verbosity.copy()
        dest.progress_bar_enables = self.progress_bar_enables.copy()
        dest._progress_bar_type = self._progress_bar_type
        # do not copy state variables: creation_time and start_time
        return dest

    def __copy__(self):
        return self.copy()

    def start_task(self, total:int, *, file_count:int=None, position:int=0, file_index:int=0, level:CkanCallbackLevel=None,
                 info:Any=None, context:str=None, lines_chunk:int=None, total_lines_read:int=0, **kwargs) -> None:
        if level is not None:
            self.start_time[level] = time.time()
            self.last_progress_position[level] = 0
            self.last_progress_file_index[level] = 0
        self.task_progress(position=position, total=total, file_index=file_index, file_count=file_count, level=level,
            info=info, context=context, lines_chunk=lines_chunk, total_lines_read=total_lines_read,
            canceled_request=False, end_message=False, **kwargs)

    def end_task(self, total:int, *, file_count:int=None, position:int=None, file_index:int=None, level:CkanCallbackLevel=None,
                 info:Any=None, context:str=None, lines_chunk:int=None, total_lines_read:int=None, **kwargs) -> None:
        if position is None:
            position = total
        if file_index is None:
            file_index = file_count
        self.task_progress(position=position, total=total, file_index=file_index, file_count=file_count, level=level,
            info=info, context=context, lines_chunk=lines_chunk, total_lines_read=total_lines_read,
            canceled_request=False, end_message=True, **kwargs)

    @abstractmethod
    def task_progress(self, position:int, total:int, *, info:Any=None, context:str=None,
        file_index:int=0, file_count:int=None, lines_chunk:int=None, total_lines_read:int=None,
        canceled_request: bool=False, end_message: bool=False, level:CkanCallbackLevel=None,
        **kwargs) -> Union[str,None]:
        raise NotImplementedError()
