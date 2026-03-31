#!python3
# -*- coding: utf-8 -*-
"""
Progress callback function definition
"""
from typing import Any, Union, Callable
from threading import Semaphore
import time

from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_abc import CkanCallbackLevel, CkanProgressBarType, CkanProgressUnits
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_simple import CkanProgressCallbackSimple
from ckanapi_harvesters.auxiliary.ckan_errors import RequirementError

try:
    from tqdm import tqdm
    from tqdm.autonotebook import tqdm as tqdm_auto
    from tqdm.notebook import tqdm as tqdm_notebook
except ImportError:
    tqdm = None
    tqdm_auto = None
    tqdm_notebook = None


class CkanProgressCallbackTqdm(CkanProgressCallbackSimple):
    default_progress_bar_type = CkanProgressBarType.TqdmAuto
    progress_bar_update_threshold_pct = 0.5
    progress_bar_update_min_interval_s = 0.25

    def __init__(self, callback_fun:Union[Callable,"CkanProgressCallbackSimple"]=None,
                 *, progress_bar_type:CkanProgressBarType=None):
        super().__init__(callback_fun, progress_bar_type=progress_bar_type)
        progress_bar_type = self.progress_bar_type  # update
        self.progress_bar_enables[CkanCallbackLevel.Packages] = True  # activate this one
        self.progress_bar_enables[CkanCallbackLevel.ResourceChunks] = True  # activate this one
        # self.progress_bar_enables[CkanCallbackLevel.Requests] = True  # activate this one
        if False:
            if not progress_bar_type == CkanProgressBarType.NoBar:
                for level in CkanCallbackLevel:
                    if self.progress_bar_enables[level]:
                        self.verbosity[level] = False
        if tqdm is None and not progress_bar_type == CkanProgressBarType.NoBar:
            raise RequirementError('tqdm', "CkanProgressCallbackTqdm")
        # state variables
        self.last_progress_displayed:dict[CkanCallbackLevel,int] = {level: 0 for level in CkanCallbackLevel}
        self.last_progress_update_time:dict[CkanCallbackLevel,float] = {level: 0.0 for level in CkanCallbackLevel}
        self.tqdm_semaphore = Semaphore()

    @property
    def progress_bar_type(self) -> CkanProgressBarType:
        return self._progress_bar_type
    @progress_bar_type.setter
    def progress_bar_type(self, value: CkanProgressBarType):
        self._progress_bar_type = value

    def copy(self, *, dest=None):
        if dest is None:
            dest = CkanProgressCallbackTqdm()
        return super().copy(dest=dest)

    def start_task(self, total:int, *, file_count:int=None, position:int=0, file_index:int=0, level:CkanCallbackLevel=None,
                   info:Any=None, context:str=None, lines_chunk:int=None, total_lines_read:int=0,
                   units:CkanProgressUnits=None, **kwargs) -> None:
        super().start_task(total=total, file_count=file_count, level=level, position=position, file_index=file_index,
                           info=info, context=context, lines_chunk=lines_chunk, total_lines_read=total_lines_read,
                           units=units, **kwargs)
        if (level is not None and total is not None and total > 1
                and not self._progress_bar_type == CkanProgressBarType.NoBar
                and self.progress_bar_enables[level]):
            if self.progress_bars[level] is None:
                self.last_progress_displayed[level] = 0
                desc = level.name
                # extra context
                if self.extra_context[level]:
                    if context:
                        context = self.extra_context[level] + " - " + context
                    else:
                        context = self.extra_context[level]
                if context:
                    desc = desc + " (" + context + ")"
                unit_short_name = units.short_name() if units is not None else "U"
                if self._progress_bar_type == CkanProgressBarType.TqdmAuto:
                    self.progress_bars[level] = tqdm_auto(total=total, unit=unit_short_name, unit_scale=True, desc=desc)  #, position=int(level))
                elif self._progress_bar_type == CkanProgressBarType.TqdmConsole:
                    self.progress_bars[level] = tqdm(total=total, unit=unit_short_name, unit_scale=True, desc=desc)  #, position=int(level))
                elif self._progress_bar_type == CkanProgressBarType.TqdmJupyter:
                    self.progress_bars[level] = tqdm_notebook(total=total, unit=unit_short_name, unit_scale=True, desc=desc)  #, position=int(level))
                else:
                    raise NotImplementedError(self._progress_bar_type.name)
                if position > 0:
                    self.progress_bars[level].update(position)
                    self.last_progress_displayed[level] = position
                    self.last_progress_update_time[level] = time.time()

    def end_task(self, total:int, *, file_count:int=None, position:int=None, file_index:int=None, level:CkanCallbackLevel=None,
                 info:Any=None, context:str=None, lines_chunk:int=None, total_lines_read:int=None, **kwargs) -> None:
        super().end_task(total=total, position=position, file_index=file_index, file_count=file_count, level=level,
                         info=info, context=context, lines_chunk=lines_chunk, total_lines_read=total_lines_read, **kwargs)
        if level is not None:
            if self.progress_bars[level] is not None:
                if total is not None:
                    delta = max(0, total - self.progress_bars[level].last_print_n)
                    self.progress_bars[level].update(delta)
                    self.last_progress_displayed[level] += delta
                    self.last_progress_update_time[level] = time.time()
                self.progress_bars[level].close()
                self.progress_bars[level] = None

    def update_task(self, position:int, total:int, *, info:Any=None, context:str=None,
                    file_index:int=0, file_count:int=None, lines_chunk:int=None, total_lines_read:int=None,
                    canceled_request: bool=False, end_message: bool=False, level:CkanCallbackLevel=None,
                    **kwargs) -> Union[str,None]:
        self.tqdm_semaphore.acquire()
        # if level is not None:
        #     # last_position = self.last_progress_position[level]
        #     last_position = self.progress_bars[level].last_print_n
        msg = super().update_task(position=position, total=total, file_index=file_index, file_count=file_count, level=level,
                                  context=context, lines_chunk=lines_chunk, total_lines_read=total_lines_read,
                                  canceled_request=canceled_request, end_message=end_message, info=info, **kwargs)
        if level is not None and position is not None:
            if self.progress_bars[level] is not None:
                if total < 0: total = 1  # avoid division by zero
                delta = position - self.progress_bars[level].last_print_n
                if position >= total:  # or end_message:
                    delta = total - self.progress_bars[level].last_print_n
                    update_needed = True  # end of progress
                else:
                    delta_pct = delta / total * 100.
                    time_elapsed = time.time() - self.last_progress_update_time[level]
                    update_needed = (delta_pct > self.progress_bar_update_threshold_pct
                                     or time_elapsed > self.progress_bar_update_min_interval_s)
                if update_needed:
                    self.progress_bars[level].update(delta)
                    self.last_progress_displayed[level] += delta
                    self.last_progress_update_time[level] = time.time()
        self.tqdm_semaphore.release()
        return msg

    def release_resources(self) -> None:
        for progress_bar in self.progress_bars.values():
            if progress_bar is not None:
                progress_bar.close()
