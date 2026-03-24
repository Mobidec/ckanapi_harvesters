#!python3
# -*- coding: utf-8 -*-
"""
Progress callback function definition
"""
from typing import Any

from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanCallbackLevel


# from ipywidgets import IntProgress
# from IPython.display import display
# f = IntProgress(min=0,max=100)
f = None

def jupyter_progress_callback(position:int, total:int, info:Any=None, *, context:str=None,
                              file_index:int=None, file_count:int=None,
                              lines_chunk:int=None, total_lines_read:int=None,
                              canceled_upload: bool=False, end_message: bool=False,
                              level:CkanCallbackLevel=None, start_time: float=None,
                              last_position:int=None, last_progress_position:int=None,
                              **kwargs) -> None:
    """
    Example of a progress_callback function which can be copied into a Jupyter Notebook using a progress bar:
    ```python
    from ipywidgets import IntProgress
    from IPython.display import display
    f = IntProgress(min=0,max=100)
    ```
    """
    if level == CkanCallbackLevel.ResourceChunks:
        f.value = int(position/total*100)

