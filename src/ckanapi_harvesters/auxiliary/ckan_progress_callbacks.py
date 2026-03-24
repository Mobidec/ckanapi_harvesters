#!python3
# -*- coding: utf-8 -*-
"""
Progress callback function definition
"""

from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_abc import CkanCallbackLevel, CkanProgressBarType, CkanProgressCallbackABC
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_simple import CkanProgressCallbackSimple, default_progress_callback
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks_tqdm import CkanProgressCallbackTqdm, tqdm

if tqdm is None:  # ImportError
    CkanProgressCallback = CkanProgressCallbackSimple
else:
    CkanProgressCallback = CkanProgressCallbackTqdm



