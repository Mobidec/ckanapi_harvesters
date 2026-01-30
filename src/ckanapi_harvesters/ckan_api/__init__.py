#!python3
# -*- coding: utf-8 -*-
"""
Package with helper functions for CKAN requests using pandas DataFrames.
"""

from . import ckan_api_params
from . import ckan_api_0_base
from . import ckan_api_1_map
from . import ckan_api_2_readonly
from . import ckan_api_3_policy
from . import ckan_api_4_readwrite
from . import ckan_api_5_manage
from . import ckan_api
# from . import deprecated

# usage shortcuts
from ckanapi_harvesters.ckan_api.ckan_api import CkanApi, CkanApiParams, CkanApiABC, CKAN_API_VERSION, CkanApiMap


