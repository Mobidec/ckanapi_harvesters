#!python3
# -*- coding: utf-8 -*-
"""
Alias to most complete CkanApi implementation
"""

from ckanapi_harvesters.ckan_api.ckan_api_0_base import CkanApiABC, CKAN_API_VERSION
from ckanapi_harvesters.ckan_api.ckan_api_1_map import CkanApiMap
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiManage as CkanApi  # alias
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiExtendedParams as CkanApiParams  # alias

