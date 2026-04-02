#!python3
# -*- coding: utf-8 -*-
"""
Alias to most complete CkanApi implementation
"""

from ckanapi_harvesters.ckan_api.ckan_api_0_base import CkanApiABC, CKAN_API_VERSION
from ckanapi_harvesters.ckan_api.ckan_api_1_map import CkanApiMap
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiManage as CkanApi  # alias
from ckanapi_harvesters.ckan_api.ckan_api_5_manage import CkanApiExtendedParams as CkanApiParams  # alias
from ckanapi_harvesters.auxiliary.external_code_import import unlock_external_code_execution
from ckanapi_harvesters.auxiliary.ckan_configuration import (unlock_external_url_resource_download,
                                                             allow_no_server_ca, unlock_no_server_ca)


if __name__ == '__main__':
    sample_instance = CkanApi()
    print("CKAN API CLI-format options:")
    sample_instance.print_help_cli()
