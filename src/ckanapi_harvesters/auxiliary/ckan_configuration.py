#!python3
# -*- coding: utf-8 -*-
"""
Parameters which apply to the package
"""
from typing import Union


## Package containing CKAN data format policy
configuration_package_name = "configuration"
policy_resource = "data_format_policy.json"

default_ckan_has_postgis: bool = True
epsg_wgs84 = 4326  # WGS-84
default_ckan_target_epsg: Union[int,None] = epsg_wgs84  # default target geodesic system


allow_no_ca = False

def unlock_no_ca(value:bool=True) -> None:
    """
    This function enables you to disable the CA verification of the CKAN server.

    __Warning__:
    Only allow in a local environment!

    :return:
    """
    global allow_no_ca
    allow_no_ca = value

## Resource download from external urls
download_external_resource_urls:bool = True

def unlock_external_url_resource_download(value:bool=True) -> None:
    """
    This function enables the download of resources external from the CKAN server.

    :return:
    """
    global download_external_resource_urls
    download_external_resource_urls = value


## External code execution
# see: external_code_import

## Defining a data policy from an url
allow_policy_from_url:bool = False

