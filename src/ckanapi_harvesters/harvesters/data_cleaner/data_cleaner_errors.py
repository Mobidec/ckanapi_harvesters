#!python3
# -*- coding: utf-8 -*-
"""
Error codes for data cleaner
"""

from ckanapi_harvesters.auxiliary.ckan_errors import RequirementError


class CleanError(Exception):
    pass

class CleanerRequirementError(RequirementError):
    def __init__(self, requirement:str, data_type:str):
        super().__init__(f"The package {requirement} is required to clean using this data type ({data_type}).")

class UnexpectedGeometryError(Exception):
    def __init__(self, found_type:str, expected_type:str):
        super().__init__(f"Unexpected GeoJSON type: {found_type}. Expected {expected_type}.")

class FormatError(Exception):
    def __init__(self, data:str, data_type:str):
        super().__init__(f"Format not recognized for type {data_type}: {data}.")
