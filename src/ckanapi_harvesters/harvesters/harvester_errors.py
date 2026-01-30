#!python3
# -*- coding: utf-8 -*-
"""
Errors specific to harvesting data
"""

from ckanapi_harvesters.auxiliary.ckan_errors import RequirementError
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_errors import CleanError, CleanerRequirementError  # alias


class HarvestMethodRequiredError(Exception):
    def __init__(self):
        super().__init__("The harvesting method argument --harvester is required.")

class HarvesterArgumentError(Exception):
    pass

class HarvesterArgumentRequiredError(HarvesterArgumentError):
    def __init__(self, argument:str, harvest_method:str, help:str=None):
        if help is None: help = ""
        super().__init__(f"The argument {argument} is required for harvest method {harvest_method}. " + help)

class HarvesterRequirementError(RequirementError):
    def __init__(self, requirement:str, harvest_method:str):
        super().__init__(f"The package {requirement} is required for this harvester ({harvest_method}).")

class ResourceNotFoundError(Exception):
    def __init__(self, resource_type:str, table_name:str, host:str):
        super().__init__(f"{resource_type} {table_name} was not found on host ({host}).")


