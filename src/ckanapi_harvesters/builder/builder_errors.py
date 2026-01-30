#!python3
# -*- coding: utf-8 -*-
"""
Data model to represent a CKAN database architecture
"""
from typing import Iterable
from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel

## Exceptions ------------------
class EmptyPackageNameException(RuntimeError):
    def __init__(self):
        super().__init__("Run-time error: the attribute package_name cannot be empty")

class MissingDataStoreInfoError(Exception):
    def __init__(self):
        super().__init__("DataStore info must be requested to initiate resource builder. Use option datastore_info=True for the map_resources function.")

class RequiredDataFrameFieldsError(Exception):
    def __init__(self, missing_fields:Iterable[str]):
        super().__init__("The following fields are required but absent from the sample DataFrame: {}".format(", ".join(missing_fields)))

class UnsupportedBuilderVersionError(Exception):
    def __init__(self, file_version):
        super().__init__(f"Version error: package builder version {file_version} is not supported")

class MissingCodeFileError(Exception):
    def __init__(self):
        super().__init__("Function names were provided but Auxiliary functions file was not specified")

class ResourceFileNotExistMessage(ContextErrorLevelMessage):
    def __init__(self, resource_name:str, error_level:ErrorLevel, specific_message: str):
        super().__init__(f"Resource {resource_name}", error_level, specific_message)

class IncompletePatchError(Exception):
    pass

