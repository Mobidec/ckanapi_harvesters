#!python3
# -*- coding: utf-8 -*-
"""
CKAN error types
"""
from typing import Iterable
import requests

# import to make these error codes available from here:
from ckanapi_harvesters.auxiliary.ckan_action import (CkanActionError, CkanAuthorizationError, CkanNotFoundError,
                                                      CkanSqlCapabilityError)
from ckanapi_harvesters.auxiliary.path import BaseDirUndefError


## Specific error types ------------------
class ApiKeyFileError(Exception):
    pass

class InvalidParameterError(Exception):
    pass

class FileOrDirNotExistError(Exception):
    def __init__(self, path: str):
        super().__init__(f"Path doesn't lead to a file or directory: {path}")

class CkanMandatoryArgumentError(Exception):
    def __init__(self, action_name: str, attribute_name: str):
        super().__init__(f"Argument '{attribute_name}' is required for {action_name}")

class MandatoryAttributeError(Exception):
    def __init__(self, object_type: str, attribute_name: str):
        super().__init__(f"Attribute '{attribute_name}' is required for {object_type} to initiate builder")

class MissingIdError(Exception):
    def __init__(self, object_type: str, object_name):
        super().__init__(f"Attribute 'id' is required for {object_type} '{object_name}' to update CKAN map")

class CkanServerError(Exception):
    def __init__(self, ckan, response: requests.Response, msg:str, display_request:bool=True):
        super().__init__(msg)
        self.response = response
        self.status_code = response.status_code
        if display_request:
            ckan._error_print_debug_response(response)

    def __str__(self):
        return f"Server code [{self.status_code}]: " + super().__str__()

class DataStoreNotFoundError(Exception):
    def __init__(self, resource_id:str, error_message: str):
        super().__init__(f"DataStore not found for resource id {resource_id}. This could mean the DataStore was not initialized. Server message: {error_message}")

class DuplicateNameError(Exception):
    def __init__(self, object_type:str, names:Iterable[str]):
        super().__init__(f"Duplicate names were found for {object_type}: {','.join(names)}")

class ForbiddenNameError(Exception):
    def __init__(self, object_type:str, names:Iterable[str]):
        super().__init__(f"Forbidden name for {object_type}: {','.join(names)}")

class IntegrityError(Exception):
    pass

class ReadOnlyError(Exception):
    def __init__(self):
        super().__init__("Mode is set to read only. Please set the read_only flag to False.")

class AdminFeatureLockedError(Exception):
    def __init__(self):
        super().__init__("Admin features are locked. Please set the enable_admin flag to True.")

class NotMappedObjectNameError(Exception):
    pass

class UnexpectedError(RuntimeError):
    pass

class UrlError(Exception):
    pass

class MaxRequestsCountError(Exception):
    def __init__(self):
        super().__init__("Maximum requests count was reached.")

class CkanArgumentError(Exception):
    def __init__(self, api_name:str, argument_name:str):
        super().__init__(f"Argument {argument_name} is not supported by API {api_name}.")

class ArgumentError(Exception):
    pass

class SearchAllNoCountsError(ArgumentError):
    def __init__(self, api_name:str, argument_name_value:str=None):
        if argument_name_value is None:
            super().__init__(f"{api_name} must parse results to compute the number of rows returned. Argument return_df=False is incompatible with multi-request mode search_all=True")
        else:
            super().__init__(f"{api_name} must parse results to compute the number of rows returned. Arguments return_df=False and {argument_name_value} are incompatible with multi-request mode search_all=True")

class FunctionMissingArgumentError(Exception):
    def __init__(self, function_name:str, argument_name:str):
        super().__init__(f"Argument {argument_name} is mandatory for function {function_name}.")

class NoDefaultView(Exception):
    def __init__(self, resource_format:str):
        super().__init__(f"No default view defined for resource format {resource_format}")

class ExternalUrlLockedError(Exception):
    def __init__(self, url:str):
        super().__init__(f"Downloading external urls is blocked by parameter download_external_urls (url {url}). Run unlock_external_url_resource_download to enable this feature.")

class NoCAVerificationError(Exception):
    def __init__(self):
        super().__init__("The CA verification cannot be disabled. To unlock this feature, run unlock_no_ca to enable this feature. Warning: Only allow in a local environment!")

class RequestError(Exception):
    pass

class RequirementError(Exception):
    pass

class FileFormatRequirementError(RequirementError):
    def __init__(self, requirement:str, file_format:str):
        super().__init__(f"The package {requirement} is required to support this file format ({file_format}).")

# PostGIS
class UnknownTargetCRSError(RequirementError):
    def __init__(self, source_crs, context:str):
        super().__init__(f"Unknown destination CRS (source={source_crs}) for {context}.")

