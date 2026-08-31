#!python3
# -*- coding: utf-8 -*-
"""
CKAN error types
"""
from typing import Iterable, List
import shlex

import requests

# import to make these error codes available from here:
from ckanapi_harvesters.auxiliary.ckan_action import (CkanActionError, CkanAuthorizationError, CkanActionNotFoundError,
                                                      CkanNotFoundError, CkanSqlCapabilityError)
from ckanapi_harvesters.auxiliary.path import BaseDirUndefError


class MultipleErrors(BaseException):
    def __init__(self, errors: List[Exception]):
        self.errors = errors
        super().__init__(f"Multiple errors occurred: \n- " + "\n- ".join([str(e) for e in errors]))

## Specific error types ------------------
class ApiKeyFileError(BaseException):
    pass

class LocalApiKeyError(BaseException):
    def __init__(self):
        super().__init__("LocalCkanApi action call does not support use of apikey parameter, use context['user'] instead")

class HostContraintError(BaseException):
    def __init__(self, target_host_url:str, url:str):
        super().__init__(f"URL {url} does not match constraint from file: {target_host_url}")

class LoginFileError(BaseException):
    pass

class InvalidParameterError(BaseException):
    pass

class FileOrDirNotExistError(BaseException):
    def __init__(self, path: str):
        super().__init__(f"Path doesn't lead to a file or directory: {path}")

class CkanMandatoryArgumentError(BaseException):
    def __init__(self, action_name: str, attribute_name: str):
        super().__init__(f"Argument '{attribute_name}' is required for {action_name}")

class MandatoryAttributeError(BaseException):
    def __init__(self, object_type: str, attribute_name: str):
        super().__init__(f"Attribute '{attribute_name}' is required for {object_type} to initiate builder")

class UnknownCliArgumentError(BaseException):
    def __init__(self, extra_args: List[str], context: str):
        super().__init__(f"CLI arguments were not parsed for {context} because unexpected: {shlex.join(extra_args)}")

class MissingIdError(BaseException):
    def __init__(self, object_type: str, object_name):
        super().__init__(f"Attribute 'id' is required for {object_type} '{object_name}' to update CKAN map")

class CkanServerError(BaseException):
    def __init__(self, ckan, response: requests.Response, msg:str, display_request:bool=True):
        super().__init__(msg)
        self.response = response
        self.status_code = response.status_code
        if display_request:
            ckan._error_print_debug_response(response)

    def __str__(self):
        return f"Server code [{self.status_code}]: " + super().__str__()

class DataStoreNotFoundError(BaseException):
    def __init__(self, resource_id:str, error_message: str):
        super().__init__(f"DataStore not found for resource id {resource_id}. This could mean the DataStore was not initialized. Server message: {error_message}")

class MissingDataStoreInfoError(BaseException):
    def __init__(self, resource_id:str):
        super().__init__("DataStore info was not requested for resource {resource_id}. Use option datastore_info=True for the map_resources function.")

class DuplicateNameError(BaseException):
    def __init__(self, object_type:str, names:Iterable[str]):
        super().__init__(f"Duplicate names were found for {object_type}: {','.join(names)}")

class ForbiddenNameError(BaseException):
    def __init__(self, object_type:str, names:Iterable[str]):
        super().__init__(f"Forbidden name for {object_type}: {','.join(names)}")

class MultipleResultsError(BaseException):
    pass

class IntegrityError(BaseException):
    pass

class ReadOnlyError(BaseException):
    def __init__(self):
        super().__init__("Mode is set to read only. Please set the read_only flag to False.")

class AdminFeatureLockedError(BaseException):
    def __init__(self):
        super().__init__("Admin features are locked. Please set the enable_admin flag to True.")

class NotMappedObjectNameError(BaseException):
    pass

class NoPackageSizeError(BaseException):
    def __init__(self, package_name:str):
        super().__init__(f"Package size was not computed for package {package_name}.")

class UnexpectedError(RuntimeError):
    pass

class UrlError(BaseException):
    pass

class MaxRequestsCountError(BaseException):
    def __init__(self):
        super().__init__("Maximum requests count was reached.")

class IncompletePatchError(BaseException):
    pass

class MaxAttemptsError(BaseException):
    def __init__(self, accumulated_traceback:List[str]):
        super().__init__("Maximum attempts reached. Combined traceback:\n" + "\n".join(accumulated_traceback))

class CkanArgumentError(BaseException):
    def __init__(self, api_name:str, argument_name:str):
        super().__init__(f"Argument {argument_name} is not supported by API {api_name}.")

class ArgumentError(BaseException):
    pass

class SearchAllNoCountsError(ArgumentError):
    def __init__(self, api_name:str, argument_name_value:str=None):
        if argument_name_value is None:
            super().__init__(f"{api_name} must parse results to compute the number of rows returned. Argument return_df=False is incompatible with multi-request mode search_all=True")
        else:
            super().__init__(f"{api_name} must parse results to compute the number of rows returned. Arguments return_df=False and {argument_name_value} are incompatible with multi-request mode search_all=True")

class FunctionMissingArgumentError(BaseException):
    def __init__(self, function_name:str, argument_name:str):
        super().__init__(f"Argument {argument_name} is mandatory for function {function_name}.")

class NoDefaultView(BaseException):
    def __init__(self, resource_format:str):
        super().__init__(f"No default view defined for resource format {resource_format}")

class ExternalUrlLockedError(BaseException):
    def __init__(self, url:str):
        super().__init__(f"Downloading external urls is blocked by parameter download_external_urls (url {url}). Run unlock_external_url_resource_download to enable this feature.")

class NoCAVerificationError(BaseException):
    def __init__(self):
        super().__init__("The CA verification cannot be disabled. To unlock this feature, run unlock_no_ca to enable this feature. Warning: Only allow in a local environment!")

class RequestError(BaseException):
    pass

class HttpRetryCodeError(BaseException):
    def __init__(self, status_code:int, description:str=None):
        if description is None:
            description = ""
        else:
            description = f" ({description})"
        super().__init__(f"HTTP status code {status_code} received{description}. An attempt should be made to retry this request.")

class RequirementError(BaseException):
    def __init__(self, requirement:str, function:str):
        super().__init__(f"The package {requirement} is required for function {function}.")

class FileFormatRequirementError(RequirementError):
    def __init__(self, requirement:str, file_format:str):
        Exception.__init__(self,f"The package {requirement} is required to support this file format ({file_format}).")

class NameFormatError(BaseException):
    pass

# PostGIS
class UnknownTargetCRSError(BaseException):
    def __init__(self, source_crs, context:str):
        super().__init__(f"Unknown destination CRS (source={source_crs}) for {context}.")

# Custom code execution
class MissingCodeFileError(BaseException):
    def __init__(self):
        super().__init__("Function names were provided but Auxiliary functions file was not specified")

class MissingIOFunctionError(BaseException):
    def __init__(self, function_type:str):
        super().__init__(f"User custom IO function name was not provided for {function_type}")

