#!python3
# -*- coding: utf-8 -*-
"""
Action response common treatments
"""
from typing import Union
import json

import requests


class CkanActionResponse:
    """
    Class which decodes and checks the response of a CKAN request
    """
    def __init__(self, response: requests.Response, dry_run: bool=False):
        self.response: requests.Response = response  # for debug purposes
        self.response_dict:Union[dict,None] = None
        self.status_code:int = response.status_code
        self.success:bool = False
        self.success_json_loads:bool = False
        self.result:Union[dict,None] = None
        self.error_message: Union[None,str,dict] = None
        self.len:Union[int,None] = None
        self.dry_run:bool = dry_run

        if response.content is None and response.request is None:
            # dry run
            assert(dry_run)
            self.success = True
            self.success_json_loads = False
            self.status_code = 1
            self.error_message = "Request not sent: dry run mode"
            self.len = 0
        else:
            try:
                response_dict = json.loads(response.content.decode())
                self.response_dict = response_dict
                self.success_json_loads = True
                if (response.status_code == 200 and "success" in response_dict.keys() and "result" in response_dict.keys()
                        and response_dict["success"]):
                    self.success = True
                    self.result = response_dict["result"]
                else:
                    if "error" in response_dict.keys():
                        self.error_message = response_dict["error"]
                    else:
                        self.error_message = response.content.decode()
            except Exception as json_error:
                self.error_message = f"JSON decode error {json_error} & CKAN error {response.content.decode()}"

    def __len__(self):
        if self.len is None:
            raise RuntimeError("queried len but does not have len")
        return self.len

    def default_error(self, ckan) -> "CkanActionError":
        """
        Raise specific error codes depending on response
        """
        if self.status_code == 404 and self.success_json_loads and self.error_message["__type"] == "Not Found Error":
            return CkanNotFoundError(ckan, "(Generic)", self)
        elif self.status_code == 403 and self.success_json_loads and self.error_message["__type"] == "Authorization Error":
            return CkanAuthorizationError(ckan, self)
        else:
            return CkanActionError(ckan, self)

## action error codes
class CkanActionError(Exception):
    def __init__(self, ckan, response: CkanActionResponse, display_request:bool=True):
        super().__init__(response.error_message)
        self.response = response
        self.status_code = response.status_code
        if display_request:
            ckan._error_print_debug_response(response.response)

    def __str__(self):
        return f"Server code [{self.status_code}]: " + super().__str__()

class CkanNotFoundError(CkanActionError):
    def __init__(self, ckan, object_type:str, response: CkanActionResponse, display_request:bool=True):
        response.error_message = f"{object_type} not found: {response.error_message}"
        super().__init__(ckan, response, display_request=display_request)
        self.object_type = object_type

class CkanAuthorizationError(CkanActionError):
    pass

class CkanSqlCapabilityError(CkanActionError):
    def __init__(self, ckan, response: CkanActionResponse, display_request:bool=True):
        response.error_message = f"sql capabilities are not activated on CKAN server. See documentation for option ckan.datastore.sqlsearch.enabled"
        super().__init__(ckan, response, display_request=display_request)

