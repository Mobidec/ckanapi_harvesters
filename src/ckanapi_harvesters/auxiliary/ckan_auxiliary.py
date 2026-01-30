#!python3
# -*- coding: utf-8 -*-
"""
Data model to represent a CKAN database architecture
"""
from typing import Iterable, Union, Set, Tuple, final
from enum import IntEnum
import json
import numbers
import os
import io
import shlex
import argparse
import re

import pandas as pd
import numpy as np

from ckanapi_harvesters.auxiliary.path import path_rel_to_dir, make_path_relative


ckan_package_name_re = "^[0-9a-z-_]*$"
datastore_id_col = "_id"


class CkanIdFieldTreatment(IntEnum):
    Keep = 0
    SetIndex = 1
    Remove = 2

re_geometry = r"geometry\((\w+),\s*(\d+)\)"
def parse_geometry_native_type(geometry_type:str) -> Tuple[str,int]:
    match = re.search(re_geometry, geometry_type)
    geometry_type = match.group(1)
    geo_epsg = int(match.group(2))
    return geometry_type, geo_epsg

class CkanFieldInternalAttrs:
    """
    Custom information for internal use
    """
    def __init__(self):
        self.geometry_as_source: Union[bool, None] = None
        self.geometry_type: Union[str, None] = None
        self.epsg_target:Union[int,None] = None
        self.epsg_source:Union[int,None] = None

    def __copy__(self):
        return self.copy()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def copy(self) -> "CkanFieldInternalAttrs":
        dest = CkanFieldInternalAttrs()
        # from: native type (geometries)
        dest.geometry_type = self.geometry_type
        dest.epsg_target = self.epsg_target
        # user options
        dest.epsg_source = self.epsg_source
        return dest

    def merge(self, new_values: "CkanFieldInternalAttrs") -> "CkanFieldInternalAttrs":
        dest = self.copy()
        if new_values.geometry_type is not None:
            dest.geometry_type = new_values.geometry_type
        if new_values.epsg_source is not None:
            dest.epsg_source = new_values.epsg_source
        if new_values.epsg_target is not None:
            dest.epsg_target = new_values.epsg_target
        return dest

    @staticmethod
    def _setup_cli_ckan_parser(parser:argparse.ArgumentParser=None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="CKAN internal field parameters")
        parser.add_argument("--epsg-src", type=int,
                            help="Source EPSG (geographic coordinate system) for the column, used by data_cleaner")
        return parser

    def _cli_ckan_args_apply(self, args: argparse.Namespace) -> None:
        if args.epsg_src:
            self.epsg_source = args.epsg_src

    def init_from_options_string(self, options_string:str) -> None:
        if options_string is None:
            return
        parser = self._setup_cli_ckan_parser()
        args = parser.parse_args(shlex.split(options_string))
        self._cli_ckan_args_apply(args)

    def init_from_native_type(self, native_type:str) -> None:
        if native_type is None:
            return
        if native_type.lower().strip().startswith("geometry("):
            geometry_type, geo_epsg = parse_geometry_native_type(native_type)
            self.geometry_type = geometry_type
            self.epsg_target = geo_epsg

    def update_from_ckan(self, ckan):
        if self.epsg_source is not None:
            self.epsg_target = ckan.params.ckan_default_target_epsg


## Requests ------------------
json_headers = {"Content-Type": "application/json", 'Accept': 'text/plain'}
max_len_debug_print = 5000


def json_encode_params(params:dict) -> Tuple[str, dict]:
    """
    For upload requests, with a records field, it is necessary to specify the params in the data argument
    instead of the json argument of requests.
    In the case there are NaN values, these are not supported by the requests encoder.

    ___Requirement___: add headers=json_headers !!!

    :param params:
    :return:
    """
    data_payload = json.dumps(params, separators=(',', ':'))
    return data_payload, json_headers

class RequestType(IntEnum):
    Get = 1
    Post = 2

def requests_multipart_data(json_dict:dict, files:dict) -> dict:
    """
    Generate the multipart data for a request containing json and a file.
    Used to fill the files argument of requests.post
    json_headers must not be used

    :param json_dict:
    :param files:
    :return:
    """
    json_payload = json.dumps(json_dict)
    multipart_data = {"json": (None, json_payload, "application/json")}
    assert_or_raise(isinstance(files, dict) and not "json" in files.keys(), ValueError("files"))
    multipart_data.update(files)
    return multipart_data

df_upload_to_csv_kwargs = dict()
df_download_to_csv_kwargs = dict()

def upload_prepare_requests_files_arg(*, files:dict=None, file_path:str=None, df:pd.DataFrame=None,
                                      payload:Union[bytes, io.BufferedIOBase]=None, payload_name:str=None) -> dict:
    """
    Create files argument for requests.post, by order of priority:

    :param files: files pass through argument to the requests.post function. Use to send other data formats.
    :param payload: bytes to upload as a file
    :param payload_name: name of the payload to use (associated with the payload argument) - this determines the format recognized in CKAN viewers.
    :param file_path: path of the file to transmit (binary and text files are supported here)
    :param df: pandas DataFrame to replace resource

    :return:
    """
    if files is not None:
        assert (file_path is None and df is None and payload is None)
    elif payload is not None:
        assert (file_path is None and df is None)
        if payload_name is not None:
            payload_file_name = payload_name
            files = {"upload": (payload_file_name, payload)}
        else:
            files = {"upload": payload}
    elif file_path is not None:
        # tested with text files only, use files pass-through argument for other formats
        assert (df is None)
        file_name = os.path.basename(file_path)
        payload_file_name = file_name
        # files = {file_name: (os.path.basename(file_path), open(file_path, "r"), "text/plain")}
        files = {"upload": (payload_file_name, open(file_path, "r"))}
    elif df is not None:
        payload_file_name = "file.csv"
        files = {"upload": (payload_file_name, df.to_csv(index=False, **df_upload_to_csv_kwargs), "text/plain")}
    else:
        files = None
    return files


## Path for specific objects ------------------
def ca_file_rel_to_dir(ca_file:Union[str,None], base_dir:str=None) -> Tuple[Union[bool,str,None], Union[str,None]]:
    if ca_file is not None:
        bool_keyword = ca_file.strip().lower()
        if bool_keyword == "true":
            return True, None
        elif bool_keyword == "false":
            return False, None
        else:
            return path_rel_to_dir(ca_file, base_dir), ca_file
    else:
        return None, None

def ca_arg_to_str(ca_cert:Union[bool,str,None], base_dir:str=None, source_string:str=None) -> Union[str,None]:
    if ca_cert is not None and isinstance(ca_cert, bool) and not ca_cert:
        return "False"
    elif ca_cert is not None and isinstance(ca_cert, str):
        return make_path_relative(ca_cert, base_dir, source_string=source_string)
    else:
        return None

def ssl_arguments_decompose(ca_cert:Union[bool,str,None], *, default_ssl:bool=True) -> Tuple[bool, Union[str,None]]:
    """
    Decompose requirements argument verify into boolean and path to a certificate file.

    :param ca_cert:
    :param default_ssl: option to indicate if SSL should be enabled if ca_cert is None
    :return: Tuple ssl, ssl_certfile
    """
    if ca_cert is None:
        return default_ssl, None
    elif isinstance(ca_cert, bool):
        return ca_cert, None
    elif isinstance(ca_cert, str):
        return True, ca_cert

## Auxiliary functions ------------------
def assert_or_raise(condition: bool, e: Exception) -> None:
    if not condition:
        raise e

def find_duplicates(list_str:Iterable) -> list:
    seen = set()
    uniq = []
    duplicates = []
    for x in list_str:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
        else:
            duplicates.append(x)
    return duplicates

def dict_recursive_update(d:dict,u:dict) -> dict:
    for k,v in u.items():
        if isinstance(v, dict):
            d[k] = dict_recursive_update(d.get(k, {}),v)
        else:
            d[k] = v
    return d

def _bool_from_string(string:str, default_value:Union[bool,None]=False) -> Union[bool,None]:
    if isinstance(string, bool):
        return string
    else:
        keyword = string.lower().strip()
        if keyword == "true":
            return True
        elif keyword == "false":
            return False
        else:
            return default_value

def _string_from_element(element: pd.Series, empty_value=None) -> str:
    if isinstance(element, pd.Series):
        value = element.values[0]
    else:
        value = element
    if ((value is None)
            or (isinstance(value, numbers.Number) and np.isnan(value))
            or (isinstance(value, str) and len(value) == 0)):
        return empty_value
    else:
        return value

def bytes_to_megabytes(size_bytes:int) -> float:
    return round(size_bytes / 1024 / 1024, 2)

## json
def _jsons_repl_func(match):
        return " ".join(match.group().split())
def to_jsons_indent_lists_single_line(obj, *args, reduced_size:bool=False, **kwargs) -> str:
    """
    Modified json representation of an object.
    Lists with strings / integers are displayed on one line.

    :param obj: object to encode
    :param args: args to pass to json.dumps()
    :param reduced_size: option to not indent the json output (not human-readable)
    :param kwargs: kwargs to pass to json.dumps()
    :return:
    """
    if reduced_size:
        return json.dumps(obj, *args, **kwargs)
    else:
        output = json.dumps(obj, *args, indent=4, **kwargs)
        output = re.sub(r"(?<=\[)[^\[\]\{\}]+(?=\])", _jsons_repl_func, output)
        # output = re.sub(r"(?<=\{)[^\[\]\{\}]+(?=\})", _jsons_repl_func, output)
        return output

