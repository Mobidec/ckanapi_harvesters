#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: CSV
"""
from typing import Union, Dict
import io
from types import SimpleNamespace

import pandas as pd

try:
    import scipy.io
except ImportError:
    scipy = SimpleNamespace(io=None)

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords
from ckanapi_harvesters.auxiliary.ckan_auxiliary import df_download_to_csv_kwargs
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


csv_file_upload_read_csv_kwargs = dict(dtype=str, keep_default_na=False)


class UserFileFormat(FileFormatABC):
    """
    Pass directly file path to user and user has the responsability to load
    """
    def __init__(self, options_string:str) -> None:
        self.options_string = options_string

    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        data = scipy.io.loadmat(file_path)
        df = pd.DataFrame.from_dict(data, orient='index')
        return df

    def read_buffer(self, buffer: io.BytesIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        data = scipy.io.loadmat(buffer)
        df = pd.DataFrame.from_dict(data, orient='index')
        return df

    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        data = {"data": df.to_numpy()}
        scipy.io.savemat(file_path, data)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.BytesIO()
        data = {"data": df.to_numpy()}
        scipy.io.savemat(buffer, data)
        return buffer.getvalue()

    def copy(self):
        return MatFileFormat(self.options_string)

