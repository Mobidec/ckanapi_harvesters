#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: CSV
"""
from typing import Union, Dict
import io

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords
from ckanapi_harvesters.auxiliary.ckan_auxiliary import df_download_to_csv_kwargs
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


csv_file_upload_read_csv_kwargs = dict(dtype=str, keep_default_na=False)


class CsvFileFormat(FileFormatABC):
    def __init__(self, read_csv_kwargs: dict=None, to_csv_kwargs: dict=None) -> None:
        if read_csv_kwargs is None: read_csv_kwargs = csv_file_upload_read_csv_kwargs
        if to_csv_kwargs is None: to_csv_kwargs = df_download_to_csv_kwargs
        self.read_csv_kwargs:dict = read_csv_kwargs
        self.to_csv_kwargs:dict = to_csv_kwargs

    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        return pd.read_csv(file_path, **self.read_csv_kwargs)

    def read_buffer(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        return pd.read_csv(buffer, **self.read_csv_kwargs)

    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        df.to_csv(file_path, index=False, **self.to_csv_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, **self.to_csv_kwargs)
        return buffer.getvalue().encode("utf8")

    def copy(self):
        return CsvFileFormat(self.read_csv_kwargs, self.to_csv_kwargs)

