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

default_read_chunksize:int = 50


class CsvFileFormat(FileFormatABC):

    def __init__(self, read_csv_kwargs: dict=None, to_csv_kwargs: dict=None) -> None:
        if read_csv_kwargs is None: read_csv_kwargs = CsvFileFormat.default_csv_file_upload_read_csv_kwargs
        if to_csv_kwargs is None: to_csv_kwargs = df_download_to_csv_kwargs
        self.read_csv_kwargs:dict = read_csv_kwargs
        self.to_csv_kwargs:dict = to_csv_kwargs

    @classmethod
    def setup_chunksize(cls, value: Union[int,None]):
        """
        Setup chunk size, either for all CSV files or only this instance
        """
        global default_read_chunksize
        default_read_chunksize = value
        cls.default_csv_file_upload_read_csv_kwargs['chunksize'] = value

    # read -------------------
    default_csv_file_upload_read_csv_kwargs = dict(dtype=str, keep_default_na=False, sep=None, engine='python', chunksize=default_read_chunksize)

    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        return pd.read_csv(file_path, **self.read_csv_kwargs)

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        return pd.read_csv(buffer, **self.read_csv_kwargs, chunksize=None)

    # write ------------------
    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        df.to_csv(file_path, index=False, **self.to_csv_kwargs)

    @staticmethod
    def append_allowed() -> bool:
        return True

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str,
                    fields: Union[Dict[str, CkanField], None]) -> None:
        df.to_csv(file_path, index=False, mode='a', **self.to_csv_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, **self.to_csv_kwargs)
        return buffer.getvalue().encode("utf8")

    def append_in_memory(self, buffer: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.StringIO(buffer.decode("utf8"))
        df.to_csv(buffer, index=False, mode='a', **self.to_csv_kwargs)
        return buffer.getvalue().encode("utf8")

    # misc ------------------
    def copy(self):
        return CsvFileFormat(self.read_csv_kwargs, self.to_csv_kwargs)

