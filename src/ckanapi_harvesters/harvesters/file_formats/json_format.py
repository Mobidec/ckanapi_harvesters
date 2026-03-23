#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: JSON
"""
from typing import Union, Dict
import io

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC



class JsonFileFormat(FileFormatABC):
    """
    JSON file IO using pandas.read_json.

    Reading by chunks is allowed in mode `lines=True` (reads file as one JSON object per line).
    In this case, use CLI arguments `--allow-chunks --read-kwargs lines=True`

    Recommended: use with CLI argument `--allow-chunks --read-kwargs orient=records,lines=True`

    NB: argument typ="frame" cannot be overridden for read arguments.
    """
    # NB: here, default is orient=records, lines=False for read
    #     pandas default for typ="frame" is orient=index, lines=False
    default_read_kwargs = dict(orient="records", lines=False)
    # by default, write one line per row (lines=True)
    default_write_kwargs = dict(orient="records", lines=True)

    # read -------------------
    def read_by_chunks_allowed(self) -> bool:
        return "lines" in self.read_kwargs.keys() and self.read_kwargs["lines"]

    def _get_read_kwargs(self, allow_chunks:bool=True) -> dict:
        kwargs = super()._get_read_kwargs(allow_chunks=allow_chunks)
        if self.read_by_chunks_enabled(allow_chunks=allow_chunks):
            kwargs["chunksize"] = self.chunk_size
        elif "chunksize" in kwargs.keys():
            kwargs.pop("chunksize")
        return kwargs

    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True) -> Union[pd.DataFrame, ListRecords]:
        read_kwargs = self._get_read_kwargs(allow_chunks=allow_chunks)
        return pd.read_json(file_path, typ="frame", **read_kwargs)

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        read_kwargs = self._get_read_kwargs(allow_chunks=False)
        return pd.read_json(buffer, typ="frame", **read_kwargs)

    # write ------------------
    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        write_kwargs = self._get_write_kwargs()
        df.to_json(file_path, index=False, **write_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.BytesIO()
        write_kwargs = self._get_write_kwargs()
        df.to_json(buffer, index=False, **write_kwargs)
        return buffer.getvalue()

    def append_allowed(self) -> bool:
        return ("lines" in self.write_kwargs and self.write_kwargs["lines"]
                and "orient" in self.write_kwargs and self.write_kwargs["orient"] == "records")

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str,
                    fields: Union[Dict[str, CkanField], None]) -> None:
        write_kwargs = self._get_write_kwargs()
        df.to_json(file_path, index=False, mode='a', **write_kwargs)

    def append_in_memory(self, buffer: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.StringIO()
        write_kwargs = self._get_write_kwargs()
        df.to_json(buffer, index=False, **write_kwargs)
        return buffer.getvalue().encode("utf8")

    # misc ------------------
    def copy(self, dest=None):
        if dest is None:
            dest = JsonFileFormat(self.options_string, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        super().copy(dest=dest)
        return dest



