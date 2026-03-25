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



class CsvFileFormat(FileFormatABC):
    default_read_kwargs = dict(dtype=str, keep_default_na=False, sep=None, engine='python')  # read, (sep=None, engine='python') enables the detection of the column separator
    default_write_kwargs = df_download_to_csv_kwargs  # write

    # read -------------------
    def read_by_chunks_allowed(self) -> bool:
        return True

    def _get_read_kwargs(self, allow_chunks:bool=True) -> dict:
        kwargs = super()._get_read_kwargs(allow_chunks=allow_chunks)
        if self.read_by_chunks_enabled(allow_chunks=allow_chunks):
            kwargs["chunksize"] = self.chunk_size
        else:
            kwargs["chunksize"] = None
        return kwargs

    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True) -> Union[pd.DataFrame, ListRecords]:
        read_kwargs = self._get_read_kwargs(allow_chunks=allow_chunks)
        return pd.read_csv(file_path, **read_kwargs)

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        read_kwargs = self._get_read_kwargs(allow_chunks=False)
        return pd.read_csv(buffer, **read_kwargs)

    # write ------------------
    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        write_kwargs = self._get_write_kwargs()
        df.to_csv(file_path, index=False, **write_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        with io.StringIO() as stream:
            write_kwargs = self._get_write_kwargs()
            df.to_csv(stream, index=False, **write_kwargs)
            return stream.getvalue().encode("utf8")

    def append_allowed(self) -> bool:
        return True

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str,
                    fields: Union[Dict[str, CkanField], None]) -> None:
        write_kwargs = self._get_write_kwargs()
        df.to_csv(file_path, index=False, mode='a', **write_kwargs)

    def append_in_memory(self, stream: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        with io.StringIO(stream.decode("utf8")) as string_stream:
            write_kwargs = self._get_write_kwargs()
            df.to_csv(string_stream, index=False, mode='a', **write_kwargs)
            return string_stream.getvalue().encode("utf8")

    # misc ------------------
    def copy(self, dest=None):
        if dest is None:
            dest = CsvFileFormat(self.options_string, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        super().copy(dest=dest)
        return dest


if __name__ == '__main__':
    sample_instance = CsvFileFormat("--read-kwargs compression=gzip header=10")
    print("File format reader CLI-format options:")
    sample_instance.print_help_cli()

