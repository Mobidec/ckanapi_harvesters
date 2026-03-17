#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: CSV
"""
from typing import Union, Dict, Callable, Any, List, Generator
import io
import argparse
from contextlib import contextmanager

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.auxiliary.ckan_errors import MissingCodeFileError, MissingIOFunctionError
from ckanapi_harvesters.auxiliary.external_code_import import PythonUserCode
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


# user custom IO function examples
def read_function_example(file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True, params:"UserFileFormat" = None, **kwargs) -> Union[Union[pd.DataFrame, List[dict]], Generator[Union[pd.DataFrame, List[dict]], None, None]]:
    return pd.DataFrame()

# use of a context manager when returning a custom DataFrame iterator in order to properly close the file if the process is interrupted (use of a with statement)
@contextmanager
def read_function_chunk_example(file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True, params:"UserFileFormat" = None, **kwargs) -> Generator[Union[pd.DataFrame, List[dict]], None, None]:
    file_handle = open(file_path_or_buffer, 'r')
    try:
        yield pd.DataFrame()
    finally:
        file_handle.close()

def write_function_example(df: Union[pd.DataFrame, List[dict]], file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None], append:bool=False, params:"UserFileFormat" = None, **kwargs) -> None:
    raise NotImplementedError()
    df.to_csv(file_path_or_buffer)


# class implementation
class UserFileFormat(FileFormatABC):
    def __init__(self, options_string: str, *, df_read_fun:Callable[[Any], GeneralDataFrame] = None,
                 df_write_fun:Callable[[GeneralDataFrame, Any], Any] = None,
                 read_kwargs:dict=None, write_kwargs:dict=None) -> None:
        super().__init__(options_string=options_string, read_kwargs=read_kwargs, write_kwargs=write_kwargs)
        self.df_read_fun:Union[Callable[[Any], GeneralDataFrame], None] = df_read_fun
        self.df_write_fun:Union[Callable[[pd.DataFrame, Any], pd.DataFrame], None] = df_write_fun
        self.option_append_allowed: bool = False

    @staticmethod
    def _setup_cli_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        parser = FileFormatABC._setup_cli_parser(parser)
        parser.add_argument("--allow-append",
                            help="Option to signal the append mode is available for the write function", action="store_true", default=False)
        return parser

    def _apply_arguments(self, args: argparse.Namespace, extra_args: list):
        super()._apply_arguments(args, extra_args)
        self.option_append_allowed = args.allow_append

    def _connect_aux_functions(self, module: PythonUserCode, aux_read_fun_name:str, aux_write_fun_name:str) -> None:
        if (aux_read_fun_name or aux_write_fun_name) and module is None:
            raise MissingCodeFileError()
        if aux_read_fun_name:
            self.df_read_fun = module.function_pointer(aux_read_fun_name)
        if aux_write_fun_name:
            self.df_write_fun = module.function_pointer(aux_write_fun_name)

    # read -------------------
    def read_by_chunks_allowed(self) -> bool:
        return True

    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True) -> Union[pd.DataFrame, ListRecords]:
        if self.df_read_fun is None:
            raise MissingIOFunctionError("Read function")
        read_kwargs = self._get_read_kwargs(allow_chunks=allow_chunks)
        return self.df_read_fun(file_path, fields=fields, allow_chunks=self.read_by_chunks_enabled(allow_chunks=allow_chunks), params=self, **read_kwargs)

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        if self.df_read_fun is None:
            raise MissingIOFunctionError("Read function")
        read_kwargs = self._get_read_kwargs(allow_chunks=False)
        return self.df_read_fun(buffer, fields=fields, allow_chunks=False, params=self, **read_kwargs)

    # write ------------------
    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        write_kwargs = self._get_write_kwargs()
        self.df_write_fun(df, file_path, append=False, fields=fields, params=self, **write_kwargs)

    def append_allowed(self) -> bool:
        return self.option_append_allowed

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str,
                    fields: Union[Dict[str, CkanField], None]) -> None:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        write_kwargs = self._get_write_kwargs()
        self.df_write_fun(df, file_path, append=True, fields=fields, params=self, **write_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        buffer = io.StringIO()
        write_kwargs = self._get_write_kwargs()
        self.df_write_fun(df, buffer, append=False, fields=fields, params=self, **write_kwargs)
        return buffer.getvalue().encode("utf8")

    def append_in_memory(self, buffer: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        buffer = io.StringIO(buffer.decode("utf8"))
        write_kwargs = self._get_write_kwargs()
        self.df_write_fun(df, buffer, append=True, fields=fields, params=self, **write_kwargs)
        return buffer.getvalue().encode("utf8")

    # misc ------------------
    def copy(self, dest=None):
        if dest is None:
            dest = UserFileFormat(self.options_string, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs,
                                  df_read_fun=self.df_read_fun, df_write_fun=self.df_write_fun)
        super().copy(dest=dest)
        return dest

