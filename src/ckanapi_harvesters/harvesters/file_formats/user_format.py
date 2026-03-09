#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: CSV
"""
from typing import Union, Dict, Callable, Any
import io
import argparse

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.auxiliary.ckan_errors import MissingCodeFileError, MissingIOFunctionError
from ckanapi_harvesters.auxiliary.external_code_import import PythonUserCode
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


# user custom IO function prototypes
def read_function_example(file_path_or_buffer:Union[str, io.IOBase], fields: Union[Dict[str, CkanField],None], allow_chunks:bool=False, params:"UserFileFormat" = None, **kwargs) -> GeneralDataFrame:
    return pd.DataFrame()

def write_function_example(df: GeneralDataFrame, file_path_or_buffer:Union[str, io.IOBase], fields: Union[Dict[str, CkanField],None], append:bool=False, params:"UserFileFormat" = None, **kwargs) -> None:
    raise NotImplementedError()
    df.to_csv(file_path_or_buffer)


class UserFileFormat(FileFormatABC):
    def __init__(self, options_string: str, *, df_read_fun:Callable[[Any], GeneralDataFrame] = None,
                 df_write_fun:Callable[[GeneralDataFrame, Any], Any] = None) -> None:
        super().__init__(options_string=options_string)
        self.df_read_fun:Union[Callable[[Any], GeneralDataFrame], None] = df_read_fun
        self.df_write_fun:Union[Callable[[pd.DataFrame, Any], pd.DataFrame], None] = df_write_fun
        self.option_append_allowed: bool = False
        self.extra_args: Union[list,None] = None

    @staticmethod
    def _setup_cli_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        parser = super()._setup_cli_parser(parser)
        parser.add_argument("--allow-append",
                            help="Option to signal the append mode is available for the write function", action="store_true", default=False)

    def _apply_arguments(self, args: argparse.Namespace, extra_args: list):
        super()._apply_arguments(args, extra_args)
        self.option_append_allowed = args.allow_append
        self.extra_args = extra_args

    def _connect_aux_functions(self, module: PythonUserCode, aux_read_fun_name:str, aux_write_fun_name:str) -> None:
        if (aux_read_fun_name or aux_write_fun_name) and module is None:
            raise MissingCodeFileError()
        if aux_read_fun_name:
            self.df_read_fun = module.function_pointer(aux_read_fun_name)
        if aux_write_fun_name:
            self.df_write_fun = module.function_pointer(aux_write_fun_name)

    # read -------------------
    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=False) -> Union[pd.DataFrame, ListRecords]:
        if self.df_read_fun is None:
            raise MissingIOFunctionError("Read function")
        return self.df_read_fun(file_path, fields=fields, allow_chunks=allow_chunks and self.allow_chunks, params=self, **self.read_kwargs)

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        if self.df_read_fun is None:
            raise MissingIOFunctionError("Read function")
        return self.df_read_fun(buffer, fields=fields, allow_chunks=False, params=self, **self.read_kwargs)

    # write ------------------
    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        self.df_write_fun(df, file_path, append=False, fields=fields, params=self, **self.write_kwargs)

    def append_allowed(self) -> bool:
        return self.option_append_allowed

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str,
                    fields: Union[Dict[str, CkanField], None]) -> None:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        self.df_write_fun(df, file_path, append=True, fields=fields, params=self, **self.write_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        buffer = io.StringIO()
        self.df_write_fun(df, buffer, append=False, fields=fields, params=self, **self.write_kwargs)
        return buffer.getvalue().encode("utf8")

    def append_in_memory(self, buffer: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        buffer = io.StringIO(buffer.decode("utf8"))
        self.df_write_fun(df, buffer, append=True, fields=fields, params=self, **self.write_kwargs)
        return buffer.getvalue().encode("utf8")

    # misc ------------------
    def copy(self, dest=None):
        if dest is None:
            dest = UserFileFormat(self.options_string)
        super().copy(dest=dest)
        dest.df_read_fun = self.df_read_fun
        dest.df_write_fun = self.df_write_fun
        return dest

