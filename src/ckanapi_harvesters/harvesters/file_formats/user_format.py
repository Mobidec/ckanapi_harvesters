#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: CSV
"""
from typing import Union, Dict, Callable, Any
import io
import argparse
from warnings import warn

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField, CkanResourceInfo
from ckanapi_harvesters.auxiliary.list_records import ListRecords, GeneralDataFrame
from ckanapi_harvesters.auxiliary.ckan_errors import MissingCodeFileError, MissingIOFunctionError, UnknownCliArgumentError
from ckanapi_harvesters.auxiliary.external_code_import import PythonUserCode
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


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

    def _process_extra_args(self):
        # transmit extra CLI arguments to user-defined functions?
        # this could cause issues: some arguments could not be recognized
        # => raise an error, like in other file formats
        super()._process_extra_args()
        # if len(self.extra_args) > 0:
        #     msg = str(UnknownCliArgumentError(self.extra_args, context="Resource options"))
        #     warn(msg)

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
        self.resource_attributes_from_file = None
        output = self.df_read_fun(file_path, fields=fields, allow_chunks=self.read_by_chunks_enabled(allow_chunks=allow_chunks), params=self, **read_kwargs)
        if isinstance(output, tuple):
            self.resource_attributes_from_file = output[1]
            return output[0]
        else:
            return output

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        if self.df_read_fun is None:
            raise MissingIOFunctionError("Read function")
        read_kwargs = self._get_read_kwargs(allow_chunks=False)
        self.resource_attributes_from_file = None
        output = self.df_read_fun(buffer, fields=fields, allow_chunks=False, params=self, **read_kwargs)
        if isinstance(output, tuple):
            assert(isinstance(output[1], CkanResourceInfo))
            self.resource_attributes_from_file = output[1]
            return output[0]
        else:
            return output

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
        with io.StringIO() as buffer:
            write_kwargs = self._get_write_kwargs()
            self.df_write_fun(df, buffer, append=False, fields=fields, params=self, **write_kwargs)
            return buffer.getvalue().encode("utf8")

    def append_in_memory(self, stream: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        if self.df_write_fun is None:
            raise MissingIOFunctionError("Write function")
        with io.StringIO(stream.decode("utf8")) as string_stream:
            write_kwargs = self._get_write_kwargs()
            self.df_write_fun(df, string_stream, append=True, fields=fields, params=self, **write_kwargs)
            return string_stream.getvalue().encode("utf8")

    # misc ------------------
    def copy(self, dest=None):
        if dest is None:
            dest = UserFileFormat(self.options_string, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs,
                                  df_read_fun=self.df_read_fun, df_write_fun=self.df_write_fun)
        super().copy(dest=dest)
        return dest

