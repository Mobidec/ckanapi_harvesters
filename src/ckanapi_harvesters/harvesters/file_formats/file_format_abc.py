#!python3
# -*- coding: utf-8 -*-
"""
File format base class
"""
import argparse
import shlex
from abc import ABC, abstractmethod
from typing import Union, Dict
import io

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords
from ckanapi_harvesters.auxiliary.ckan_auxiliary import import_args_kwargs_dict


class FileFormatABC(ABC):
    default_read_chunksize: int = 1000

    def __init__(self, options_string: str=None):
        self.options_string:Union[str,None] = options_string
        self.allow_chunks:bool = False
        self.chunk_size:int = FileFormatABC.default_read_chunksize
        self.read_kwargs:dict = {}
        self.write_kwargs:dict = {}
        self._apply_options_string()

    @staticmethod
    def _setup_cli_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="File format reader arguments", add_help=False,
                                             epilog=
                                             "Examples: \n"
                                             "- Enabling reading files by chunks: --allow-chunks --chunk-size 1000 \n"
                                             "- Additional arguments for pandas.read_csv for a CSV file: --read-kwargs compression=gzip header=10")
        parser.add_argument("--chunk-size", type=int,
                            help="Chunk size for reading files by chunks (number of records). If given, this enables reading files by chunk (option --allow-chunks)")
        parser.add_argument("--allow-chunks",
                            help="Option to enable reading files by chunks, with the default chunk size or given with --chunk-size.", action="store_true", default=False)
        parser.add_argument("--read-kwargs", nargs="*",
                            help="Keyword arguments for the read function in key=value format")
        parser.add_argument("--write-kwargs", nargs="*",
                            help="Keyword arguments for the write function in key=value format")
        return parser

    def print_help_cli(self, display:bool=True) -> str:
        parser = self._setup_cli_parser()
        if display:
            parser.print_help()
        buffer = io.StringIO()
        parser.print_help(buffer)
        return buffer.getvalue()

    def _apply_arguments(self, args: argparse.Namespace, extra_args: list):
        if args.allow_chunks is not None:
            self.allow_chunks = args.allow_chunks
        if args.chunk_size is not None:
            self.chunk_size = args.chunk_size
            self.allow_chunks = True
        self.read_kwargs.update(import_args_kwargs_dict(args.read_kwargs))
        self.write_kwargs.update(import_args_kwargs_dict(args.write_kwargs))

    def _apply_options_string(self, options_string:str=None, *, parser: argparse.ArgumentParser = None):
        if options_string is None:
            options_string = self.options_string
        if options_string is None:
            options_string = ""
        parser = self._setup_cli_parser(parser)
        args, extra_args = parser.parse_known_args(shlex.split(options_string))
        self._apply_arguments(args, extra_args)

    # read -------------------
    @abstractmethod
    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=False) -> Union[pd.DataFrame, ListRecords]:
        """
        Read a file from the file system, either fully (returning DataFrame or ListRecords) or by chunks (Iterator over a number of records).
        """
        raise NotImplementedError()

    @abstractmethod
    def read_buffer_full(self, buffer: io.IOBase, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        """
        Read a file from memory, as a DataFrame or ListRecords. This function reads entirely the file.
        """
        raise NotImplementedError()

    # write -------------------
    @abstractmethod
    def write_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        """
        Write a DataFrame or ListRecords to a file.
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def append_allowed() -> bool:
        """
        This function announces that the file format is allowed to be written in append mode, and that the function append_file is implemented.
        """
        return False

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        """
        Write a DataFrame or ListRecords to a file, appending it to the end of the file.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_in_memory(self, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        """
        This function writes a DataFrame or ListRecords to a file in memory.
        """
        raise NotImplementedError()

    @abstractmethod
    def append_in_memory(self, buffer: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        """
        This function writes a DataFrame or ListRecords to a file in memory, appending its to the end of the buffer.
        """
        raise NotImplementedError()

    # misc -------------------
    @abstractmethod
    def copy(self, dest=None):
        dest.options_string = self.options_string
        dest.allow_chunks = self.allow_chunks
        dest.chunk_size = self.chunk_size
        dest.read_kwargs = self.read_kwargs
        dest.write_kwargs = self.write_kwargs
        return dest

    def __copy__(self):
        return self.copy()

