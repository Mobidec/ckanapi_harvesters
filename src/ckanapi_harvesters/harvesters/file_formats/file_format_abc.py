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


class FileFormatABC(ABC):
    default_read_chunksize: int = 1000

    def __init__(self, options_string: str=None):
        self.options_string:Union[str,None] = options_string
        self.allow_chunks:bool = True
        self.chunksize:int = FileFormatABC.default_read_chunksize
        self._apply_options_string()

    @staticmethod
    def _setup_cli_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        if parser is None:
            parser = argparse.ArgumentParser(description="File format base class arguments")
        parser.add_argument("--chunksize", type=int,
                            help="Chunk size for reading files by chunks (number of records)")
        parser.add_argument("--no-chunks",
                            help="Option to disable reading files by chunks", action="store_true", default=False)
        return parser

    def _apply_arguments(self, args: argparse.Namespace, extra_args: list):
        self.allow_chunks = not args.no_chunks
        if args.chunksize is not None:
            self.chunksize = args.chunksize

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
    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True) -> Union[pd.DataFrame, ListRecords]:
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
        dest.chunksize = self.chunksize
        return dest

    def __copy__(self):
        return self.copy()

