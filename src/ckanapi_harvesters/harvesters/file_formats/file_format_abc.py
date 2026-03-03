#!python3
# -*- coding: utf-8 -*-
"""
File format base class
"""
from abc import ABC, abstractmethod
from typing import Union, Dict
import io

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords


class FileFormatABC(ABC):
    # read -------------------
    @abstractmethod
    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
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
    def copy(self):
        raise NotImplementedError()

    def __copy__(self):
        return self.copy()

