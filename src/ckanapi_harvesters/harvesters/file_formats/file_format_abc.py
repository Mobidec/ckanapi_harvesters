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
    @abstractmethod
    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        raise NotImplementedError()

    @abstractmethod
    def read_buffer(self, buffer: io.IOBase, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        raise NotImplementedError()

    @abstractmethod
    def write_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def write_in_memory(self, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def copy(self):
        raise NotImplementedError()

    def __copy__(self):
        return self.copy()

