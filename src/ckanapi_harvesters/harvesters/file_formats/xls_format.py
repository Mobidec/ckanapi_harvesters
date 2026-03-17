#!python3
# -*- coding: utf-8 -*-
"""
The basic file format for DataStore: XLS
"""
from typing import Union, Dict
import io
import argparse

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


class ExcelFileFormat(FileFormatABC):
    """
    Excel file IO using pandas.read_excel. Supports xls, xlsx, xlsm, xlsb, odf, ods and odt formats.

    Recommended: use with CLI argument `--read-kwargs sheet_name=your_sheet` (default is 0 i.e. the first sheet)
    """
    def __init__(self, options_string: str, *, read_kwargs: dict=None, write_kwargs: dict=None) -> None:
        super().__init__(options_string=options_string, read_kwargs=read_kwargs, write_kwargs=write_kwargs)
        self.allow_chunks = False  # override: reading by chunks is not possible

    @staticmethod
    def _setup_cli_parser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
        parser = FileFormatABC._setup_cli_parser(parser)
        parser.add_argument("--sheet-name", type=str,
                            help="The name of the sheet to use (defaults to first sheet)")
        return parser

    def _apply_arguments(self, args: argparse.Namespace, extra_args: list):
        super()._apply_arguments(args, extra_args)
        if args.sheet_name is not None:
            self.read_kwargs["sheet_name"] = args.sheet_name
            self.write_kwargs["sheet_name"] = args.sheet_name

    # read -------------------
    def read_file(self, file_path: str, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=True) -> Union[pd.DataFrame, ListRecords]:
        read_kwargs = self._get_read_kwargs(allow_chunks=False)
        return pd.read_excel(file_path, **read_kwargs)

    def read_buffer_full(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        read_kwargs = self._get_read_kwargs(allow_chunks=False)
        return pd.read_excel(buffer, **read_kwargs)

    # write ------------------
    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        write_kwargs = self._get_write_kwargs()
        df.to_excel(file_path, index=False, **write_kwargs)

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        buffer = io.BytesIO()
        write_kwargs = self._get_write_kwargs()
        df.to_excel(buffer, index=False, **write_kwargs)
        return buffer.getvalue()

    def append_allowed(self) -> bool:
        return False

    def append_file(self, df: Union[pd.DataFrame, ListRecords], file_path: str,
                    fields: Union[Dict[str, CkanField], None]) -> None:
        raise NotImplementedError()

    def append_in_memory(self, buffer: bytes, df: Union[pd.DataFrame, ListRecords], fields: Union[Dict[str, CkanField],None]) -> bytes:
        raise NotImplementedError()

    # misc ------------------
    def copy(self, dest=None):
        if dest is None:
            dest = ExcelFileFormat(self.options_string)
        super().copy(dest=dest)
        return dest



