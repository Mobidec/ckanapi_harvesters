#!python3
# -*- coding: utf-8 -*-
"""
File format keyword selection
"""
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC
from ckanapi_harvesters.harvesters.file_formats.csv_format import CsvFileFormat
from ckanapi_harvesters.harvesters.file_formats.shp_format import ShapeFileFormat
from ckanapi_harvesters.harvesters.file_formats.xls_format import ExcelFileFormat
from ckanapi_harvesters.harvesters.file_formats.json_format import JsonFileFormat
from ckanapi_harvesters.harvesters.file_formats.user_format import UserFileFormat

file_format_dict = {
    "csv": CsvFileFormat,
    "shp": ShapeFileFormat,
    "xls": ExcelFileFormat,
    "json": JsonFileFormat,
}

def init_file_format_datastore(format:str, options_string:str=None, aux_read_fun_name:str=None, aux_write_fun_name:str=None) -> FileFormatABC:
    if format is None or len(format) == 0:
        format = 'csv'
    format = format.lower().strip()
    if aux_read_fun_name or aux_write_fun_name:
        return UserFileFormat(options_string=options_string)  # the functions will be connected later
    if format in file_format_dict.keys():
        file_format_class = file_format_dict[format]
        return file_format_class(options_string=options_string)
    else:
        raise NotImplementedError('File format {} not implemented'.format(format))


