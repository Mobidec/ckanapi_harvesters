#!python3
# -*- coding: utf-8 -*-
"""
File format keyword selection
"""
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC
from ckanapi_harvesters.harvesters.file_formats.csv_format import CsvFileFormat
from ckanapi_harvesters.harvesters.file_formats.shp_format import ShapeFileFormat

file_format_dict = {
    "csv": CsvFileFormat,
    "shp": ShapeFileFormat,
}

def init_file_format_datastore(format:str) -> FileFormatABC:
    if format is None or len(format) == 0:
        format = 'csv'
    format = format.lower().strip()
    if format in file_format_dict.keys():
        file_format_class = file_format_dict[format]
        return file_format_class()
    else:
        raise NotImplementedError('File format {} not implemented'.format(format))


