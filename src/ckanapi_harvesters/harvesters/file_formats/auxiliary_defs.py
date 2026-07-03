#!python3
# -*- coding: utf-8 -*-
"""
Auxiliary definitions
"""
from enum import IntEnum

class GeoFileStoreEpsgConversion(IntEnum):
    CsvWkb = 0
    ShapefileProjection = 2
    ShapefileAsIs = 3
