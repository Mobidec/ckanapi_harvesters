#!python3
# -*- coding: utf-8 -*-
"""
File format keyword selection
"""
from typing import Union

from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC, DataCleanerNone
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload import CkanDataCleanerUpload
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_1_basic import CkanDataCleanerUploadBasic
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_2_geom import CkanDataCleanerUploadGeom
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_3_check import CkanDataCleanerUploadCheckOnly

data_cleaner_dict = {
    "all": CkanDataCleanerUpload,
    "basic": CkanDataCleanerUploadBasic,
    "geojson": CkanDataCleanerUploadGeom,
    "check": CkanDataCleanerUploadCheckOnly,
    "none": DataCleanerNone,
}

def init_data_cleaner(data_cleaner_string:Union[str,None]) -> Union[CkanDataCleanerABC,None]:
    if data_cleaner_string is None or data_cleaner_string == "":
        return None
    data_cleaner_name = data_cleaner_string.lower().strip()
    if format in data_cleaner_dict.keys():
        data_cleaner_class = data_cleaner_dict[data_cleaner_name]
        return data_cleaner_class()
    else:
        raise NotImplementedError('Data cleaner {} not implemented'.format(data_cleaner_name))


