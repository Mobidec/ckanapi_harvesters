#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements functions to convert formats between database and local files.
"""
from typing import Dict, List, Union

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField


# user custom transformation function prototypes
def upload_function_example(df_local: Union[pd.DataFrame, List[dict]], *,
                            fields:Dict[str, CkanField]=None, file_query:str=None, total_lines_read:int=None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    return df_local

def download_function_example(df_download: pd.DataFrame, *,
                              fields:Dict[str, CkanField]=None, file_query:str=None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    return df_download


# specific examples
def simple_upload_fun(df_local: Union[pd.DataFrame, List[dict]], *,
                      fields:Dict[str, CkanField]=None, file_query:str=None, total_lines_read:int=None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    for field in df_local.columns:
        if df_local[field].dtype == pd.Timestamp:
            df_local[field] = df_local[field].apply(pd.Timestamp.isoformat)  # ISO-8601 format
    return df_local

def replace_empty_str(df_local: Union[pd.DataFrame, List[dict]], *,
                      fields:Dict[str, CkanField]=None, file_query:str=None, total_lines_read:int=None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    df_local = df_local.astype(object)  # set all columns to object dtype
    df_local.replace("", None, inplace=True)
    return df_local

