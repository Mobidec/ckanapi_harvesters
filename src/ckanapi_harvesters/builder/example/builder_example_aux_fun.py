#!python3
# -*- coding: utf-8 -*-
"""
Auxiliary functions for package upload/download example
"""
from typing import Union, List, Dict

import pandas as pd

from ckanapi_harvesters import CkanField

def users_upload(df_local: Union[pd.DataFrame, List[dict]], *,
                 fields:Dict[str, CkanField], file_name:str=None, total_lines_read:int=None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    print("<<< Upload function example called on users dataframe containing ids " + ",".join([str(id) for id in df_local["user_id"].to_list()]))
    print(f"<<< File {file_name}")
    return df_local

def users_download(df_download: pd.DataFrame, *,
                   fields:Dict[str, CkanField], file_query:str=None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    print("<<< Download function example called on users dataframe containing ids " + ",".join([str(id) for id in df_download["user_id"].to_list()]))
    print(f"<<< File query {file_query}")
    return df_download


if __name__ == '__main__':
    df_users = pd.DataFrame({"user_id": [1, 2, 3]})
    df_users = users_upload(df_users, fields={})
    print(df_users)

