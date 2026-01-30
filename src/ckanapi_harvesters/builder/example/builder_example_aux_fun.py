#!python3
# -*- coding: utf-8 -*-
"""
Auxiliary functions for package upload/download example
"""

import pandas as pd

def users_upload(df_users: pd.DataFrame, file_name:str, **kwargs) -> pd.DataFrame:
    print("<<< Upload function example called on users dataframe containing ids " + ",".join([str(id) for id in df_users["user_id"].to_list()]))
    print(f"<<< File {file_name}")
    return df_users

def users_download(df_users: pd.DataFrame, file_query, **kwargs) -> pd.DataFrame:
    print("<<< Download function example called on users dataframe containing ids " + ",".join([str(id) for id in df_users["user_id"].to_list()]))
    print(f"<<< File query {file_query}")
    return df_users


if __name__ == '__main__':
    df_users = pd.DataFrame({"user_id": [1, 2, 3]})
    df_users = users_upload(df_users)
    print(df_users)

