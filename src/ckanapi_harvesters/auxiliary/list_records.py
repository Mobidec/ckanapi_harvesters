#!python3
# -*- coding: utf-8 -*-
"""
Give partial DataFrame behavior to a list of dictionaries
"""
import pandas as pd
import numpy as np
from typing import Union, List

class _ListRecords_index:
    def __init__(self, parent) -> None:
        self.parent = parent

    def __getitem__(self, slice):
        return self.parent[slice]

    # def __setitem__(self, slice, value):
    #     self.parent[slice] = value

class ListRecords(list):  # List[dict]
    """
    Give partial DataFrame behavior to a list of dictionaries
    """
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.columns: Union[List[str],None] = None
        if len(self) > 0:
            self.columns = list(self[0].keys())
        else:
            self.columns = []

    @property
    def iloc(self):
        return _ListRecords_index(self)

def records_to_df(records: Union[List[dict], ListRecords], df_args:dict=None, *,
                  missing_value="", none_value="None") -> pd.DataFrame:
    """
    Keep source values (lesser type inference) and replace cells with missing keys with a fixed value.
    None values are also preserved using the none_value.

    :param records: input data
    :param df_args: arguments to pass to DataFrame constructor
    :param missing_value: value to set if a column is not specified on a row.
    :param none_value: value to set if a value is None in the input data.
    :return:
    """
    if df_args is None:
        df_args = {}
    df = pd.DataFrame(records, dtype=object, **df_args)
    fieldnames = df.columns
    nrows = len(df)
    # df.fillna(np.nan, inplace=True, downcast="object")
    for (i, row), record in zip(df.iterrows(), records):
        for field in fieldnames:
            if field not in record.keys():
                df.loc[i, field] = missing_value
            elif record[field] is None:
                df.loc[i, field] = none_value
    return df
