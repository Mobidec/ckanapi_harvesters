#!python3
# -*- coding: utf-8 -*-
"""
Auxiliary functions
"""
from warnings import warn

import pandas as pd
import numpy as np

from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_types import ReplaceNALevel
from ckanapi_harvesters.auxiliary.ckan_field_types import field_type_synonyms

default_replace_na: ReplaceNALevel = ReplaceNALevel.ReplaceAllColumns  # by default, you cannot transmit a cell with value NaN
non_finite_authorized_types = field_type_synonyms["double precision"].union({"numeric", "float", "float4", "float2"})
non_finite_authorized_dtypes = {"float16", "float32", "float64", "longdouble", "float128"}


def df_replace_na(df: pd.DataFrame, replace_na: ReplaceNALevel, verbose: bool = True, allow_nans: bool = False) -> pd.DataFrame:
    """
    Most basic data cleaner to solve the issues with NaN values replacing empty cell values when loading a CSV with pandas.
    The options for this function are set in ckan.params and can be bypassed at the request level with allow_nans.

    :param df:
    :param replace_na: level of conversion authorized to this function.
    The option ReplaceNonFloatColumns leaves NaN values in columns of float dtypes. The dtype is based on the DataFrame dtypes and not on the field metadata.
    :param verbose: Option to display a warning message
    :param allow_nans: Option to bypass the function, at the request level
    :return:
    """
    if replace_na == ReplaceNALevel.DisableChecks or allow_nans:
        return df
    if replace_na == ReplaceNALevel.ReplaceAllColumns and not verbose:
        # no need to list the columns if no message will be shown
        # this implementation is faster
        df = df.astype(object).where(df.notna(), None)
        return df
    columns_nan_values = df.map(lambda x: isinstance(x, float) and np.isnan(x)).sum()
    # columns_nan_values = np.isnan(df.select_dtypes(include="number")).sum()  # faster but does not scan non-numeric columns
    if columns_nan_values.sum() > 0:
        columns_nan_values = columns_nan_values[columns_nan_values > 0]
        converted_cols = set()
        should_convert_cols = set()
        for col_name, na_count in columns_nan_values.items():
            is_float_column = str(df.dtypes[col_name]) in non_finite_authorized_dtypes
            if replace_na == ReplaceNALevel.ReplaceAllColumns or \
                    (replace_na == ReplaceNALevel.ReplaceNonFloatColumns and not is_float_column):
                df[col_name] = df[col_name].astype(object).where(df[col_name].notna(), None)
                converted_cols.add(col_name)
            elif not is_float_column:
                should_convert_cols.add(col_name)
        untouched_cols = set(columns_nan_values.index) - converted_cols
        if len(converted_cols) == 0:
            msg = f"DataFrame contained lines with NaN values but were not altered: {untouched_cols}"
        elif replace_na == ReplaceNALevel.MessageOnly:
            msg = f"DataFrame contained lines with NaN values: {set(columns_nan_values.index)}. The following should be replaced with None: {should_convert_cols}"
        else:
            msg = f"DataFrame contained lines with NaN values which were replaced with None: {converted_cols}"
            if len(untouched_cols) > 0:
                msg = msg + f"\nColumns were left as is: {untouched_cols}"
        if verbose:
            warn(msg)
    return df
