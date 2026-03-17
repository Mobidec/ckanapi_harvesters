#!python3
# -*- coding: utf-8 -*-
"""
User custom IO function examples
"""
from typing import Union, Dict, List, Generator
import io
from contextlib import contextmanager

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.harvesters.file_formats.user_format import UserFileFormat


def read_function_example_df(file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None],
                             allow_chunks:bool=True, params:UserFileFormat = None, **kwargs) \
        -> Union[pd.DataFrame, List[dict]]:
    """
    Read a file/IO buffer and return a unique DataFrame.
    This case is the simplest implementation.
    """
    return pd.DataFrame()

# use of a context manager when returning a custom DataFrame iterator in order to properly close the file if the process is interrupted (use of a with statement)
@contextmanager
def read_function_example_by_chunks(file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None],
                                    allow_chunks:bool=True, params:UserFileFormat = None, **kwargs) \
        -> Generator:
    """
    Read a file/IO buffer and return a DataFrame generator.
    This function implements a context manager that ensures the file is closed properly when it is released.
    The DataFrame generator must be defined in a sub-function, such as in this example.

    Implementation prototype
    --------
    file_handle = open(file_path_or_buffer, 'r')
    try:
        yield read_function_example_by_chunks_generator(file_handle)
    finally:
        file_handle.close()
    """
    file_handle = open(file_path_or_buffer, 'r')
    try:
        yield read_function_example_by_chunks_generator(file_handle)
    finally:
        file_handle.close()

def read_function_example_by_chunks_generator(file_handle) -> Generator[Union[pd.DataFrame, List[dict]], None, None]:
    """
    This is the function which properly yields DataFrame chunks. It is called by read_function_example_by_chunks.
    """
    for df_chunk in pd.read_csv(file_handle, chunksize=100):
        yield df_chunk


def write_function_example(df: Union[pd.DataFrame, List[dict]], file_path_or_buffer:Union[str, io.IOBase],
                           *, fields: Union[Dict[str, CkanField],None], append:bool=False,
                           params:UserFileFormat = None, **kwargs) -> None:
    """
    This function writes a DataFrame to the given file path.
    """
    mode = 'a' if append else 'w'
    df.to_csv(file_path_or_buffer, mode=mode, index=False)
