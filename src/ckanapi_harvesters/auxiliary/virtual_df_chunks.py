#!python3
# -*- coding: utf-8 -*-
"""
Virtual chunked DataFrame
"""
from typing import Iterator
# from contextlib import contextmanager

# import numpy as np
import pandas as pd


# def df_as_virtual_chunks_generator(df: pd.DataFrame, chunk_size:int) -> Generator[pd.DataFrame, None, None]:
#     """
#     Iterate over chunks of an existing DataFrame.
#     The file was read entirely but this method allows to apply the same treatments as other "chunkable" files.
#     """
#     assert(chunk_size>0)
#     for start in range(0, len(df), chunk_size):
#         yield df.iloc[start:start + chunk_size]
#     # for chunk in np.array_split(df, chunk_size):
#     #     yield chunk
#
# @contextmanager
# def df_as_virtual_chunks_incomplete(df: pd.DataFrame, chunk_size:int) -> Generator[pd.DataFrame, None, None]:
#     try:
#         yield df_as_virtual_chunks_generator(df, chunk_size)
#     finally:
#         pass


class VirtualChunkedDataFrameBuffer:
    def __init__(self, parent: "VirtualChunkedDataFrameGenerator"):
        self.parent_generator: "VirtualChunkedDataFrameGenerator" = parent

    def tell(self):
        return self.parent_generator.start


class VirtualChunkedDataFrameHandle:
    def __init__(self, parent: "VirtualChunkedDataFrameGenerator"):
        self.buffer = VirtualChunkedDataFrameBuffer(parent)


class VirtualChunkedDataFrameHandles:
    def __init__(self, parent: "VirtualChunkedDataFrameGenerator"):
        self.handle = VirtualChunkedDataFrameHandle(parent)


class VirtualChunkedDataFrameGenerator(Iterator):
    """
    Emulate a DataFrame used for a file read by chunks, with __next__, __exit__ behaviors and handles reporting the position in the DataFrame
    """
    def __init__(self, df: pd.DataFrame, chunk_size:int) -> None:
        self.df: pd.DataFrame = df
        self.chunk_size: int = chunk_size
        self.start: int = 0
        self.handles: VirtualChunkedDataFrameHandles = VirtualChunkedDataFrameHandles(self)

    def __next__(self) -> pd.DataFrame:
        start = self.start
        end = start + self.chunk_size
        self.start = end  # for next iteration
        if start >= len(self.df):
            raise StopIteration
        else:
            df_chunk = self.df.iloc[start:end]
            # df_chunk.attrs["file_position"] = start
            return df_chunk

    # Context Manager behavior ----------
    # to use VirtualChunkedDataFrameGenerator in a "with" statement
    def __enter__(self) -> "VirtualChunkedDataFrameGenerator":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def df_as_virtual_chunks(df: pd.DataFrame, chunk_size:int) -> Iterator[pd.DataFrame]:
    return VirtualChunkedDataFrameGenerator(df, chunk_size)

