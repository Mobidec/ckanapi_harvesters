#!python3
# -*- coding: utf-8 -*-
"""
Virtual chunked DataFrame
"""
from typing import Iterator

# import pandas as pd

from ckanapi_harvesters.auxiliary.list_records import GeneralDataFrame


class VirtualChunkedDataFrameBuffer:
    def __init__(self, parent: "VirtualChunkedDataFrameGenerator"):
        self.__parent_generator: "VirtualChunkedDataFrameGenerator" = parent

    def tell(self):
        return self.__parent_generator._start


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
    def __init__(self, df: GeneralDataFrame, chunk_size:int) -> None:
        self.df: GeneralDataFrame = df
        self.chunk_size: int = chunk_size
        self._start: int = 0
        self.__handles: VirtualChunkedDataFrameHandles = VirtualChunkedDataFrameHandles(self)

    # read-only property
    @property
    def handles(self) -> VirtualChunkedDataFrameHandles:
        return self.__handles

    def __next__(self) -> GeneralDataFrame:
        start = self._start
        end = start + self.chunk_size
        self._start = end  # for next iteration
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

    def __del__(self):
        self.df = None

    def restart(self):
        self._start = 0


def df_as_virtual_chunks(df: GeneralDataFrame, chunk_size:int) -> Iterator[GeneralDataFrame]:
    return VirtualChunkedDataFrameGenerator(df, chunk_size)

