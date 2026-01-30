#!python3
# -*- coding: utf-8 -*-
"""
Auxiliary functions
"""
from typing import List, Union
import os

def positive_end_index(end_index:Union[int,None], total:int) -> int:
    """
    Return stop index for a loop, following pythonic definition for slices (last index treated = end_index-1).
    If end_index is negative, the index is taken from the end of the slice. end_index = -1 means end just before the last element.
    """
    if end_index is None:
        return total
    elif end_index < 0:
        return max(0, total + end_index)
    else:
        return end_index

