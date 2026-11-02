#!python3
# -*- coding: utf-8 -*-
"""
Enumerations for data cleaner
"""

from enum import IntEnum

class ReplaceNALevel(IntEnum):
    DisableChecks = 0
    MessageOnly = 1
    ReplaceNonFloatColumns = 2
    ReplaceAllColumns = 3


