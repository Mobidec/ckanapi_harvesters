#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
from typing import List, Dict, Tuple, Union
from enum import IntEnum

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep


newline_char = '\n'


class ListChoiceMode(IntEnum):
    Any = 0
    MaxOne = 1
    MandatoryOne = 2
    MandatoryMulti = 3
    NoExtra = 4

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "any":
            return ListChoiceMode.Any
        elif s == "maxone":
            return ListChoiceMode.MaxOne
        elif s == "mandatoryone":
            return ListChoiceMode.MandatoryOne
        elif s == "mandatorymulti":
            return ListChoiceMode.MandatoryMulti
        elif s == "noextra":
            return ListChoiceMode.NoExtra
        else:
            raise ValueError(s)


class StringMatchMode(IntEnum):
    Any = 0
    NotEmpty = 1
    Match = 2
    MatchCaseSensitive = 3
    Regex = 4
    RegexCaseSensitive = 5
    Wildcard = 6
    WildcardCaseSensitive = 7

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "any":
            return StringMatchMode.Any
        elif s == "notempty":
            return StringMatchMode.NotEmpty
        elif s == "match":
            return StringMatchMode.Match
        elif s == "matchcasesensitive":
            return StringMatchMode.MatchCaseSensitive
        elif s == "regex":
            return StringMatchMode.Regex
        elif s == "regexcasesensitive":
            return StringMatchMode.RegexCaseSensitive
        elif s == "wildcard":
            return StringMatchMode.Wildcard
        elif s == "wildcardcasesensitive":
            return StringMatchMode.WildcardCaseSensitive
        else:
            return StringMatchMode.Any  # default value


class DataType(IntEnum):
    Text = 1
    Numeric = 2
    TimeStamp = 3
    Bool = 4

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "text":
            return DataType.Text
        elif s == "numeric":
            return DataType.Numeric
        elif s == "timestamp":
            return DataType.TimeStamp
        elif s == "bool":
            return DataType.Bool
        else:
            raise ValueError(s)


class StringValueSpecification:
    def __init__(self, value:str, help:str=None):
        if help is None:
            help = ""
        self.value: str = value
        self.help: str = help

    def to_tuple(self) -> Tuple[str,str]:
        return self.value, self.help

    @staticmethod
    def from_tuple(values: Union[str, Tuple[str,str]]) -> "StringValueSpecification":
        if isinstance(values, str):
            value, help = values, ""
        elif len(values) == 1:
            value, help = values[0], ""
        else:
            value, help = values
        return StringValueSpecification(value, help)

    def to_dict(self) -> dict:
        return {"value": self.value, "help": self.help}

    @staticmethod
    def from_dict(values: dict) -> "StringValueSpecification":
        value = values["value"]
        help = values["help"] if "help" in values.keys() else ""
        return StringValueSpecification(value, help)



