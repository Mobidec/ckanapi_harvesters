#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
from typing import List, Any, Iterable, Union, Dict, Set
from abc import ABC, abstractmethod
import re
import fnmatch

from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element
from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError, ErrorLevel, _policy_msg
from ckanapi_harvesters.policies.data_format_policy_defs import StringMatchMode, ListChoiceMode, newline_char
from ckanapi_harvesters.policies.data_format_policy_defs import StringValueSpecification


class DataPolicyABC(ABC):
    def __init__(self, error_level:ErrorLevel=ErrorLevel.Information):
        self.error_level: ErrorLevel = error_level

    @abstractmethod
    def to_dict(self) -> dict:
        return {"error_level": self.error_level.name}

    @staticmethod
    @abstractmethod
    def from_dict(d:dict):
        raise NotImplementedError()

    @abstractmethod
    def _load_from_dict(self, d:dict):
        self.error_level = ErrorLevel.from_str(d["error_level"])

    @abstractmethod
    def enforce(self, values: Any, *, context:str=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        raise NotImplementedError()


class DataPolicyElementABC(DataPolicyABC, ABC):
    def __init__(self, mandatory:bool=False, error_level:ErrorLevel=ErrorLevel.Information):
        super().__init__(error_level=error_level)
        self.mandatory:bool = mandatory
        self.match_mode: StringMatchMode = StringMatchMode.Match

    @abstractmethod
    def to_dict(self) -> dict:
        d = {"mandatory": self.mandatory, "match_mode": self.match_mode.name}
        d.update(super().to_dict())
        return d

    @staticmethod
    @abstractmethod
    def from_dict(d:dict):
        raise NotImplementedError()

    @abstractmethod
    def _load_from_dict(self, d:dict) -> None:
        super()._load_from_dict(d)
        self.mandatory = d["mandatory"]
        self.match_mode = StringMatchMode.from_str(d["match_mode"])

    def _enforce_unit_string(self, values: Union[str, List[str]], spec: Union[str, Iterable[str]], *, context:str, verbose:bool,
                             buffer:List[DataPolicyError], add_buffer:bool=True) -> bool:
        if values is None or len(values) == 0:
            return not self.match_mode == StringMatchMode.NotEmpty
        if isinstance(values, str):
            values = [values]
        success = True
        for value in values:
            success_value = True
            if isinstance(spec, str):
                spec = [spec]
            if value is None:
                if self.mandatory:
                    success_value = False
            elif self.match_mode == StringMatchMode.Match:
                success_value = value.lower() in {unit_spec.lower() for unit_spec in spec}
            elif self.match_mode == StringMatchMode.MatchCaseSensitive:
                success_value = value in spec
            elif self.match_mode == StringMatchMode.Regex:
                # TODO: test
                success_value = any([re.match(unit_spec,value,flags=re.IGNORECASE) is not None for unit_spec in spec])
            elif self.match_mode == StringMatchMode.RegexCaseSensitive:
                # TODO: test
                success_value = any([re.match(unit_spec,value) is not None for unit_spec in spec])
            elif self.match_mode == StringMatchMode.Wildcard:
                success_value = any([fnmatch.fnmatch(value, unit_spec) is not None for unit_spec in spec])
            elif self.match_mode == StringMatchMode.WildcardCaseSensitive:
                success_value = any([fnmatch.fnmatchcase(value, unit_spec) is not None for unit_spec in spec])
            if add_buffer and not success_value:
                msg = DataPolicyError(context, self.error_level, f"Value does not match spec '{spec}' ({self.match_mode}): {value}")
                _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
            success &= success_value
        return success



