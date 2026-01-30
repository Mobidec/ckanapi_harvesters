#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
from typing import List, Any, Iterable, Union, Dict, Set
from abc import ABC, abstractmethod
from warnings import warn
import re
import datetime

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, _bool_from_string
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.policies.data_format_policy_defs import DataType
from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError, ErrorLevel, _policy_msg
from ckanapi_harvesters.policies.data_format_policy_defs import newline_char, StringMatchMode, ListChoiceMode
from ckanapi_harvesters.policies.data_format_policy_defs import StringValueSpecification
from ckanapi_harvesters.policies.data_format_policy_abc import DataPolicyElementABC


class CustomFieldSpecification(DataPolicyElementABC):
    def __init__(self, key: str=None, values: List[str]=None, data_type:DataType=None,
                 match_mode:StringMatchMode=StringMatchMode.Any,
                 help:str=None,
                 mandatory:bool=False, error_level:ErrorLevel=ErrorLevel.Information):
        super().__init__(mandatory=mandatory, error_level=error_level)
        self.key: str = key
        self.values: List[str] = values
        self.data_type:DataType = data_type
        self.match_mode: StringMatchMode = match_mode
        self.help: str = help

    def to_dict(self) -> dict:
        d = {"key": self.key,
             "values": self.values,
             "data_type": self.data_type.name if self.data_type is not None else "",
             "match_mode": self.match_mode.name}
        d.update(super().to_dict())
        return d

    @staticmethod
    def from_dict(d:dict) -> "CustomFieldSpecification":
        obj = CustomFieldSpecification()
        obj._load_from_dict(d)
        return obj

    def _load_from_dict(self, d:dict):
        super()._load_from_dict(d)
        self.key = d["key"]
        self.values = d["values"] if "values" in d.keys() else None
        self.data_type = DataType.from_str(d["data_type"]) if "data_type" in d.keys() and not d["data_type"] == "" else None
        self.match_mode = StringMatchMode.from_str(d["match_mode"]) if "match_mode" in d.keys() else None

    @staticmethod
    def from_df_row(row: pd.Series) -> "CustomFieldSpecification":
        key = _string_from_element(row["key"]).strip()
        values_str = _string_from_element(row["values"])
        values = values_str.split(ckan_tags_sep)
        mode_str = _string_from_element(row["mode"])
        mode = StringMatchMode.from_str(mode_str) if mode_str is not None else StringMatchMode.Any
        help:Union[str,None] = None
        if "help" in row.keys():
            help = _string_from_element(row["help"])
        return CustomFieldSpecification(key=key, values=values, match_mode=mode, help=help)

    def enforce(self, values: str, *, context:str=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        key_context = context + " / custom key " + self.key
        value = values
        specs = self.values
        if specs is None:
            return not self.match_mode == StringMatchMode.NotEmpty
        if self.data_type is None or self.data_type == DataType.Text:
            success = self._enforce_unit_string(value, specs, context=key_context, verbose=verbose, buffer=buffer)
        elif self.data_type == DataType.Bool:
            self.match_mode = StringMatchMode.Match
            success = self._enforce_unit_string(value, {"True", "False"}, context=key_context, verbose=verbose, buffer=buffer)
        elif self.data_type == DataType.TimeStamp:
            if value is not None and len(value) > 0:
                try:
                    timestamp = datetime.datetime.fromisoformat(value)
                except Exception as e:
                    return False
                success = True
            else:
                success = True
        elif self.data_type == DataType.Numeric:
            self.match_mode = StringMatchMode.Regex
            success = self._enforce_unit_string(value, "/d+", context=key_context, verbose=verbose, buffer=buffer)
        else:
            raise NotImplementedError("Unsupported data type: " + str(self.data_type))
        return success


class CustomFieldsPolicy(DataPolicyElementABC):
    def __init__(self, custom_fields_spec:List[CustomFieldSpecification]=None,
                 restrict_to_list:ErrorLevel=ErrorLevel.Information, keys_case_sensitive:bool=True,
                 mandatory:bool=False, error_level:ErrorLevel=ErrorLevel.Information):
        super().__init__(mandatory=mandatory, error_level=error_level)
        if custom_fields_spec is None:
            custom_fields_spec = []
        self.restrict_to_list: ErrorLevel = restrict_to_list
        self.keys_case_sensitive:bool = keys_case_sensitive
        self.custom_fields_spec:Dict[str,CustomFieldSpecification] = {}
        if keys_case_sensitive:
            self.custom_fields_spec = {keypair_spec.key: keypair_spec for keypair_spec in custom_fields_spec}
        else:
            self.custom_fields_spec = {keypair_spec.key.lower(): keypair_spec for keypair_spec in custom_fields_spec}

    def to_dict(self) -> dict:
        d = {"custom_fields": [spec.to_dict() for spec in self.custom_fields_spec.values()],
             "keys_case_sensitive": self.keys_case_sensitive, "restrict_to_list": self.restrict_to_list.name}
        d.update(super().to_dict())
        return d

    @staticmethod
    def from_dict(d:dict) -> "CustomFieldsPolicy":
        obj = CustomFieldsPolicy()
        obj._load_from_dict(d)
        return obj

    def _load_from_dict(self, d:dict):
        super()._load_from_dict(d)
        self.custom_fields_spec = {spec["key"]: CustomFieldSpecification.from_dict(spec) for spec in d["custom_fields"]}
        self.keys_case_sensitive = _bool_from_string(d["keys_case_sensitive"]) if "keys_case_sensitive" in d.keys() else None
        self.restrict_to_list = ErrorLevel.from_str(d["restrict_to_list"]) if "restrict_to_list" in d.keys() else None

    def enforce(self, values: Dict[str, str], *, context:str=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        success = True
        if self.keys_case_sensitive:
            keys = set(values.keys())
        else:
            keys = {key.lower() for key in values.keys()}
        extra_keys = keys - set(self.custom_fields_spec.keys())
        if len(extra_keys) > 0:
            msg = DataPolicyError(context, self.restrict_to_list, f"Custom keys do not make part of the defined list: {','.join(extra_keys)}")
            _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
            success = False
        mandatory_keys = {key for key, keypair in self.custom_fields_spec.items() if keypair.mandatory}
        missing_keys = mandatory_keys - keys
        if len(missing_keys) > 0:
            msg = DataPolicyError(context, self.restrict_to_list, f"Mandatory custom keys were not found: {', '.join(missing_keys)}")
            _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
            success = False
        for key, value in values.items():
            key_context = context + " / custom key " + key
            if not self.keys_case_sensitive:
                key = key.lower()
            spec = self.custom_fields_spec[key] if key in self.custom_fields_spec.keys() else None
            if spec is not None:
                success_value = spec.enforce(value, context=context, verbose=verbose, buffer=buffer)
                success &= success_value
        return success


