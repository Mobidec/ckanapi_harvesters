#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing for lists of values such as tags
"""
from typing import List, Any, Iterable, Union, Dict, Set
from abc import ABC, abstractmethod
from warnings import warn
import re

from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError, ErrorLevel, _policy_msg
from ckanapi_harvesters.policies.data_format_policy_defs import ListChoiceMode
from ckanapi_harvesters.policies.data_format_policy_defs import StringValueSpecification
from ckanapi_harvesters.policies.data_format_policy_abc import DataPolicyElementABC


extra_group_name = "extra"


class ValueListPolicy(DataPolicyElementABC):
    _group_type_str = "group"

    def __init__(self, list_specs:List[StringValueSpecification]=None, group_name:str=None,
                 value_select:ListChoiceMode=ListChoiceMode.Any,
                 mandatory:bool=False, error_level:ErrorLevel=ErrorLevel.Information):
        super().__init__(mandatory=mandatory, error_level=error_level)
        if list_specs is None:
            list_specs = []
        self.list_specs:List[StringValueSpecification] = list_specs
        self.value_select: ListChoiceMode = value_select
        self.group_name: str = group_name

    def to_dict(self) -> dict:
        d = {}
        if self.group_name:
            d["group_name"] = self.group_name
        d.update(super().to_dict())
        d.update({"values": [spec.to_dict() for spec in self.list_specs],
                  "value_select": self.value_select.name})
        return d

    def list_specs_str(self) -> List[str]:
        return [value_spec.value for value_spec in self.list_specs]

    @staticmethod
    def from_dict(d:dict) -> "ValueListPolicy":
        obj = ValueListPolicy()
        obj._load_from_dict(d)
        return obj

    def _load_from_dict(self, d:dict):
        super()._load_from_dict(d)
        self.group_name = d["group_name"] if len(d["group_name"]) > 0 else None
        self.list_specs = [StringValueSpecification.from_dict(value)  for value in d["values"]]
        self.value_select = ListChoiceMode.from_str(d["value_select"]) if "value_select" in d.keys() else ListChoiceMode.Any

    def enforce(self, values: Union[str, List[str]], *, context:str=None,
                verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        if self.group_name is not None:
            context = context + " / " + self._group_type_str + " " + self.group_name
        success = True
        spec = [tag_spec.value for tag_spec in self.list_specs]
        if values is None:
            values = []
        elif isinstance(values, str):
            values = values.split(ckan_tags_sep)
        values = list(set(values).intersection(set(spec)))
        msg = None
        value_context = context + " / value '" + ','.join(values).join(values) + "'"
        if (self.value_select == ListChoiceMode.MaxOne and len(values) > 1):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"Too many values for value list group '{self.group_name}'. Max one value is admitted within {spec}.")
        if (self.value_select == ListChoiceMode.NoExtra and len(values) > 0):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"Too many values for value list group '{self.group_name}'. No values can be selected for this group ({spec}).")
        if (self.value_select == ListChoiceMode.MandatoryOne and not len(values) == 1):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"Exactly one value must be present for value list group '{self.group_name}' ({spec}).")
        if (self.value_select == ListChoiceMode.MandatoryMulti and not len(values) < 1):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"At least one value must be present for value list group '{self.group_name}' ({spec}).")
        if not success:
            _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
        if len(spec) > 0:
            for tag in values:
                success &= self._enforce_unit_string(tag, spec, context=context, verbose=verbose, buffer=buffer)
        return success


class ExtraValueListPolicy(ValueListPolicy):
    def __init__(self, list_specs:List[StringValueSpecification]=None,
                 value_select:ListChoiceMode=ListChoiceMode.Any,
                 mandatory:bool=False, error_level:ErrorLevel=ErrorLevel.Information):
        super().__init__(list_specs=list_specs, group_name=extra_group_name,
                         value_select=value_select, mandatory=mandatory, error_level=error_level)

    @staticmethod
    def from_ValueListPolicy(value: ValueListPolicy) -> "ExtraValueListPolicy":
        obj = ExtraValueListPolicy()
        obj.__dict__.update(value.__dict__)
        return obj

    @staticmethod
    def from_dict(d:dict) -> "ExtraValueListPolicy":
        obj = ExtraValueListPolicy()
        obj._load_from_dict(d)
        return obj

    def enforce(self, values: Union[str, List[str]], *, context:str=None,
                verbose: bool = True, buffer:List[DataPolicyError]=None, extra_spec_rm:Set[str]=None) -> bool:
        if self.group_name is not None:
            context = context + " / group " + self.group_name
        success = True
        spec = [tag_spec.value for tag_spec in self.list_specs]
        if values is None:
            values = []
        elif isinstance(values, str):
            values = values.split(ckan_tags_sep)
        values = list(set(values) - extra_spec_rm)
        msg = None
        value_context = context + " / value '" + ','.join(values).join(values) + "'"
        if (self.value_select == ListChoiceMode.MaxOne and len(values) > 1):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"Too many values for value list group '{self.group_name}'. Max one value is admitted within {spec}.")
        if (self.value_select == ListChoiceMode.NoExtra and len(values) > 0):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"Too many values for value list group '{self.group_name}'. No values can be selected for this group ({spec}). Admitted values from other groups: {extra_spec_rm}")
        if (self.value_select == ListChoiceMode.MandatoryOne and not len(values) == 1):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"Exactly one value must be present for value list group '{self.group_name}' ({spec}).")
        if (self.value_select == ListChoiceMode.MandatoryMulti and not len(values) < 1):
            success = False
            msg = DataPolicyError(value_context, self.error_level, f"At least one value must be present for value list group '{self.group_name}' ({spec}).")
        if not success:
            _policy_msg(msg, error_level=self.error_level, buffer=buffer, verbose=verbose)
        if len(spec) > 0:
            for tag in values:
                success &= self._enforce_unit_string(tag, spec, context=context, verbose=verbose, buffer=buffer)
        return success


class GroupedValueListPolicy(DataPolicyElementABC):
    def __init__(self, value_group_specs:List[ValueListPolicy]=None,
                 extra_values:ExtraValueListPolicy=None,
                 mandatory:bool=False, error_level:ErrorLevel=ErrorLevel.Information):
        super().__init__(mandatory=mandatory, error_level=error_level)
        if value_group_specs is None:
            value_group_specs = []
        self.value_group_specs:List[ValueListPolicy] = value_group_specs
        self.extra_values_spec:ExtraValueListPolicy = extra_values
        self._extract_extra_values()

    def _extract_extra_values(self):
        i_rm = []
        extra_values = self.extra_values_spec
        for i, value_group_spec in enumerate(self.value_group_specs):
            if value_group_spec.group_name.lower() == extra_group_name.lower():
                assert(extra_values is None)
                extra_values = ExtraValueListPolicy.from_ValueListPolicy(value_group_spec)
                i_rm.append(i)
        for i in reversed(i_rm):
            self.value_group_specs.pop(i)
        if extra_values is not None:
            self.extra_values_spec:ExtraValueListPolicy = extra_values

    def to_dict(self) -> dict:
        d = super().to_dict()
        if self.extra_values_spec is not None:
            self.extra_values_spec.group_name = extra_group_name
            extra_values_dict = [self.extra_values_spec.to_dict()]
        else:
            extra_values_dict = []
        d.update({"groups": [spec.to_dict() for spec in self.value_group_specs] + extra_values_dict})
        return d

    @staticmethod
    def from_dict(d:dict) -> "GroupedValueListPolicy":
        obj = GroupedValueListPolicy()
        obj._load_from_dict(d)
        return obj

    def _load_from_dict(self, d:dict, child_cls:type=None):
        super()._load_from_dict(d)
        if child_cls is None:
            child_cls = ValueListPolicy
        self.value_group_specs = [child_cls.from_dict(group_spec) for group_spec in d["groups"]]
        self.extra_values_spec = None
        self._extract_extra_values()

    def enforce(self, values: Union[str, List[str]], *, context:str=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        success = True
        extra_spec_rm = set()
        for value_group_spec in self.value_group_specs:
            if not value_group_spec.group_name == extra_group_name.lower():
                extra_spec_rm = extra_spec_rm.union({tag_spec.value for tag_spec in value_group_spec.list_specs})
        for value_group_spec in self.value_group_specs:
            success &= value_group_spec.enforce(values, context=context, verbose=verbose, buffer=buffer)
        if self.extra_values_spec is not None:
            self.extra_values_spec.group_name = extra_group_name
            success &= self.extra_values_spec.enforce(values, context=context, verbose=verbose, buffer=buffer, extra_spec_rm=extra_spec_rm)
        return success


class SingleValueListPolicy(DataPolicyElementABC):
    def __init__(self, base_list:ValueListPolicy=None, extra_values:ListChoiceMode=ListChoiceMode.Any, mandatory:bool=False):
        super().__init__(mandatory=mandatory)
        self.base_list: GroupedValueListPolicy = GroupedValueListPolicy(extra_values=ExtraValueListPolicy(value_select=extra_values))
        self.update_base_list(base_list)

    def to_dict(self) -> dict:
        return self.base_list.to_dict()

    @staticmethod
    def from_dict(d:dict) -> "SingleValueListPolicy":
        obj = SingleValueListPolicy()
        obj._load_from_dict(d)
        return obj

    def _load_from_dict(self, d:dict):
        # super()._load_from_dict(d)
        self.base_list._load_from_dict(d)

    def update_base_list(self, base_list:ValueListPolicy):
        if base_list is not None:
            base_list.group_name = "base"
            self.base_list.value_group_specs = [base_list]
        else:
            self.base_list.value_group_specs = []

    def enforce(self, values: Union[str, List[str]], *, context:str=None, verbose: bool = True, buffer:List[DataPolicyError]=None) -> bool:
        success = self.base_list.enforce(values, context=context, verbose=verbose, buffer=buffer)
        return success
