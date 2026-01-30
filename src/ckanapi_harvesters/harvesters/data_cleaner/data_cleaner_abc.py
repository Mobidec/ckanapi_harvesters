#!python3
# -*- coding: utf-8 -*-
"""
Functions to clean data before upload.
"""
from typing import Union, List, Any, Dict, Set, Type, Tuple
from abc import ABC, abstractmethod
from collections import OrderedDict

import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_auxiliary import dict_recursive_update

non_finite_authorized_types = {"numeric", "float4", "float8", "float2"}
real_number_types = non_finite_authorized_types
dtype_mapper = {
    "float64": "numeric",
    "int64": "numeric",
    "datetime64[ns]": "timestamp",
}


class CkanDataCleanerABC(ABC):
    """
    Data cleaner abstract base class.

    A table is defined by a list of fields with a data type.
    Each row can specify the value of all/some fields.
    When a value is nested (dictionary or list), the functions iterate over the values of these elements with a recursive implementation.
    These elements are called sub-values.
    """
    def __init__(self):
        # options
        self.param_enable:bool = True  # global activation flag
        self.param_replace_forbidden:bool = False  # option to replace all other forbidden values (Infs) by None
        self.param_cast_types:bool = True  # option to cast to strings fields which have text data type
        self.param_apply_field_subs:bool = True  # option to apply suggested field renamings (True by default because these are suggested only when necessary)
        self.param_apply_field_changes:bool = False  # option to apply suggested field type changes
        self.param_raise_error:bool = False  # recommended: do not raise an error: the CKAN server will
        self.param_create_new_fields:bool = True  # option to enable the requests to create missing fields in the CKAN DataStore (this requires the specific function to be called)
        self.param_verbose:bool = True
        self.param_field_subs:Dict[str,str] = {}  # user-imposed field name substitutions
        self.param_field_primary_key:Union[List[str],None] = None
        # outputs
        self.fields_encountered:OrderedDict[str,None] = OrderedDict()
        self.warnings:Dict[str,Set[str]] = {}
        self.fields_new:OrderedDict[str,CkanField] = OrderedDict()
        self.field_changes:Dict[str,CkanField] = {}
        self.field_subs:Dict[str, str] = {}
        self.field_subs_path:Dict[str, str] = {}
        self.field_suggested_primary_key:Union[List[str],None] = None
        self.field_suggested_index:Set[str] = set()
        self._new_columns_in_row: Dict[str,Any] = None  # is initialized at each row

    def clear_outputs_new_dataframe(self):
        self.fields_encountered = OrderedDict()
        self.warnings = {}
        self.fields_new = OrderedDict()
        self.field_changes = {}
        self.field_subs = {}
        self.field_subs_path = {}
        self.field_suggested_primary_key = self.param_field_primary_key
        self.field_suggested_index = set()
        self._new_columns_in_row = None

    def clear_all_outputs(self):
        """
        Some values must not be cleared for each DataFrame upload.
        The cleaner is stateful for certain values cleared only here.
        """
        self.clear_outputs_new_dataframe()

    @abstractmethod
    def copy(self, dest=None):
        dest.param_enable = self.param_enable
        dest.param_replace_forbidden = self.param_replace_forbidden
        dest.param_apply_field_subs = self.param_apply_field_subs
        dest.param_apply_field_changes = self.param_apply_field_changes
        dest.param_raise_error = self.param_raise_error
        dest.param_create_new_fields = self.param_create_new_fields
        dest.param_verbose = self.param_verbose
        dest.clear_outputs_new_dataframe()
        return dest

    def __copy__(self):
        return self.copy()

    ## Field type detection ------------------
    def _detect_standard_field_bypass(self, field_name: str, values: Union[Any, pd.Series]) -> Union[CkanField,None]:
        """
        Auxiliary function of create_new_field to detect field type used to bypass the default criteria.
        """
        return None

    def _detect_non_standard_field(self, field_name: str, values: Union[Any, pd.Series]) -> CkanField:
        """
        Auxiliary function of create_new_field to detect field type used if the default criteria did not match any specific case.
        """
        return CkanField(field_name, "text")

    @abstractmethod
    def create_new_field(self, field_name:str, values: Union[Any, pd.Series]) -> CkanField:
        """
        This method adds a new field definition
        """
        raise NotImplementedError()

    @abstractmethod
    def detect_field_types_and_subs(self, records: Union[List[dict], pd.DataFrame]) -> OrderedDict[str, str]:
        """
        This function detects the initial fields and necessary field renamings
        """
        raise NotImplementedError()

    ## Records cleansing -------------
    @abstractmethod
    def clean_value_field(self, value: Any, field: CkanField) -> Any:
        """
        Cleaning of a value. A value is directly the value of a cell.
        """
        raise NotImplementedError()

    def _replace_standard_value_bypass(self, value: Any, field: CkanField, *, field_data_type: str) -> Tuple[Any, bool]:
        """
        Auxiliary function of clean_value_field to perform type castings/checks used to bypass the default criteria.
        """
        return None, False

    def _replace_non_standard_value(self, value: Any, field: CkanField, *, field_data_type: str) -> Any:
        """
        Auxiliary function of clean_value_field to perform type castings/checks used if none of the default criteria were met.
        """
        return value

    @abstractmethod
    def _clean_subvalue(self, subvalue: Any, field: CkanField, path: str, level: int,
                                   *, field_data_type: str) -> Any:
        """
        Cleaning of a subvalue. A subvalue is a value within a nested cell.
        """
        raise NotImplementedError()

    def _replace_standard_subvalue_bypass(self, subvalue:Any, field:CkanField, path:str, level:int,
                                          *, field_data_type:str) -> Tuple[Any,bool]:
        """
        Auxiliary function of _clean_subvalue to perform type castings/checks used to bypass the default criteria.
        """
        return None, False

    def _replace_non_standard_subvalue(self, subvalue:Any, field:CkanField, path:str, level:int,
                                       *, field_data_type:str) -> Any:
        """
        Auxiliary function of _clean_subvalue to perform type castings/checks used if none of the default criteria were met.
        """
        return subvalue

    def _add_field_from_path(self, path:str, data_type:str, new_field_name:str=None,
                             suggest_index:bool=True, notes:str=None) -> None:
        """
        Auxiliary method to define a new column from a nested object.
        """
        if new_field_name is None:
            new_field_name = path.replace(".", "_")
        assert_or_raise(new_field_name not in self.fields_encountered, KeyError(f"{new_field_name} already exists and cannot be replaced"))
        self.fields_new[new_field_name] = CkanField(new_field_name, data_type, notes=notes)
        self.field_subs_path[path] = new_field_name
        if suggest_index:
            self.field_suggested_index.add(new_field_name)
        self.fields_encountered[new_field_name] = None

    @abstractmethod
    def clean_records(self, records: Union[List[dict], pd.DataFrame], known_fields:Union[OrderedDict[str, CkanField], None],
                      *, inplace:bool=False) -> Union[List[dict], pd.DataFrame]:
        """
        Main function to clean a list of records.

        :param records:
        :param known_fields:
        :param inplace:
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def _clean_final_steps(self, records: Union[List[dict], pd.DataFrame], fields:Union[OrderedDict[str, CkanField], None],
                           known_fields:Union[OrderedDict[str, CkanField], None]) -> Union[List[dict], pd.DataFrame]:
        """
        Method called at the end of clean_records
        """
        raise NotImplementedError()

    def _extra_checks(self, records: Union[List[dict], pd.DataFrame], fields:Union[OrderedDict[str, CkanField], None]) -> None:
        """
        Method called at the end of _clean_final_steps
        """
        pass

    ### post-treatments -------------
    def apply_new_fields_request(self, ckan, resource_id:str):
        """
        This method performs the field patch if a new field was detected.
        Call before upsert.
        """
        if self.param_create_new_fields and len(self.fields_new) > 0:
            ckan.datastore_field_patch(resource_id, fields_update=self.fields_new)

    def merge_field_changes(self, fields:List[dict]=None) -> List[dict]:
        """
        This method merges the fields argument of a datastore_create with the fields detected by the data cleaner.
        Fields already defined in the fields argument are not overwritten.
        """
        if fields is not None:
            fields_dict = OrderedDict([(field_dict["id"], CkanField.from_ckan_dict(field_dict))  for field_dict in fields])
        else:
            fields_dict = OrderedDict()
        if len(self.fields_new) > 0:
            for field_name, field_info in self.fields_new.items():
                if field_name not in fields_dict.keys():
                    fields_dict[field_name] = field_info
                else:
                    # was not new?  => merge changes?
                    fields_dict[field_name] = fields_dict[field_name].merge(field_info)
                    raise RuntimeError()
            # fields_dict = dict_recursive_update(fields_dict, {field_info.name: field_info.to_ckan_dict() for field_info in self.fields_new.values()})
        if self.param_apply_field_changes:
            if len(self.field_changes) > 0:
                for field_name, field_info in self.field_changes.items():
                    if field_name not in fields_dict.keys():
                        # new?  => create?
                        fields_dict[field_name] = field_info
                        raise RuntimeError()
                    else:
                        fields_dict[field_name] = fields_dict[field_name].merge(field_info)
                # fields_dict = dict_recursive_update(fields_dict, {field_info.name: field_info.to_ckan_dict() for field_info in self.field_changes.values()})
            return [field_info.to_ckan_dict() for field_info in fields_dict.values()]
        else:
            return fields

