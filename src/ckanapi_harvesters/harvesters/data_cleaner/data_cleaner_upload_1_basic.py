#!python3
# -*- coding: utf-8 -*-
"""
Functions to clean data before upload.
"""
from typing import Union, List, Any, Type
from collections import OrderedDict
import copy
import math
import numbers
from warnings import warn
import datetime
import json
import re
import base64

import pandas as pd
try:
    import bson
except ImportError:
    bson = None

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_timestamp_sep
from ckanapi_harvesters.auxiliary.ckan_errors import IntegrityError
from ckanapi_harvesters.auxiliary.list_records import ListRecords, records_to_df
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_auxiliary import datastore_id_col
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_errors import CleanError, CleanerRequirementError
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC

non_finite_authorized_types = {"numeric", "float4", "float8", "float2"}
real_number_types = non_finite_authorized_types
# see also: ckan_api_2_readonly ckan_dtype_mapper
dtype_ckan_mapper = {
    "float64": "numeric",
    "int64": "numeric",
    "datetime64[ns]": "timestamp",
}


def _pd_series_type_detect(values: pd.Series, test_type:Type):
    """
    This function checks that the test_type matches all rows which are not NaN/None/NA in a pandas Series.
    """
    return values.map(lambda x: isinstance(x, test_type)).where(values.notna(), True).all()


class CkanDataCleanerUploadBasic(CkanDataCleanerABC):
    """
    Data cleaner for basic data types
    """
    def __init__(self):
        super().__init__()
        # options
        self.param_json_as_text:bool = False  # option to convert json fields (dicts and lists) to str
        self.param_replace_nan:bool = True  # option to replace non-authorized nan values by None
        self.param_round_values:bool = True  # option to round values when treating an integer field
        self.param_rename_fields_underscore:bool = True  # option to rename fields beginning with an underscore (in the subs step)

    def copy(self, dest=None) -> "CkanDataCleanerUploadBasic":
        if dest is None:
            dest = CkanDataCleanerUploadBasic()
        super().copy(dest=dest)
        dest.param_json_as_text = self.param_json_as_text
        dest.param_replace_nan = self.param_replace_nan
        dest.param_round_values = self.param_round_values
        dest.param_rename_fields_underscore = self.param_rename_fields_underscore
        dest.param_field_subs = self.param_field_subs.copy()
        return dest

    ## field type detection
    def create_new_field(self, field_name:str, values: Union[Any, pd.Series]) -> CkanField:
        if field_name in self.fields_new.keys():
            return self.fields_new[field_name]
        else:
            # detect type
            if isinstance(values, pd.Series):
                dtype = str(values.dtype)
                if dtype == "object":
                    field_info = self._detect_standard_field_bypass(field_name, values)
                    if field_info is not None:
                        return field_info
                    elif _pd_series_type_detect(values, str):
                        return CkanField(field_name, "text")
                    elif _pd_series_type_detect(values, bool):
                        return CkanField(field_name, "bool")
                    elif (_pd_series_type_detect(values, dict)
                          or _pd_series_type_detect(values, list)):
                        if self.param_json_as_text:
                            return CkanField(field_name, "text")
                        else:
                            return CkanField(field_name, "json")
                    elif (_pd_series_type_detect(values, datetime.datetime)
                          or _pd_series_type_detect(values, pd.Timestamp)):
                        return CkanField(field_name, "timestamp")
                    else:
                        return self._detect_non_standard_field(field_name, values)
                elif dtype in dtype_ckan_mapper.keys():
                    return CkanField(field_name, dtype_ckan_mapper[dtype])
                else:
                    return CkanField(field_name, dtype)
            else:
                return CkanField(field_name, str(type(values)))

    def _initial_field_subs(self, fields: OrderedDict[str, CkanField]) -> OrderedDict[str, CkanField]:
        # rename fields beginning with '_'
        for field_name, value in fields.items():
            if field_name not in self.field_subs.keys():
                if field_name in self.param_field_subs.keys():
                    self.field_subs[field_name] = self.param_field_subs[field_name]
                elif self.param_rename_fields_underscore and field_name.startswith("_") and not field_name == datastore_id_col:
                    index = re.search(r"[a-zA-Z]", field_name)
                    if index is not None:
                        self.field_subs[field_name] = field_name[index.start():]
                    else:
                        raise NameError(f"Field {field_name} is invalid")
        return fields

    def detect_field_types_and_subs(self, records: Union[List[dict], pd.DataFrame],
                                    known_fields:OrderedDict[str, CkanField]=None) -> OrderedDict[str, CkanField]:
        self.clear_outputs_new_dataframe()
        fields = OrderedDict()
        if known_fields is not None:
            for field_name, value in known_fields.items():
                fields[field_name] = value
        if isinstance(records, list):
            df = records_to_df(records)
        else:
            df = records
        for column in df.columns:
            if column in self.field_subs.keys():
                column_new = self.field_subs[column]
            else:
                column_new = column
            if known_fields is None or column_new not in known_fields.keys():
                fields[column_new] = self.create_new_field(column_new, df[column])
                self.fields_new[column_new] = fields[column_new]
        fields = self._initial_field_subs(fields)
        return fields

    ## Data cleaning
    def _clean_subvalues_recursive(self, subvalue:Any, field:CkanField, path:str, level:int,
                                   *, field_data_type:str) -> Any:
        if isinstance(subvalue, dict):
            for key, element in subvalue.items():
                if not isinstance(key, str):
                    raise TypeError(f"Key {key} is of invalid type")
                subvalue[key] = self._clean_subvalues_recursive(element, field, path + "." + str(key), level + 1,
                                                                field_data_type=field_data_type)
            return subvalue
        elif isinstance(subvalue, list):
            for i, element in enumerate(subvalue):
                subvalue[i] = self._clean_subvalues_recursive(element, field, path + "[" + str(i) + "]", level + 1,
                                                              field_data_type=field_data_type)
            return subvalue
        else:
            return self._clean_subvalue(subvalue, field, path, level, field_data_type=field_data_type)

    def _clean_subvalue(self, subvalue: Any, field: CkanField, path: str, level: int,
                                   *, field_data_type: str) -> Any:
        field_name = field.name if field is not None else None
        new_subvalue, bypass = self._replace_standard_subvalue_bypass(subvalue, field, path, level, field_data_type=field_data_type)
        if bypass:
            pass  # return new_subvalue
        else:
            new_subvalue = subvalue
            if isinstance(subvalue, numbers.Number):
                if not math.isfinite(subvalue):
                    if math.isnan(subvalue):
                        if self.param_replace_nan:
                            new_subvalue = None  # replace nans with None when not authorized
                        else:
                            self.warnings[field_name].add("nan")
                    else:
                        self.warnings[field_name].add("inf")  # infinite values are not authorized and no replacement can be made
                        if self.param_replace_forbidden:
                            new_subvalue = None
            elif isinstance(subvalue, datetime.datetime):
                if self.param_cast_types:
                    new_subvalue = subvalue.isoformat(sep=ckan_timestamp_sep)
            else:
                new_subvalue = self._replace_non_standard_subvalue(subvalue, field, path, level, field_data_type=field_data_type)
        if path in self.field_subs_path.keys():
            self._new_columns_in_row[path] = new_subvalue
        return new_subvalue

    def clean_value_field(self, value: Any, field:CkanField) -> Any:
        field_name = field.name if field is not None else None
        field_data_type = field.data_type if field is not None else None
        field_data_type = field_data_type.lower() if field_data_type is not None else None
        if field_name not in self.warnings:
            self.warnings[field_name] = set()
            self.fields_encountered[field_name] = None
        new_value, bypass = self._replace_standard_value_bypass(value, field, field_data_type=field_data_type)
        if bypass:
            pass  # return new_value
        else:
            new_value = value
            if isinstance(value, dict) or isinstance(value, list):
                if field_data_type == "text" and self.param_cast_types:
                    return json.dumps(value, default=str)
                elif field_data_type == "bson":
                    if bson is None:
                        raise CleanerRequirementError("bson", "bson")
                    return base64.b64encode(bson.BSON.encode(value))  # TODO: confirm need to encode in base64
                else:
                    return self._clean_subvalues_recursive(subvalue=value, field=field, path=field_name, level=0,
                                                           field_data_type=field_data_type)
            elif isinstance(value, numbers.Number):
                if (not math.isfinite(value)) and field_data_type not in non_finite_authorized_types:
                    if math.isnan(value):
                        if self.param_replace_nan:
                            return None  # replace nans with None when not authorized
                        else:
                            self.warnings[field_name].add("nan")
                    else:
                        self.warnings[field_name].add("inf")  # infinite values are not authorized and no replacement can be made
                        if self.param_replace_forbidden:
                            return None
                elif isinstance(value, bool):
                    if field_data_type == "text":
                        if self.param_cast_types:
                            return str(value)
                    elif field_data_type == "numeric":
                        if self.param_cast_types:
                            return int(value)
                    elif not field_data_type == "bool":
                        self.field_changes[field_name] = CkanField(field_name, "bool")
                elif field_data_type not in real_number_types and not round(value) == value:
                    if self.param_round_values:
                        return round(value)
                    else:
                        self.warnings[field_name].add("float")
            elif isinstance(value, datetime.datetime):
                if field_data_type == "timestamp":
                    if self.param_cast_types:
                        return value.isoformat(sep=ckan_timestamp_sep)
                elif not field_data_type == "timestamp":
                    self.field_changes[field_name] = CkanField(field_name, "timestamp")
            else:
                new_value = self._replace_non_standard_value(value, field, field_data_type=field_data_type)
        return new_value

    def clean_records(self, records: Union[List[dict], pd.DataFrame],
                      known_fields:Union[OrderedDict[str, CkanField], OrderedDict[str,dict], List[Union[dict,CkanField]], None],
                      *, inplace:bool=False) -> Union[List[dict], pd.DataFrame]:
        self.clear_outputs_new_dataframe()
        if known_fields is not None and isinstance(known_fields, list):
            fields_list = known_fields
            known_fields = OrderedDict()
            for field_info in fields_list:
                if isinstance(field_info, dict):
                    field_dict = field_info
                    field_info = CkanField.from_ckan_dict(field_dict)
                known_fields[field_info.name] = field_info
        elif known_fields is not None and isinstance(known_fields, dict):
            for field_name, field_info in known_fields.items():
                if isinstance(field_info, dict):
                    field_dict = field_info
                    field_info = CkanField.from_ckan_dict(field_dict)
                if field_info.name is None:
                    field_info.name = field_name
                else:
                    assert_or_raise(field_info.name == field_name, IntegrityError(f"Field name {field_info.name} neq {field_name}"))
                known_fields[field_info.name] = field_info
        fields = self.detect_field_types_and_subs(records, known_fields=known_fields)
        if not inplace:
            records = copy.deepcopy(records)
        if not self.param_enable:
            return records
        # iterate on records
        mode_df = isinstance(records, pd.DataFrame)
        if mode_df:
            for new_field in self.field_subs_path.values():
                records[new_field] = None
            for column in records.columns:
                field = fields[column]
                # records[column] = records[column].apply(self.clean_value_field, field=field)
                for index, value in enumerate(records[column]):
                    self._new_columns_in_row = {}
                    records.loc[index, column] = self.clean_value_field(value, field=field)
                    for path, new_value in self._new_columns_in_row.items():
                        if path in self.field_subs_path.keys():
                            new_field = self.field_subs_path[path]
                            records.loc[index, new_field] = new_value
        else:
            for row in records:
                self._new_columns_in_row = {}
                for key, value in row.items():
                    field = fields[key]
                    row[key] = self.clean_value_field(value, field=field)
                for path, new_value in self._new_columns_in_row.items():
                    if path in self.field_subs_path.keys():
                        new_field = self.field_subs_path[path]
                        assert(new_field not in row.keys())
                        row[new_field] = new_value
                if self.param_apply_field_subs:
                    for field_name, substitution in self.field_subs.items():
                        if field_name in row.keys():
                            assert_or_raise(substitution not in row.keys(), KeyError(substitution))
                            row[substitution] = row.pop(field_name)
        return self._clean_final_steps(records, fields, known_fields)

    def _clean_final_steps(self, records: Union[List[dict], pd.DataFrame], fields:Union[OrderedDict[str, CkanField], None],
                           known_fields:Union[OrderedDict[str, CkanField], None]) -> Union[List[dict], pd.DataFrame]:
        # apply final modifications
        mode_df = isinstance(records, pd.DataFrame)
        self.warnings = {key: value for key, value in self.warnings.items() if len(value) > 0}
        if len(self.warnings) > 0:
            msg = "Some fields had anomalies: " + str(self.warnings)
            if self.param_raise_error:
                raise CleanError(msg)
            elif self.param_verbose:
                warn(msg)
        if len(self.field_subs) > 0:
            for field_name, substitution in self.field_subs.items():
                if substitution in self.fields_encountered.keys():
                    msg = f"Substitution cannot be done for field '{field_name}' because '{substitution}' already exists"
                    if self.param_raise_error or self.param_apply_field_subs:
                        raise KeyError(msg)
                    elif self.param_verbose:
                        warn(msg)
            if self.param_apply_field_subs:
                if mode_df:
                    if len(self.field_subs) > 0:
                        records.rename(columns=self.field_subs, inplace=True)
                    # for field_name, substitution in self.field_subs.items():
                        # records[substitution] = records.pop(field)
                else:
                    pass # already done above
                    # for row in records:
                    #     for field_name, substitution in self.field_subs.items():
                    #         if field_name in row.keys():
                    #             row[substitution] = row.pop(field_name)
                new_encountered_fields = self.fields_encountered
                self.fields_encountered = OrderedDict()
                for field_name in new_encountered_fields.keys():
                    if field_name in self.field_subs.keys():
                        substitution = self.field_subs[field_name]
                        self.fields_encountered[substitution] = None
                    else:
                        self.fields_encountered[field_name] = None
                new_fields_copy = self.fields_new
                self.fields_new = OrderedDict()
                for field_name, field_info in new_fields_copy.items():
                    if field_name in self.field_subs.keys():
                        substitution = self.field_subs[field_name]
                        if known_fields is None or substitution not in known_fields.keys():
                            self.fields_new[substitution] = field_info
                            self.fields_new[substitution].name = substitution
                    elif known_fields is None or field_name not in known_fields.keys():
                        self.fields_new[field_name] = field_info
                    else:
                        pass  # field already known
                for field_name, substitution in self.field_subs.items():
                    if field_name in self.field_changes.keys():
                        self.field_changes[substitution] = self.field_changes.pop(field_name)
                        self.field_changes[substitution].name = substitution
                    if field_name in self.field_suggested_index:
                        self.field_suggested_index.remove(field_name)
                        self.field_suggested_index.add(substitution)
                    if self.field_suggested_primary_key is not None and field_name in self.field_suggested_primary_key:
                        index = self.field_suggested_primary_key.index(field_name)
                        self.field_suggested_primary_key[index] = substitution
        if not mode_df:
            # add columns attribute to List[dict]
            if not(isinstance(records, ListRecords)):
                records = ListRecords(records)  # this is not compatible with the inplace=True argument
            records.columns = list(self.fields_encountered.keys())
        if len(self.field_changes) > 0:
            if self.param_verbose:
                msg = "Recommended field changes: " + ", ".join({field.name: field.data_type for field in self.field_changes.values()})
                print(msg)
        if self.field_suggested_primary_key is not None:
            if not all([field_name in self.fields_encountered.keys() for field_name in self.field_suggested_primary_key]):
                self.field_suggested_primary_key = None  # cancel suggestion
        if self.field_suggested_primary_key is not None and self.field_suggested_index is not None:
            self.field_suggested_index = self.field_suggested_index - set(self.field_suggested_primary_key)
        if len(self.fields_new) > 0 and self.param_verbose:
            msg = ("The following new fields were detected: "
                   + str({field.name: field.data_type for field in self.fields_new.values()}))
            warn(msg)
            # user must call apply_new_fields_request in order to transmit new fields to CKAN
        self._extra_checks(records, fields)
        return records



def default_cleaner() -> CkanDataCleanerABC:
    return CkanDataCleanerUploadBasic()


if __name__ == "__main__":
    NaN = math.nan
    date_example = datetime.datetime.today()
    timestamp_example = date_example.isoformat(ckan_timestamp_sep)

    A = {"text": "A",   "int": 1,     "number": 2,     "json": {"key": "field"},   "timestamp": timestamp_example, "test": True}
    B = {"text": "B",   "int": 1.5,   "number": 2.5,   "json": {"key": [1, 2, "A"]}, "timestamp": None, "test": None}
    C = {"text": None,  "int": None,  "number": None,  "json": {"key": [1, 2, None]}, "timestamp": pd.NaT}
    D = {"text": 1,     "int": NaN,   "number": NaN,   "json": {"key": [1, 2, NaN]}}
    E = {"text": "E",   "int": 2,     "number": 5.5,   "json": None}
    F = {"text": NaN,   "int": None,  "number": None,  "json": NaN}
    G = {"text": "G",   "int": math.inf}
    H = {"text": "H",   "extra_field": 2}

    records = [A, B, C, D, E, F, G, H]
    df = records_to_df(records)

    fields_list = [
        CkanField("text", "text"),
        CkanField("int", "int"),
        CkanField("number", "numeric"),
        CkanField("json", "json"),
        CkanField("timestamp", "timestamp"),
    ]
    fields = OrderedDict([(field_info.name, field_info) for field_info in fields_list])

    cleaner = CkanDataCleanerUploadBasic()
    auto_fields = cleaner.detect_field_types_and_subs(records, known_fields=None)
    df_cleaned = cleaner.clean_records(df, fields)
    df_warnings = cleaner.warnings
    fields_new = cleaner.fields_new
    df_records = df_cleaned.to_dict(orient="records")
    records_cleaned = cleaner.clean_records(records, fields)
    records_warnings = cleaner.warnings

    print("Done.")

