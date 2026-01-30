#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements the field definition
"""
from typing import Union
import pandas as pd

from ckanapi_harvesters.auxiliary.ckan_model import CkanFieldType, CkanField
from ckanapi_harvesters.auxiliary.ckan_auxiliary import CkanFieldInternalAttrs
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, _bool_from_string




class BuilderField:
    def __init__(self, *, name:str=None, type_override:CkanFieldType=None,
                 label:str=None, description:str=None):
        self.name: str = name
        self.type_override: Union[CkanFieldType,None] = type_override
        self.label: Union[str,None] = label
        self.description: Union[str,None] = description
        self.is_index: Union[bool,None] = None
        self.uniquekey: Union[bool,None] = None
        self.notnull: Union[bool,None] = None
        self.options_string: Union[str,None] = None
        self.internal_attrs: CkanFieldInternalAttrs = CkanFieldInternalAttrs()
        self.comment: Union[str,None] = None

    def __str__(self):
        return f"Field builder for {self.name}"

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderField()
        dest.name = self.name
        dest.type_override = self.type_override
        dest.label = self.label
        dest.description = self.description
        dest.is_index = self.is_index
        dest.uniquekey = self.uniquekey
        dest.notnull = self.notnull
        dest.options_string = self.options_string
        dest.internal_attrs = self.internal_attrs.copy()
        dest.comment = self.comment
        return dest

    def _load_from_df_row(self, row: pd.Series):
        self.name = _string_from_element(row["field name"]).strip()
        type_override_string = _string_from_element(row["type override"])
        self.type_override = None
        if type_override_string is not None:
            self.type_override = CkanFieldType.from_str(type_override_string)
        else:
            self.type_override = None
        self.label = None
        self.description = None
        if "label" in row.keys():
            self.label = _string_from_element(row["label"])
        if "description" in row.keys():
            self.description = _string_from_element(row["description"])
        if "index" in row.keys():
            self.is_index = _bool_from_string(row["index"], default_value=None)
        if "unique" in row.keys():
            self.uniquekey = _bool_from_string(row["unique"], default_value=None)
        if "not null" in row.keys():
            self.notnull = _bool_from_string(row["not null"], default_value=None)
        if "options" in row.keys():
            self.options_string = _string_from_element(row["options"])
        if "comment" in row.keys():
            self.comment = _string_from_element(row["comment"])
        self.internal_attrs.init_from_native_type(self.type_override)
        self.internal_attrs.init_from_options_string(self.options_string)

    @staticmethod
    def from_df_row(row: pd.Series) -> "BuilderField":
        field_builder = BuilderField()
        field_builder._load_from_df_row(row)
        return field_builder

    def _to_dict(self) -> dict:
        return {
            "Field Name": self.name,
            "Type override": str(self.type_override) if self.type_override is not None else "",
            "Label": self.label if self.label else "",
            "Description": self.description if self.description else "",
            "Index": str(self.is_index) if self.is_index is not None else "",
            "Unique": str(self.uniquekey) if self.uniquekey is not None else "",
            "Not null": str(self.notnull) if self.notnull is not None else "",
            "Options": self.options_string if self.options_string else "",
            "Comment": self.comment if self.comment else "",
        }

    def _to_ckan_field(self) -> CkanField:
        field_info = CkanField(name=self.name, data_type=str(self.type_override) if self.type_override is not None else "",
                               notes=self.description, label=self.label)
        field_info.is_index = self.is_index
        field_info.uniquekey = self.uniquekey
        field_info.notnull = self.notnull
        field_info.internal_attrs = self.internal_attrs.copy()
        return field_info

    def _to_ckan_dict(self) -> dict:
        return self._to_ckan_field().to_ckan_dict()

    @staticmethod
    def _from_ckan_field(field_info: CkanField) -> "BuilderField":
        field_row = pd.Series({"field name": field_info.name,
                               "type override": str(field_info.data_type),  # if field_info.type_override else "",
                               "label": field_info.label,
                               "description": field_info.notes,
                               "index": field_info.is_index,
                               "unique": field_info.uniquekey,
                               "not null": field_info.notnull,
                               })
        field_builder = BuilderField()
        field_builder._load_from_df_row(field_row)
        field_builder.internal_attrs = field_info.internal_attrs.copy()
        return field_builder

