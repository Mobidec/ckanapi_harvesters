#!python3
# -*- coding: utf-8 -*-
"""
Functions to clean data before upload.
"""
from typing import Any

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_2_geom import CkanDataCleanerUploadGeom


class CkanDataCleanerUploadAssist(CkanDataCleanerUploadGeom):
    """
    Implementation which raises an exception if a data change is recommended by the data cleaner and assists in field typing.
    """

    def __init__(self):
        super().__init__()
        self.param_apply_field_changes = True

    @staticmethod
    def get_class_keyword() -> str:
        return "Assist"

    def clean_value_field(self, value: Any, field:CkanField) -> Any:
        suggested_value = super().clean_value_field(value, field)
        if suggested_value is not None and not suggested_value == value:
            raise ValueError(f"Value '{str(value)}' is badly formatted (column {field.name})")
        return suggested_value

    def _clean_subvalue(self, subvalue: Any, field: CkanField, path: str, level: int,
                        *, field_data_type: str) -> Any:
        suggested_subvalue = super().clean_value_field(subvalue, field)
        if suggested_subvalue is not None and not suggested_subvalue == subvalue:
            raise ValueError(f"Sub-value '{str(subvalue)}' is badly formatted (column {path}")
        return suggested_subvalue

