#!python3
# -*- coding: utf-8 -*-
"""
Harvester base class
"""
from typing import Union, List
from collections import OrderedDict
import copy

from ckanapi_harvesters.auxiliary.ckan_auxiliary import CkanFieldInternalAttrs


class FieldMetadata:
    def __init__(self):
        self.name:str = ""
        self.description:Union[str,None] = None
        self.label:Union[str,None] = None
        self.data_type:Union[str,None] = None
        self.is_index:Union[bool,None] = None
        self.uniquekey:Union[bool,None] = None
        self.notnull:Union[bool,None] = None
        self.internal_attrs: CkanFieldInternalAttrs = CkanFieldInternalAttrs()
        self.harvester_attrs: dict = {}

    def copy(self):
        return copy.deepcopy(self)


class TableMetadata:
    def __init__(self):
        self.name: str = ""
        self.primary_key: Union[List[str],None] = None
        self.indexes: Union[List[str],None] = None
        self.unique_keys: Union[List[str],None] = None
        self.description: Union[str,None] = None
        self.fields: Union[OrderedDict[str,FieldMetadata],None] = None

    def copy(self):
        return copy.deepcopy(self)


class DatasetMetadata:
    def __init__(self):
        self.name:str = ""
        self.description:Union[str,None] = None
        self.tables:Union[OrderedDict[str,TableMetadata],None] = None

    def copy(self):
        return copy.deepcopy(self)
