#!python3
# -*- coding: utf-8 -*-
"""
Harvest from a mongo database using pymongo
"""
from typing import Union, List, Any, Dict, Set
from types import SimpleNamespace
from collections import OrderedDict
from warnings import warn
import copy

import pandas as pd


try:
    import bson
except ImportError:
    bson = SimpleNamespace(ObjectId=None, DBRef=None)


from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.list_records import ListRecords, records_to_df
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_errors import CleanerRequirementError
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload import CkanDataCleanerUpload, _pd_series_type_detect


mongodb_keep_id_column_trace:bool = True
mongodb_keep_class_column_trace:bool = True
mongodb_id_column:str = "_id"
mongodb_id_new_column:str = "ObjectId"
mongodb_id_datatype_numeric:bool = False  # option to store ids as a numeric datatype - there is no numeric datatype which corresponds
mongodb_id_alt_type_numeric:str = "int12"  # the ObjectIds are 12 byte integers (int96) - there is no such data type in Postgre (oid?)
mongodb_dbref_extract_new_id_column_max_level:int = 1  # option to create a new column if a DBRef is found in a json field
mongodb_dbref_alt_type:str = "json"  # used if not resumed in one column


def pymongo_default_df_conversion(documents: List[dict], **kwargs) -> Union[pd.DataFrame, ListRecords]:
    df = ListRecords(documents)
    # if df.columns is not None:
    #     for i, field_name in enumerate(df.columns):
    #         if field_name == mongodb_id_column:
    #             df.columns[i] = mongodb_id_new_column
    # df = records_to_df(documents)
    return df


class BrokenMongoRefError(Exception):
    pass


class MongoDataCleanerUpload(CkanDataCleanerUpload):
    """
    Data cleaner operations specific to MongoDB objects.
    """
    def __init__(self):
        super().__init__()
        # change default parameters
        self.param_field_subs[mongodb_id_column] = mongodb_id_new_column
        self.param_field_primary_key = [mongodb_id_new_column]
        self.param_apply_field_changes = True
        # specific options
        self.param_mongodb_dbref_as_one_column:bool = True  # option to extract only the ObjectId referenced by the DBRefs
        self.collection_refs:Dict[str,Set[str]] = {}
        self.database_refs:Dict[str,Set[str]] = {}
        self.broken_collection_refs:List[str] = []
        self.broken_database_refs:List[str] = []
        if bson.DBRef is None:
            raise CleanerRequirementError("bson", "DBRef, ObjectId")

    def clear_outputs_new_dataframe(self):
        super().clear_outputs_new_dataframe()
        self.broken_collection_refs:List[str] = []
        self.broken_database_refs:List[str] = []

    def clear_all_outputs(self):
        super().clear_all_outputs()
        self.collection_refs:Dict[str,Set[str]] = {}
        self.database_refs:Dict[str,Set[str]] = {}

    def copy(self, dest=None) -> "MongoDataCleanerUpload":
        if dest is None:
            dest = MongoDataCleanerUpload()
        super().copy(dest=dest)
        dest.param_mongodb_dbref_as_one_column = self.param_mongodb_dbref_as_one_column
        dest.collection_refs = copy.deepcopy(self.collection_refs)
        dest.database_refs = copy.deepcopy(self.database_refs)
        return dest

    def _detect_standard_field_bypass(self, field_name: str, values: Union[Any, pd.Series]) -> Union[CkanField,None]:
        if _pd_series_type_detect(values, bson.DBRef):
            if self.param_mongodb_dbref_as_one_column:
                return CkanField(field_name, mongodb_id_alt_type_numeric if mongodb_id_datatype_numeric else "text")
            else:
                return CkanField(field_name, mongodb_dbref_alt_type)
        elif _pd_series_type_detect(values, bson.ObjectId):
            return CkanField(field_name, mongodb_id_alt_type_numeric if mongodb_id_datatype_numeric else "text")
        return None

    def _replace_non_standard_subvalue(self, subvalue:Any, field:CkanField, path:str, level:int,
                                       *, field_data_type:str) -> Any:
        field_name = field.name if field is not None else None
        if isinstance(subvalue, bson.ObjectId):
            if mongodb_id_datatype_numeric:
                new_subvalue = int(str(subvalue), 16)
                # new_subvalue = str(subvalue)
            else:
                new_subvalue = str(subvalue)
            if level == 0:
                self.field_suggested_index.add(field_name)
            return new_subvalue
        elif isinstance(subvalue, bson.DBRef):
            id_field = path.replace(".", "_")
            if level == 0 and self.param_mongodb_dbref_as_one_column:
                id_path = None
            elif self.param_mongodb_dbref_as_one_column:  # and level > 0
                id_path = path
            elif level == 0 and not self.param_mongodb_dbref_as_one_column:
                id_path = path + "." + mongodb_id_new_column
                id_field = id_field + "_id"
            else:
                id_path = path + "." + mongodb_id_new_column
            if (level <= mongodb_dbref_extract_new_id_column_max_level and
                    id_path is not None and id_path not in self.field_subs_path.keys()):
                self._add_field_from_path(id_path,
                                          data_type=mongodb_id_alt_type_numeric if mongodb_id_datatype_numeric else "text",
                                          new_field_name=id_field,
                                          notes=f"Column extracted from {id_path}")
            if mongodb_id_datatype_numeric:
                id_value = int(str(subvalue.id), 16)
                # id_value = str(subvalue.id)
            else:
                id_value = str(subvalue.id)
            if self.param_mongodb_dbref_as_one_column:
                if path in self.collection_refs.keys():
                    self.collection_refs[path].add(str(subvalue.collection))
                    self.database_refs[path].add(str(subvalue.database))
                else:
                    self.collection_refs[path] = {str(subvalue.collection)}
                    self.database_refs[path] = {str(subvalue.database)}
                new_subvalue = id_value
            else:
                new_subvalue = {mongodb_id_new_column: id_value,
                                "collection": subvalue.collection,
                                "database": subvalue.database,
                                }
                if id_path in self.field_subs_path.keys():
                    self._new_columns_in_row[id_path] = new_subvalue[mongodb_id_new_column]
            return new_subvalue
        elif level == 0:
            return super()._replace_non_standard_value(subvalue, field, field_data_type=field_data_type)
        else:
            return super()._replace_non_standard_subvalue(subvalue, field, path, level, field_data_type=field_data_type)

    def _replace_non_standard_value(self, value:Any, field:CkanField,
                                       *, field_data_type:str) -> Any:
        field_name = field.name if field is not None else None
        return self._replace_non_standard_subvalue(value, field, path=field_name,
                                                   level=0, field_data_type=field_data_type)

    def _extra_checks(self, records: Union[List[dict], pd.DataFrame], fields:Union[OrderedDict[str, CkanField], None]) -> None:
        self.broken_collection_refs = [path for path, refs in self.collection_refs.items() if len(refs) > 1]
        self.broken_database_refs = [path for path, refs in self.database_refs.items() if len(refs) > 1]
        if len(self.broken_collection_refs) > 0 or len(self.broken_database_refs) > 0:
            broken_refs = set(self.broken_collection_refs).union(set(self.broken_database_refs))
            msg = f"DBRefs do not point to an unique collection: {', '.join(broken_refs)}"
            if self.param_raise_error or self.param_mongodb_dbref_as_one_column:
                raise BrokenMongoRefError(msg)
            else:
                warn(msg)

def pymongo_default_data_cleaner() -> MongoDataCleanerUpload:
    return MongoDataCleanerUpload()

