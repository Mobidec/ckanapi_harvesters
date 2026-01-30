#!python3
# -*- coding: utf-8 -*-
"""
Code to define the bondage between a file and a database query
in the context of a large DataStore defined by the concatenation of multiple files.
"""
from warnings import warn
from abc import ABC, abstractmethod
from typing import Dict, List, Iterable, Callable, Any, Tuple, Generator, Set, Union

import numpy as np
import pandas as pd

from ckanapi_harvesters.builder.builder_resource_datastore import DataSchemeConversion
from ckanapi_harvesters.auxiliary.ckan_model import UpsertChoice
from ckanapi_harvesters.auxiliary.ckan_defs import ckan_tags_sep
from ckanapi_harvesters.ckan_api import CkanApi


class RequestMapperABC(DataSchemeConversion, ABC):
    """
    Class to define how to reconstruct a file from the full dataset
    This class overloads some data scheme conversion class functions
    This abstract class can be derived to specify custom data treatments
    """
    def __init__(self,
                 *, df_upload_fun:Callable[[pd.DataFrame], Any] = None,
                 df_download_fun:Callable[[pd.DataFrame], Any] = None):
        super().__init__(df_upload_fun=df_upload_fun, df_download_fun=df_download_fun)
        self.upsert_only_missing_rows:bool = False

    ## upsert request preparation  ----------------
    def get_file_query_of_df(self, df_upload:pd.DataFrame) -> Union[dict,None]:
        """
        Return the dict of {field: value} combinations representing the arguments of the query to reconstruct a file

        :param df_upload: the DataFrame representing the file
        :return:
        """
        return None

    def last_inserted_row_request(self, ckan:CkanApi, resource_id:str, file_query:dict) -> Union[pd.DataFrame,None]:
        """
        Request in CKAN the last inserted row(s) corresponding to a given file_query

        :param ckan:
        :param resource_id:
        :param file_query: a dict of {field: value} combinations representing the arguments of the query to reconstruct a file
        :return: The last row(s) in the database or None (if no specific method was defined)
        """
        return None

    def last_inserted_index_request(self, ckan:CkanApi, resource_id:str, file_query:dict, df_upload:pd.DataFrame) -> Tuple[int, bool, int, Union[pd.DataFrame,None]]:
        """
        Knowing the data which needs to be uploaded, this function compares the last known row(s) to the dataframe
        and returns the index to restart the upload process.

        :param ckan:
        :param resource_id:
        :param file_query: a dict of {field: value} combinations representing the arguments of the query to reconstruct a file
        :param df_upload: the known data corresponding to the file_query to be sent
        :return: a tuple (i_restart, upload_needed, row_count, df_last_row):
         - i_restart: the last known index in the dataframe
         - upload_needed: a boolean indicating if an update is necessary
         - row_count: the number of rows corresponding to the file_query
         - df_last_row: the last found row in the dataframe
        """
        return 0, True, -1, None

    ## download preparation  ----------------
    @abstractmethod
    def download_file_query_list(self, ckan: CkanApi, resource_id: str) -> List[dict]:
        """
        Function to list the {key: value} combinations present in the CKAN datastore to reconstruct the file database before downloading.

        :param ckan:
        :param resource_id:
        :return: a list of query arguments defining each file
        """
        raise NotImplementedError()

    def download_file_query_generator(self, ckan: CkanApi, resource_id: str) -> Generator[dict, Any, None]:
        """
        Generator for download_file_query_list which can be customized

        :param ckan:
        :param resource_id:
        :return:
        """
        for file_query in self.download_file_query_list(ckan=ckan, resource_id=resource_id):
            yield file_query

    def download_file_query(self, ckan: CkanApi, resource_id: str, file_query:dict) -> pd.DataFrame:
        return ckan.datastore_search(resource_id=resource_id, **file_query, search_all=True)


class RequestFileMapperABC(RequestMapperABC, ABC):
    """
    Class to define how to reconstruct a file from the full dataset
    This abstract class is oriented to treating files in the file system
    """
    def __init__(self,
                 *, df_upload_fun:Callable[[pd.DataFrame], Any] = None,
                 df_download_fun:Callable[[pd.DataFrame], Any] = None):
        super().__init__(df_upload_fun=df_upload_fun, df_download_fun=df_download_fun)
        self.file_name_prefix:str = "table_"
        self.file_name_suffix:str = ".csv"
        self.file_name_function:Union[Callable[[dict], str], None] = None

    def get_file_name_of_query(self, file_query:dict) -> str:
        if self.file_name_function is None:
            file_filters_str = '_'.join([str(key)+'_'+str(value) for key,value in file_query.items()])
        else:
            file_filters_str = self.file_name_function(file_query)
        return f"{self.file_name_prefix}{file_filters_str}{self.file_name_suffix}"


class RequestFileMapperUser(RequestFileMapperABC):
    """
    Use this basic implementation if the file query list is provided by the user or if the builder is only used to upload files.
    """
    def __init__(self, file_query_list: Iterable[Tuple[str, dict]],
                 *, df_upload_fun:Callable[[pd.DataFrame], Any] = None,
                 df_download_fun:Callable[[pd.DataFrame], Any] = None):
        super().__init__(df_upload_fun=df_upload_fun, df_download_fun=df_download_fun)
        # file_query_list must be stored in the BuilderDataStoreMultiAbc instance

    def download_file_query_list(self, ckan: CkanApi, resource_id: str) -> List[dict]:
        raise RuntimeError("File query list is provided by user")


class RequestFileMapperLimit(RequestFileMapperABC):
    """
    In this implementation, a file is defined by a certain amount of rows
    """
    default_limit = 10000

    def __init__(self, limit:int=None,
                 *, df_upload_fun:Callable[[pd.DataFrame], Any] = None,
                 df_download_fun:Callable[[pd.DataFrame], Any] = None):
        super().__init__(df_upload_fun=df_upload_fun, df_download_fun=df_download_fun)
        if limit is None:
            limit = RequestFileMapperLimit.default_limit
        self.limit:int = limit

    ## download preparation  ----------------
    def get_file_name_of_query(self, file_query:dict) -> str:
        if self.file_name_function is None:
            # file_filters_str = str(file_query["offset"] // self.limit)
            file_filters_str = f'{file_query["offset"]}_{file_query["offset"]+self.limit-1}'
        else:
            file_filters_str = self.file_name_function(file_query)
        return f"{self.file_name_prefix}{file_filters_str}{self.file_name_suffix}"

    def download_file_query_list(self, ckan: CkanApi, resource_id: str) -> List[dict]:
        # get number of rows and return a list of [offset,limit] combinations
        row_count = ckan.datastore_search_row_count(resource_id)
        return [{"offset": self.limit*counter, "limit": self.limit} for counter in range(row_count // self.limit + 1)]

    def download_file_query(self, ckan: CkanApi, resource_id: str, file_query:dict) -> pd.DataFrame:
        return ckan.datastore_search(resource_id=resource_id, offset=file_query["offset"], limit=file_query["limit"], search_all=True)


class RequestFileMapperIndexKeys(RequestFileMapperABC):
    """
    In this implementation, a file is defined by a combination of file_keys values
    It is optionally ordered by an index_keys which enables to restart a transfer when interrupted
    By default, the index_keys is the last field of the primary key
    and the file_keys are the fields preceding the index_keys in the primary key
    """
    last_rows_limit = 1
    def __init__(self, group_by_keys:List[str], sort_by_keys:List[str] = None,
                 *, df_upload_fun:Callable[[pd.DataFrame], Any] = None,
                 df_download_fun:Callable[[pd.DataFrame], Any] = None):
        super().__init__(df_upload_fun=df_upload_fun, df_download_fun=df_download_fun)
        self.group_by_keys: List[str] = group_by_keys        # fields to filter to obtain one file
        self.sort_by_keys: Union[List[str],None] = None      # field to order the document
        if sort_by_keys is not None:
            self.sort_by_keys = sort_by_keys

    def get_necessary_fields(self) -> Set[str]:
        fields = set(self.group_by_keys)
        if self.sort_by_keys is not None:
            fields = fields.union(set(self.sort_by_keys))
        return fields

    def df_upload_alter(self, df_local: pd.DataFrame, file_name:str=None, mapper_kwargs:dict=None, **kwargs) -> pd.DataFrame:
        # overload of df_upload_alter calling self.df_upload_fun
        # order dataframes before sending to database in order to be able to restart transfer from last transmitted index
        df_database = super().df_upload_alter(df_local, file_name=file_name, mapper_kwargs=mapper_kwargs, **kwargs)
        if self.sort_by_keys is not None:
            if self.df_upload_fun is None:
                df_database = df_database.copy()
            df_database.sort_values(self.sort_by_keys, inplace=True)
        return df_database

    ## upsert request preparation  ----------------
    def get_file_query_of_df(self, df_upload:pd.DataFrame) -> Union[dict,None]:
        df_file_query = df_upload[self.group_by_keys].drop_duplicates(subset=self.group_by_keys)
        if len(df_file_query) == 1:
            return {"filters": df_file_query.to_dict(orient="records")[0]}
        else:
            return None

    def last_inserted_row_request(self, ckan:CkanApi, resource_id:str, file_query:dict) -> Union[pd.DataFrame,None]:
        if self.sort_by_keys is None or not self.upsert_only_missing_rows:
            return None
        else:
            df = ckan.datastore_search(resource_id, filters=file_query["filters"], sort=ckan_tags_sep.join(self.sort_by_keys) + " desc",
                                       limit=self.last_rows_limit, search_all=False)  #, fields=self.file_keys + self.index_keys)
            return df

    def last_inserted_index_request(self, ckan:CkanApi, resource_id:str, file_query:dict, df_upload:pd.DataFrame) -> Tuple[int, bool, int, pd.DataFrame]:
        # df_upload is in the database format (df_upload_fun has been applied)
        # df_last_row has just been downloaded but no field typing has been applied
        df_last_row = self.last_inserted_row_request(ckan=ckan, resource_id=resource_id, file_query=file_query)
        if df_last_row is None or df_last_row.empty:
            return 0, True, df_last_row.attrs["total"] if df_last_row is not None else 0, df_last_row
        else:
            for key in self.sort_by_keys:
                if key in df_upload.columns:
                    # apply field typing from df_upload in order to perform line-by-line comparison
                    df_last_row[key] = df_last_row[key].astype(df_upload[key].dtype)
            match_table = np.column_stack([df_upload[key] == df_last_row[key].iloc[0] for key in self.sort_by_keys])
            match_array = np.logical_and.reduce(match_table, 1)
            i_restart = np.argwhere(match_array) + 1
            if len(i_restart) == 1 and len(i_restart[0]) == 1:
                i_restart_py = int(i_restart[0][0])
                return i_restart_py, i_restart_py < len(df_upload), df_last_row.attrs["total"], df_last_row
            else:
                msg = "Multiple results obtained when querying the last inserted index"
                warn(msg)
                return 0, True, df_last_row.attrs["total"], df_last_row

    ## download preparation  ----------------
    def get_file_name_of_query(self, file_query:dict) -> str:
        if self.file_name_function is None:
            file_filters_str = '_'.join([str(key)+'_'+str(value) for key,value in file_query['filters'].items()])
        else:
            file_filters_str = self.file_name_function(file_query['filters'])
        return f"{self.file_name_prefix}{file_filters_str}{self.file_name_suffix}"

    def download_file_query_list(self, ckan: CkanApi, resource_id: str) -> List[dict]:
        # function to list the files which are defined by unique file_keys combinations in the database
        df_list = ckan.datastore_search(resource_id, fields=self.group_by_keys, search_all=True, distinct=True)
        # df_list = ckan.datastore_search(resource_id, filters={key: 0 for key in self.order_keys}, fields=self.file_keys, search_all=True)
        filters_list = df_list.to_dict(orient="records")
        return [{"filters": file_filter} for file_filter in filters_list]

#    def download_file_query(self, ckan: CkanApi, resource_id: str, file_query:dict) -> pd.DataFrame:
#        return ckan.datastore_search(resource_id=resource_id, filters=file_query["filters"], search_all=True)


def default_file_mapper_from_primary_key(primary_key:List[str]=None, file_query_list: Iterable[Tuple[str,dict]]=None) -> RequestFileMapperABC:
    if primary_key is None or len(primary_key) <= 1:
        if file_query_list is not None:
            return RequestFileMapperUser(file_query_list)
        else:
            return RequestFileMapperLimit()
    else:
        return RequestFileMapperIndexKeys(group_by_keys=primary_key[:-1], sort_by_keys=[primary_key[-1]])

