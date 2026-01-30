#!python3
# -*- coding: utf-8 -*-
"""
Harvest from a PostgreSQL database using sqlalchemy
"""
from typing import Union, List, Any, Dict
from types import SimpleNamespace
from collections import OrderedDict
import urllib.parse

import pandas as pd

try:
    import sqlalchemy
    import psycopg2
except ImportError:
    sqlalchemy = SimpleNamespace(Engine=None, Connection=None)
    psycopg2 = None


from ckanapi_harvesters.harvesters.harvester_errors import (HarvesterRequirementError, HarvesterArgumentRequiredError)
from ckanapi_harvesters.harvesters.harvester_abc import TableHarvesterABC, DatasetHarvesterABC, DatabaseHarvesterABC
from ckanapi_harvesters.harvesters.harvester_model import FieldMetadata, TableMetadata, DatasetMetadata
from ckanapi_harvesters.harvesters.harvester_params import DatabaseParams
from ckanapi_harvesters.harvesters.postgre_params import DatasetParamsPostgreSchema, TableParamsPostgre
from ckanapi_harvesters.auxiliary.urls import url_join, url_insert_login
from ckanapi_harvesters.auxiliary.ckan_auxiliary import ssl_arguments_decompose
from ckanapi_harvesters.auxiliary.ckan_auxiliary import parse_geometry_native_type
from ckanapi_harvesters.auxiliary.ckan_errors import UrlError
from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_abc import CkanDataCleanerABC
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_2_geom import CkanDataCleanerUploadGeom

postgre_type_mapper = {}


class DatabaseHarvesterPostgre(DatabaseHarvesterABC):
    """
    This class manages the connection to a PostgreSQL database server.
    It can list schemas (corresponding to CKAN datasets).
    """
    def __init__(self, params:DatabaseParams=None):
        super().__init__(params)
        if sqlalchemy.Engine is None:
            raise HarvesterRequirementError("sqlalchemy", "postgre")
        if psycopg2 is None:
            raise HarvesterRequirementError("psycopg2", "postgre")
        self.alchemy_engine: Union[sqlalchemy.Engine,None] = None
        self.alchemy_connection: Union[sqlalchemy.Connection,None] = None
        if self.params.auth_url is None and self.params.host is None and self.params.port is None and self.params.database is None:
            raise HarvesterArgumentRequiredError("auth-url", "postgre", "This argument defines the url used to authenticate.")

    @staticmethod
    def init_from_options_string(options_string:str, base_dir:str=None) -> "DatabaseHarvesterPostgre":
        params = DatabaseParams()
        params.parse_options_string(options_string, base_dir=base_dir)
        return DatabaseHarvesterPostgre(params)

    def copy(self, *, dest=None):
        if dest is None:
            dest = DatabaseHarvesterPostgre()
        return super().copy(dest=dest)

    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        if cancel_if_connected and self.alchemy_engine is not None:
            return self.alchemy_engine
        else:
            if self.alchemy_engine is not None:
                self.alchemy_connection.close()
                self.alchemy_engine.dispose()
                self.alchemy_connection = None
                self.alchemy_engine = None
            ssl, ssl_certfile = ssl_arguments_decompose(self.params.verify_ca)
            auth_url = self.params.auth_url
            if auth_url is None:
                if self.params.url is not None:
                    auth_url = self.params.url
                elif self.params.host is not None:
                    auth_url = f"postgresql+psycopg2://{self.params.host}"
                    if self.params.port is not None:
                        auth_url += f":{self.params.port}"
                else:
                    raise UrlError("No Postgre URL provided")
                if self.params.auth_url_suffix is not None:
                    auth_url = url_join(auth_url, self.params.auth_url_suffix)
                elif self.params.database is not None:
                    auth_url = url_join(auth_url, self.params.database)
                self.params.auth_url = auth_url
            auth_url_with_login = url_insert_login(auth_url, self.params.login)
            self.alchemy_engine = sqlalchemy.create_engine(auth_url_with_login)
                                                           # ssl=ssl, tlscafile=ssl_certfile,
                                                           # timeoutMS=self.params.timeout*1000.0 if self.params.timeout is not None else None)
            self.alchemy_connection = self.alchemy_engine.connect()
            if self.params.host is None and self.params.port is None:
                # complete with host and port parsed by sqlalchemy
                parsed_url = urllib.parse.urlparse(auth_url)
                self.params.host, self.params.port = parsed_url.hostname, parsed_url.port
            return self.alchemy_engine

    def is_connected(self) -> bool:
        return self.alchemy_engine is not None

    def disconnect(self) -> None:
        if self.alchemy_engine is not None:
            self.alchemy_connection.close()
            self.alchemy_engine.dispose()
            self.alchemy_engine = None
            self.alchemy_connection = None

    def check_connection(self, *, new_connection:bool=False, raise_error:bool=False) -> Union[None, ContextErrorLevelMessage]:
        try:
            self.connect(cancel_if_connected=not new_connection)
            remote_collections = self.list_datasets(return_metadata=False)
        except Exception as e:
            if raise_error:
                raise e from e
            else:
                return ContextErrorLevelMessage("Postgre Harvester", ErrorLevel.Error, f"Failed to connect to {self.params.auth_url}: {e}")

    def get_dataset_harvester(self, dataset_name: str) -> "DatasetHarvesterPostgre":
        params_dataset = self.params.copy(dest=DatasetParamsPostgreSchema())
        params_dataset.dataset = dataset_name
        dataset_harvester = DatasetHarvesterPostgre(params_dataset)
        self.copy(dest=dataset_harvester)
        dataset_harvester.params = params_dataset
        dataset_harvester._finalize_connection()
        return dataset_harvester

    def list_datasets(self, return_metadata: bool = True) -> Union[List[str], OrderedDict[str, DatasetMetadata]]:
        self.connect()
        query = "SELECT schema_name FROM information_schema.schemata;"
        df_schemas = pd.read_sql(query, self.alchemy_engine)
        dataset_list = df_schemas["schema_name"].tolist()
        if return_metadata:
            return OrderedDict([(name, self.get_dataset_harvester(name).query_dataset_metadata()) for name in dataset_list])
        else:
            return dataset_list


class DatasetHarvesterPostgre(DatabaseHarvesterPostgre, DatasetHarvesterABC):
    """
    A CKAN dataset corresponds to a PostgreSQL schema (set of tables).
    """
    def __init__(self, params:DatasetParamsPostgreSchema=None):
        super().__init__(params)
        self.dataset_metadata: Union[DatasetMetadata, None] = None  # DatasetHarvesterABC
        if self.params.dataset is None:
            raise HarvesterArgumentRequiredError("dataset", "postgre", "This argument defines the Postgre schema to be used")

    @staticmethod
    def init_from_options_string(options_string:str, base_dir:str=None) -> "DatasetHarvesterPostgre":
        params = DatasetParamsPostgreSchema()
        params.parse_options_string(options_string, base_dir=base_dir)
        return DatasetHarvesterPostgre(params)

    def _finalize_connection(self):
        if super().is_connected():
            pass

    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        if not (cancel_if_connected and self.is_connected()):
            super().connect(cancel_if_connected=cancel_if_connected)
            self._finalize_connection()
        return self.alchemy_connection

    def is_connected(self) -> bool:
        return super().is_connected()

    def disconnect(self) -> None:
        if super().is_connected():
            super().disconnect()

    def check_connection(self, *, new_connection: bool = False, raise_error: bool = False) -> Union[None, ContextErrorLevelMessage]:
        try:
            super().check_connection(new_connection=new_connection, raise_error=raise_error)
            tables_list = self.list_tables(return_metadata=False)
        except Exception as e:
            if raise_error:
                raise e from e
            else:
                return ContextErrorLevelMessage("Postgre Harvester", ErrorLevel.Error,
                                                f"Failed to connect to {self.params.auth_url}: {e}")

    def query_dataset_metadata(self, cancel_if_present:bool=True) -> DatasetMetadata:
        self.connect()
        if cancel_if_present and self.dataset_metadata is not None:
            return self.dataset_metadata
        else:
            # query schema comment
            postgre_schema_name = self.params.dataset
            query = f"""
            SELECT 
                n.nspname AS {postgre_schema_name},
                d.description AS schema_comment
            FROM 
                pg_namespace n
            LEFT JOIN 
                pg_description d ON n.oid = d.objoid
            WHERE 
                n.nspname = '{postgre_schema_name}';
            """
            table_df = pd.read_sql(query, self.alchemy_engine)
            schema_comment = table_df.iloc[0]['schema_comment']
            self.dataset_metadata = DatasetMetadata()
            self.dataset_metadata.name = self.params.dataset
            self.dataset_metadata.description = schema_comment
            self.dataset_metadata.tables = self.list_tables(return_metadata=True)
            return self.dataset_metadata

    def clean_dataset_metadata(self) -> DatasetMetadata:
        return self.query_dataset_metadata().copy()

    def get_table_harvester(self, table_name:str) -> "TableHarvesterPostgre":
        params_table = self.params.copy(dest=TableParamsPostgre())
        if self.params.options_string is not None:
            # reparse options_string for table-specific arguments
            params_table.parse_options_string(self.params.options_string, base_dir=self.params.base_dir)
        params_table.table = table_name
        table_harvester = TableHarvesterPostgre(params_table)
        self.copy(dest=table_harvester)
        table_harvester.params = params_table
        table_harvester._finalize_connection()
        return table_harvester

    def list_tables(self, return_metadata:bool=True) -> Union[List[str], OrderedDict[str, TableMetadata]]:
        self.connect()
        postgre_schema_name = self.params.dataset
        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{postgre_schema_name}'
          AND table_type = 'BASE TABLE';
        """
        df_tables = pd.read_sql(query, self.alchemy_engine)
        tables_list = df_tables["table_name"].tolist()
        if return_metadata:
            return OrderedDict([(name, self.get_table_harvester(name).query_table_metadata()) for name in tables_list])
        else:
            return tables_list


class TableHarvesterPostgre(DatasetHarvesterPostgre, TableHarvesterABC):
    """
    A CKAN table (DataStore) corresponds to a PostgreSQL table.
    """
    _default_upload_fun = None
    _default_primary_key = None

    def __init__(self, params:TableParamsPostgre=None):
        super().__init__(params)
        self.params: TableParamsPostgre = params
        self.table_metadata: Union[TableMetadata, None] = None  # TableHarvesterABC
        if self.params.file_url_attr is not None:
            # File/URL attribute has priority over CLI
            self.params.table = self.params.file_url_attr
        if self.params.table is None:
            raise HarvesterArgumentRequiredError("table", "postgre", "This argument defines the Postgre table used")

    @staticmethod
    def init_from_options_string(options_string:str, *, base_dir:str=None, file_url_attr:str=None) -> "TableHarvesterPostgre":
        params = TableParamsPostgre()
        params.parse_options_string(options_string, file_url_attr=file_url_attr, base_dir=base_dir)
        return TableHarvesterPostgre(params)

    def copy(self, *, dest=None):
        if dest is None:
            dest = TableHarvesterPostgre()
        super().copy(dest=dest)
        return dest

    def disconnect(self) -> None:
        if super().is_connected():
            super().disconnect()

    def _finalize_connection(self):
        super()._finalize_connection()
        if super().is_connected():
            pass

    def connect(self, *, cancel_if_connected:bool=True) -> Any:
        if not (cancel_if_connected and self.is_connected()):
            super().connect()
            self._finalize_connection()
        return self.alchemy_engine

    def check_connection(self, *, new_connection:bool=False, raise_error:bool=False) -> Union[None, ContextErrorLevelMessage]:
        super().check_connection(new_connection=new_connection, raise_error=raise_error)

    def query_table_metadata(self, cancel_if_present:bool=True) -> TableMetadata:
        self.connect()
        if cancel_if_present and self.table_metadata is not None:
            return self.table_metadata
        else:
            postgre_schema_name = self.params.dataset
            postgre_table_name = self.params.table
            # request comment on table
            query = f"""
            SELECT 
                obj_description('{postgre_schema_name}.{postgre_table_name}'::regclass) AS table_comment;
            """
            table_df = pd.read_sql(query, self.alchemy_engine)
            table_comment = table_df.iloc[0]['table_comment']
            # request information on fields
            query = f"""
            SELECT
                cols.column_name,
                cols.ordinal_position,
                cols.is_nullable = 'NO' AS is_not_null,
                cols.data_type AS apparent_data_type,
                COALESCE(pt.typname, cols.data_type) AS full_data_type, -- Use user-defined type name if available
                pgd.description AS column_comment,
                EXISTS (
                    SELECT 1
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = ('{postgre_schema_name}.' || cols.table_name)::regclass
                      AND a.attname = cols.column_name
                ) AS is_indexed,
                con.constraint_type = 'UNIQUE' AS is_unique
            FROM
                information_schema.columns AS cols
            LEFT JOIN
                pg_catalog.pg_statio_all_tables AS st
                ON cols.table_schema = st.schemaname AND cols.table_name = st.relname
            LEFT JOIN
                pg_catalog.pg_description AS pgd
                ON pgd.objoid = st.relid AND pgd.objsubid = cols.ordinal_position
            LEFT JOIN
                information_schema.key_column_usage AS kcu
                ON cols.table_schema = kcu.table_schema
                   AND cols.table_name = kcu.table_name
                   AND cols.column_name = kcu.column_name
            LEFT JOIN
                information_schema.table_constraints AS con
                ON kcu.constraint_name = con.constraint_name
                   AND kcu.table_schema = con.table_schema
            LEFT JOIN
                pg_type pt
                ON cols.udt_name = pt.typname -- Match user-defined type name
            LEFT JOIN
                pg_namespace pn
                ON pt.typnamespace = pn.oid
            WHERE
                cols.table_schema = '{postgre_schema_name}'
                AND cols.table_name = '{postgre_table_name}'
            ORDER BY
                cols.ordinal_position;
            """
            # DataFrame with columns: ["column_name", "order", "is_not_null", "apparent_data_type", "full_data_type", "description", "is_indexed", "is_unique"]
            fields_df = pd.read_sql(query, self.alchemy_engine)
            fields_df.set_index("column_name", inplace=True, drop=False, verify_integrity=True)
            # querying details on column types
            fields_df["definitive_data_type"] = fields_df["full_data_type"]
            fields_df["geo_type"] = ""
            fields_df["srid"] = 0
            # PostGIS geometry type
            if any(fields_df["full_data_type"] == "geometry"):
                query = f"""
                SELECT
                  f_geometry_column,
                  type,
                  srid
                FROM geometry_columns
                WHERE f_table_schema = '{postgre_schema_name}'
                  AND f_table_name = '{postgre_table_name}';
                """
                geo_df = pd.read_sql(query, self.alchemy_engine)
                for index, row in geo_df.iterrows():
                    column_name = row["f_geometry_column"]
                    fields_df.loc[column_name, "definitive_data_type"] = f"geometry({row['type']}, {row['srid']})"
                    fields_df.loc[column_name, "geo_type"] = row['type']
                    fields_df.loc[column_name, "geo_srid"] = row['srid']
            # query primary key
            query = f"""
            SELECT
                a.attname AS column_name
            FROM
                pg_index i
            JOIN
                pg_attribute a ON a.attnum = ANY(i.indkey)
            WHERE
                i.indrelid = '{postgre_schema_name}.{postgre_table_name}'::regclass
                AND i.indisprimary;
            """
            primary_key_df = pd.read_sql(query, self.alchemy_engine)
            primary_key = primary_key_df["column_name"].tolist()
            if len(primary_key) == 0:
                primary_key = None
            self.table_metadata = TableMetadata()
            self.table_metadata.name = self.params.table
            self.table_metadata.description = table_comment
            self.table_metadata.fields = OrderedDict()
            for index, row in fields_df.iterrows():
                field_metadata = FieldMetadata()
                field_metadata.name = row["column_name"]
                field_metadata.data_type = row["definitive_data_type"]
                field_metadata.harvester_attrs["datatype_keyword"] = row["full_data_type"]
                field_metadata.internal_attrs.geometry_as_source = row["geo_srid"] > 0
                field_metadata.internal_attrs.geometry_type = row["geo_type"] if row["geo_type"] else None
                field_metadata.internal_attrs.epsg_source = row["geo_srid"] if row["geo_srid"] > 0 else None
                field_metadata.internal_attrs.init_from_native_type(field_metadata.data_type)
                field_metadata.description = row["column_comment"]
                field_metadata.uniquekey = row["is_unique"] if row["is_unique"] is not None else False
                field_metadata.is_index = row["is_indexed"]
                field_metadata.notnull = row["is_not_null"]
                self.table_metadata.fields[field_metadata.name] = field_metadata
            if primary_key is None:
                # first field with unicity can be used as primary key
                primary_key = [field_metadata.name for field_metadata in self.table_metadata.fields.values() if field_metadata.uniquekey]
                if len(primary_key) > 0:
                    primary_key = primary_key[0]
                else:
                    primary_key = None
            self.table_metadata.primary_key = primary_key
            self.table_metadata.indexes = [field_metadata.name for field_metadata in self.table_metadata.fields.values() if field_metadata.is_index]
            return self.table_metadata

    def _data_type_map_to_ckan(self, field_metadata:FieldMetadata) -> None:
        """
        Some data types need to be translated
        """
        if field_metadata.harvester_attrs["datatype_keyword"] == "geometry":
            if self.params.ckan_postgis:
                if self.params.ckan_default_target_epsg is not None:
                    # TODO: at this point, the ckan_default_target_epsg does not inherit from ckan
                    geometry_type, geo_epsg = parse_geometry_native_type(field_metadata.data_type)
                    field_metadata.data_type = f"geometry({geometry_type},{self.params.ckan_default_target_epsg})"
                    field_metadata.internal_attrs.init_from_native_type(field_metadata.data_type)
            else:
                field_metadata.data_type = "json"
        return

    def _get_field_query_function(self, field_metadata: FieldMetadata) -> str:
        """
        Force some data types to return as text
        """
        if field_metadata.harvester_attrs["datatype_keyword"] == "geometry":
            if self.params.ckan_postgis:
                return f"{field_metadata.name}"  # TODO: test if transfer is successful without converting to a GeoJSON string
            else:
                return f"ST_AsGeoJSON({field_metadata.name})"
        elif field_metadata.data_type == "jsonb":
            return f"{field_metadata.name}::text"
        else:
            return field_metadata.name

    def clean_table_metadata(self) -> TableMetadata:
        table_metadata = self.query_table_metadata().copy()
        for field_metadata in table_metadata.fields.values():
            self._data_type_map_to_ckan(field_metadata)
        return table_metadata

    def update_from_ckan(self, ckan):
        super().update_from_ckan(ckan)
        for field_name, field_metadata in self.table_metadata.fields.items():
            field_metadata.internal_attrs.update_from_ckan(ckan)

    def get_default_primary_key(self) -> List[str]:
        table_metadata = self.query_table_metadata()
        return table_metadata.primary_key

    def _get_sql_fields_query(self):
        return ", ".join([self._get_field_query_function(field_metadata) for field_metadata in self.table_metadata.fields.values()])

    def get_default_data_cleaner(self) -> CkanDataCleanerABC:
        data_cleaner = CkanDataCleanerUploadGeom()
        return data_cleaner

    def list_queries(self, *, new_connection:bool=False) -> List[str]:
        self.connect(cancel_if_connected=not new_connection)
        postgre_schema_name = self.params.dataset
        postgre_table_name = self.params.table
        if self.params.verbose_harvester:
            print(f"Counting documents of table {self.params.table}")
        count_query = f"SELECT COUNT(*) FROM {postgre_schema_name}.{postgre_table_name}"
        if self.params.query_string is not None:
            count_query += " " + self.params.query_string
        count_df = pd.read_sql(count_query, self.alchemy_engine)
        num_rows = count_df["count"].iloc[0]
        fields_query = self._get_sql_fields_query()
        request_query_base = f"SELECT {fields_query} FROM {postgre_schema_name}.{postgre_table_name}"
        if self.params.query_string is not None:
            request_query_base += " " + self.params.query_string
        num_queries = num_rows // self.params.limit + 1
        if self.params.single_request:
            return [f"{request_query_base} LIMIT {self.params.limit} OFFSET {i * self.params.limit}" for i in range(1)]
        else:
            queries_exact = [f"{request_query_base} LIMIT {self.params.limit} OFFSET {i * self.params.limit}" for i in range(num_queries)]
            query_extra = f"{request_query_base} LIMIT {self.params.limit} OFFSET {num_queries * self.params.limit}"
            return queries_exact + [query_extra]

    def query_data(self, query:Dict[str,Any]) -> pd.DataFrame:
        df = pd.read_sql(query, self.alchemy_engine)
        return df

