#!python3
# -*- coding: utf-8 -*-
"""
Shapefile format support
"""
from typing import Union, Dict
from types import SimpleNamespace
import io
from warnings import warn
from enum import IntEnum

import pandas as pd
try:
    import geopandas as gpd
except ImportError:
    gpd = SimpleNamespace(GeoDataFrame=None)
try:
    import pyproj
except ImportError:
    pyproj = None

from ckanapi_harvesters.auxiliary.list_records import ListRecords
from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.ckan_errors import FileFormatRequirementError, UnknownTargetCRSError
from ckanapi_harvesters.auxiliary.ckan_configuration import default_ckan_target_epsg
from ckanapi_harvesters.harvesters.file_formats.file_format_abc import FileFormatABC


shp_upload_read_file_kwargs = dict(encoding='utf-8')

class DownloadedShapeFileConversion(IntEnum):
    CsvWkb = 0
    ShapefileProjection = 2
    ShapefileAsIs = 3

class ShapeFileFormat(FileFormatABC):
    def __init__(self, read_file_kwargs=None) -> None:
        if gpd.GeoDataFrame is None:
            raise FileFormatRequirementError("geopandas", "SHP")
        if pyproj is None:
            raise FileFormatRequirementError("pyproj", "SHP")
        if read_file_kwargs is None: read_file_kwargs = shp_upload_read_file_kwargs
        self.read_file_kwargs:dict = read_file_kwargs
        self.require_field_crs:bool = True
        self.download_conversion = DownloadedShapeFileConversion.ShapefileProjection

    # loading a file before upload ----------------
    def read_file(self, file_path: Union[str,io.StringIO], fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        # target EPSG = EPSG used in CKAN, source EPSG read from SHP file
        gdf = gpd.read_file(file_path, **self.read_file_kwargs)
        geo_columns = list(gdf.select_dtypes('geometry'))
        for field_name in geo_columns:
            gdf.set_geometry(field_name, inplace=True)  # select the current column for geometry computations
            crs_source = gdf.crs
            if field_name in fields.keys():
                field = fields[field_name]
                field_data_type = field.data_type
                epsg_target = field.internal_attrs.epsg_target
                epsg_source_from_params = field.internal_attrs.epsg_source
            else:
                # default field data type, with a generic geometry type and the default EPSG
                epsg_target = default_ckan_target_epsg
                field_data_type = f"geometry(geometry,{epsg_target})"
                fields[field_name] = CkanField(field_name, field_data_type)  # TODO: update field data type in caller? user can change data type afterwards?
                epsg_source_from_params = None
                msg = f"PostGIS geometric destination type was not specified and will not be transmitted to CKAN. Assuming default {field_data_type}."
                warn(msg)
            if field_data_type == "geometry" or field_data_type.startswith("geometry("):  # and field.internal_attrs.geometry_as_source:
                if epsg_target is not None:
                    crs_target = pyproj.CRS.from_epsg(epsg_target)
                    if not crs_source == crs_target:
                        gdf.to_crs(crs_target, inplace=True)
                elif self.require_field_crs:
                    raise UnknownTargetCRSError(crs_source, file_path)
                if epsg_source_from_params is not None:
                    crs_source_from_params = pyproj.CRS.from_epsg(epsg_source_from_params)
                    if not crs_source_from_params == crs_source:
                        msg = f"EPSG in SHP file ({crs_source}) does not match given source EPSG ({crs_source_from_params}). The downloaded result will differ from original format."
                        warn(msg)
            else:
                raise NotImplementedError(f"Field {field_data_type} is not implemented or not compatible with geometric representations.")
        df = gdf.to_wkb(hex=True)  # converts all geometric fields to WKB and returns a standard DataFrame object
        return df

    def read_buffer(self, buffer: io.StringIO, fields: Union[Dict[str, CkanField],None]) -> Union[pd.DataFrame, ListRecords]:
        return self.read_file(buffer, fields=fields)

    # saving a file after download -------------
    def downloaded_df_to_gdf(self, df: pd.DataFrame, *, fields: Union[Dict[str, CkanField],None], context:str=None) -> gpd.GeoDataFrame:
        # NB: target EPSG = CRS in database (required), source = option to recover original CRS
        gdf = gpd.GeoDataFrame(df)
        if self.download_conversion == DownloadedShapeFileConversion.CsvWkb:
            # do not look at CRS information and leave in WKB format
            return gdf
        for field_name in df.columns:
            if field_name in fields.keys():
                field = fields[field_name]
                field_data_type = field.data_type
                if field_data_type == "geometry" or field_data_type.startswith("geometry("):
                    crs_target = pyproj.CRS.from_epsg(field.internal_attrs.epsg_target)
                    if crs_target is None and self.require_field_crs:
                        raise UnknownTargetCRSError(field.internal_attrs.epsg_source, context)
                    gdf[field_name] = gpd.geoseries.from_wkb(df[field_name], crs=crs_target)
                    if (self.download_conversion == DownloadedShapeFileConversion.ShapefileProjection
                            and field.internal_attrs.epsg_source is not None and crs_target is not None
                            and not field.internal_attrs.epsg_target == field.internal_attrs.epsg_source):
                        crs_source = pyproj.CRS.from_epsg(field.internal_attrs.epsg_source)
                        if not crs_source == crs_target:
                            gdf.to_crs(crs_source, inplace=True)
        return gdf

    def write_file(self, df: pd.DataFrame, file_path: str, fields: Union[Dict[str, CkanField],None]) -> None:
        # this writes the shp file and auxiliary shx, dbf, cpg, prj files
        gdf = self.downloaded_df_to_gdf(df, fields=fields, context=file_path)
        gdf.to_file(file_path, driver="ESRI Shapefile")

    def write_in_memory(self, df: pd.DataFrame, fields: Union[Dict[str, CkanField],None]) -> bytes:
        # how could this work because there are multiple files?
        gdf = self.downloaded_df_to_gdf(df, fields=fields)
        buffer = io.StringIO()
        gdf.to_file(buffer, driver="ESRI Shapefile")
        return buffer.getvalue().encode("utf8")

    def copy(self):
        dest = ShapeFileFormat(self.read_file_kwargs)
        dest.download_conversion = self.download_conversion
        dest.require_field_crs = self.require_field_crs
        return dest

