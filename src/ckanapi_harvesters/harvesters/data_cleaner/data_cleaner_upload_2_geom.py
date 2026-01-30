#!python3
# -*- coding: utf-8 -*-
"""
Adding support for geometries
"""
from typing import Any, Tuple, Union
from types import SimpleNamespace
import json
import re

try:
    import shapely
except ImportError:
    shapely = SimpleNamespace(Geometry=None)

try:
    import pyproj
except ImportError:
    pyproj = None

from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload_1_basic import CkanDataCleanerUploadBasic
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_errors import UnexpectedGeometryError, FormatError, CleanerRequirementError

# mapping from Postgre geometric types to GeoJSON equivalents
# This does not enable the use of MultiPoint, MultiLine and MultiPolygon
postgre_geojson_mapping = {
    "point": "Point",
    "path": "LineString",
    "polygon": "Polygon",
   }

def shapely_geometry_from_value(value:Any) -> Union[shapely.Geometry,None]:
    if shapely.Geometry is None:
        raise CleanerRequirementError("shapely", "geometry")
    if value is None:
        return None
    elif isinstance(value, shapely.Geometry):
        return value
    elif isinstance(value, str):
        if len(value) == 0:
            return None
        elif value[0] in {'{', '[', '('}:
            return shapely.from_geojson(value)
        elif re.match("[a-zA-Z]+\(.+\)", value):
            return shapely.from_wkt(value)
        elif re.match("[0-9A-F]+", value):
            return shapely.from_wkb(value)
        else:
            raise FormatError(value, "geometry")
    elif isinstance(value, dict):
        return shapely.geometry.shape(value)
    else:
        raise FormatError(value, "geometry")


class CkanDataCleanerUploadGeom(CkanDataCleanerUploadBasic):
    def __init__(self):
        super().__init__()

    def _replace_standard_value_bypass(self, value: Any, field: CkanField, *, field_data_type: str) -> Tuple[Any, bool]:
        if field_data_type == "geometry" or field_data_type.startswith("geometry("):  #  and field.internal_attrs.geometry_as_source:
            value_shape = shapely_geometry_from_value(value)
            geojson_type = field.internal_attrs.geometry_type
            if geojson_type is not None:
                assert_or_raise(value_shape.geom_type.casefold() == geojson_type.casefold(), UnexpectedGeometryError(value_shape.geom_type, geojson_type))
            if field.internal_attrs.epsg_source is not None and field.internal_attrs.epsg_target is not None:
                if not field.internal_attrs.epsg_source == field.internal_attrs.epsg_target:
                    if pyproj is None:
                        raise CleanerRequirementError("pyproj", "geometry projection")
                    crs_source = pyproj.CRS.from_epsg(field.internal_attrs.epsg_source)
                    crs_target = pyproj.CRS.from_epsg(field.internal_attrs.epsg_target)
                    transformer = pyproj.Transformer.from_crs(crs_source, crs_target, always_xy=True)
                    value_shape = shapely.transform(value_shape, transformer.transform, interleaved=False)
            return shapely.to_wkb(value_shape, hex=True), True
        elif field_data_type in postgre_geojson_mapping.keys():
            if field.internal_attrs.geometry_as_source:
                value_shape = shapely_geometry_from_value(value)
                geojson_type = postgre_geojson_mapping[field_data_type]
                assert_or_raise(value_shape.geom_type == geojson_type, UnexpectedGeometryError(value_shape.geom_type, geojson_type))
                coordinates = shapely.get_coordinates(value_shape)
                if field_data_type == "point":
                    # representation: (x,y)
                    return str(tuple(coordinates)), True
                elif field_data_type == "path":
                    # representation: [(x1,y1),...]
                    return str([tuple(point) for point in coordinates]), True
                elif field_data_type == "polygon":
                    # representation: ((x1,y1),...)
                    return str(tuple([tuple(point) for point in coordinates])), True
                else:
                    raise NotImplementedError()
            else:
                return str(value), True
        else:
            return super()._replace_standard_value_bypass(value, field, field_data_type=field_data_type)

