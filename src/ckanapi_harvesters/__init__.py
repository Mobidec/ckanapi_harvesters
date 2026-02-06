#!python3
# -*- coding: utf-8 -*-
"""
Package with helper function for CKAN requests using pandas DataFrames.
"""

# builder_file_format_version = "0.0.1"
try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:  # Python <3.8
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("ckanapi_harvesters")
except PackageNotFoundError:
    __version__ = None


import os
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


from . import auxiliary
from . import policies
from . import harvesters
from . import ckan_api
from . import builder
from . import reports

# usage shortcuts
from .auxiliary import CkanMap, RequestType
from .policies import CkanPackageDataFormatPolicy
from .ckan_api import CkanApi, CkanApiParams, CKAN_API_VERSION
from .builder import BUILDER_FILE_FORMAT_VERSION
from .builder import BuilderPackage, BuilderDataStoreMultiABC, BuilderDataStoreFolder, RequestFileMapperIndexKeys


