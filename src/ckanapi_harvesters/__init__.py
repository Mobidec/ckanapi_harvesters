#!python3
# -*- coding: utf-8 -*-
"""
ckanapi_harvesters
Package with helper function for CKAN requests using pandas DataFrames.
"""

time_package_loading = False

if time_package_loading:
    import time
    start_time = time.time()

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:  # Python <3.8
    from importlib_metadata import version, PackageNotFoundError

if time_package_loading:
    importlib_time = time.time()

try:
    __version__ = version("ckanapi_harvesters")
except PackageNotFoundError:
    __version__ = None

if time_package_loading:
    version_time = time.time()


import os
package_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

if time_package_loading:
    package_dir_time = time.time()

from . import auxiliary
from . import policies
from . import harvesters
from . import ckan_api
from . import builder
from . import reports

# usage shortcuts
from .auxiliary import (CkanMap, RequestType, CkanField, CkanState, CkanProgressCallback, CkanCallbackLevel,
                        CkanProgressBarType, CkanProgressCallbackEmpty)
from .policies import PackagePolicyReport, CkanPackageDataFormatPolicy
from .reports import CkanAdminReport
from .ckan_api import CkanApi, CkanApiParams, CKAN_API_VERSION
from .builder import BUILDER_FILE_FORMAT_VERSION
from .builder import BuilderPackage, BuilderResourceABC, BuilderDataStoreABC, BuilderDataStoreMultiABC, BuilderDataStoreFolder, RequestFileMapperIndexKeys


if time_package_loading:
    final_time = time.time()

    version_duration = version_time - start_time
    package_dir_duration = package_dir_time - version_time
    real_load_duration = final_time - package_dir_time
    overall_duration = final_time - start_time
    print(f"Package ckanapi_harvesters loaded in {overall_duration} seconds ({real_load_duration} itself, {package_dir_duration} for package_dir, {version_duration} for version)")
    # import time; start_time = time.time(); import ckanapi_harvesters; print(f"import done in {time.time() - start_time} seconds")
