#!python3
# -*- coding: utf-8 -*-
"""
Section of the package dedicated to the initialization of a CKAN package
"""

BUILDER_FILE_FORMAT_VERSION = "0.0.0"  # version of the Excel & JSON file format

from . import builder_aux
from . import builder_errors
from . import builder_field
from . import builder_resource
from . import builder_resource_multi_abc
from . import builder_resource_multi_file
from . import mapper_datastore
from . import builder_resource_datastore
from . import builder_resource_multi_datastore
from . import builder_resource_datastore_url
from . import builder_resource_datastore_unmanaged
from . import mapper_datastore_multi
from . import builder_resource_datastore_multi_abc
from . import builder_resource_datastore_multi_folder
from . import builder_resource_datastore_multi_harvester
from . import builder_resource_init
from . import builder_ckan
from . import builder_package_1_basic
from . import builder_package_2_harvesters
from . import builder_package_3_multi_threaded
from . import builder_package

from . import specific
from . import example

# usage shortcuts
from .builder_package import BuilderPackage
from .mapper_datastore_multi import RequestFileMapperIndexKeys
from .builder_resource_datastore_multi_abc import BuilderDataStoreMultiABC
from .builder_resource_datastore_multi_folder import BuilderDataStoreFolder


