#!python3
# -*- coding: utf-8 -*-
"""
Section of the package dedicated to the conversion of records to a CKAN-compatible format.
This is linked to the data harvesters.
"""

from . import data_cleaner_errors
from . import data_cleaner_abc
from . import data_cleaner_upload_1_basic
from . import data_cleaner_upload_2_geom
from . import data_cleaner_upload

# usage shortcuts
from ckanapi_harvesters.harvesters.data_cleaner.data_cleaner_upload import CkanDataCleanerUpload


