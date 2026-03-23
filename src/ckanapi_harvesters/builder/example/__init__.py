#!python3
# -*- coding: utf-8 -*-
"""
Section of the package dedicated to the initialization of a CKAN package
"""

import os

# usage shortcuts
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
from ..builder_package import example_package_xls, example_package_resources_dir

from . import builder_example
# from . import builder_example_aux_fun
from . import builder_example_generate_data
from . import builder_example_patch_upload
from . import builder_example_sample_dataset
from . import builder_example_tests_dev
from . import builder_example_test_sql
from . import builder_example_tests_offline
from . import builder_example_policy
from . import builder_example_download
from . import builder_example_tests

