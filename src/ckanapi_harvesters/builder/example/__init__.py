#!python3
# -*- coding: utf-8 -*-
"""
Section of the package dedicated to the initialization of a CKAN package
"""

import os

# usage shortcuts
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
example_package_dir = os.path.join(self_dir, "package")
from ..builder_package import example_package_xls

from . import builder_example
from . import builder_example_aux_fun
from . import builder_example_generate_data
from . import builder_example_patch_upload
from . import builder_example_tests
from . import builder_example_policy
from . import builder_example_download

