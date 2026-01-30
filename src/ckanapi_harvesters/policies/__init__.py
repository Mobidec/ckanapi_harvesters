#!python3
# -*- coding: utf-8 -*-
"""
Package to enforce CKAN data policies.
"""

POLICY_FILE_FORMAT_VERSION = "0.0.0"  # version of the data format policy file format

from . import data_format_policy_defs
from . import data_format_policy_errors
from . import data_format_policy_abc
from . import data_format_policy_lists
from . import data_format_policy_tag_groups
from . import data_format_policy_custom_fields
from . import data_format_policy

# usage shortcuts
from ckanapi_harvesters.policies.data_format_policy import CkanPackageDataFormatPolicy


