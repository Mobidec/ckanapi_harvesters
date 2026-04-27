#!python3
# -*- coding: utf-8 -*-
"""
Policy report return argument
"""
from typing import Dict, List
from collections import OrderedDict

from ckanapi_harvesters.policies.data_format_policy_errors import DataPolicyError, ErrorCount


class PackagePolicyReport:
    def __init__(self, package_name:str):
        self.package_name: str = package_name
        self.messages: List[DataPolicyError] = []
        self.error_count: ErrorCount = None
        self.success: bool = False

class PolicyReport:
    def __init__(self):
        self.package_reports:Dict[str, PackagePolicyReport] = OrderedDict()


