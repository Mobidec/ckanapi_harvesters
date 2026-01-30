#!python3
# -*- coding: utf-8 -*-
"""
Dead code from auxiliary functions
"""

from enum import IntEnum

import numpy as np


class CkanBasicDataFieldType(IntEnum):
    Default = 0  # no override
    Text = 1
    Numeric = 2
    TimeStamp = 3

    def __str__(self):
        if self == CkanBasicDataFieldType.Default:
            return ""
        else:
            return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "text":
            return CkanBasicDataFieldType.Text
        elif s == "numeric":
            return CkanBasicDataFieldType.Numeric
        elif s == "timestamp":
            return CkanBasicDataFieldType.TimeStamp
        elif s == "" or np.isnan(s):
            return CkanBasicDataFieldType.Default
        else:
            raise ValueError(s)

class CkanCollaboratorCapacity(IntEnum):
    """
    Collaboration capacities of users associated to a package/dataset
    """
    Excluded = 0
    Member = 1
    Editor = 2

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "excluded":
            return CkanCollaboratorCapacity.Excluded
        elif s == "member":
            return CkanCollaboratorCapacity.Member
        elif s == "editor":
            return CkanCollaboratorCapacity.Editor
        else:
            raise ValueError(s)

class CkanGroupCapacity(IntEnum):
    """
    Capacities of users in a group
    """
    Excluded = 0
    Member = 1
    Admin = 3

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "excluded":
            return CkanGroupCapacity.Excluded
        elif s == "member":
            return CkanGroupCapacity.Member
        elif s == "admin":
            return CkanGroupCapacity.Admin
        else:
            raise ValueError(s)
