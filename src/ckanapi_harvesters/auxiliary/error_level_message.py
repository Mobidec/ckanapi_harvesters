#!python3
# -*- coding: utf-8 -*-
"""
Functions to define messages with an error level
"""
from enum import IntEnum
from collections import OrderedDict
from warnings import warn


class ErrorLevel(IntEnum):
    Information = 0
    Warning = 1
    Error = 2

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_str(s):
        s = s.lower().strip()
        if s == "information":
            return ErrorLevel.Information
        elif s == "warning":
            return ErrorLevel.Warning
        elif s == "error":
            return ErrorLevel.Error
        else:
            raise ValueError(s)


class ErrorLevelMessage(Exception):
    def __init__(self, error_level:ErrorLevel, message: str):
        super().__init__(message)
        self.error_level: ErrorLevel = error_level
        self.message: str = message

    def to_dict(self) -> dict:
        return OrderedDict([
            ("level", str(self.error_level)),
            ("message", self.message),
        ])


class ContextErrorLevelMessage(ErrorLevelMessage):
    def __init__(self, context:str, error_level:ErrorLevel, specific_message: str):
        message = f"In {context} / {error_level.name}: {specific_message}"
        super().__init__(error_level, message)
        self.context: str = context
        self.specific_message: str = specific_message

