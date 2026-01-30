#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing
"""
from typing import List, Tuple
from warnings import warn
from collections import OrderedDict

from ckanapi_harvesters.auxiliary.error_level_message import ErrorLevelMessage, ErrorLevel


class DataPolicyError(ErrorLevelMessage):
    def __init__(self, context:str, error_level:ErrorLevel, policy_message: str):
        message = f"In {context} / Data format policy {error_level.name}: {policy_message}"
        super().__init__(error_level, message)
        self.context: str = context
        self.specific_message: str = policy_message

    def to_dict(self) -> dict:
        return OrderedDict([
            ("level", str(self.error_level)),
            ("context", self.context),
            ("message", self.specific_message),
        ])


class UnsupportedPolicyVersionError(Exception):
    def __init__(self, file_version):
        super().__init__(f"Version error: policy file version {file_version} is not supported")

class UrlPolicyLockedError(Exception):
    def __init__(self, url):
        super().__init__(f"Url is not allowed a policy definition - feature locked (url: {url})")

def _policy_msg(msg:DataPolicyError, *, error_level:ErrorLevel, buffer:List[DataPolicyError], verbose:bool) -> None:
    if buffer is not None:
        buffer.append(msg)
    elif error_level == ErrorLevel.Information and verbose:
        print(str(msg))
    elif error_level == ErrorLevel.Warning:
        msg = str(msg)
        warn(msg)
    elif error_level == ErrorLevel.Error:
        raise msg


class ErrorCount:
    def __init__(self, messages_list:List[DataPolicyError]):
        self.messages_list:List[DataPolicyError] = messages_list
        self.information:int = 0
        self.warning:int = 0
        self.error:int = 0
        self.total:int = len(messages_list)
        for message in messages_list:
            if message.error_level == ErrorLevel.Information:
                self.information += 1
            elif message.error_level == ErrorLevel.Warning:
                self.warning += 1
            elif message.error_level == ErrorLevel.Error:
                self.error += 1

    def error_count_message(self) -> str:
        if self.total == 0:
            return "All tests passed"
        else:
            return f"{self.error} errors, {self.warning} warnings, {self.information} messages"

    def __str__(self) -> str:
        return "ErrorCount: " + self.error_count_message()

    def __add__(self, other):
        return ErrorCount(self.messages_list + other.messages_list)

    def to_tuple(self) -> Tuple[int, int, int]:
        return (self.error, self.warning, self.information)

    def to_dict(self) -> dict[str,int]:
        return OrderedDict([("errors", self.error), ("warnings", self.warning), ("information", self.information)])
