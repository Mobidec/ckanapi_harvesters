#!python3
# -*- coding: utf-8 -*-
"""
This implements functionality to dynamically call functions specified by the user.
This functionality is disabled by default. You must call unlock_external_code_execution to enable external code execution.
__Warning__:
only run code if you trust the source!
"""
from typing import Callable
import os
import re
from warnings import warn
import importlib
import importlib.util

from ckanapi_harvesters.auxiliary.path import path_rel_to_dir



def unlock_external_code_execution(value:bool=True) -> None:
    """
    This function enables external code execution for the PythonUserCode class.

    __Warning__:
    only run code if you trust the source!

    :return:
    """
    PythonUserCode.enable_external_code = value
    if value:
        msg = "External code is enabled. Only run code if you trust the source!"
        warn(msg)


var_name_subs_re = '\W|^(?=\d)'

def clean_var_name(variable_name: str) -> str:
    return re.sub(var_name_subs_re,'_', variable_name)


class ExternalUserCodeDisabledException(Exception):
    def __init__(self, function_name:str, source_file:str) -> None:
        super().__init__(f"{function_name} in {source_file} cannot be executed because the external code execution is locked. Use unlock_external_code_execution to unlock. Warning: Only run code if you trust the source!")


class PythonUserCode:
    """
    This class imports an arbitrary Python file as a module and makes it available to the rest of the code.
    This functionality is disabled by default. You must call unlock_external_code_execution to enable external code execution.

    __Warning__:
    only run code if you trust the source!
    """
    enable_external_code = False  # remain False to ensure no custom code is executed from the builder specification

    def __init__(self, python_file:str, base_dir:str=None):
        self.python_file: str = ""
        if python_file is None:
            # only use None argument to initialize copy
            self.python_file = ""
        else:
            self.python_file = path_rel_to_dir(python_file, base_dir=base_dir)
        self.module = None
        self.imported_module_name: str = ""
        if PythonUserCode.enable_external_code and python_file is not None:
            module_base = clean_var_name(os.path.splitext(os.path.split(self.python_file)[1])[0])
            self.imported_module_name = "ckan_builder_aux_funcs__" + module_base
            spec = importlib.util.spec_from_file_location(self.imported_module_name, self.python_file)
            self.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.module)
        elif python_file is not None:  # and not enable_external_code
            # External python script execution is locked
            raise ExternalUserCodeDisabledException("code", os.path.split(self.python_file)[1])

    def __copy__(self):
        return self.copy()

    def copy(self) -> "PythonUserCode":
        # do not execute module twice and do not make copies either
        dest = PythonUserCode(python_file=None, base_dir=None)
        dest.module = self.module
        dest.imported_module_name = self.imported_module_name
        dest.python_file = self.python_file
        return dest

    def function_pointer(self, function_name:str) -> Callable:
        """
        Obtain function pointer for a given name in the loaded Python module.

        :param function_name:
        :return:
        """
        if not PythonUserCode.enable_external_code:
            raise ExternalUserCodeDisabledException(function_name, os.path.split(self.python_file)[1])
        fun = getattr(self.module, function_name)
        assert(isinstance(fun, Callable))
        return fun

