#!python3
# -*- coding: utf-8 -*-
"""
Extensions of os.path and operations on urls
"""
from typing import Union, Set, List
from warnings import warn
import os
import re


disable_relative_path_constraint = False

def unlock_relative_path_constraint(value:bool=True) -> None:
    """
    This function disables relative path error messages when a relative path is required.

    :return:
    """
    global disable_relative_path_constraint
    disable_relative_path_constraint = value
    if value:
        msg = "Relative path constraint is disabled to your own risk."
        warn(msg)


class BaseDirUndefError(Exception):
    def __init__(self, path: str):
        super().__init__(f"Could not determine the file path because no base_dir was provided: {path}")

class AbsolutePathError(Exception):
    def __init__(self, field:str, path: str):
        super().__init__(f"A relative path is highly suggested for this field ({field}): {path}. To disable this error message, run unlock_relative_path_constraint().")


def sanitize_path(path:Union[str,None],
                  *, expand_path:bool=False, keyword_exceptions:Set[str]=None) -> Union[str,None]:
    """
    Sanitize paths from user inputs
    """
    if path is not None:
        if os.path.sep == '\\':
            path = re.sub(r"[\\/]", "\\\\", path)
        else:
            path = re.sub(r"[\\/]", os.path.sep, path)
        if expand_path:
            path = os.path.expandvars(path)
            path_keyword = path.lower().strip()
            if path_keyword in keyword_exceptions:
                return path_keyword
            else:
                path = os.path.expanduser(path)  # cover the case where the path starts with '~'
        return path
    else:
        return None


def path_rel_to_dir(path:Union[str,None], base_dir:str=None, *, keyword_exceptions:Set[str]=None,
                    error_base_dir_undef:bool=False, default_value:str=None,
                    only_relative:bool=False, abs_error:bool=False, field:str=None) -> Union[str,None]:
    """
    Returns the absolute path. If relative, the base directory can be specified. If not specified, the cwd is used.

    :param path: original path string
    :param base_dir: the base directory, for relative paths if provided (default = cwd)
    :param keyword_exceptions: some values are not replaced and must be treated after this function call.
    :param error_base_dir_undef: Option to raise an error if no base_dir was provided (cwd is used by default).
    :param default_value: the value to return if path is None.
    :param only_relative: If set to True, a warning or error message is raised if an absolute path is provided.
    :param abs_error: Condition to choose between a warning or an error message.
    :param field: name of the field for the error message.
    :return: absolute path or keyword
    """
    if keyword_exceptions is None:
        keyword_exceptions = set()
    path_src = path
    path = sanitize_path(path)
    if path is None:
        return default_value
    else:
        path = os.path.expandvars(path)  # replace environment variables mentioned in the path, if any
        path_keyword = path.lower().strip()
        if path_keyword in keyword_exceptions:
            return path_keyword
        else:
            path = os.path.expanduser(path)  # cover the case where the path starts with '~'
            if os.path.isabs(path):
                if only_relative:
                    msg = AbsolutePathError(field, path_src)
                    if abs_error:
                        raise msg
                    else:
                        warn(str(msg))
                return path.strip()
            elif base_dir is not None:
                return os.path.join(base_dir, path.strip())
            elif error_base_dir_undef:
                raise BaseDirUndefError(path)
            else:
                return os.path.abspath(path)


def resolve_rel_path(base_dir:str, rel_path:str, *args: str, field:str, only_relative:bool=True) -> str:
    """
    Alias to path_rel_to_dir, with arguments order similar to os.path.join and requirement for a relative path.
    Relative path verification can be removed by calling unlock_relative_path_constraint.
    field: name of the field for the error message.

    :return:
    """
    if len(args) > 0:
        rel_path = os.path.join(rel_path, *args)
    return path_rel_to_dir(rel_path, base_dir=base_dir, field=field,
                           only_relative=only_relative, abs_error=not disable_relative_path_constraint,
                           error_base_dir_undef=True)


# reverse function:
def make_path_relative(path:str, to_base_dir:str = None, *, default_value:str=None,
                       source_string:str=None, keyword_exceptions:Set[str]=None, same_destination:bool=True) -> str:
    """
    When you save a file to a new location, make relative paths relative to the new file location,
    pointing to the same destination (except if same_destination is False -> source_string is used in this case, if present and relative path)
    The source_string is the path present in the original document.

    :param path: full file path (absolute, ideally output from path_rel_to_dir)
    :param to_base_dir: the new base directory, to derive the relative paths from
    :param default_value: the value to return if the path is None
    :param source_string: string representing the path in the original document, without any treatments
    :param keyword_exceptions: keywords to return as-is
    :return: path relative to to_base_dir or keyword/path relative to environment variable/home directory symbol (~)
    """
    if path is None:
        return default_value
    if source_string is not None:
        if (not same_destination) and (not os.path.isabs(source_string)):
            return source_string
        if keyword_exceptions is None:
            keyword_exceptions = set()
        source_string = sanitize_path(source_string)
        source_keyword = source_string.lower().strip()
        if source_keyword in keyword_exceptions:
            return source_keyword
        elif source_string.startswith('~') or source_string.startswith('$'):
            return source_string
        # elif not (os.path.expanduser(source_string) == source_string or os.path.expandvars(source_string) == source_string):
        #     # condition to confirm
        #     return source_string
    if path.startswith('~') or path.startswith('$'):
        return path
    else:
        return path if to_base_dir is None else os.path.relpath(path, to_base_dir)

# File search
def list_files_scandir(path:str) -> List[str]:
    # see also: glob.glob - this does not apply any filter
    list_files = []
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_file():
                list_files.append(entry.path)
            elif entry.is_dir():
                list_files += list_files_scandir(entry.path)
        entries.close()
    return list_files

glob_chars = r"*?![]"
glob_re = "[\\*\\?\\!\\[\\]]+"

def glob_rm_glob(glob_str:str, *, default_rec_dir:str=None) -> str:
    """
    Extract directory name from a glob string (first elements of path without glob characters).

    :param glob_str: the glob string
    :param default_rec_dir: if the last removed element is "**" (directory recursion), the name of the directory to use instead
    :return: a path without glob characters

    Examples:
    >>> glob_rm_glob(r"test\*.csv")
    'test'

    >>> glob_rm_glob(r"**\*.csv", default_rec_dir="hello")
    'hello'
    """
    glob_free = glob_str
    while re.search(glob_re, glob_free):
        glob_free, glob_sub = os.path.split(glob_free)
        if glob_sub == "**" and re.search(glob_re, glob_free) is None and default_rec_dir is not None:
            return os.path.join(glob_free, default_rec_dir)
    return glob_free

def glob_name(glob_str:str):
    """
    Extract file name glob from a glob string (last element of path, except if it is "**")

    :param glob_str:
    :return:

    Example:
    >>> glob_name(r"**\*.csv")
    '*.csv'
    """
    glob_dir, glob_file = os.path.split(glob_str)
    if not glob_file == "**":
        return glob_file
    else:
        return ""

