#!python3
# -*- coding: utf-8 -*-
"""
Helper functions used externally (not used in current package)
"""
import re

def replace_prefix(s: str, prefix_old: str, prefix_new: str, flags: int = 0) -> str:
    """
    Replace the prefix `prefix_old` with `prefix_new` only if `s` starts with `prefix_old`.
    If `prefix_old` is empty, return `s` unchanged.
    `flags` can be used to pass re.IGNORECASE, etc.
    """
    if not prefix_old:
        return s
    elif not s:
        return s
    pattern = re.compile(r'^' + re.escape(prefix_old), flags)
    return pattern.sub(prefix_new, s, count=1)

