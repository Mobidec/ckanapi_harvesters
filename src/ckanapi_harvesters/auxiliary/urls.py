#!python3
# -*- coding: utf-8 -*-
"""
Operations on urls
"""
import urllib.parse

from ckanapi_harvesters.auxiliary.login import Login


urlsep = '/'


def is_valid_url(url:str) -> bool:
    try:
        result = urllib.parse.urlparse(url)
        return all([result.scheme, result.netloc, result.path])
    except ValueError as e:
        return False

def url_join(base:str, *args:str) -> str:
    url = base
    for arg in args:
        if len(arg) > 0:
            if not url.endswith(urlsep):
                url += urlsep
            url += arg
    return url

def url_insert_login(url:str, login:Login):
    """
    Insert user authentication parameters in a url
    """
    if login is None:
        return url
    parsed_url = urllib.parse.urlparse(url)
    netloc_with_auth = f"{login.username}:{login.password}@{parsed_url.netloc}"
    updated_url = parsed_url._replace(netloc=netloc_with_auth)
    final_url = urllib.parse.urlunparse(updated_url)
    return final_url
