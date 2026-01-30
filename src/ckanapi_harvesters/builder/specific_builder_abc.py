#!python3
# -*- coding: utf-8 -*-
"""
Abstract class to implement specific builders from code
"""
from abc import ABC
from typing import List

from ckanapi_harvesters.auxiliary.ckan_model import CkanState
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.builder.builder_package import BuilderPackage

class SpecificBuilderABC(BuilderPackage, ABC):
    def __init__(self, ckan:CkanApi, package_name:str, organization_name:str, *,
                 title: str = None, description: str = None, private: bool = None, state: CkanState = None,
                 version: str = None,
                 url: str = None, tags: List[str] = None,
                 license_name:str=None):
        super().__init__(package_name=package_name, title=title,
                         description=description, private=private, state=state, version=version, url=url,
                         tags=tags, organization_name=organization_name, license_name=license_name)
        self.ckan_builder.from_ckan(ckan)

