#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server, with one thread per resource
"""
from typing import List, Union, Dict, Callable, Any
import threading
import copy

from ckanapi_harvesters.auxiliary.ckan_model import CkanState
from ckanapi_harvesters.auxiliary.ckan_progress_callbacks import CkanProgressCallback
from ckanapi_harvesters.builder.builder_package_2_harvesters import BuilderPackageWithHarvesters
from ckanapi_harvesters.builder.builder_resource_multi_abc import BuilderMultiABC
from ckanapi_harvesters.ckan_api import CkanApi


class BuilderPackageMultiThreaded(BuilderPackageWithHarvesters, BuilderMultiABC):
    def __init__(self, package_name: str = None, *, package_id: str = None,
                 title: str = None, description: str = None, private: bool = None, state: CkanState = None,
                 version: str = None,
                 url: str = None, tags: List[str] = None,
                 organization_name: str = None, license_name: str = None):
        super().__init__(package_name=package_name, package_id=package_id,
                         title=title, description=description, private=private, state=state, version=version,
                         url=url, tags=tags, organization_name=organization_name, license_name=license_name)
        # BuilderMultiABC:
        self.progress_callback = CkanProgressCallback()
        self.stop_event = threading.Event()
        self.thread_ckan: Dict[str, CkanApi] = {}
        self.enable_multi_threaded_upload:bool = True
        self.enable_multi_threaded_download:bool = True

    def copy(self, dest=None) -> "BuilderPackageWithHarvesters":
        if dest is None:
            dest = BuilderPackageWithHarvesters()
        super().copy(dest=dest)
        dest.progress_callback = self.progress_callback.copy()
        dest.enable_multi_threaded_upload = self.enable_multi_threaded_upload
        dest.enable_multi_threaded_download = self.enable_multi_threaded_download
        return dest

    # TODO: implement abstract methods


