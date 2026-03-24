#!python3
# -*- coding: utf-8 -*-
"""
Call existing tests
"""
import os

from ckanapi_harvesters.ckan_api import CkanApi

from ckanapi_harvesters.builder.example.builder_example_tests_offline import run as run_tests_offline
from ckanapi_harvesters.builder.example.builder_example_patch_upload import run as run_patch_upload
from ckanapi_harvesters.builder.example.builder_example_download import run as run_download


def run(ckan:CkanApi = None):
    run_tests_offline()
    run_patch_upload(ckan=ckan)
    run_download(ckan=ckan)
    print("Tests done.")


if __name__ == '__main__':
    ckan = CkanApi(None)
    ckan.initialize_from_cli_args()
    run(ckan)

