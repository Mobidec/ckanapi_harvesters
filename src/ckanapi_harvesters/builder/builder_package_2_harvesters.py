#!python3
# -*- coding: utf-8 -*-
"""
Code to initiate a package builder from a Dataset harvester
"""
from typing import List

from ckanapi_harvesters.builder.builder_package_1_basic import BuilderPackageBasic
from ckanapi_harvesters.auxiliary.ckan_model import CkanState
from ckanapi_harvesters.builder.builder_resource_datastore_multi_harvester import BuilderDataStoreHarvester
from ckanapi_harvesters.harvesters.harvester_abc import DatasetHarvesterABC


class BuilderPackageWithHarvesters(BuilderPackageBasic):
    @staticmethod
    def init_from_harvester(dataset_harvester: DatasetHarvesterABC) -> "BuilderPackageWithHarvesters":
        builder = BuilderPackageWithHarvesters()
        params = dataset_harvester.params
        builder.package_name = f"harvest_{params.harvest_method}_{params.database}_{params.dataset}".lower()
        builder.package_attributes.title = f"Harvest result of {params.harvest_method} / database {params.database} / dataset {params.dataset}"
        builder.package_attributes.description = f"Harvested from {params.url} / database {params.database} / dataset {params.dataset}"
        builder.package_attributes.private = True
        # builder.package_attributes.state = CkanState.Draft
        tables: List[str] = dataset_harvester.list_tables(return_metadata=False)
        for table_name in tables:
            table_harvester = dataset_harvester.get_table_harvester(table_name)
            resource_builder = BuilderDataStoreHarvester()
            resource_builder.name = table_name
            resource_builder.harvester = table_harvester
            if resource_builder.description is None:
                resource_builder.description = f"dataset {params.dataset} / table {table_name}"
            # metadata is imported after a clean of metadata
            builder.resource_builders[table_name] = resource_builder
        return builder

    def copy(self, dest=None) -> "BuilderPackageWithHarvesters":
        if dest is None:
            dest = BuilderPackageWithHarvesters()
        super().copy(dest=dest)
        return dest
