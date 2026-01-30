CKAN API client package ckanapi_harvesters
=====

## Overview

This package enables users to benefit from the CKAN API and provides functions which
realize complex API calls to achieve specific operations.
In this package, DataStores are returned/inputted as pandas DataFrames.
The underlying request mechanism uses the requests Session object, which improves performance with multiple requests.
This package is oriented in the management of CKAN datasets and resources. 
Only a selection of API calls has been implemented in this objective.
To perform custom API calls, the function `api_action_call` is provided to the end user.
This package was initially designed to harvest a large DataStores from your local file system.
It also implements particular requests which can define a large DataStore.
Large datasets composed of multiple files can be uploaded/downloaded 
through scripts into a single resource or multiple resources. 
For a DataStore, large files are uploaded with a limited number of rows per request.  

The package is divided in the following sections:
- `ckan_api`: functions interacting with the CKAN API. 
  In addition to the base class which manages basic parameters and requests, API functions are divided as follows:
  1) functions to map the CKAN packages and resources. The remote data structures are mapped in a mirrored data structure.
     CKAN DataStore information, organizations, licenses and resource views are optionally tracked.
  2) functions to query a DataStore or to download file resources.
  3) functions to apply a test a data format policy on a given package.
  4) functions to upsert data to a DataStore or to upload files to a resource.
  5) functions to manage CKAN objects 
     (creating, patching, or removing packages, resources, and DataStores).
     These functions enable the user to change the metadata for these objects.
     The other objects are meant to be managed through the API. 
- `policies`: functions to check data format policies. A data format policy defines which attributes 
  are mandatory for a package or resource. 
  Specific rules can be implemented to restrict   package tags to certain lists, 
  grouped by [vocabulary](https://docs.ckan.org/en/2.9/maintaining/tag-vocabularies.html).
  Extra key-pair values of packages can be enforced. Resource formats can be restricted to a certain list.
- `reports`: functions to extract a report on the CKAN database in order to monitor 
  package user access rights, resource memory occupation, modification dates and data format policy messages. 
- `harvesters`: this module implements ways to load data from your local machine. 
  - `file_formats`: The primary approach is to use files on you local file system. The CSV and SHP (shape file) formats are currently supported. 
  - In addition to the file formats, harvesters have been implemented to transfer data from a database. 
  This is particularly useful if the database cannot be accessed by CKAN harvester extensions 
  because it would only be available locally. MongoDB and PostgreSQL databases are currently supported.
- `builder`: functions to automate package and resource metadata patching and data uploads or downloads.
  These parameters can be defined in an Excel workbook and files from the local file system can be referred as inputs for the data upload.
  The parameters can also be deduced from an online CKAN package through the API.
  - Example scripts are given in this module, referring to an example Excel workbook.
    The Excel workbook is available in the package and at this link:
    [builder_package_example.xlsx](../../src/ckanapi_harvesters/builder/builder_package_example.xlsx)
    See also the notebook example in the current documentation here: 
    [builder_example_notebook.ipynb](../notebooks/builder_example_notebook.ipynb).

