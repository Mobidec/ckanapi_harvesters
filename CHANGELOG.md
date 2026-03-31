# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

When publishing a new release, copy the relevant section on the [Github release page](https://github.com/Mobidec/ckanapi_harvesters/releases).


## [Unreleased] - 2026-03-31


## [0.0.22] - 2026-03-31

### Fixed

- Data format policy checks for mandatory attributes.


## [0.0.21] - 2026-03-31

### Changed

- `package_search_all` is no longer limited to `ckan.owner_org` when argument `owner_org` is `None`.

### Fixed

- `package_search_all` now includes draft datasets by default (option `include_drafts`).
- `ckan.owner_org` is not applied when patching an existing package without specifying the `owner_org` argument.
- Raise an error if a CLI argument is not recognized in the resource __Options__ field.

### Added

- _Assist_ mode __Data Cleaner__ which detects field types which were not specified. 
This feature is for debug use as it is time-consuming.
- Option `inhibit_datastore_patch_indexes` to ignore primary key and indexes arguments when calling datastore_create 
on an existing dataset (patch mode). This argument has been routed up to package builder `patch_request_full`.


## [0.0.20] - 2026-03-27

### Fixed

- Package builder removed custom field metadata which were already present on CKAN side.


## [0.0.19] - 2026-03-27

### Added

- Database harvesters can display a description message.


## [0.0.18] - 2026-03-27

### Fixed

- Parent/child link between package and resource builders is now explicit, which enables direct operations on resource builders.
- When the URL in the CKAN API key file does not match CKAN URL, the API key is cleared with a warning message. 
Raising an error was not compatible with auto-loading the API key file.


## [0.0.17] - 2026-03-25

### Fixed

- Bug ignoring __Primary key__ specified in the Excel workbook from ckanapi-harversters 
[version 0.0.13 (18/03/2026, commit a8dd7b32)](#0013---2026-03-18). 
`py_upload_index` was created even though a primary key was specified.
- Reduced time to import the Python package by using lazy imports for optional dependencies.

### Changed

- Function headers for DataStore reading API calls (`datastore_search`, `datastore_dump`, `datastore_search_sql`)
and the `page_generator` and `cursor` variants. The latter has the default argument `search_all=True` changed.
The limitation of the number of requests is managed by the user. 

### Added

- If the first line of the API key file is an URL, it is checked against the URL of the CKAN server.
- Added charset=utf-8 in the Content-type HTTP header to ensure characters are correctly interpreted by CKAN server.
- Added support for `tqdm` progress bar appearing by default in the console output, if package is installed.


## [0.0.16] - 2026-03-23

### Added

- Functions to initiate a sample package from an existing CKAN package builder: 
`setup_sample_package` and `download_sample`. An example was added to document usage.

### Changed

- DataStore resource builder function `download_sample_df` removes the `_id` column by default. 


## [0.0.15] - 2026-03-23

### Added

- Option `--include-source-file` to add a column `py_source_file` naming the file each line of a DataStore originates from.

### Changed

- Documentation format issues.
- Changed default JSON file reading option to not expect one document per line.


## [0.0.14] - 2026-03-23

### Changed

- Minimum Python version requirement was changed from 3.10 to 3.12 for compatibility issues with certain dependencies.
- Dependency versions were changed to minimum values. Missing optional dependency `sqlalchemy` for PostgreSQL database harvesting was added. 
- User-defined upload function prototypes: keyword argument `file_name` has been renamed `file_query`. 
Argument `file_name` still works but is marked as deprecated.


## [0.0.13] - 2026-03-18

NB: version 0.0.12 was not successfully published because of a wrong test configuration. This version replaces it.

### Added

- Support for reading CSV files by chunks using pandas.read_csv `chunksize` argument (only for DataStores). 
This modification is compatible with multi-threading and returns a more accurate progression indicator.
It is enabled by default. To disable it, use the `allow_chunks=False` argument or enter `--no-chunks` in the __Options__ column of the resources worksheet.
- File formats:
  - Added CLI argument `--read-kwargs`/`--write-kwargs` in the __Options__ column to customize the arguments for the read/write functions.
  e.g. for CSV file format, the arguments of the `pandas.read_csv` function can be changed with the following 
  CLI argument `--read-kwargs compression=gzip,header=10`.
  - Added support for the following file formats: Excel (xls, xlsx, xlsm, xlsb, odf, ods, odt), JSON. 
  - Support for user-defined file format loading functions with new Excel columns __Read function__ and __Write function__ per resource.
  See documentation for function prototypes.
- Excel builder workbook:
  - Extra column __Data cleaner__ per DataStore to activate a function which corrects values according to the destination type specified in the fields metadata (only for uploads). 
  - CKAN upload parameterization fields (_ckan_ sheet):
    - Field __Limit__ to change the number of rows sent per request.
    - Field __Time between requests__ to change the delay between each request (upload/download), in seconds.
    - Field __Thread count__ to change the default number of threads used for large datasets (upload/download).
- The upload data cleaner can be used to replace empty string values with None for columns indicated as numeric in the field metadata.
- The admin report can output the storage space used by a dataset in custom fields defined in the data format policy JSON.
- Additional default location for the CKAN API key file: `~/.config/__CKAN_API_KEY__.txt`.
- Automatically re-attempt failed API calls with a delay for certain HTTP error codes. This robustifies the scripts when the CKAN server is overloaded.

### Changed

- Datasets are uploaded in ___Draft___ state by default. When the upload is finished, the state specified in the Excel workbook is applied.
Datasets with ___Draft___ state are visible by clicking on your profile name at the top of the CKAN web interface (if you originally created it).
  - Upon dataset creation, if was found in ___Deleted___ state, its ressources are deleted. 
- Primary key when uploading to a DataStore:
  - When no primary key is specified, the program adds a new column named `py_upload_index` as a primary key. 
  - The default behavior when a multi-column primary key has changed. It is specified is to systematically upsert all records
  (update existing, add new primary key combinations). 
  The mode `--one-frame-per-primary-key` checks the last record corresponding to the columns designated by the `--group-by` argument. 
  It expects that an input file represents all the data for a group-by key combination. The group-by key must be a subset of the primary key.
  This was the default mode used for previous versions of the package. 
- The output fields generated by the admin report and data format policy are not None anymore. 
The JSON parameters for this feature were renamed.
- The `progress_callback` function keyword arguments have been renamed.
- Implementation:
  - Auxiliary functions in resource builder classes have been refactored.
  - `BuilderDataStoreFile` now inherits from `BuilderDataStoreFolder` to support multi-threading of a file reading by chunks. 
  Function `BuilderDataStoreFolder.from_file_datastore` has been moved to `BuilderDataStoreFile.to_builder_datastore_folder`.

### Deprecated

- Excel workbook:
  - The attribute for the package name used in its URL was renamed from __Name__ to __Name in URL__. 
  Attribute __Name__ still functions but is marked as deprecated.


## [0.0.11] - 2026-03-09  [YANKED]

### Fixed

- Data cleaner could mix up values between lines, if the lines were sorted previously. 
Lines could be sorted by the last column of the primary key, if defined.
The data cleaner was used only for MongoDB/PostgreSQL imports or files with geometries.


## [0.0.11] - 2026-03-09  [YANKED]

### Fixed

- Data cleaner could mix up values between lines, if the lines were sorted previously. 
Lines could be sorted by the last column of the primary key, if defined.
The data cleaner was used only for MongoDB/PostgreSQL imports or files with geometries.


## [0.0.10] - 2026-03-03

### Added

- Issue #3: Added an extra column __DataStore Columns Sheet__ in the ___resources___ sheet of the Excel workbook
overriding the name of the resource (used by default). This overcomes Excel naming sheet naming limitations.
- Timeouts and requests count for multi-requests: default to 0, meaning no limitation by default.
- Metadata policy report: option to upload the messages and policy score in the dataset metadata custom fields.


## [0.0.9] - 2026-02-11

### Fixed

- pandas.read_csv: automatically detect separator using Python engine (arguments `sep=None, engine='python'`).


## [0.0.8] - 2026-02-06

First functional release of the package.

