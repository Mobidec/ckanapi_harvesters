# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

When publishing a new release, copy the relevant section on the [Github release page](https://github.com/Mobidec/ckanapi_harvesters/releases).


## [Unreleased] - 2026-03-04


### Added

- Support for reading CSV files by chunks using pandas.read_csv `chunksize` argument (only for DataStores). 
This modification is compatible with multi-threading and returns a more accurate progression indicator.

### Changed

- Auxiliary functions in resource builder classes have been refactored.
- The `progress_callback` function arguments have been renamed into
```python
def default_progress_callback(position:int, total:int, info:Any=None, *, context:str=None,
                              file_index:int=None, file_count:int=None, end_message:bool=False, **kwargs):
    ...
```


## [0.0.10] - 2026-03-03

### Added

- Issue #3: Added an extra column `DataStore Columns Sheet` in the `resources` sheet of the Excel workbook
overriding the name of the resource (used by default). This overcomes Excel naming sheet naming limitations.
- Timeouts and requests count for multi-requests: default to 0, meaning no limitation by default.
- Metadata policy report: option to upload the messages and policy score in the dataset metadata custom fields.


## [0.0.9] - 2026-02-11

### Fixed

- pandas.read_csv: automatically detect separator using Python engine (arguments `sep=None, engine='python'`).


## [0.0.8] - 2026-02-06

First functional release of the package.

