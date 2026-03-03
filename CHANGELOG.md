# Changelog

All notable changes to this project will be documented in this file.

When publishing a new release, copy the relevant section in GitHub Releases.


## [Unreleased]


## [0.0.10] - 2026-03-03

### Added

- Issue #3: Added an extra column `DataStore Columns Sheet` in the `resources` sheet of the Excel workbook
overriding the name of the resource (used by default). This overcomes Excel naming sheet naming limitations.
- Timeouts and requests count for multi-requests: default to 0, meaning no limitation by default.
- Metadata policy report: option to upload the messages and policy score in the dataset metadata.


## [0.0.9] - 2026-02-11

### Fixed

- pandas.csv_read: automatically detect separator using Python engine (arguments `sep=None, engine='python'`).


## [0.0.8] - 2026-02-06

First functional release of the package.

