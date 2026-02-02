# ckanapi_harvesters

<img src="https://raw.githubusercontent.com/Mobidec/ckanapi_harvesters/refs/heads/main/doc/assets/France2030-Logo-1024x576.png" alt="logo">

---

## Description

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
    [builder_package_example.xlsx](src/ckanapi_harvesters/builder/builder_package_example.xlsx)
    See also the notebook example in the current documentation here: 
    [builder_example_notebook.ipynb](sphinx/notebooks/builder_example_notebook.ipynb).


## Github Pages

Your automaticly generated documentation (with Sphinx) is [Here](https://mobidec.github.io/ckanapi_harvesters/)

## Python Package Template Architecture


```
.
├── sphinx
│   ├── conf.py
│   │    └── Sphinx documentation configuration file
│   └── index.rst
│        └── Root file for Sphinx documentation, structuring and linking source documents into complete documentation.
├── src
│   └── ckanapi_harvesters
│        ├── __init__.py
│        ├── main.py
│        │    └── Main file of your package, it references what is usable in your package
│        └── module_name
│             ├── __init__.py
│             └── module.py
│                  └── Module file, each module holds a logic of the package
├── tests
│    └── Directory for testing the package and verifying that everything works
├── .gitattributes
│    └── Ensures that all text files use LF as the line ending, improving consistency across different development environments.
├── .bumpversion.toml
│    └── Configuration file for bumping the package version
├── .gitignore
│    └── File explicitly instructed for Git to ignore
├── .github
│    └── workflows
│         └── Github Ci/CD files
├── .pre-commit-config.yaml
│    └── Pre-commit configuration file
├── CONTRIBUTING.md
│    └── Contribution guidelines file
├── LICENSE
├── README.md
│    └── File with general information about the project
├── pyproject.toml
│    └── Package configuration file
└── tox.ini
     └── Configuration file for `tox`, used to automate testing and linting tasks across multiple Python environments. This file is configured to use Python 3.10 and runs commands for the linter `ruff` as well as for tests with `pytest`. The specified commands check code style, format files according to defined standards, and run unit tests to ensure the code works as expected. This file is also used to facilitate version management tasks with `bump-my-version`.
```

## Getting Started

### Prerequisites

This project requires **Python 3.10**. Python 3.10 introduces many new features and improvements that are essential for the proper functioning of this project. Ensure that Python is correctly installed on your system by running `python --version`.

### About the `pyproject.toml` File

The `pyproject.toml` file is a central configuration file for the Python project. It contains TOML tables specifying the basic metadata of the project, the dependencies needed to build your project, and specific configurations for the tools used.
The `[project]` table is used to specify the basic metadata of your project, such as dependencies, your name, etc. The `[tool]` table contains sub-tables specific to each tool, such as `[tool.setuptools]` or `[tool.ruff]`. For more information on configuring your `pyproject.toml`, refer to the [Python documentation](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/).

### Installing Dependencies

The `pyproject.toml` file is used to manage the dependencies of this project. To install these dependencies, follow these steps:

1. Open a terminal and navigate to the project directory.
2. Run the command `pip install .` to install the necessary dependencies for the project.

This process ensures that all required dependencies are correctly installed in your environment, allowing you to work on the project with all necessary resources.

To add or modify project dependencies, you must list them in your `pyproject.toml` file under the `dependencies` section.

```bash
dependencies = [
    "pytest == 8.0.1",
    # add necessary dependencies
]
```

### Developing the Package

The `CONTRIBUTING.md` file is an essential guide for developing this Python package. It describes the steps to set up the development environment, the coding conventions to follow, and how to submit changes. 
Once your changes are ready, push your contribution to the desired branch to trigger the integration pipeline, which will create the Python package and deploy it to the Python server.
For more details on contributing and best practices, please refer to the `CONTRIBUTING.md` file.

## Using the Python Package

### Installation

The package and its optional dependencies can be installed with the following command:

```bash
pip install ckanapi_harvesters[extras]
```


### Example Usage of the Python Package in Your Code

After installation, you can import and use your package and its functions in your Python code:

```python
from ckanapi_harvesters import CkanApi

ckan = CkanApi()
```

To use sub-modules defined in the package:

```python
from ckanapi_harvesters.ckan_api import CkanApi

ckan = CkanApi()
```

These instructions will allow you to access the package and utilize its features effectively and in line with your development configuration.

## License

This project is licensed under the MIT License, which means it is freely usable for personal and commercial purposes. The MIT License is one of the most permissive open source licenses. It allows you to do almost anything with the source code, as long as you retain the original license notice and copyright information when redistributing the software or substantial portions of it. This license comes without any warranties, so the software is provided "as is." For more details, please refer to the included LICENSE file.

---