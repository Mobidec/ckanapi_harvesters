Python CKAN package builder Excel workbook
======

The `ckanapi_harvesters` Python package includes a section called `builder` which provides functions 
to initiate a CKAN dataset using metadata defined in an Excel workbook and resources 
from your local file system.
Reciprocally, this file can be initiated from the CKAN API and modified by the user before being 
used to patch/update the metadata of a package.

An example workbook is embedded in the `ckanapi_harvesters` package in the section 
`ckanapi_harvesters.builder.example` ([builder_package_example.xlsx](../../src/ckanapi_harvesters/builder/builder_package_example.xlsx)). 
This file can be extracted and used a base to define new packages. This document explains the contents of the Excel workbook. 

This Excel workbook is designed to specify a CKAN dataset (previously called a "package"). The page "package" is the main page. You can define the dataset metadata and some additional fields. You can fill the metadata according to your CKAN data format policy. The page "resources" lists the different resources of the dataset, providing functionality to upload the data with a Python script from your local file system. In CKAN, certain resources can be stored and accessed like a database table. These resources are known as DataStores. You can define a DataStore either from one CSV file or a directory of CSV files. For such a resource, it is recommended to define a primary key, ensuring you do not upload a row twice. The fields of the DataStore can be defined in a sheet having the same name as the resource (this is case-sensitive).

The CKAN API documentation can be found 
[here](https://docs.ckan.org/en/2.9/api/)
and specifics for DataStore manipulation 
[here](https://docs.ckan.org/en/2.9/maintaining/datastore.html).

The following sheet names and field names must follow the layout of the example Excel workbook.
The names are not case-sensitive. 

The package builder can also be exported in a JSON file. The organization of the JSON file is similar.
For DataStores, the field list with metadata is located under the associated resource rather than at the top level.

Fields left empty are not interpreted by the upload scripts. Values from CKAN are prioritary on values which could be deduced from the data source, except if option `override_ckan` is used in the function call.


### Sheet "info"

This sheet contains information necessary for the Python script.

Field specification:
- __Builder format version__: This field is provided to distinguish future versions of the Excel file format.
- __Auxiliary functions file__: The auxiliary functions file enables the user to define functions to apply to the DataFrames on upload/download of DataStores. See the resources sheet help for more information.
___Warning___: only execute code if you trust the source !
- __Resources local directory__: By default, the resources mentioned in this file are in the same folder as the file. This field enables the user to point to a specific folder, relative to this Excel file or as an absolute path. If the path points to a text file, the first line of the text file defines the resources directory.
- __Download directory__: Default path to download the resources to, relative to this Excel workbook folder.
- __Comment__: A place to leave a comment on the file e.g. for specific instructions. This field is preserved during import/exports.


### Sheet "ckan"

This sheet enables the user to specify the CKAN URL, proxy and API key file to initiate the connexion to the CKAN database. You can generate your API key in your user account settings. 
The API key cannot be stored in this workbook in order to share it with other users. 
This sheet is optional.

Field specification:
- __CKAN URL__: URL of the CKAN server e.g. https://demo.ckan.org/.
  - The string `environ` loads the URL from the environment variable `CKAN_URL`. 
- __CKAN API key file__: Path to a file containing the API key in the first line, or:
  - The string `environ` loads the API key from the environment variables `CKAN_API_KEY_FILE`, or not recommended `CKAN_API_KEY`.
- __Proxies__: Proxies configuration. It is applied to all requests made by the package (to CKAN server and external sources). 
If not specified, the proxies set in the environment variables are used (at least `http_proxy`, `https_proxy`, `no_proxy`). The values entered can be either:
  - one unique url or
  - `'{"http": "http://proxy:8082", "https": "http://proxy:8082", "no": "localhost"}'`. 
  - The string `environ` loads the proxy configuration from environment variables (at least `http_proxy`, `https_proxy`, `no_proxy`). 
  - The string `unspecified` does not specify the proxies to the requests library. This is equivalent to leaving this field empty. 
  - The string `noproxy` explicitly specifies to use a direct connection, without any proxy.
  - The string `default` uses the proxies defined in the `default_proxies` argument of the initialization functions.
  You can always specify a proxy with the `proxies` arguments of the initialization functions.
- __Proxy authentication file__: The proxy authentication can be specified through a text file here. It is applied to all requests made by the package (to CKAN server and external sources).
If this field is `environ`, this file has to be set in the environment variable `PROXY_AUTH_FILE`. 
This file must contain 3 lines:
  - The authentication method (`HTTPBasicAuth`, `HTTPProxyAuth` or `HTTPDigestAuth`)
  - The username
  - The password
For basic authentication, this can be included in the url such as in `http://user:password@proxy:port` but it is not recommended to store sensitive information in environment variables.
The `proxy_auth` argument can be also used in your script. Additional headers can also be passed to the requests using the `headers` arguments. 
- __CKAN remote CA__: Path to a custom certificate for the CKAN server (.pem). For a global configuration across all requests, set the `REQUESTS_CA_BUNDLE` environment variable.
- __External CA__: Path to a custom CA certificate used for connexions other than the CKAN server, relative to this Excel workbook folder (.pem). 
- __Data format policy file__: Path to a JSON file containing the CKAN data format policy, relative to this Excel workbook folder. The data format policy enables you to check your information against data format policy rules provided by your organization.
- __Options__: List of options to initialize the CKAN API object in CLI format. Notable options are:
  - `--ckan-postgis` to signal the data transformation operations to handle PostGIS objects properly.
- __Limit__: Maximum number of records to return/send per request.
- __Time between requests__: Delay between each request (upload/download), in seconds - recommended: 0.1 seconds. Default value used if left empty.
- __Thread count__: default number of threads used to upload/download large datasets. Default is 1.


### Sheet "package"

This sheet contains the definition of the package. Help for each field is given in the sheet.

Field specification:
- __Name in URL__: Name of the package used in the urls to refer to the package (_mandatory_). The package name should be specific. 
Characters allowed are lowercase letters (a-z), digits (0-9), `-` and `_`, no accents, no spaces. Length must be 2-100 characters.
- __Title__: Title appearing in the CKAN interface. 
- __Description__: Some text describing the package. Markdown features are allowed. 
- __Version__: Packages can be versioned, which enables tracking of data updates. 
- __Visibility__: The package can be exposed to all users (Public) or a subset of authentified users, defined by CKAN groups (Private). Values accepted:
  - ___Private___
  - ___Public___
- __State__: The state field enables to prepare a package without making it visible to all users. 
By default, during the package upload, it has the state ___Draft___. 
And if not specified here, the state remains ___Draft___. 
However, this behavior can be changed with `BuilderPackage.setup_auto_draft_state`.
Available states are:
  - ___Active___
  - ___Draft___
  - ___Deleted___ (step before definitive deletion)
- __Organization__: This field holds the owner organization name, ID or title. It is mandatory to initialize a package. 
- __License__: This field holds the license title or ID for the package, if required.
- __URL__: A URL for the dataset's source.
- __Tags__: A list of comma-separated tags. Please refer to your organization's data format policy to know the pre-defined allowed tags. 
- __Author__: The name of the dataset's author.
- __Author Email__: The email address of the dataset's author.
- __Maintainer__: The name of the dataset's maintainer.
- __Maintainer Email__: The email address of the dataset's maintainer.

The other fields are added as __custom key-value pairs__. 
Please refer to your organization's data format policy to know the additional fields which are required. 


### Sheet "resources"

This sheet lists the resources of the package with their metadata. There are several types of resources defined by the field "Mode".

Field specification:
- __Name__: Name of the resource appearing in the CKAN list.
- __DataStore Columns Sheet__: Name of the sheet specifying the metadata for fields (only for DataStores). 
By default, the resource name is used. Because of Excel limitations concerning the naming of sheets, 
the user can specify the sheet used for a given DataStore in this column. 
When given, the specified sheet must be present. If omitted, the load function looks for a sheet named after the resource (default sheet name). 
Indeed, Excel sheet names cannot exceed 31 characters, contain the following characters: `/ \ ? * : [ ]`,
begin or end with an apostrophe (`'`) or be named "History". 
When using the resource name, the default sheet name replaces the forbidden characters with `#`. 
- __Description__: Description of the resource, appearing in the CKAN package home page. Markdown features are allowed. 
- __Format__: This determines the method used to read/write files. Formats are defined in `ckanapi_harvesters.builder.file_formats`. 
Default format is CSV for DataStores. 
If a custom read/write method is defined (see below, columns Read/Write function), this column is ignored.
- __State__: The state field enables to prepare a resource without making visible to all users. Available states are:
  - ___Active___ (_default_)
  - ___Draft___
  - ___Deleted___ (step before definitive deletion)
- __Mode__: The following modes are implemented:
  - ___File___: resource defined by a binary file, with no DataStore functionality
  - ___URL___: resource defined by an external URL
  - ___DataStore from File___: DataStore which is defined by a single csv file
  - ___DataStore from Folder___: DataStore which is defined by a directory containing multiple files (specify file filters using `glob` wildcard).
  The first file in the directory is taken as an example and uploaded first.
  - ___DataStore from URL___: DataStore which queries an external URL to mirror its contents
  - ___DataStore from Harvester___: DataStore which is harvested from requests such as an API or database.
  In this case, the _File/URL_ attribute has a meaning depending on the selected harvester.
  The _Options_ attribute is reserved for this mode to specify how to harvest the data. See below for documentation on the implemented requests.
  - ___Unmanaged___: the script is only used to update resource metadata and does not upload any data to CKAN (must be done manually).
  - ___Unmanaged DataStore___: the script is only used to update resource and fields metadata and does not upload any data to CKAN (must be done manually)
  - ___MultiFile___: This manages a set of files to be uploaded as separate resources. The file names are identical to the 
  resource names. The resource name field is only indicative. 
  The _File/URL_ attribute can be used to specify a separate folder on your local file system, relative to the resources base directory. 
  Specify file filters using a `glob` wildcard.
  The files used by single-resource lines are ignored by the wildcard selection.
  - ___MultiDataStore___: A MultiFile which initiates a DataStore and manages field metadata.
  The forbidden wildcard characters in the name of the Excel spreadsheet for field metadata must be replaced by `#`.
- __Options__: Options in CLI format: 
  - In file mode, use to add specific arguments for file reading/writing functions e.g.
    - To disable reading a CSV file by chunks, use option `--no-chunks`.
    - For a CSV file, add options to the pandas.read_csv function in key=value format e.g. 
    `--read-kwargs compression=gzip header=10`
  - In mode _DataStore from Requests_, use to configure database connection parameters. 
  See [specific documentation](package_builder_cli_resource_options.md).
- __Download__: By default, all the resources are downloaded. You can specify not to download a specific resource with the keyword `false`.
Resources which are not listed in the Excel spreadsheet will not be downloaded by the default methods.
- __File/URL__: Source URL if the resource refers to an URL.
Source file if the resource refers to a file. The path is relative to the package Resources local directory.
For folders, the whole folder is scanned for files recursively.
Leave empty for unmanaged resource modes.
- __Primary key__: Defining a primary key is crucial for DataStores which are meant to be updated. This is the case for the DataStore from folder mode. (only applies to DataStores)
Comma-separated list of field names.
In DataStore from Folder mode and when the primary key is defined by two or more fields, the default download behavior is to map a file to a combination of the first fields of the primary key. The last field is used as an index. 
- __Indexes__: Indexed fields can increase the speed of certain queries. (only applies to DataStores)
Comma-separated list of field names.
- __Read function__: custom function to execute when loading the data from a file. This function must be implemented in the Auxiliary functions file.
It must take a file/IO buffer and return a DataFrame.
- __Write function__: custom function to execute when saving records to a file. This function must be implemented in the Auxiliary functions file.
It must take a DataFrame and a file/IO buffer as input arguments.
- __Upload function__: If an Auxiliary functions file is specified, this represents the name of the function which is called before uploading a DataFrame from a local file. (only applies to DataStores)
This function must have one argument which is a pandas DataFrame and must return the modified DataFrame. If not provided, no modification is performed.
- __Download function__: This should be the reverse function of the upload function. It is called when a DataFrame is downloaded from the server. (only applies to DataStores)
- __Data cleaner__ option to activate a function which corrects values according to the destination type specified in the fields metadata (only for uploads). 
By default, no modifications are made to the data before upload except when importing data from external databases (PostgreSQL/MongoDB). The ___GeoJSON___ data cleaner is used in this case. 
Values for this field are:
  - ___None___: explicitly do not use a data cleaner
  - ___Basic___: basic data conversions e.g. replace empty values by `null` for numeric columns.
  - ___GeoJSON___: conversions adapted for geometry columns (like GeoJSON) - this includes the ___Basic___ features.
  - ___Check___: this method raises an error if a data edit was recommended by the ___GeoJSON___ mode. The data is not modified with this implementation.
- __Aliases__: Names for read only aliases of the resource (only for DataStores). 
Resource aliases are used to construct SQL read-only queries with identifiers more easily memorizable than resource ids.

Paths of resources are necessarily relative to the resources directory because the download step 
will reconstruct the same hierarchy as the upload directory, in a different location.


#### Data upload process steps

When uploading data, the steps are the following:
1) Read file: the selected method depends on the indicated format or, if specified, the custom __Read function__. 
This function returns either a DataFrame or a list of dicts. The function can also return a generator of these objects.
2) Add index: if no primary key is defined, an extra column named `py_upload_index` is added to the DataFrame. 
3) Alter function: the __Upload function__ is called, if defined by user. 
See below for function prototypes.
4) Sort lines by the primary key, if it is defined by more than one column. 
When multiple primary keys were provided, the default behavior is to associate a file with the values of the primary key except its last column (when downloading a resource).  
5) Data cleaner: the selected __Data cleaner__ is executed, if specified (GeoJSON selected by default for mode ___DataStore from Harvester___).
6) `datastore_upsert`: the DataFrame is uploaded through the API. 


#### Specific modes for _DataStore from Harvester_

The _Options_ attribute serves to specify the harvesting method and parameters.

##### Configuration for a mongo database

The _File/URL_ column serves to specify the database collection used. 



### Sheet "help"

This sheet displays help on each field, following the contents of this document.


### Specific sheets for field metadata of DataStores

The sheets named after the DataStores (case-sensitive names) specify the properties of the fields of the DataStore.

Field specification:
- __Field name__: Name appearing in the CSV file
- __Type override__: Leave empty if the default type recognized when the data is uploaded is correct. Otherwise, specify the data type which should be enforced. A change in this value requires a new upload of the full DataStore. Values accepted:
  - text
  - numeric
  - timestamp
- __Description__: The description of the field. Markdown features are allowed.
- __Index__: Boolean to indicate if this field must be indexed.
- __Unique__: Boolean to indicate if this field must have a unique constraint.
- __Not null__: Boolean to indicate if this field cannot accept null values.
- __Options__: CLI arguments for additional attributes of a field conversion, notably:
  - `--epsg-src` is used to specify the original EPSG of a geometric object (integer). 
  If provided, the geometry can be converted to the destination EPSG if a data cleaner is specified.


## Auxiliary function prototypes


### Upload/Download function

An intermediary data editing function can be inserted between the reading from the data source and the upload request.
These steps are described in a [section above](Data-upload-process-steps).
This function must be referenced by the user in the __Upload function__ column of the resources attribute.

In the other way, a __Download function__ can be referenced. This function may be used to restore the data format of the original data source.

The arguments before the asterisk (`*`) are positional and mandatory. The keyword arguments can be used to display more information.
The `**kwargs` argument must be implemented in order to ensure compatibility with future versions.

```python
from typing import Union, List, Dict
import pandas as pd
from ckanapi_harvesters import CkanField

...

def upload_function_example(df_local: Union[pd.DataFrame, List[dict]], *,
                            fields:Dict[str, CkanField], file_name:str=None, total_lines_read:int=None, **kwargs) \
        -> Union[pd.DataFrame, List[dict]]:
    return df_local

def download_function_example(df_download: pd.DataFrame, *,
                              fields:Dict[str, CkanField], file_query:str=None, **kwargs) \
        -> Union[pd.DataFrame, List[dict]]:
    return df_download
```


### Progress callback

By default, progress is printed in the command line output. 
The progress display may be customized such as in this example (used in Jupyter Notebooks).
The arguments before the asterisk (`*`) are positional and mandatory. The keyword arguments can be used to display more information.
The `**kwargs` argument must be implemented in order to ensure compatibility with future versions.

```python
from ckanapi_harvesters import CkanCallbackLevel
from typing import Any

...

from ipywidgets import IntProgress
from IPython.display import display
f = IntProgress(min=0,max=100)

def progress_callback_example(position:int, total:int, info:Any=None, *, context:str=None,
                              file_index:int=None, file_count:int=None,
                              lines_chunk:int=None, total_lines_read:int=None,
                              canceled_upload: bool=False, end_message: bool=False, level:CkanCallbackLevel=None,
                              **kwargs) -> None:
    if level == CkanCallbackLevel.ResourceChunks:
        f.value = int(position/total*100)

...

display(f)
```

