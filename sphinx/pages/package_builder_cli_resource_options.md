Resource builder specific options & database harvesters
======

This page documents specific options 
which can be used in the ___Options___ column of the _resources_ sheet of the Excel builder workbook.
These options are formatted using a UNIX CLI format e.g. 
`--option value`. 


## Resource specific options

```
DataStore resource specific options

options:
  --data-cleaner DATA_CLEANER
                        Data cleaner to call before uploading data
  --one-frame-per-primary-key
                        Enabling this option makes the upload process expect
                        one DataFrame per primary key combination (except the
                        last field of the primary key, which could be an index
                        in the file). 
                        This option should be associated with the file format 
                        option --no-chunks to ensure a file is treated at once
  --group-by GROUP_BY   Fields of the primary key defining the request to
                        reconstruct a file, in --one-frame-per-primary-key
                        mode, separated by a comma (no spaces). By default,
                        the first columns of the primary, except the last one
                        is used. At least one field of the primary key must be
                        unused here.
  --include-source-file
                        Add a column for the file name of the original data
                        (named py_source_file)
  --no-upload-index     Disable the generation of an upload index column in
                        case no primary key was given (named py_upload_index)

Examples: 
- Selecting a Data Cleaner: --data-cleaner GeoJSON 
- Process one file per primary key combination 
(first columns of the primary key, except the last one): --one-frame-per-primary-key --no-chunks
```


## File harvester options

The base file reader functions can be customized using the following arguments:

```
File format reader arguments

options:
  --chunk-size CHUNK_SIZE
                        Chunk size for reading files by chunks (number of
                        records). The number of lines sent per request is the
                        minimum of chunk size and CKAN parameter
                        ckan.params.default_limit_write Enabling this option
                        activates reading by chunks (if supported by the file
                        format)
  --no-chunks           Option to disabling reading files by chunks
  --allow-chunks        Option to enable reading files by chunks (useful for
                        file formats not enabling this feature by default)
  --read-kwargs [READ_KWARGS ...]
                        Keyword arguments for the read function in key=value
                        format
  --write-kwargs [WRITE_KWARGS ...]
                        Keyword arguments for the write function in key=value
                        format

Examples: 
- Changing chunk size: --chunk-size 10000 
- Disabling reading files by chunks: --no-chunks 
- Additional arguments for pandas.read_csv 
for a CSV file: --read-kwargs compression=gzip header=10
```


### Pre-defined file format I/O functions

The following section lists the pre-defined file format I/O functions. 
The choice of the function is determined by the __Format__ attribute.
The __Options__ attribute described above can be used to customize the functions behavior according to the documentation of the underlying functions.


#### CSV: delimited text file format

This method can read text tabular file formats with the underlying function, which is 
[`pandas.read_csv`](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas.read_csv).
The default read parameters are `dtype=str, keep_default_na=False, sep=None, engine='python'`. 
For the write parameters, `index=False` is imposed.


#### SHP: shape file format

This method can read geographic file formats manages by `geopandas`. The underlying function is 
[`geopandas.read_file`](https://geopandas.org/en/latest/docs/reference/api/geopandas.read_file.html).
The default arguments for read/write are `encoding='utf-8'`.


#### XLS: Excel/ODS file format

This method reads the following file formats: xls, xlsx, xlsm, xlsb, odf, ods and odt. 
The underlying function is 
[`pandas.read_excel`](https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html#pandas.read_excel).
It accepts a special CLI argument to specify the sheet name: `--sheet-name`.
This argument can also be set using the `--read-kwargs` argument such as in `--read-kwargs sheet_name=YourSheetName`.
The default arguments of the read/write functions are those of the `pandas` documentation.


#### JSON: JSON file format

This method enables reading JSON files. It relies on
[`pandas.read_json`](https://pandas.pydata.org/docs/user_guide/io.html#io-json-reader).
The I/O functions are configured by default to write one line per record. 
This mode enables reading the file by chunks and appending lines to an existing file.
The corresponding default read/write arguments are `orient="records", lines=True`. 
These can be changed with the CLI arguments `--read-kwargs` and `--write-kwargs`. 


### User-defined file format I/O functions

In addition to these parameters, the user can specify his own read/write functions 
with the __Read function__ / __Write function__ columns in the _resources_ sheet.
If one function is defined, the reciprocal function must be defined, if used (there is no fallback to the default file format function).


#### Extra argument to enable append mode

An extra CLI argument enables writing a file in append mode. In this case, the DataFrames can be written to disk as they are received.
The option is called `--allow-append`.


#### Basic I/O prototypes

The function prototype should be as follows. 
The positional arguments (before the asterisk `*`) are mandatory. As well as the `**kwargs` argument in order to remain compatible with future versions of the Python package.
The parameters defined above also apply to the user-defined functions. The example below returns a DataFrame for the read function.
```python
from typing import Union, Dict, List, Generator
import io
import pandas as pd
from ckanapi_harvesters.auxiliary.ckan_model import CkanField
from ckanapi_harvesters.harvesters.file_formats.user_format import UserFileFormat

...

def read_function_example_df(file_path_or_stream:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None],
                             allow_chunks:bool=True, params:UserFileFormat = None, **kwargs) \
        -> Union[pd.DataFrame, List[dict]]:
    return pd.read_csv(file_path_or_stream)

def write_function_example(df: Union[pd.DataFrame, List[dict]], file_path_or_stream:Union[str, io.IOBase],
                           *, fields: Union[Dict[str, CkanField],None], append:bool=False,
                           params:UserFileFormat = None, **kwargs) -> None:
    mode = 'a' if append else 'w'
    df.to_csv(file_path_or_stream, mode=mode, index=False)
```


#### Reading a file by chunks

Returning a DataFrame generator requires implementing a `ContextManager` such as in the example below.

```python
from contextlib import contextmanager

...

@contextmanager
def read_function_example_by_chunks(file_path_or_stream:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None],
                                    allow_chunks:bool=True, params:UserFileFormat = None, **kwargs) \
        -> Generator:
    file_handle = open(file_path_or_stream, 'r')
    try:
        yield read_function_example_by_chunks_generator(file_handle)
    finally:
        file_handle.close()

def read_function_example_by_chunks_generator(file_handle) -> Generator[Union[pd.DataFrame, List[dict]], None, None]:
    # function called by read_function_example_by_chunks
    for df_chunk in pd.read_csv(file_handle, chunksize=100):
        yield df_chunk
```


## Database harvester options

These options are used to define the connection method (login, URL) and the database/dataset/table to harvest.


### PostgreSQL

#### Examples

Example of arguments enabling a connection to a database without using SSL:

```
--harvester Postgre --login-file postgre_login.txt 
--ca False --host my-postgre-server.com --port 5432 
--database My_Database --schema My_Schema 
--limit 1000
```

The table name is specified in the ___File/URL___ column of the _resource_ sheet.
The PostgreSQL schema is comparable with a CKAN dataset (set of tables).

If you need to setup a secure connection to the database, you can setup an SSH tunnel outside of the Python package. 
This feature is not handled by the present package.


#### Documentation

```
Harvester parameters

options:
  --harvester HARVESTER
                        Type of harvester to use
  --proxy PROXY         Proxy for HTTP and HTTPS
  --http-proxy HTTP_PROXY
                        HTTP proxy
  --https-proxy HTTPS_PROXY
                        HTTPS proxy
  --no-proxy NO_PROXY   Proxy exceptions
  --proxy-auth-file PROXY_AUTH_FILE
                        Path to a proxy authentication file with 3 lines
                        (authentication method, username, password)
  --ca CA               Server CA certificate location (.pem file)
  --timeout TIMEOUT     Server timeout (seconds)
  --host HOST           Host for queries
  --port PORT           Port for queries
  --auth-url-suffix AUTH_URL_SUFFIX
                        URL suffix used to authenticate user
  --auth-url AUTH_URL   URL to authenticate user
  --url URL             Base URL for queries
  --apikey APIKEY       API key
  --apikey-file APIKEY_FILE
                        Path to a file containing the API key (first line)
  --login-file LOGIN_FILE
                        Path to a text file containing login credentials for
                        authentification (user, password)
  -v, --verbose         Option to set verbosity
  --database DATABASE   Database name
  --ckan-postgis        Option to use CKAN with PostGIS geometric types
  --ckan-epsg CKAN_EPSG
                        Default EPSG for CKAN
  --dataset DATASET     Dataset name
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory of download, relative to the download
                        directory (normally provided by File/URL attribute)
  --no-download NO_DOWNLOAD
                        Option to disable download
  --resource-url RESOURCE_URL
                        URL of resource
  --table TABLE         Table name
  --query QUERY         Query to restrict the lines of the table
  -l LIMIT, --limit LIMIT
                        Number of rows per request
  --once                Option to perform only one request with the default
                        limit. This will limit the size of the Data.
  --schema SCHEMA       PostgreSQL schema name
```


### MongoDB

#### Examples

Example of arguments enabling a connection to a database without using SSL:

```
--harvester MongoDB --login-file mongodb_login.txt 
--ca False --host mongodb://my-postgre-server.com:27017/admin 
--url --host mongodb://my-postgre-server.com:27017
--dataset My_Schema 
--limit 1000
```

The table name is specified in the ___File/URL___ column of the _resource_ sheet.
In MongoDB, tables are called collections.

If you need to setup a secure connection to the database, you can setup an SSH tunnel outside of the Python package. 
This feature is not handled by the present package.


#### Documentation

```
Harvester parameters

options:
  --harvester HARVESTER
                        Type of harvester to use
  --proxy PROXY         Proxy for HTTP and HTTPS
  --http-proxy HTTP_PROXY
                        HTTP proxy
  --https-proxy HTTPS_PROXY
                        HTTPS proxy
  --no-proxy NO_PROXY   Proxy exceptions
  --proxy-auth-file PROXY_AUTH_FILE
                        Path to a proxy authentication file with 3 lines
                        (authentication method, username, password)
  --ca CA               Server CA certificate location (.pem file)
  --timeout TIMEOUT     Server timeout (seconds)
  --host HOST           Host for queries
  --port PORT           Port for queries
  --auth-url-suffix AUTH_URL_SUFFIX
                        URL suffix used to authenticate user
  --auth-url AUTH_URL   URL to authenticate user
  --url URL             Base URL for queries
  --apikey APIKEY       API key
  --apikey-file APIKEY_FILE
                        Path to a file containing the API key (first line)
  --login-file LOGIN_FILE
                        Path to a text file containing login credentials for
                        authentification (user, password)
  -v, --verbose         Option to set verbosity
  --database DATABASE   Database name
  --ckan-postgis        Option to use CKAN with PostGIS geometric types
  --ckan-epsg CKAN_EPSG
                        Default EPSG for CKAN
  --dataset DATASET     Dataset name
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory of download, relative to the download
                        directory (normally provided by File/URL attribute)
  --no-download NO_DOWNLOAD
                        Option to disable download
  --resource-url RESOURCE_URL
                        URL of resource
  --table TABLE         Table name
  --query QUERY         Query to restrict the lines of the table
  -l LIMIT, --limit LIMIT
                        Number of rows per request
  --once                Option to perform only one request with the default
                        limit. This will limit the size of the Data.
  --collection COLLECTION
                        MongoDB collection name
  --dbref-expand        Option to expand DBRefs
```
