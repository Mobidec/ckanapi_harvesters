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
                        in the file)

Examples: 
- Selecting a Data Cleaner: --data-cleaner GeoJSON 
- Process one file per primary key combination 
(first columns of the primary key, except the last one): --one-frame-per-primary-key
```


## File harvester options

The base file reader functions can be customized using the following arguments:

```
File format reader arguments

options:
  --chunk-size CHUNK_SIZE
                        Chunk size for reading files by chunks (number of
                        records). If given, this enables reading files by
                        chunk (option --allow-chunks)
  --allow-chunks        Option to enable reading files by chunks, with the
                        default chunk size or given with --chunk-size.
  --read-kwargs [READ_KWARGS ...]
                        Keyword arguments for the read function in key=value
                        format
  --write-kwargs [WRITE_KWARGS ...]
                        Keyword arguments for the write function in key=value
                        format

Examples: 
- Enabling reading files by chunks: --allow-chunks --chunk-size 1000
- Additional arguments for pandas.read_csv for a CSV file: --read-kwargs
compression=gzip header=10
```

#### User-defined file format I/O functions

In addition to these parameters, the user can specify his own read/write functions 
with the __Read function__ / __Write function__ columns in the _resources_ sheet.
If one function is defined, the reciprocal function must be defined, if used (there is no fallback to the default file format function).
The function prototype should be as follows. 
The positional arguments (before the asterisk `*`) are mandatory. As well as the `**kwargs` argument in order to remain compatible with future versions of the Python package.
The parameters defined above also apply to the user-defined functions. 
```python
def read_function_example(file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None], allow_chunks:bool=False, params:UserFileFormat = None, **kwargs) -> Union[pd.DataFrame, List[dict]]:
    return pd.DataFrame()

def write_function_example(df: Union[pd.DataFrame, List[dict]], file_path_or_buffer:Union[str, io.IOBase], *, fields: Union[Dict[str, CkanField],None], append:bool=False, params:UserFileFormat = None, **kwargs) -> None:
    raise NotImplementedError()
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
--harvester Pymongo --login-file mongodb_login.txt 
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
