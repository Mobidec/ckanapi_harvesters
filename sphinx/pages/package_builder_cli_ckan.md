CKAN instance CLI parameters
======

An instance of the `CkanApi` class can be initialized using CLI arguments. 
These arguments are available as well in the Excel workbook: column __Options__ in the _ckan_ sheet.

```
CKAN API connection parameters initialization

options:
  --ckan-url CKAN_URL   CKAN URL
  --apikey APIKEY       API key
  --apikey-file APIKEY_FILE
                        Path to a file containing the API key (first line)
  --proxy PROXY         Proxy for HTTP and HTTPS
  --http-proxy HTTP_PROXY
                        HTTP proxy
  --https-proxy HTTPS_PROXY
                        HTTPS proxy
  --no-proxy NO_PROXY   Proxy exceptions
  --proxy-auth-file PROXY_AUTH_FILE
                        Path to a proxy authentication file with 3 lines
                        (authentication method, username, password)
  --ckan-ca CKAN_CA     CKAN CA certificate location (.pem file)
  --extern-ca EXTERN_CA
                        CA certificate location for extern connexions (.pem
                        file)
  --user-agent USER_AGENT
                        User agent for HTTP requests
  -l LIMIT, --limit LIMIT
                        Number of rows per request (upload/download)
  -v, --verbose         Option to set verbosity
  --time-between-requests TIME_BETWEEN_REQUESTS
                        Time between upload/download requests (seconds) -
                        recommended: 0.1 seconds
  --admin               Option to enable admin mode
  --ckan-postgis        Option to notify that CKAN is compatible with PostGIS
  --ckan-epsg CKAN_EPSG
                        Default EPSG for CKAN
  --owner-org OWNER_ORG
                        CKAN Owner Organization
  --policy-file POLICY_FILE
                        Path to a file containing the CKAN data format policy
                        (json format)

Example: --ckan-url https://demo.ckan.org/ --apikey-file __CKAN_API_KEY__.txt
--owner-org demo-organization
```

