CKAN field naming recommendations
======

## Fields for DataStores

The DataStore is distinct but complementary to the FileStore
(see FileStore and file uploads). In contrast to the FileStore
which provides ‘blob’ storage of whole files with no way to access or query
parts of that file, the DataStore is like a database in which individual
data elements are accessible and queryable. To illustrate this distinction,
consider storing a spreadsheet file like a CSV or Excel document. In the
FileStore this file would be stored directly. To access it you would download
the file as a whole. By contrast, if the spreadsheet data is stored in the
DataStore, one would be able to access individual spreadsheet rows via a simple
web API, as well as being able to make queries over the spreadsheet contents.

Source: [CKAN documentation maintainer's guide](https://docs.ckan.org/en/2.9/maintaining/datastore.html)

Field naming conventions can be defined by your organization.
Always specify the units of values.
Here are some field naming conventions which are recommended for maximum compatibility
with common CKAN data visualization plugins.

#### DataSore basic visualizations

DataStore views are contained in the [Data Explorer plugin](https://docs.ckan.org/en/2.9/maintaining/data-viewer.html#data-explorer):
- DataStore grid: table representation of the DataStore,
- DataStore graph: to represent a graph,
- DataStore map: to display datapoints on a map.

#### DataStore field formats

The field formats visible in the CKAN web interface are the following:
- `text`
- `numeric`
- `timestamp`

There are additional field formats listed in the
[CKAN documentation](https://docs.ckan.org/en/latest/maintaining/datastore.html#field-types)
but this was not explored.

#### Type override 

It is recommended to manually specify the data type of each field. 
If not specified, the type is automatically recognized according to the first upload of data. 
Adding more lines can reveal type-detection errors which will generate errors when uploading other data
e.g. if you have a column with timestamps and a few values are NaT (not a time), there will be an error 
when you upload the row containing this value.
If a field data type is overridden after the initialization of a DataStore, the DataStore 
must be reloaded from a source file. This deletes all rows added by the DataStore API requests.

### Time series

For a series indexed by a timestamp, the recommended field name is `timestamp` 
and the data format to be uploaded is ISO-8601 timestamp format 
(e.g. 2025-01-01T12:00:01).

### Geospatial data

For a series containing one coordinate per row, the fields `latitude` and `longitude` 
are recommended to be recognized by the CKAN data visualizations.
Source: [DataStore Map](https://docs.ckan.org/en/2.9/maintaining/data-viewer.html#datastore-map)

For more complex geometries, the CKAN visualization plugins support GeoJSON fields. 
The recommended field name is `spatial` and it should contain a valid GeoJSON representation. 
Source: [ckanext-spatial](https://docs.ckan.org/projects/ckanext-spatial/en/latest/spatial-search/#the-spatial-extra-field)

Example of a GeoJSON geometry with latitude/longitude pairs representing a polygon:
```json
{
  "type":"Polygon",
  "coordinates":[[[2.05827, 49.8625],[2.05827, 55.7447], [-6.41736, 55.7447], [-6.41736, 49.8625], [2.05827, 49.8625]]]
}
```


## FileStore formats

CKAN can display other data formats, which are not stored as Postgre DataStore tables but as files.

### Geospatial formats

The following formats were reported in [ckanext-geoview](https://github.com/ckan/ckanext-geoview).
This requires the installation of a specific view plugin.

| Type                      | Resource format (*) |
|---------------------------|---------------------|
| Web Map Service (WMS)     | `wms`               |
| Web Feature Service (WFS) | `wfs`               |
| GeoJSON                   | `geojson`           |
| GML                       | `gml`               |
| KML                       | `kml`               |
| ArcGIS REST API           | `arcgis_rest`       |
| Google Fusion Tables      | `gft`               |


### File formats which are installed with CKAN

The view plugins are listed in
[CKAN documentation maintainer's guide on data preview and visualization](https://docs.ckan.org/en/2.9/maintaining/data-viewer.html).

These views are:
- Text view
- Image view
- Web page view

___Warning:___ Do not activate web page view unless you trust the URL sources.
It is not recommended to enable this view type on instances where all users
can create datasets.


### Other view plugins

[Other view plugins](https://docs.ckan.org/en/2.9/maintaining/data-viewer.html#other-view-plugins) can be installed:
- [Dashboard](https://github.com/ckan/ckanext-dashboard): Allows to combine multiple views into a single dashboard.
- [PDF viewer](https://github.com/ckan/ckanext-pdfview): Allows to render PDF files on the resource page.
- [GeoJSON map](https://github.com/ckan/ckanext-spatial): Renders [GeoJSON](http://geojson.org/) files on an interactive map.
- [Choropleth map](https://github.com/ckan/ckanext-mapviews): Displays data on the DataStore on a choropleth map.
- [Basic charts](https://github.com/ckan/ckanext-basiccharts): Provides alternative graph types and renderings.

