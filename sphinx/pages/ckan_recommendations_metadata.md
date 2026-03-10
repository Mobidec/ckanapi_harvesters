Recommendations for metadata
======

## Dataset metadata [Section under construction]

### Dataset time span

When the dataset contains data within a given time period, the following custom fields should be filled 
with timestamps in ISO-8601 date formats (`YYYY-MM-DD`):
- `temporal_start`: start date of the dataset (e.g., 2020-01-01).
- `temporal_end`: end date of the dataset (e.g., 2021-12-31).

No extension was found linked to this need.

### Geospatial metadata

Datasets can be filtered using geospatial bounding boxes using the extension 
[ckanext-spatial](https://ckan.org/features/geospatial). 
The geographic extent of a dataset has to be stored in a custom field named `spatial`.
This field must contain a valid GeoJSON in WGS84 coordinate system. 
Examples are given on the [extension documentation page](https://docs.ckan.org/projects/ckanext-spatial/en/latest/spatial-search/#the-spatial-extra-field).

```json
{
  "type":"Polygon",
  "coordinates":[[[2.05827, 49.8625],[2.05827, 55.7447], [-6.41736, 55.7447], [-6.41736, 49.8625], [2.05827, 49.8625]]]
}
```
or
```json
{
  "type": "Point",
  "coordinates": [-3.145,53.078]
}
```

### Dataset update frequency

https://www.dublincore.org/specifications/dublin-core/collection-description/frequency/

### License expiration

### Repository

