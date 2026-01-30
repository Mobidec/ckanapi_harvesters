Data format policy recommendations
=====

Package and resource metadata should be well-defined for users to navigate inside the CKAN datasets. 
To organize a CKAN website with consistency, a data format policy can be defined by your organization.
This policy defines how the metadata of packages and resources should be organized. 
This package provides functions to check if the policy was respected or not. 

Descriptions of objects (packages, resources, DataStore fields) can use Markdown formatting. 

### Packages

#### Package tags

According to the [CKAN documentation](https://docs.ckan.org/en/2.9/maintaining/tag-vocabularies.html), 
package tags can be organized into vocabularies (lists of pre-defined tags). 

The data format policy functions can enforce the presence of certain tag groups or invalidate tags which are not defined.

#### Package extra key-value pairs

In CKAN, extra key-value pairs can be defined. It is recommended to specify which fields are allowed and their usage.

The data format policy can enforce certain keys and restrict the values 
using a pre-defined list or regular expressions. 

### Package attributes

The data format policy can define a list of mandatory attributes for packages. 


### Resource attributes

#### Resources

The resource formats can be restricted to a specific list. 

#### Resource attributes

The data format policy can define a list of mandatory attributes for resources. 

#### Field attributes (DataStores)

The data format policy can define a list of mandatory attributes for fields. 

