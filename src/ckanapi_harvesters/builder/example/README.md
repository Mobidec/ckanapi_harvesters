Package builder example scripts
======

Example scripts demonstrating the capabilities of the package builder module. 
The scripts can upload the package example defined in `builder_package_example.py`.
Once initialized, the data can be re-downloaded and certain tests can be done.

The example scripts must be run with CLI arguments specifying the CKAN target instance and owner organization.
Example:
```
python example_script.py --ckan-url https://demo.ckan.org/ --apikey-file CKAN_Token.txt --owner-org demo-organization
```
