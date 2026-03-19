import pytest

from ckanapi_harvesters import CkanApi, CkanAdminReport

@pytest.fixture(scope='module')
def ckan():
    # initialization
    ckan = CkanApi("https://demo.ckan.org/")
    ckan.apikey.clear()
    ckan.set_verbosity(True)
    return ckan

def test_01_init(ckan):
    pass

def test_02_connection(ckan):
    ckan.test_ckan_connection()

def test_03_map_resources(ckan):
    ckan.params.map_all_aliases = False
    ckan.map_resources()
    ckan.organization_list_all()
    print(f"Mapped {len(ckan.map.resources)} resources of CKAN repository ({len(ckan.map.packages)} packages, {len(ckan.map.organizations)} organizations).")

def test_04_report(ckan):
    ckan.map_resources()
    package_list = [list(ckan.map.packages.keys())[0]]
    report = CkanAdminReport(ckan=ckan, package_list=package_list, full_report=True, auto_exec=False)
    report.allow_downgraded_queries = True
    report.execute(ckan)
    print(report.to_jsons())

