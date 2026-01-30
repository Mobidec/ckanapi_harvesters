import pytest
import os

from ckanapi_harvesters.builder.example import example_package_xls
from ckanapi_harvesters import BuilderPackage

@pytest.fixture(scope='module')
def mdl():
    # initialization
    BuilderPackage.unlock_external_code_execution()
    yield BuilderPackage.from_excel(example_package_xls)

def test_mdl_load(mdl):
    pass

def test_mdl_base_dir_exists(mdl):
    base_dir = mdl.get_base_dir()
    assert(os.path.exists(base_dir))

def test_mdl_load_checks(mdl):
    assert(mdl.external_python_code is not None)

def test_to_dict(mdl):
    base_dir = mdl.get_base_dir()
    mdl_dict = mdl.to_dict(base_dir=base_dir)
    assert(mdl_dict["Package"]["Name"] == "builder-example-py")

def test_mdl_from_dict(mdl):
    base_dir = mdl.get_base_dir()
    mdl_dict = mdl.to_dict(base_dir=base_dir)
    mdl_from_dict = BuilderPackage.from_dict(mdl_dict, base_dir=base_dir)
    mdl_dict_bis = mdl_from_dict.to_dict(base_dir=base_dir)
    assert(mdl_dict == mdl_dict_bis)

def test_mdl_copy(mdl):
    base_dir = mdl.get_base_dir()
    mdl_dict = mdl.to_dict(base_dir=base_dir)
    mdl_copy = mdl.copy()
    mdl_copy_dict = mdl_copy.to_dict(base_dir=base_dir)
    assert(mdl_dict == mdl_copy_dict)

