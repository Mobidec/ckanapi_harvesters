import pytest

from ckanapi_harvesters.auxiliary.urls import url_matches_host

def test_same_host():
    try:
        url_matches_host(target_host_url="demo.ckan.org", url="https://demo.ckan.org")
    except:
        pass
    else:
        raise AssertionError()
    assert(not(url_matches_host(target_host_url="https://demo.ckan.org", url="demo.ckan.org")))
    assert(url_matches_host(target_host_url="https://demo.ckan.org", url="https://demo.ckan.org"))
    assert(url_matches_host(target_host_url="https://demo.ckan.org", url="https://demo.ckan.org/"))
    assert(url_matches_host(target_host_url="https://demo.ckan.org", url="https://DEMO.CKAN.ORG"))

def test_same_root():
    assert(not(url_matches_host(target_host_url="https://demo.ckan.org", url="https://ckan.org")))
    assert(not(url_matches_host(target_host_url="https://demo.ckan.org", url="https://test.demo.ckan.org")))

def test_different():
    assert(not(url_matches_host(target_host_url="https://demo.ckan.org", url="https://google.com")))

