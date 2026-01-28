from ckanapi_harvesters import hello_world


def test_hello_world():
    assert hello_world() is None
