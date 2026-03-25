#!python3
# -*- coding: utf-8 -*-
"""
Central implementation of lazy imports of optional dependencies / dependencies rarely used
"""
from types import SimpleNamespace

pyproj = None
def lazy_import_pyproj():
    global pyproj
    try:
        import pyproj
    except ImportError:
        pyproj = None
    return pyproj

shapely = SimpleNamespace(Geometry=None)
def lazy_import_shapely():
    global shapely
    try:
        import shapely
    except ImportError:
        shapely = SimpleNamespace(Geometry=None)
    return shapely

gpd = SimpleNamespace(GeoDataFrame=None)
def lazy_import_geopandas_gpd():
    global gpd
    try:
        import geopandas as gpd
    except ImportError:
        gpd = SimpleNamespace(GeoDataFrame=None)
    return gpd

bson = SimpleNamespace(ObjectId=None, DBRef=None)
def lazy_import_bson():
    global bson
    try:
        import bson
    except ImportError:
        bson = SimpleNamespace(ObjectId=None, DBRef=None)
    return bson

pymongo = SimpleNamespace(MongoClient=None, client_session=SimpleNamespace(ClientSession=None),
                          database=SimpleNamespace(Database=None), collection=SimpleNamespace(Collection=None))
def lazy_import_pymongo():
    global pymongo
    try:
        import pymongo
        import pymongo.client_session
        import pymongo.database
    except ImportError:
        pymongo = SimpleNamespace(MongoClient=None, client_session=SimpleNamespace(ClientSession=None),
                                  database=SimpleNamespace(Database=None), collection=SimpleNamespace(Collection=None))
    return pymongo

sqlalchemy = SimpleNamespace(Engine=None, Connection=None)
def lazy_import_sqlalchemy():
    global sqlalchemy
    try:
        import sqlalchemy
    except ImportError:
        sqlalchemy = SimpleNamespace(Engine=None, Connection=None)
    return sqlalchemy

psycopg2 = None
def lazy_import_psycopg2():
    global psycopg2
    try:
        import psycopg2
    except ImportError:
        psycopg2 = None
    return psycopg2

SSHTunnelForwarder = None
def lazy_import_ssh_tunnel_SSHTunnelForwarder():
    global SSHTunnelForwarder
    try:
        from sshtunnel import SSHTunnelForwarder
    except ImportError:
        SSHTunnelForwarder = None
    return SSHTunnelForwarder