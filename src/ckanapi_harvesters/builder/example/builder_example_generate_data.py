#!python3
# -*- coding: utf-8 -*-
"""
Code to generate sample data for the dataset example
"""
from typing import Tuple
import os
import re

import pandas as pd
import numpy as np

from ckanapi_harvesters.builder.example import example_package_dir
self_dir = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))


GPS_DIGITS = 6
def degrees_DMS(lat:str, lon:str) -> Tuple[float, float]:
    """
    Returns angles for GPS coordinates in the form
    :param lat: example: 48° 51' 12.24845" N
    :param lon: example: 2° 20' 55.62563" E
    :return:
    """
    deg, minutes, seconds, direction = re.split('[°\'"]', lat)
    lat_val = (float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60)) * (-1 if direction in ['W', 'S'] else 1)
    deg, minutes, seconds, direction = re.split('[°\'"]', lon)
    lon_val = (float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60)) * (-1 if direction in ['W', 'S'] else 1)
    return (lat_val, lon_val)


def run():
    N = 100
    df_users = pd.DataFrame([{"user_id": 1, "age": 30},
                             {"user_id": 2, "age": 80},
                             ])
    point_0 = degrees_DMS('48° 51\' 12.24845" N', '2° 20\' 55.62563" E')
    traces = [{"user_id": 1, "origin": point_0, "destination": degrees_DMS('48°53\'37.6"N', '2°23\'24.9"E')},
              {"user_id": 1, "origin": point_0, "destination": degrees_DMS('48°50\'01.7"N', '2°19\'57.1"E')},
              {"user_id": 2, "origin": point_0, "destination": degrees_DMS('48°52\'45.2"N', '2°18\'32.8"E')},
    ]
    users_file = os.path.join(example_package_dir, "users_local.csv")
    df_users.to_csv(users_file, index=False)
    traces_dir = os.path.join(example_package_dir, "traces")
    traces_dir_multi = os.path.join(example_package_dir, "traces_multi")
    for trace_id, trace in enumerate(traces):
        table_index = np.array([0, N-1])
        table_lat = np.array([trace["origin"][0], trace["destination"][0]])
        table_lon = np.array([trace["origin"][1], trace["destination"][1]])
        df_trace = pd.DataFrame()
        index = np.arange(N)
        df_trace["index_in_trace"] = index
        df_trace.insert(loc=0, column="trace_id", value=trace_id)
        df_trace["user_id"] = trace["user_id"]
        df_trace["timestamp"] = pd.Timestamp(year=2025, month=1, day=1, hour=12, minute=0, second=0, microsecond=0) + index.astype('timedelta64[s]')
        df_trace["timestamp_local"] = df_trace["timestamp"] + pd.Timedelta(hours=1)  # local = UTC+1 for winter light saving time in Paris
        df_trace["timestamp"] = df_trace["timestamp"].apply(pd.Timestamp.isoformat)  # ISO-8601 format
        df_trace["timestamp_local"] = df_trace["timestamp_local"].apply(pd.Timestamp.isoformat)  # ISO-8601 format
        df_trace["latitude"] = np.interp(index, xp=table_lat, fp=table_lat)
        df_trace["longitude"] = np.interp(index, xp=table_lat, fp=table_lon)
        df_trace["latitude"] = df_trace["latitude"].values.round(GPS_DIGITS)
        df_trace["longitude"] = df_trace["longitude"].values.round(GPS_DIGITS)
        df_trace.set_index(keys=["trace_id", "index_in_trace"], drop=False, inplace=True, verify_integrity=True)
        trace_file = os.path.join(traces_dir, f"trace_{trace_id:03d}.csv")
        df_trace.to_csv(trace_file, index=False)
        trace_file_multi = os.path.join(traces_dir_multi, f"trace_{trace_id:03d}.csv")
        df_trace.to_csv(trace_file_multi, index=False)
    print("Example data regenerated")


if __name__ == '__main__':
    run()

