import requests
import json
import zlib
import gzip
import io
import os
import glob
import multiprocessing 
from functools import reduce, partial
from datetime import datetime, timedelta

def fetch_data(filename):
    """
    Fetch data from Github Archive.
    
    Args:
        filename: e.g. '2015-12-24-1.json'
    """
    req_content = requests.get('https://data.gharchive.org/' + filename + '.gz').content
    try:
        data = gzip.decompress(req_content)
    except:
        print('Failed to fetch ' + filename)
        return
    with open('./cache/' + filename, 'wb') as f: 
        f.write(data)
    return

def get_data(dates, spark_ctx):
    """
    Get Github Archive data for given dates.
    
    Args:
        datetimes: List of datetimes e.g. [ datetime(2015, 12, 24, 1), datetime(2015, 12, 24, 2) ]
        spark_ctx: Spark context
    
    Returns:
        Dataframe of events.
    """
    filenames = list(map(lambda d: d.strftime('%Y-%m-%d-') + str(d.hour) + '.json', dates))
    for f in filenames: 
        if (f not in os.listdir('./cache')): 
            print('Fetching ' + f)
            fetch_data(f) 
        else:
            print('Loading ' + f)
    cache_files = os.listdir('./cache')
    filenames = list(filter(lambda f: f in cache_files, filenames))
    df = spark_ctx.read.json(list(map(lambda f: './cache/' + f, filenames)))
    return df

def get_date_range(start_date, end_date):
    """
    Return range of dates spanning each hour.
    
    Args:
        start_date: e.g. datetime(2015, 12, 24, 1)
        end_date: e.g. datetime(2015, 12, 24, 4)
    
    Returns:
        List of dates.
    """
    start_str = start_date.strftime('%Y-%m-%d-%H')
    end_str = end_date.strftime('%Y-%m-%d-%H')
    start_time = datetime.strptime(start_str, '%Y-%m-%d-%H')
    end_time = datetime.strptime(end_str, '%Y-%m-%d-%H')
    hours = range(0, 1 + int((end_time-start_time).total_seconds()) // 3600)
    return [start_time + timedelta(hours=h) for h in hours]

def clear_data():
    files = glob.glob('./cache/*')
    for f in files: os.remove(f)
    return

# Example
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dates = get_date_range(datetime(2016, 12, 24, 15), datetime(2016, 12, 24, 18))
df = get_data(dates, spark)
df.createOrReplaceTempView("events")
total_evts = spark.sql("SELECT COUNT(*) as count FROM events")
total_evts.show(truncate=False)
count_evts = spark.sql("SELECT type, COUNT(*) AS count FROM events GROUP BY type")
count_evts.show(truncate=False)
"""