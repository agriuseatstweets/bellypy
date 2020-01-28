from gcsfs import GCSFileSystem
import pickle
import os
from pyspark.sql import SparkSession
from tweepy.utils import parse_datetime
from confluent_kafka import Consumer, KafkaError
import json
from clize import run
from copy import deepcopy
from os import path
from time import sleep


def consume(c, max_):
    msgs = c.consume(max_, 300.) #300s timeout
    for msg in msgs:
        if msg.error():
            err = msg.error()
            raise err
    return msgs

def read_schema(path):
    fs = GCSFileSystem(project='trollhunters')
    with fs.open(path, 'rb') as f:
        schema = pickle.load(f)
    return schema


def parse_dates(obj, key, fn):
    _parse = lambda k,v: parse_dates(v,key,fn) if k != key else fn(v)

    if isinstance(obj, dict):
        return {k:_parse(k,v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [parse_dates(vi, key, fn) for vi in obj]
    else:
        return obj

def replace_timestamps_in_dat(dat, fn):
    dat = parse_dates(dat, 'created_at', fn)
    dat = parse_dates(dat, 'created', fn)
    dat = parse_dates(dat, 'iDate', fn)
    return dat

def _safe_cast_double(a):
    try:
        return float(a)
    except ValueError:
        return a
    except TypeError:
        return a


def _cast_doubles(a):
    if type(a) == list:
        return [_cast_doubles(i) for i in a]
    if type(a) == dict:
        return {k:_cast_doubles(v) for k,v in a.items()}
    return _safe_cast_double(a)


def cast_coords(tw):
    tw = deepcopy(tw)
    if type(tw) != dict:
        return tw

    for k,v in tw.items():
        if k == 'coordinates':
            tw[k] = _cast_doubles(v)
        else:
            tw[k] = cast_coords(v)

    return tw

def cast_originals(tw):
    og = tw['th_original']
    try:
        og['lng'] = _safe_cast_double(og['lng'])
        og['lat'] = _safe_cast_double(og['lat'])
    except KeyError:
        pass
    return tw

def messages_to_df(spark, schema, messages, partitions):
    tweets = [json.loads(msg.value()) for msg in messages]
    tweets = [replace_timestamps_in_dat(t, parse_datetime) for t in tweets]
    tweets = [cast_coords(tw) for tw in tweets]
    tweets = [cast_originals(tw) for tw in tweets]
    return spark.createDataFrame(tweets, schema).repartition(partitions)

def get_consumer():
    kafka_brokers = os.getenv('KAFKA_BROKERS') # "localhost:9092"
    topic = os.getenv('BELLY_TOPIC') # tweets

    c = Consumer({
        'bootstrap.servers': kafka_brokers,
        'group.id': 'belly',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
	"max.poll.interval.ms": "960000" # 16min
    })

    c.subscribe([topic])

    # Sleep a bit to wait for other consumers to join
    sleep(5)

    return c


# NOTE: change cleanup-failures.ignore to true if causing problems
def build_spark():
    spark = SparkSession \
        .builder \
        .appName('belly') \
        .config("spark.jars", "/home/jupyter/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/jupyter/work/keys/key.json") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "false") \
        .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .getOrCreate()

    return spark

def dedup_data(spark, d, week, year, inpath):
    ids = spark.read.parquet(inpath).select('id').where(f'week = {week} and year = {year}')
    d = d.where(f'week = {week} and year = {year}')
    d = d.join(ids, on='id', how='left_anti')
    return d

# Coalesces to one -- belly should be run in small chunks
def write_out(d, week, year, outpath):
    f = path.join(outpath, f'year={year}', f'week={week}')
    d.where(f'week = {week} and year = {year}') \
     .coalesce(1) \
     .write \
     .mode('append') \
     .parquet(f)

def indempotent_write(spark, df, warehouse):
    df.registerTempTable('tweets')
    dd = spark.sql('select *, weekofyear(created_at) as week, month(created_at) as month, year(created_at) as year from tweets')
    dd.registerTempTable('tweets')
    combos = spark.sql('select distinct year, week from tweets').collect()
    for combo in combos:
        week,year = combo.week, combo.year
        d = dedup_data(spark, dd, week, year, warehouse)
        write_out(d, week, year, warehouse)


def commit_messages(c, messages):
    for m in messages:
        c.commit(m, asynchronous=True)

def main():
    spark = build_spark()
    schema = read_schema('gs://spain-tweets/schemas/tweet-clean.pickle')

    warehouse_path = os.getenv('BELLY_LOCATION')
    N = int(os.getenv('BELLY_SIZE'))

    c = get_consumer()
    messages = consume(c, N)

    partitions = round(N/200)
    df = messages_to_df(spark, schema, messages, partitions)

    indempotent_write(spark, df, warehouse_path)

    commit_messages(c, messages)


if __name__ == '__main__':
    run(main)
