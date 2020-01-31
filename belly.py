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
import logging
from math import ceil
from pyspark.sql.functions import col

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
    try:
        og = tw['th_original']
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
    tweets = spark.sparkContext.parallelize(tweets, partitions)
    return spark.createDataFrame(tweets, schema)

def get_consumer():
    kafka_brokers = os.getenv('KAFKA_BROKERS') # "localhost:9092"
    topic = os.getenv('BELLY_TOPIC') # tweets
    poll_interval = os.getenv('KAFKA_CONSUMER_POLL_INTERVAL', '1920000') # 32min

    c = Consumer({
        'bootstrap.servers': kafka_brokers,
        'group.id': 'belly',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
	"max.poll.interval.ms": poll_interval
    })

    c.subscribe([topic])

    # Sleep a bit to wait for other consumers to join
    sleep(5)

    return c


# NOTE: change cleanup-failures.ignore to true if causing problems
def build_spark():
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")

    spark = SparkSession \
        .builder \
        .master('local[2]') \
        .appName('belly') \
        .config("spark.jars", "/home/jupyter/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/jupyter/work/keys/key.json") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "false") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .config("spark.driver.memory", driver_memory) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    return spark

def dedup_data(spark, d, dates, inpath):
    ids = spark.read.parquet(inpath) \
                    .where(col('datestamp').isin(dates)) \
                    .select('id')
    d = d.join(ids, on='id', how='left_anti')
    return d

def write_out(d, outpath):
    d.write \
     .mode('append') \
     .parquet(outpath, partitionBy='datestamp')

def indempotent_write(spark, df, warehouse):
    df.registerTempTable('tweets')
    df = spark.sql('select *, CAST(created_at AS DATE) as datestamp from tweets')
    df.registerTempTable('tweets')
    dates = spark.sql('select distinct datestamp from tweets').collect()
    dates = [r.datestamp for r in dates]
    df = dedup_data(spark, df, dates, warehouse)
    write_out(df, warehouse)


def commit_messages(c, messages):
    for m in messages:
        c.commit(m, asynchronous=True)

def main():
    spark = build_spark()

    schema = read_schema('gs://spain-tweets/schemas/tweet-clean.pickle')

    warehouse_path = os.getenv('BELLY_LOCATION')
    N = int(os.getenv('BELLY_SIZE'))
    partition_size = int(os.getenv('PARTITION_SIZE', '10000'))

    c = get_consumer()
    logging.info(f'Belly consumer subscribed to: {c.assignment()}')
    messages = consume(c, N)
    print(f'BELLY CONSUMED {len(messages)} MESSAGES FROM QUEUE.')

    if len(messages) < round(N/4):
        logging.info('Not enough messages for Belly, exiting.')
        return

    partitions = ceil(N/partition_size)
    df = messages_to_df(spark, schema, messages, partitions)
    indempotent_write(spark, df, warehouse_path)
    commit_messages(c, messages)
    logging.info(f'Belly wrote {len(messages)} to warehouse.')

if __name__ == '__main__':
    run(main)
