
# commit messages

from gcsfs import GCSFileSystem
import pickle
import os
from pyspark.sql import SparkSession
from tweepy.utils import parse_datetime
from confluent_kafka import Consumer, KafkaError
import json
from clize import run

def consume(c, max_):
    msgs = c.consume(max_, 30.) #30s timeout
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

def parse_dates(obj, key):
    _parse = lambda k,v: parse_dates(v, key) if k != key else parse_datetime(v)

    if isinstance(obj, dict):
        return {k:_parse(k,v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [parse_dates(vi, key) for vi in obj]
    else:
        return obj

def replace_timestamps_in_dat(dat):
    dat = parse_dates(dat, 'created_at')
    return dat


def messages_to_df(spark, schema, messages):
    tweets = [json.loads(msg.value()) for msg in messages]
    tweets = [replace_timestamps_in_dat(t) for t in tweets]

    # do things to tweets to make them work with schema fo sho
    df = spark.createDataFrame(tweets, schema)
    return df

def get_consumer():
    kafka_brokers = os.getenv('KAFKA_BROKERS') # "localhost:9092"
    topic = os.getenv('BELLY_TOPIC') # tweets

    c = Consumer({
        'bootstrap.servers': kafka_brokers,
        'group.id': 'belly-test',
        'auto.offset.reset': 'earliest',
        'enable.auto.offset.store': False
    })

    c.subscribe([topic])

    return c

def build_spark():

    spark = SparkSession \
        .builder \
        .appName('belly') \
        .config("spark.jars", "/home/jupyter/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/jupyter/work/key.json") \
        .getOrCreate()

    return spark

def commit_messages(c, messages):
    c.commit(messages[0], asynchronous=False)


def main(outpath):
    spark = build_spark()
    schema = read_schema('gs://spain-tweets/schemas/tweet-3.pickle')
    c = get_consumer()

    messages = consume(c, 1000)
    df = messages_to_df(spark, schema, messages)

    # TODO: Deduplicate
    # groupby month
    # get ids from warehouse
    # remove anything that already exists

    df.write.mode('append').parquet(outpath)

    commit_messages(c, messages)


if __name__ == '__main__':
    run(main)
