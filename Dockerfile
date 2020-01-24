FROM nandanrao/pyspark-notebook

RUN pip install --user \
        confluent-kafka \
        gcsfs \
        tweepy \
        clize

COPY gcs-connector-hadoop2-latest.jar .
COPY keys/key.json ./work/
