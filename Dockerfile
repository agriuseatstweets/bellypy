FROM nandanrao/pyspark-notebook

RUN pip install --user \
        confluent-kafka \
        gcsfs \
        tweepy \
        clize

COPY . .

CMD ["python", "belly.py"]
