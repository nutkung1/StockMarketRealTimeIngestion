from kafka import KafkaConsumer
from json import loads
import json

consumer = KafkaConsumer(
    "demotest",  # The topic name
    bootstrap_servers=["107.22.152.42:9092"],
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

from s3fs import S3FileSystem

s3 = S3FileSystem()
for count, i in enumerate(consumer):
    with s3.open(
        f"s3://kafka-stock-market-realtime-suchanat/stock_data{count}.json", "w"
    ) as file:
        json.dump(i.value, file)
