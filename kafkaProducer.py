from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
from time import sleep
from json import dumps
import json

producer = KafkaProducer(
    bootstrap_servers=["107.22.152.42:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)

print("Finished sending message")
df = pd.read_csv("IndexProcessed.csv")
while True:
    stock_data_dict = df.sample(1).to_dict(orient="records")[0]
    producer.send("demotest", value=stock_data_dict)
    sleep(2)
producer.flush()
