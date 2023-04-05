import json
import time
from datetime import timedelta

from . import *
from .enums import *

TIMEOUT = timedelta(minutes=10)
TOPIC = "test"


def delivery_report(err: str, msg: object) -> None:
    if err is not None:
        err_msg = f'Message delivery failed: {err}'
        with open("delivery-test.log", "w") as f1:
            f1.write(err_msg)
    else:
        with open("delivery-test.log", "w") as f1:
            f1.write("msg sent")


def handle_consume(key: str, val: str):
    obj = {
        "key": key,
        "val": val
    }

    with open('kafka_test_consume.json', 'w') as f1:
        # Write the JSON-formatted data to the file
        json.dump(obj, f1)

    time.sleep(TIMEOUT)  # prevent multiple reads


key = "name"
val = "hiro"


def test_publish():
    msg = KafkaMessage(TOPIC, key, val)

    k = Kafka()
    prod = k.producer()
    prod.publish(msg, delivery_report)
    time.sleep(10)
    print("check delivery.log")

    with open("delivery-test.log", "r") as f1:
        line = f1.readline().strip()
        assert line.strip() == "msg sent"


def test_consume():
    cppt = ConsumerProperties(TOPIC, "pytest", LATEST, callback=handle_consume)
    k = Kafka()
    consumer = k.consumer(cppt)

    k.stop_consumer(TIMEOUT)

    print("starting the consumer")
    consumer.consume()

    with open('kafka_test_consume.json', 'r') as f:
        kafka_msg = json.load(f)
        consumer_key = kafka_msg["key"]
        consumer_val = kafka_msg["val"]

    assert consumer_key == key
    assert consumer_val == val

    print(consumer_key, consumer_val)
