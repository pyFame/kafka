import queue
import time
from datetime import timedelta

import pytest

from . import *
from .enums import *

TIMEOUT = timedelta(minutes=10).total_seconds()
TOPIC = "test"
msg = KafkaMessage(TOPIC, "name", "hiro")

config_file = Kafka.Download_Kafka_Gist()

consumer_queue = queue.Queue(maxsize=1)


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

    consumer_queue.put(obj, block=False)

    with open('kafka_test_consume.json', 'w') as f1:
        # Write the JSON-formatted data to the file
        json.dump(obj, f1)

    time.sleep(TIMEOUT)  # prevent multiple reads


@pytest.mark.order(1)
def test_publish():
    k = Kafka(config_file)
    prod = k.producer()
    prod.publish(msg, delivery_report)
    time.sleep(10)

    with open("delivery-test.log", "r") as f1:
        line = f1.readline().strip()
        assert line.strip() == "msg sent"


@pytest.mark.order(2)
def test_consume():
    cppt = ConsumerProperties(TOPIC, "pytest", LATEST, callback=handle_consume)
    k = Kafka(config_file)
    consumer = k.consumer(cppt)

    k.stop_consumer(TIMEOUT)

    print("starting the consumer")
    consumer.consume()

    with open('kafka_test_consume.json', 'r') as f:
        kafka_msg = json.load(f)
        consumer_key = kafka_msg["key"]
        consumer_val = kafka_msg["val"]

    assert consumer_key == msg.key
    assert consumer_val == msg.val

    q_msg = consumer_queue.get(timeout=TIMEOUT)

    assert q_msg["key"] == msg.key
    assert q_msg["val"] == msg.val

    print(consumer_key, consumer_val)
