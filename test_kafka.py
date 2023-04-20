import queue
import threading
import time
from datetime import timedelta

import pytest
from confluent_kafka import KafkaError

from . import *
from .enums import *

TIMEOUT = timedelta(minutes=10).total_seconds()
TOPIC = "test"
msg = KafkaMessage(TOPIC, "name", "hiro")

config_file = Kafka.Download_Kafka_Gist()

consumer_queue = queue.Queue(maxsize=1)
delivery_queue = queue.Queue(maxsize=1)


def delivery_report(err: str, msg: object) -> None:
    report = {}

    if err is not None:
        err_msg = f'Message delivery failed: {err}'
        report = {
            "error": True,
            "code": err.code(),
            "msg": err_msg,
        }

        if err.code() == KafkaError.REQUEST_TIMED_OUT:
            print('Message delivery failed: Message timed out')
        else:
            print(err_msg)

        with open("delivery-test.log", "w") as f1:
            f1.write(err_msg)
    else:
        report = {
            "topic": msg.topic(),
            "partition": msg.partition(),
        }
        with open("delivery-test.log", "w") as f1:
            f1.write("msg sent")

    delivery_queue.put(report, block=False, timeout=0)


def handle_consume(key: str, val: str):
    # msg = KafkaMessage(TOPIC, key, val)
    obj = {
        "key": key,
        "val": val
    }
    consumer_queue.put(msg, block=False)

    with open('kafka_test_consume.json', 'w') as f1:
        # Write the JSON-formatted data to the file
        json.dump(obj, f1)

    # FIXME time.sleep(TIMEOUT)  # prevent multiple reads


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

    print("starting the consumer")
    threading.Timer(0.1, consumer.consume)

    rcvd_msg = consumer_queue.get()  # TODO Timeout
    actual_msg = {
        "key": msg.key,
        "val": msg.val
    }

    assert rcvd_msg == actual_msg

    with open('kafka_test_consume.json', 'r') as f:
        kafka_msg = json.load(f)
        consumer_key = kafka_msg["key"]
        consumer_val = kafka_msg["val"]
        assert consumer_key == msg.key
        assert consumer_val == msg.val
        assert actual_msg == kafka_msg

    print("stopping the consumer")
    k.stop_consumer(timedelta())
