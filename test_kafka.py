import os
import queue
import random
import threading
import time
import uuid
from datetime import timedelta

import pytest
from confluent_kafka import KafkaError

from .thread import keepAlive
from . import *
from .enums import *

TIMEOUT = timedelta(minutes=1).total_seconds()
TOPIC = "test"
msg = KafkaMessage(TOPIC, "name", "hiro")

config_file = Kafka.Download_Kafka_Gist()

uid = str(uuid.uuid4())

json_consume = f'{uid}kafka_test_consume.json'
delivery_log = f"{uid}delivery_test.log"

consumer_queue = queue.Queue(maxsize=2)
delivery_queue = queue.Queue(maxsize=2)

cgid = os.getenv("CGID","test_kafka.py")


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

        with open(delivery_log, "w") as f1:
            f1.write(err_msg)
    else:
        report = {
            "topic": msg.topic(),
            "partition": msg.partition(),
        }
        with open(delivery_log, "w") as f1:
            f1.write("msg sent")

    delivery_queue.put(report, block=False, timeout=0)


def handle_consume(key: str, val: str):
    # msg = KafkaMessage(TOPIC, key, val)
    if consumer_queue.full():
        print("warning consumer queue full")

    rcvd_msg = {
        "key": key,
        "val": val
    }

    with open(json_consume, 'w') as f1:
        # Write the JSON-formatted data to the file
        json.dump(rcvd_msg, f1)

    consumer_queue.put(rcvd_msg, block=False)

    # raise Exception("got what was req")
    # FIXME time.sleep(TIMEOUT)  # prevent multiple reads


@keepAlive
def keep_publishing(msg: KafkaMessage):
    k = Kafka(config_file)
    producer = k.producer()

    while True:
        producer.publish(msg)
        print(f"published {msg}")
        time.sleep(1)


def test_publish():
    k = Kafka(config_file)
    prod = k.producer()
    prod.publish(msg, delivery_report)
    time.sleep(10)

    with open(delivery_log, "r") as f1:
        line = f1.readline().strip()
        assert line.strip() == "msg sent"


def test_consume():
    # random.seed()#TODO
    #cgid = f"pytest-{random.randint(0, 99999)}"
    cppt = ConsumerProperties(TOPIC, cgid, LATEST, callback=handle_consume)
    k = Kafka(config_file)
    consumer = k.consumer(cppt)

    print("starting the consumer")
    # threading.Timer(0, consumer.consume).start()
    threading.Thread(target=consumer.consume, args=()).start()

    print("starting publisher")
    # publish_thread = threading.Timer(0.01, test_publish)
    # publish_thread.start()
    # publish_thread.join() #wait for it to join
    keep_publishing(msg)
    k.producer().publish(msg)  # FIXME remove

    rcvd_msg = consumer_queue.get(timeout=TIMEOUT)  # TODO Timeout
    actual_msg = {
        "key": msg.key,
        "val": msg.val
    }

    assert rcvd_msg == actual_msg

    with open(json_consume, 'r') as f:
        kafka_msg = json.load(f)
        consumer_key = kafka_msg["key"]
        consumer_val = kafka_msg["val"]
        assert consumer_key == msg.key
        assert consumer_val == msg.val
        assert actual_msg == kafka_msg

    print("stopping the consumer")
    k.stop_consumer(timedelta())


@pytest.fixture(scope='session', autouse=True)
def cleanup():
    print("cleanup activated")
    temp_files = [json_consume, delivery_log]
    print("Performing cleanup tasks before running tests")
    yield
    print("Performing cleanup tasks after running tests")
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)
