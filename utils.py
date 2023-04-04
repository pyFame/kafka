
"""
Additional Class for Kafka
"""

from dataclasses import dataclass
from typing import List,Callable,Optional

from confluent_kafka import KafkaException
import logging as log

from .enums import *

from .thread import keepAlive

from datetime import timedelta

import time


def delivery_report(err:str,msg:object)->None:
    if err is not None:
        err_msg = f'Message delivery failed: {err}'
        with open("delivery.log","a") as f1:
          f1.write(err_msg)
    else:
        with open("delivery.log","a") as f1:
          f1.write(f'message delivered to {msg.topic()} [{ msg.partition()}]')
  

#To be inherits to kafka
class kafkaUtils():

  context:bool = True
  
  def create_topics(self,topics:List[str]): #todo add more sophistications later
    from confluent_kafka.admin import NewTopic

    admin_client = self.admin_client or self.admin().admin_client
    num_partition = 6
    replication_factor = 3 #minimum
    
    new_topics = [NewTopic(topic, num_partitions=num_partition, replication_factor=replication_factor) for topic in topics]
    futures = admin_client.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            log.info(f"Topic {topic} created")
        except KafkaException as e:
            log.error(f"Failed to create topic {topic}: {e}")

  @staticmethod
  def Delivery_report(err:str, msg:object)->None:
     return delivery_report(err,msg)
  
  def publish(self,message:KafkaMessage,callback:Callable[[str,object],None] = delivery_report): 
    producer = self.publisher or self.producer().publisher

    topic = message.topic
    key = message.key
    value = message.val
    producer.produce(topic, key=key, value=value,callback=callback)
    producer.flush()
    log.info(f"produced {message}")

  @keepAlive
  def stop_consumer(self,after:timedelta=timedelta(hours=4))->None:
    log.info(f"killing consumer after {after}")
    time.sleep(after.total_seconds())
    self.context = False
    log.info(f"killed consumer after {after}")

  def consume(self,consumer_ppt:Optional[ConsumerProperties]=None)->None:
    consumer_ppt = consumer_ppt or self.consumer_ppt 

    self.context = True

    if consumer_ppt==None:
      raise ValueError("ConsumerProperties not found")

    consumer = self.subscriber  or self.consumer(consumer_ppt).subscriber
    timeout = consumer_ppt.poll_timeout

    while self.context:
        try:
            msg = consumer.poll(timeout)

            if msg is None:
                log.debug("Invalid Kafka message")
                continue

            if msg.error():
                log.error(f"Kafka failed to deliver message: {msg.error()}")
                time.sleep(timeout * 20)
                continue

            key = msg.key().decode("utf-8")
            val = msg.value().decode("utf-8")
            consumer_ppt.callback(key, val)

        except KafkaException as e:
            log.error(f"Kafka failed to subscribe: {consumer_ppt} due to: {e}")

        except KeyboardInterrupt:
            log.debug("Received KeyboardInterrupt")
            break

        except Exception as e:
            log.error(f"An error occurred: {e}")
            break

    consumer.close()
    log.info("Consumer closed")
