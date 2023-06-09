import os.path
from typing import Optional

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient

from .enums import *
from .utils import kafkaUtils


class Kafka(kafkaUtils):
    __slots__ = ('publisher', 'subscriber', 'admin_client',
                 'consumer_ppt')
    """
  utils functions like produce/publish are in kafkaUtils i.e kafka_utils.py
  """

    publisher: Producer
    subscriber: Consumer
    admin_client: AdminClient
    consumer_ppt: ConsumerProperties

    def __init__(self, config_file: Optional[str] = 'conf/kafka.txt'):

        if config_file is None or not os.path.exists(config_file):
            raise ValueError("invalid config_file {config_file}")

        self.config_file = config_file
        self.config = self.Read_ccloud_config(self.config_file)

        self.publisher = None
        self.subscriber = None
        self.admin_client = None
        self.consumer_ppt = None

    @staticmethod
    def Publisher(config: dict) -> Producer:
        return Producer(config)

    @staticmethod
    def Subscriber(config: dict, consumer_properties: ConsumerProperties) -> Consumer:
        props = config
        props["group.id"] = consumer_properties.cgid
        props["auto.offset.reset"] = consumer_properties.resume_at

        consumer = Consumer(props)
        topic = consumer_properties.topic
        consumer.subscribe([topic])  # TODO:support pubsub - i.e multiple topics
        return consumer

    @staticmethod
    def Read_ccloud_config(config_file) -> dict:
        conf = {}
        with open(config_file) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()
        return conf

    @staticmethod
    def Download_Kafka_Gist() -> str:
        import requests
        gist_url = "https://gist.github.com/Nasfame/e1706606c6b51078da007b1d04dd501e"
        filename = "kafka.txt"
        raw_url = f"{gist_url}/raw/{filename}"

        response = requests.get(raw_url)

        if response.status_code == 200:
            print(response.text)
            with open(filename, "w") as f:
                f.write(response.text)
        else:
            raise FileNotFoundError(f"Failed to download {filename} from {gist_url}")

        return filename

    def producer(self) -> 'Kafka':
        producer = self.publisher or self.Publisher(self.config)
        self.publisher = producer
        return self

    def consumer(self, consumer_ppt: ConsumerProperties) -> 'Kafka':
        consumer = self.Subscriber(self.config, consumer_ppt)
        self.subscriber = consumer
        self.consumer_ppt = consumer_ppt
        return self

    def admin(self) -> 'Kafka':
        admin_client = self.admin_client or AdminClient(self.config)
        self.admin_client = admin_client
        return self
