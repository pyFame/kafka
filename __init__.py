from typing import Optional

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient

from .enums import *
from .utils import kafkaUtils


class Kafka(kafkaUtils):
    """
  utils functions like produce/publish are in kafkaUtils i.e kafka_utils.py
  """

    publisher: Producer = None
    subscriber: Consumer = None
    admin_client: AdminClient = None
    consumer_ppt: ConsumerProperties = None

    def __init__(self, config_file: Optional[str] = None):

        self.config_file = config_file or self.Download_Kafka_Gist()
        self.config = self.Read_ccloud_config(self.config_file)

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
        gist_url = "https://gist.github.com/pythoneerHiro/02b3400c9f62c278f811362d97db2d9b"
        filename = "kafka.txt"
        raw_url = f"{gist_url}/raw/{filename}"

        response = requests.get(raw_url)

        if response.status_code == 200:
            print(response.text)
            with open(filename, "w") as f:
                f.write(response.text)
        else:
            raise f"Failed to download {filename} from {gist_url}"

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
