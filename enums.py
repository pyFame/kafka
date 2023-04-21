import json
import logging as log
from dataclasses import dataclass
from typing import Final, Union, Callable

LATEST: Final[str] = "latest"
EARLIEST: Final[str] = "earliest"

valid_kafka_json = [str, int, float]


@dataclass(frozen=False)  # FIXME: frozen= true
class KafkaMessage:
    topic: str
    key: Union[str, int, float]
    val: Union[str, int, float]

    def __post_init__(self):
        self.key = self._json(self.key)
        self.val = self._json(self.val)

    def _json(self, value: Union[str, object, int]) -> Union[str, int, float]:
        if type(value) in valid_kafka_json:
            return value

        log.warning(f"invalid json - {value}")

        if hasattr(value, 'json'):
            log.debug(f"implemented json attribute {value}")
            return value.json

        value = json.dumps(value)

        return value


@dataclass
class ConsumerProperties():
    topic: str
    cgid: str = "kafka.py"  # ConsumerGroup id
    resume_at: Union[LATEST, EARLIEST] = LATEST  # in case of resuming from downtime latest by default

    callback: Callable[[str, str], None] = log.info
    poll_timeout: float = 1.0  # timeout
