import logging as log
from dataclasses import dataclass
from typing import Final, Union, Callable

LATEST: Final[str] = "latest"
EARLIEST: Final[str] = "earliest"


@dataclass(frozen=False)  # FIXME: frozen= true
class KafkaMessage:
    topic: str
    key: str
    val: str


@dataclass
class ConsumerProperties():
    topic: str
    cgid: str = "kafka.py"  # ConsumerGroup id
    resume_at: Union[LATEST, EARLIEST] = EARLIEST  # in case of resuming from downtime latest by default

    poll_timeout: float = 1.0  # timeout

    callback: Callable[[str, str], None] = log.info
