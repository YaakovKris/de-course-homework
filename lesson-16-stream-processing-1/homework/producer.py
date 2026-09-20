# Завдання 3: Kafka producer.
# Запуск із цієї директорії (homework/):  uv run python producer.py
import gzip
import json
import logging
import urllib.request
from typing import Iterator

from confluent_kafka import KafkaError, Message, Producer
from icecream import ic

from transform import event_filter, flatten_event

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Дано, не редагувати.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "github-events"
ARCHIVE_URL = "https://data.gharchive.org/2024-01-15-14.json.gz"
MAX_RAW = 100_000

# gharchive returns HTTP 403 to urllib's default User-Agent, so set our own.
_USER_AGENT = "de-course-homework/1.0"


def iter_archive(url: str, max_raw: int) -> Iterator[dict]:
    """Дано, не редагувати.

    Yield up to `max_raw` raw GitHub Archive events (parsed JSON dicts).
    Records arrive in the file's original (roughly chronological) order. No
    filtering or flattening happens here — that is your job in transform.py.
    """
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            for count, line in enumerate(gz):
                if count >= max_raw:
                    break
                yield json.loads(line)


def build_producer() -> Producer:
    """Завдання 3a.

    Поверніть налаштований confluent_kafka.Producer, що під'єднується до
    BOOTSTRAP_SERVERS. Увімкніть idempotent producer (`enable.idempotence`) і
    `acks="all"`, щоб ретраї не створювали дублікатів.
    """
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
    })


def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Async callback fired by poll()/flush() once a produce() is acked (or fails)."""
    if err is not None:
        logger.warning("delivery failed for key=%s: %s", msg.key(), err)


def run_producer() -> int:
    """Завдання 3b (разом 25 балів).

    1. Створіть producer через build_producer().
    2. Пройдіть події з iter_archive(ARCHIVE_URL, MAX_RAW).
    3. Відкиньте ті, що не проходять event_filter().
    4. Для решти: flatten_event(), потім produce у топік TOPIC,
       де key = repo_name (bytes), value = JSON-байти запису.
       Ключ за repo_name тримає події одного репозиторію в одній partition.
    5. Після кожного produce() викликайте producer.poll(0) (не блокуюче).
    6. Наприкінці producer.flush(30). Поверніть к-сть надісланих подій.
    """
    producer = build_producer()
    sent = 0
    for event in iter_archive(ARCHIVE_URL, MAX_RAW):
        if not event_filter(event):
            continue
        record = flatten_event(event)
        key = record["repo_name"].encode("utf-8")
        value = json.dumps(record).encode("utf-8")
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value, callback=_delivery_report)
                break
            except BufferError:
                # Local queue full: drain delivery callbacks to free space, then retry.
                producer.poll(0.1)
        producer.poll(0)
        sent += 1
    pending = producer.flush(30)
    if pending:
        logger.warning("%d message(s) still undelivered after flush timeout", pending)
    return sent


if __name__ == "__main__":
    ic(run_producer())
