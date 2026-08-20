import asyncio
import json
import logging
import os
from collections import Counter as Tally
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Gauge, start_http_server
from psycopg_pool import AsyncConnectionPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clicks")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clicks-consumer")
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "5"))
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", "500"))

CONSUMED = Counter("clicks_consumed_total", "Click events read from Kafka")
FLUSHED = Counter("clicks_flushed_total", "Click aggregates written to Postgres")
FLUSH_ERRORS = Counter("clicks_flush_errors_total", "Failed flushes to Postgres")
BATCH_SIZE = Gauge("clicks_pending_batch", "Events waiting in the in-memory batch")


def build_dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("PGHOST", "pg-cluster-rw")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "shortener")
    password = os.getenv("PGPASSWORD", "")
    db = os.getenv("PGDATABASE", "shortener")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


async def flush(pool: AsyncConnectionPool, tally: Tally) -> None:
    if not tally:
        return
    rows = [(code, day, count) for (code, day), count in tally.items()]
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "INSERT INTO click_stats (code, day, clicks) VALUES (%s, %s, %s) "
                    "ON CONFLICT (code, day) DO UPDATE "
                    "SET clicks = click_stats.clicks + EXCLUDED.clicks",
                    rows,
                )
        FLUSHED.inc(sum(tally.values()))
        log.info("flushed %d aggregate rows", len(rows))
        tally.clear()
    except Exception:
        FLUSH_ERRORS.inc()
        log.exception("flush failed, keeping batch for the next attempt")


async def main() -> None:
    start_http_server(8000)
    pool = AsyncConnectionPool(build_dsn(), min_size=1, max_size=5, open=False)
    await pool.open(wait=True, timeout=30)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    log.info("consuming %s from %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP)

    tally: Tally = Tally()
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=int(FLUSH_SECONDS * 1000), max_records=FLUSH_SIZE)
            for _, messages in batches.items():
                for msg in messages:
                    event = msg.value
                    day = datetime.fromtimestamp(event["ts"], tz=timezone.utc).date()
                    tally[(event["code"], day)] += 1
                    CONSUMED.inc()
            BATCH_SIZE.set(sum(tally.values()))
            if tally:
                await flush(pool, tally)
                BATCH_SIZE.set(sum(tally.values()))
                if not tally:
                    await consumer.commit()
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
