"""Runnable ETL entrypoint: wires infra and runs the consume loop.

Usage: python -m hr_etl
Requires the external Kafka broker, MongoDB, PostgreSQL and Redis to be reachable
(see docker-compose.yml). This module is intentionally not imported by tests.
"""

from __future__ import annotations

import redis
from prometheus_client import start_http_server
from pymongo import MongoClient

from hr_etl.cache.redis_buffer import RedisBuffer
from hr_etl.config import get_settings
from hr_etl.consumer.kafka_consumer import KafkaMessageConsumer
from hr_etl.lake.mongo_lake import MongoLake
from hr_etl.logging_conf import configure_logging, get_logger
from hr_etl.pipeline import Pipeline
from hr_etl.warehouse.engine import create_db_engine, init_schema, make_session_factory
from hr_etl.warehouse.person_repo import PersonRepository


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("hr_etl.main")

    # Expose Prometheus metrics.
    start_http_server(9100)

    # Lake (Mongo)
    mongo = MongoClient(settings.mongo_uri)
    collection = mongo[settings.mongo_db][settings.mongo_raw_collection]
    lake = MongoLake(collection)

    # Cache (Redis)
    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password or None,
        decode_responses=True,
    )
    buffer = RedisBuffer(redis_client, ttl=settings.redis_buffer_ttl)

    # Warehouse (Postgres)
    engine = create_db_engine(settings.postgres_dsn)
    init_schema(engine)
    repo = PersonRepository(make_session_factory(engine))

    pipeline = Pipeline(lake, buffer, repo, min_fragments=settings.consolidation_min_fragments)
    consumer = KafkaMessageConsumer(settings)

    logger.info("HR ETL started; consuming topic=%s", settings.kafka_topic)
    for message in consumer.consume():
        pipeline.process_message(message)


if __name__ == "__main__":
    main()
