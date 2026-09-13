import os
import time
import psycopg2
import logging

from utils.config import load_config, get_postgres_config, get_kafka_config
from utils.kafka import create_consumer
from utils.postgres import get_connection, process_message


# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger("fraudshield-postgres-sink")

CATALOG_FILE = os.getenv("CATALOG_FILE", "/app/config/catalog.yml")
logger.info(f"Catalog file: {CATALOG_FILE}")


if __name__ == "__main__":
    catalog = load_config(CATALOG_FILE)
    kafka_config = get_kafka_config()
    postgres_config = get_postgres_config()
    
    consumer = create_consumer(kafka_config, catalog)
    connection = None

    logger.info(
        f"PostgreSQL Sink started. "
        f"Topics: {list(catalog.keys())}"
    )

    while True:
        try:
            if connection is None or connection.closed:
                logger.info("Connecting to PostgreSQL...")
                connection = get_connection(postgres_config)
                logger.info("PostgreSQL connected.")

            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    try:
                        process_message(
                            connection=connection,
                            message=message,
                            catalog=catalog,
                        )
                        consumer.commit()

                        logger.info(
                            f"Inserted topic={message.topic} "
                            f"partition={message.partition} "
                            f"offset={message.offset}"
                        )

                    except Exception as error:
                        connection.rollback()

                        logger.error(
                            f"Failed processing "
                            f"topic={message.topic} "
                            f"partition={message.partition} "
                            f"offset={message.offset}: {error}"
                        )

                        raise

        except psycopg2.Error as error:
            logger.error(f"PostgreSQL error: {error}")

            if connection:
                connection.close()

            connection = None
            time.sleep(5)

        except Exception as error:
            logger.error(f"Sink error: {error}")
            time.sleep(5)