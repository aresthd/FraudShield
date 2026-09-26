import json
from kafka import KafkaConsumer


def create_consumer(kafka_config):
    """
    Create a Kafka consumer with the specified configuration.
    Args:
        kafka_config: Dictionary containing Kafka connection settings.
    Returns:
        KafkaConsumer: Configured Kafka consumer instance.
    Raises:
        RuntimeError: If the Kafka consumer creation fails.
    """
    try:
        consumer = KafkaConsumer(
            kafka_config["kafka_topic_alert"],
            bootstrap_servers=kafka_config["bootstrap_servers"].split(","),
            group_id=kafka_config["group_id"],
            auto_offset_reset=kafka_config["auto_offset_reset"],
            enable_auto_commit=kafka_config["enable_auto_commit"],
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            key_deserializer=lambda key: (key.decode("utf-8") if key else None),
        )
        return consumer
    except Exception as error:
        raise RuntimeError(f"Failed to create Kafka consumer: {error}")


def consume_messages(logger, consumer, topic):
    """
    Consume messages from a Kafka topic using kafka-python.
    Args:
        logger: Logger instance for logging.
        consumer: KafkaConsumer instance.
        topic: Kafka topic to consume messages from.
    Yields: Tuples of (message, payload):
        message: kafka-python ConsumerRecord.
        payload: Deserialized JSON payload.
    Raises:
        RuntimeError: If an error occurs while consuming messages.
    """
    consumer.subscribe([topic])
    logger.info(f"Subscribed to Kafka topic: {topic}")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for _, messages in records.items():
                for message in messages:
                    try:
                        payload = message.value
                        yield message, payload
                    except Exception as error:
                        logger.error(f"Failed to process Kafka message at {message.topic}[{message.partition()}] | offset={message.offset()}: {error}")
                        continue

    except Exception as error:
        raise RuntimeError(f"Error while consuming messages: {error}")

    finally:
        consumer.close()


