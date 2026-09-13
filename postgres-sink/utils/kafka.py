import json
from kafka import KafkaConsumer


def create_consumer(kafka_config, catalog):
    """
    Create a Kafka consumer with the specified configuration and topic catalog.
    Args:
        kafka_config: Dictionary containing Kafka connection settings.
        catalog: Dictionary containing topic configurations.
    Returns: A Kafka consumer instance.
    Raises:
        RuntimeError: If the Kafka consumer creation fails.
    """
    topics = list(catalog.keys())
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=kafka_config["bootstrap_servers"].split(","),
            group_id=kafka_config["group_id"],
            auto_offset_reset=kafka_config.get("auto_offset_reset", "earliest"),
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            key_deserializer=lambda key: key.decode("utf-8") if key else None,
        )
        return consumer
    except Exception as e:
        raise RuntimeError(f"Failed to create Kafka consumer: {e}")
    

def validate_message(message, topic_config):
    """
    Validate a SQL identifier such as a table or column name.
    Only letters, numbers, and underscores are allowed, and the identifier must not start with a number.
    Args:
        identifier: SQL table name or column name to validate.
    Returns: The validated identifier.
    Raises:
        ValueError: If the identifier contains invalid characters.

    """
    if topic_config is None:
        raise ValueError(f"Topic '{message.topic}' is not configured in catalog.yml")

    if not isinstance(message.value, dict):
        raise ValueError(f"Message from topic '{message.topic}' is not a JSON object")