import json
from kafka import KafkaProducer


# Kafka Producer
def create_producer(kafka_server):
    """
    Create Kafka producer.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_server.split(","),
            value_serializer=lambda value: (
                json.dumps(value, separators=(",", ":")).encode("utf-8")
            ),
            key_serializer=lambda key: (
                key.encode("utf-8")
                if key is not None
                else None
            ),
            acks="all",
            retries=10,
            retry_backoff_ms=1000,
            request_timeout_ms=30000,
            delivery_timeout_ms=120000,
            linger_ms=5,
            batch_size=32768,
        )
        return producer
    except Exception as e:
        raise RuntimeError(f"Failed to create Kafka producer: {e}")


# Send Fraud Rules
def send_fraud_rules(logger, producer, topic, rules, updated_at):
    """
    Publish fraud rules to Kafka.
    Each rule is sent as a separate Kafka event.
    Args:
        logger: Logger instance.
        producer: KafkaProducer instance.
        topic: Kafka fraud rules topic.
        rules: List of fraud rule dictionaries.
        updated_at: Timestamp representing when the rules were loaded/published.
    """
    logger.info(
        f"Publishing {len(rules)} fraud rules "
        f"to topic '{topic}'"
    )

    for rule in rules:
        # Create a copy so the original database result is not modified.
        kafka_rule = rule.copy()
        kafka_rule["updated_at"] = updated_at
        try:
            future = producer.send(
                topic,
                key=kafka_rule["rule_id"],
                value=kafka_rule,
            )
            metadata = future.get(timeout=10)
            logger.info(
                f"Sent fraud rule | "
                f"id={kafka_rule['rule_id']} | "
                f"type={kafka_rule['rule_type']} | "
                f"score={kafka_rule['score']} | "
                f"enabled={kafka_rule['enabled']} | "
                f"version={kafka_rule['version']} | "
                f"partition={metadata.partition} | "
                f"offset={metadata.offset}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to send fraud rule "
                f"id={kafka_rule['rule_id']} | "
                f"version={kafka_rule['version']} | "
                f"error={e}"
            )

    producer.flush()