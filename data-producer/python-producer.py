import json
import logging
import os
import random
import signal
import sys
import time
from faker import Faker
from kafka import KafkaProducer
from helpers import (
    load_config,
    generate_amount,
    generate_ip_address,
    generate_transaction_id,
    generate_timestamp,
    select_city,
    select_device,
)

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger("fraudshield-data-producer")

# Environment Variables
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions_raw")
KAFKA_TOPIC_TRANSACTION = os.getenv("KAFKA_TOPIC_TRANSACTION", "transactions_raw")
KAFKA_TOPIC_FRAUD_RULES = os.getenv("KAFKA_TOPIC_FRAUD_RULES", "fraud_rules")
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
TOTAL_TRANSACTIONS = int(os.getenv("TOTAL_TRANSACTIONS", "0"))
EXISTING_CUSTOMER_PROBABILITY = float(os.getenv("EXISTING_CUSTOMER_PROBABILITY", "0.95"))
SUSPICIOUS_TRANSACTION_PROBABILITY = float(os.getenv("SUSPICIOUS_TRANSACTION_PROBABILITY", "0.10"))
STATIC_DATA_FILE = os.getenv("STATIC_DATA_FILE", "static-data.yml")
FRAUD_RULES_FILE = os.getenv("FRAUD_RULES_FILE", "fraud-rules.yml")

# Random / Faker
fake = Faker("id_ID")
random.seed()

# Load Static Data
static_data = load_config(STATIC_DATA_FILE)
TRANSACTION_TYPES = static_data["transaction_types"]
CHANNELS = static_data["channels"]
PAYMENT_METHODS = static_data["payment_methods"]
CITIES = static_data["cities"]
MERCHANTS = static_data["merchants"]


# Validate Static Data
def validate_static_data():
    """
    Validate required static data sections.
    """
    required_sections = [
        "transaction_types",
        "channels",
        "payment_methods",
        "cities",
        "merchants",
    ]

    for section in required_sections:
        if section not in static_data:
            raise ValueError(f"Missing static data section: {section}")

    if not TRANSACTION_TYPES:
        raise ValueError("transaction_types cannot be empty")

    if not CITIES:
        raise ValueError("cities cannot be empty")

    if not MERCHANTS:
        raise ValueError("merchants cannot be empty")

    for transaction_type in TRANSACTION_TYPES:
        if transaction_type not in CHANNELS:
            raise ValueError(
                f"Missing channel configuration "
                f"for transaction type: "
                f"{transaction_type}"
            )

        if transaction_type not in PAYMENT_METHODS:
            raise ValueError(
                f"Missing payment method configuration "
                f"for transaction type: "
                f"{transaction_type}"
            )

    logger.info("Static data validation successful.")


# Customer State
customers = {}
def create_customer():
    """
    Create a synthetic customer.
    Customer state is maintained by the producer so that
    multiple transactions can belong to the same customer.
    """
    customer_number = len(customers) + 1
    customer_id = f"CUS-{customer_number:06d}"
    account_id = f"ACC-{customer_number:06d}"
    device_id = f"DEV-{customer_number:06d}"
    home_cities = [city for city in CITIES.keys() if CITIES[city]["country"] == "Indonesia"]
    home_city = random.choice(home_cities)
    customer = {
        "customer_id": customer_id,
        "account_id": account_id,
        "home_city": home_city,
        "device_id": device_id,
    }
    customers[customer_id] = customer
    return customer

def get_customer():
    """
    Reuse existing customers most of the time.
    This is important because the downstream Flink job
    will perform stateful processing based on customer_id.
    """
    if not customers or random.random() > EXISTING_CUSTOMER_PROBABILITY:
        return create_customer()

    customer_id = random.choice(list(customers.keys()))
    return customers[customer_id]


# Transaction Generator
def generate_transaction():
    """
    Generate one transaction according to
    transactions_raw schema.
    """
    customer = get_customer()
    suspicious = (random.random() < SUSPICIOUS_TRANSACTION_PROBABILITY)
    transaction_type = random.choice(TRANSACTION_TYPES)
    merchant = random.choice(MERCHANTS)
    city = select_city(
        customer=customer,
        cities=CITIES,
        suspicious=suspicious,
    )
    city_data = CITIES[city]
    transaction = {
        "transaction_id": generate_transaction_id(),
        "customer_id": customer["customer_id"],
        "account_id": customer["account_id"],
        "transaction_type": transaction_type,
        "amount": generate_amount(
            transaction_type=transaction_type,
            suspicious=suspicious,
        ),
        "currency": "IDR",
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "merchant_category": merchant["merchant_category"],
        "payment_method": random.choice(
            PAYMENT_METHODS[
                transaction_type
            ]
        ),
        "device_id": select_device(
            customer=customer,
            suspicious=suspicious,
        ),
        "ip_address": generate_ip_address(),
        "city": city,
        "country": city_data["country"],
        "latitude": city_data["latitude"],
        "longitude": city_data["longitude"],
        "timestamp": generate_timestamp(),
        "channel": random.choice(
            CHANNELS[
                transaction_type
            ]
        ),
    }

    return transaction


# Fraud Rules
def generate_fraud_rules():
    """
    Load fraud rules from JSON configuration.

    Fraud rules are configuration/control events rather than
    transaction events. Each rule is sent to the fraud_rules topic
    and consumed by Flink Job #2 through Broadcast State.
    """
    rules = load_config(FRAUD_RULES_FILE)
    updated_at = generate_timestamp()
    for rule in rules:
        rule["updated_at"] = updated_at

    return rules
    
    
# Kafka Producer
def create_producer():
    """
    Create Kafka producer.
    """
    logger.info(f"Connecting to Kafka: {KAFKA_SERVER}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER.split(","),
        value_serializer=lambda value: (
            json.dumps(
                value,
                separators=(",", ":"),
            ).encode("utf-8")
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


def send_fraud_rules(producer):
    """
    Send initial fraud rules to Kafka.
    Each rule is sent as a separate Kafka event.
    The rule_id is used as the Kafka message key so that
    updates for the same rule are consistently partitioned.
    """
    rules = generate_fraud_rules()
    logger.info(f"Publishing {len(rules)} fraud rules to topic '{KAFKA_TOPIC_FRAUD_RULES}'")

    for rule in rules:
        try:
            future = producer.send(
                KAFKA_TOPIC_FRAUD_RULES,
                key=rule["rule_id"],
                value=rule,
            )

            metadata = future.get(timeout=10)
            logger.info(
                f"Sent fraud rule | "
                f"id={rule['rule_id']} | "
                f"type={rule['rule_type']} | "
                f"score={rule['score']} | "
                f"enabled={rule['enabled']} | "
                f"version={rule['version']} | "
                f"partition={metadata.partition} | "
                f"offset={metadata.offset}"
            )

        except Exception as e:
            logger.exception(f"Failed to send fraud rule: {rule}")
            logger.error(f"Exception: {e}")

    producer.flush()

# Graceful Shutdown
running = True
def shutdown_handler(signum, frame):
    global running
    logger.info(
        f"Received signal {signum}. "
        "Shutting down producer..."
    )
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# Producer Loop
def run_producer():
    producer = create_producer()
    transaction_count = 0
    logger.info("=" * 70)
    logger.info("FraudShield Data Producer Started")
    logger.info(f"Kafka Server: {KAFKA_SERVER}")
    logger.info(f"Kafka Topic Transactions: {KAFKA_TOPIC_TRANSACTION}")
    logger.info(f"Kafka Topic Fraud Rules: {KAFKA_TOPIC_FRAUD_RULES}")
    logger.info(f"Producer Interval: {PRODUCER_INTERVAL_SECONDS} seconds")
    logger.info(f"Total Events: {'infinite' if TOTAL_TRANSACTIONS == 0 else TOTAL_TRANSACTIONS}")
    logger.info(f"Existing Customer: {EXISTING_CUSTOMER_PROBABILITY:.2f}")
    logger.info(f"Suspicious Event: {SUSPICIOUS_TRANSACTION_PROBABILITY:.2f}")
    logger.info(f"Static Data: {STATIC_DATA_FILE}")
    logger.info("=" * 70)

    try:
        # Publish Initial Fraud Rules
        send_fraud_rules(producer)

        # Transaction Producer Loop
        while running:
            if TOTAL_TRANSACTIONS > 0 and transaction_count >= TOTAL_TRANSACTIONS:
                logger.info(f"Target transaction count reached: {transaction_count}")
                break
            transaction = generate_transaction()
            transaction_key = transaction["customer_id"]
            try:
                future = producer.send(
                    KAFKA_TOPIC_TRANSACTION,
                    key=transaction_key,
                    value=transaction,
                )
                metadata = future.get(timeout=10)
                transaction_count += 1
                logger.info(
                    f"Sent transaction | "
                    f"id={transaction['transaction_id']} | "
                    f"customer={transaction['customer_id']} | "
                    f"type={transaction['transaction_type']} | "
                    f"amount={transaction['amount']:.2f} | "
                    f"city={transaction['city']} | "
                    f"partition={metadata.partition} | "
                    f"offset={metadata.offset}"
                )

            except Exception as e:
                logger.exception(f"Failed to send transaction: {transaction}")
                logger.error(f"Exception: {e}")

            if running:
                time.sleep(PRODUCER_INTERVAL_SECONDS)

    finally:
        logger.info("Flushing Kafka producer...")
        producer.flush()
        producer.close()
        logger.info("FraudShield Data Producer stopped.")


# Application Entry Point
if __name__ == "__main__":
    try:
        validate_static_data()
        run_producer()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    except Exception as e:
        logger.exception("Fatal error in producer.")
        logger.error(f"Exception: {e}")
        sys.exit(1)