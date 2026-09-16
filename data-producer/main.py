import logging
import os
import signal
import sys
import time

from utils.config import load_config, get_kafka_config, get_transaction_config
from utils.database import load_fraud_rules 
from utils.kafka_producer import create_producer, send_fraud_rules
from utils.generator import generate_timestamp, generate_transaction, validate_static_data


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger("fraudshield-data-producer")


# Graceful Shutdown
def shutdown_handler(signum, frame):
    """
    Handle shutdown signals gracefully.
    """
    global RUNNING_STATE

    logger.info(
        f"Received signal {signum}. "
        "Shutting down producer..."
    )

    RUNNING_STATE = False


# Producer Loop
def run_producer(static_data, customers, running_state):
    """
    Start Kafka producer and generate transactions.
    """

    # Load configuration
    try:
        logger.info("=" * 70)
        logger.info("FraudShield Data Producer Started")
        logger.info("Loading configuration...")
        config_kafka = get_kafka_config()
        config_transaction = get_transaction_config()
        logger.info(f"Kafka config: {config_kafka}")
        logger.info(f"Transaction config: {config_transaction}")
        logger.info("=" * 70)
    except Exception as e:
        logger.error(f"Error occurred while loading configuration: {e}")
        sys.exit(1)
    
    # Create Kafka producer
    try:
        producer = create_producer(config_kafka["bootstrap_servers"])
        transaction_count = 0
        logger.info("Kafka producer created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Load Fraud Rules from PostgreSQL
    try:
        logger.info("Loading fraud rules from PostgreSQL...")
        fraud_rules = load_fraud_rules()
        if not fraud_rules:
            raise RuntimeError("No fraud rules found in PostgreSQL.")
        
        # Publish Fraud Rules
        send_fraud_rules(
            logger=logger,
            producer=producer,
            topic=config_kafka["kafka_topic_fraud_rules"],
            rules=fraud_rules,
            updated_at=generate_timestamp(),
        )
    except RuntimeError as e:
        logger.error(f"Error occurred while loading fraud rules: {e}")
        sys.exit(1)

    
    # Transaction Producer Loop
    try:
        while running_state:
            if (config_transaction['total_transactions'] > 0 and transaction_count >= config_transaction['total_transactions']):
                logger.info(f"Target transaction count reached: {transaction_count}")
                break

            transaction = generate_transaction(customers, static_data, config_transaction)
            transaction_key = transaction["customer_id"]
            try:
                future = producer.send(
                    config_kafka["kafka_topic_transaction"],
                    key=transaction_key,
                    value=transaction,
                )
                metadata = future.get(timeout=10)
                transaction_count += 1
                logger.info(
                    f"Sent transaction | "
                    f"id={transaction['transaction_id']} | "
                    f"customer={transaction['customer_id']} | "
                    f"amount={transaction['amount']:.2f} | "
                    f"partition={metadata.partition} | "
                    f"offset={metadata.offset}"
                )
            except Exception as e:
                logger.exception(f"Failed to send transaction: {transaction['transaction_id']}: {e}")
                
            if running_state:
                time.sleep(config_kafka["producer_interval_seconds"])

    finally:
        logger.info("Flushing Kafka producer...")
        producer.flush()
        producer.close()
        logger.info("FraudShield Data Producer stopped.")


# Application Entry Point
if __name__ == "__main__":
    try:
        customers = {}
        RUNNING_STATE = True
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        static_data = load_config(get_transaction_config()["static_data_file"])
        validate_static_data(static_data)
        run_producer(static_data, customers, RUNNING_STATE)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    except Exception:
        logger.exception("Fatal error in producer.")
        sys.exit(1)