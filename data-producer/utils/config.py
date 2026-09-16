import os
import json
import yaml
from pathlib import Path


# Config Loader
def load_config(file_path):
    """
    Load configuration data from JSON or YAML file.
    Supported formats:
        - .json
        - .yml
        - .yaml
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    file_format = path.suffix.lower()
    with path.open("r", encoding="utf-8") as file:
        if file_format == ".json":
            data = json.load(file)
        elif file_format in (".yml", ".yaml"):
            data = yaml.safe_load(file)
        else:
            raise ValueError(
                f"Unsupported config file format: {file_format}. "
                f"Supported formats: .json, .yml, .yaml"
            )

    if not data:
        raise ValueError(f"Config file is empty: {path}")

    return data


# Kafka Config
def get_kafka_config():
    """
    Load Kafka connection settings from environment variables.
    Returns: A dictionary containing the Kafka connection configuration.
    Raises:
        ValueError: If an invalid configuration value is provided.
    """
    try:
        return {
            "kafka_server": os.getenv("KAFKA_SERVER", "kafka:9092"),
            "kafka_topic_transaction": os.getenv("KAFKA_TOPIC_TRANSACTION", "transactions_raw"),
            "kafka_topic_fraud_rules": os.getenv("KAFKA_TOPIC_FRAUD_RULES", "fraud_rules"),
            "producer_interval_seconds": float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1")),
            "bootstrap_servers": os.getenv("KAFKA_SERVER", "localhost:9092"),
            "group_id": os.getenv("KAFKA_CONSUMER_GROUP_SINK", "fraud-sink"),
        }
    except ValueError as e:
        raise ValueError(f"Invalid Kafka configuration: {e}")


# Transaction Config
def get_transaction_config():
    """
    Load transaction generation settings from environment variables.
    Returns: A dictionary containing the transaction generation configuration.
    Raises: 
        ValueError: If an invalid configuration value is provided.
    """
    try:
        return {
            "total_transactions": int(os.getenv("TOTAL_TRANSACTIONS", "0")),
            "total_events": "infinite" if int(os.getenv("TOTAL_TRANSACTIONS", "0")) == 0 else int(os.getenv("TOTAL_TRANSACTIONS", "0")),
            "existing_customer_probability": float(os.getenv("EXISTING_CUSTOMER_PROBABILITY", "0.95")),
            "suspicious_transaction_probability": float(os.getenv("SUSPICIOUS_TRANSACTION_PROBABILITY", "0.10")),
            "static_data_file": os.getenv("STATIC_DATA_FILE", "static-data.yml"),
        }
    except ValueError as e:
        raise ValueError(f"Invalid Transaction configuration: {e}")
    
# Load PostgreSQL configuration from environment variables
def get_postgres_config():
    """
    Load PostgreSQL connection settings from environment variables.
    Returns: A dictionary containing the PostgreSQL connection configuration.
    Raises: 
        ValueError: If the PostgreSQL port cannot be converted to an integer.
    """
    
    try:
        return {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "database": os.getenv("POSTGRES_DB", "postgres"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }
    except ValueError as e:
        raise ValueError(f"Invalid PostgreSQL configuration: {e}")


