import os
import yaml
import json
from pathlib import Path


# Config Loader
def load_config(file_path):
    """
    Load configuration data from JSON or YAML file.
    Supported formats:
        - .json
        - .yml
        - .yaml
    Args: file_path: Path to the configuration file.
    Returns: Configuration data loaded as a Python dictionary or object.
    Raises: 
        FileNotFoundError: If the configuration file does not exist.
        ValueError: If the file format is unsupported or the file is empty
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


# Load Kafka configuration from environment variables
def get_kafka_config():
    """
    Load Kafka connection and consumer settings from environment variables.
    Returns: A dictionary containing the Kafka configuration.
    Raises: 
        ValueError: If an invalid Kafka configuration value is provided.
    """
    
    try:
        return {
            "bootstrap_servers": os.getenv("KAFKA_SERVER", "localhost:9092"),
            "group_id": os.getenv("KAFKA_CONSUMER_GROUP_SINK", "fraud-sink"),
            "auto_offset_reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        }
    except ValueError as e:
        raise ValueError(f"Invalid Kafka configuration: {e}")