import os

def get_required_env(name):
    """
    Retrieve a required environment variable.
    Args:
        name: Name of the environment variable.
    Returns:
        str: Value of the environment variable.
    Raises:
        ValueError: If the environment variable is not set or empty.
    """
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def get_telegram_config():
    """
    Load Telegram bot configuration from environment variables.
    Returns:
        dict: Telegram configuration containing bot token, chat ID, and optional topic ID.
    """
    return {
        "bot_token": get_required_env("TELEGRAM_BOT_TOKEN"),
        "chat_id": get_required_env("TELEGRAM_CHAT_ID"),
        "topic_id": os.getenv("TELEGRAM_TOPIC_ID", None),
    }


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
            "bootstrap_servers": get_required_env("KAFKA_SERVER"),
            "group_id": get_required_env("KAFKA_ALERT_CONSUMER_GROUP"),
            "auto_offset_reset": os.getenv("KAFKA_ALERT_AUTO_OFFSET_RESET", "latest"),
            "enable_auto_commit": os.getenv("KAFKA_ALERT_ENABLE_AUTO_COMMIT", "False").lower() == "true",
            "kafka_topic_alert": os.getenv("KAFKA_ALERT_TOPIC", "fraud_alerts"),
        }
    except ValueError as error:
        raise ValueError(f"Invalid Kafka configuration: {error}")

    
# Load alerting configuration from environment variables
def get_alerting_config():
    """
    Load alert summary settings from environment variables.
    Returns:
        dict: Alerting configuration containing the summary interval and maximum number of recent alerts to display.
    Raises:
        ValueError: If a configuration value is not a valid integer.
    """
    try:
        return {
            "summary_interval_seconds": int(os.getenv("ALERT_SUMMARY_INTERVAL_SECONDS", "300")),
            "max_recent_alerts": int(os.getenv("ALERT_SUMMARY_MAX_RECENT_ALERTS", "10")),
        }
    except ValueError as error:
        raise ValueError(f"Invalid Kafka configuration: {error}")
    