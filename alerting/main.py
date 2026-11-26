import logging
import os
import time
from datetime import datetime, timezone

from utils.config import get_kafka_config, get_telegram_config, get_alerting_config
from utils.kafka import consume_messages, create_consumer
from utils.alert import send_summary
from utils.telegram import TelegramClient
from utils.aggregator import AlertAggregator

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger("fraudshield-alerting")


if __name__ == "__main__":
    kafka_config = None
    telegram = None
    consumer = None
    aggregator = None

    try:
        kafka_config = get_kafka_config()
        telegram_config = get_telegram_config()
        alerting_config = get_alerting_config()

        consumer = create_consumer(kafka_config)
        telegram = TelegramClient(
            logger=logger,
            bot_token=telegram_config["bot_token"],
            chat_id=telegram_config["chat_id"],
            topic_id=telegram_config["topic_id"]
        )

        aggregator = AlertAggregator()
        summary_start_monotonic = time.monotonic()
        summary_start_datetime = datetime.now(timezone.utc)

        logger.info("FraudShield Alerting Service started")
        logger.info(f"Kafka topic: {kafka_config['kafka_topic_alert']}")
        logger.info(f"Kafka consumer group: {kafka_config['group_id']}")
        logger.info(f"Summary interval: {alerting_config['summary_interval_seconds']} seconds")
        logger.info("Waiting for fraud alerts...")
        
        for kafka_message, payload in consume_messages(logger, consumer, kafka_config["kafka_topic_alert"]):
            alert_id = payload.get("alert_id", "UNKNOWN")
            transaction_id = payload.get("transaction_id", "UNKNOWN")
            logger.info(f"Received fraud alert: alert_id={alert_id} | transaction_id={transaction_id}")

            aggregator.add(payload)
            logger.debug(f"Alert added to aggregator: alert_id={alert_id} | buffer_size={aggregator.count()}")

            current_monotonic = time.monotonic()
            elapsed = current_monotonic - summary_start_monotonic
            if elapsed < alerting_config['summary_interval_seconds']:
                continue

            summary_end_datetime = datetime.now(timezone.utc)
            logger.info(f"Summary interval reached: buffer_size={aggregator.count()}")
            
            # Send summary
            try:
                # message = format_alert(payload)
                # success = telegram.send_message(message)
                success = send_summary(
                    logger=logger,
                    aggregator=aggregator,
                    telegram=telegram,
                    consumer=consumer,
                    alert_config=alerting_config,
                    period_start=(summary_start_datetime.isoformat()),
                    period_end=(summary_end_datetime.isoformat()),
                )
                if success:
                    # consumer.commit()
                    # logger.info(f"Alert processed successfully: alert_id={alert_id}")
                    
                    summary_start_monotonic = time.monotonic()
                    summary_start_datetime = summary_end_datetime
                    logger.info("Summary window reset successfully.")
                else:
                    logger.error(f"Failed to send alert to Telegram: alert_id={alert_id}")
            except Exception as error:
                # logger.error(f"Error while processing alert_id={alert_id}: {error}")
                logger.error(f"Error while send summary: {error}")
                logger.warning("Keeping alerts in aggregator for retry.")
    except KeyboardInterrupt as error:
        logger.info("KeyboardInterrupt received. Alerting service stopped")
        logger.error(f"KeyboardInterrupt: {error}")

    except Exception as error:
        logger.error(f"Alerting service failed: {error}")
    
    finally:
        # Flush Pending Alerts Before Shutdown
        if (
            aggregator is not None
            and telegram is not None
            and consumer is not None
            and not aggregator.is_empty()
        ):
            logger.info("Pending fraud alerts detected during shutdown. Attempting to send final summary...")
            shutdown_time = datetime.now(timezone.utc)
            send_summary(
                aggregator=aggregator,
                telegram=telegram,
                consumer=consumer,
                period_start=(
                    summary_start_datetime.isoformat()
                    if "summary_start_datetime" in locals()
                    else shutdown_time.isoformat()
                ),
                period_end=shutdown_time.isoformat(),
            )

        # Close Telegram
        if telegram is not None:
            try:
                telegram.close()
            except Exception as error:
                logger.error(f"Error while closing Telegram client: {error}")

        logger.info("FraudShield Alerting Service shutdown complete.")