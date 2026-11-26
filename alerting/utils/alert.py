from html import escape

# Format alert for each data
def format_alert(payload):
    """ 
    Format a single fraud alert into an HTML-formatted Telegram message. 
    Args: 
        payload: Dictionary containing fraud alert data such as alert ID, transaction ID, severity, fraud score, triggered rules, and message. 
    Returns: 
        str: Formatted fraud alert message ready to be sent to Telegram. 
    """
    alert_id = escape(str(payload.get("alert_id", "-")))
    result_id = escape(str(payload.get("result_id", "-")))
    transaction_id = escape(str(payload.get("transaction_id", "-")))
    customer_id = escape(str(payload.get("customer_id", "-")))
    severity = escape(str(payload.get("severity", "-")))
    fraud_score = escape(str(payload.get("fraud_score", "-")))
    status = escape(str(payload.get("status", "-")))
    alert_type = escape(str(payload.get("alert_type", "-")))
    message = escape(str(payload.get("message", "-")))
    created_at = escape(str(payload.get("created_at", "-")))

    triggered_rules = payload.get("triggered_rules", [])

    if isinstance(triggered_rules, list):
        rules = ", ".join(escape(str(rule)) for rule in triggered_rules)
    else:
        rules = escape(str(triggered_rules))

    return (
        "🚨 <b>FraudShield Alert</b>\n\n"
        f"<b>Severity:</b> {severity}\n"
        f"<b>Status:</b> {status}\n"
        f"<b>Fraud Score:</b> {fraud_score}\n"
        f"<b>Alert Type:</b> {alert_type}\n\n"
        f"<b>Transaction:</b> <code>{transaction_id}</code>\n"
        f"<b>Customer:</b> <code>{customer_id}</code>\n"
        f"<b>Result:</b> <code>{result_id}</code>\n"
        f"<b>Alert ID:</b> <code>{alert_id}</code>\n\n"
        f"<b>Triggered Rules:</b> {rules or '-'}\n"
        f"<b>Message:</b> {message}\n"
        f"<b>Created At:</b> {created_at}"
    )


# Format alert for summary data
def format_alert_summary(config, summary, alerts, period_start, period_end):
    """ 
    Format aggregated fraud alerts into a summary Telegram message. 
    Args: 
        config: Alert configuration containing summary display settings. 
        summary: Aggregated fraud statistics such as total alerts, severity, alert types, fraud scores, and triggered rules. 
        alerts: List of individual fraud alerts used to display recent alerts. 
        period_start: Start timestamp of the summary period. 
        period_end: End timestamp of the summary period. 
    Returns: 
        str: Formatted fraud summary message ready to be sent to Telegram. 
    """
    total_alerts = summary["total_alerts"]
    severity = summary["severity"]
    alert_types = summary["alert_types"]
    triggered_rules = summary["triggered_rules"]

    severity_lines = "\n".join(
        f"  • {escape(str(key))}: {value}"
        for key, value in sorted(severity.items())
    ) or "  • None"

    alert_type_lines = "\n".join(
        f"  • {escape(str(key))}: {value}"
        for key, value in sorted(alert_types.items())
    ) or "  • None"

    rule_lines = "\n".join(
        f"  • {escape(str(key))}: {value}"
        for key, value in sorted(triggered_rules.items())
    ) or "  • None"

    recent_alerts = []
    for index, alert in enumerate(alerts[-config['max_recent_alerts']:], start=1):
        transaction_id = escape(str(alert.get("transaction_id", "-")))
        customer_id = escape(str(alert.get("customer_id", "-")))
        severity_value = escape(str(alert.get("severity", "-")))
        score = escape(str(alert.get("fraud_score", "-")))
        alert_type = escape(str(alert.get("alert_type", "-")))

        recent_alerts.append(
            f"{index}. <code>{transaction_id}</code>\n"
            f"   Customer: <code>{customer_id}</code>\n"
            f"   Severity: {severity_value} | "
            f"Score: {score} | "
            f"Type: {alert_type}"
        )

    recent_alerts_text = "\n\n".join(recent_alerts)
    return (
        "🚨 <b>FraudShield Fraud Summary</b>\n\n"

        f"⏱ <b>Period</b>\n"
        f"Periode start: {escape(period_start)} - \n"
        f"Periode end: {escape(period_end)} - \n\n"
        

        "📊 <b>Summary</b>\n"
        f"Total Alerts: <b>{total_alerts}</b>\n"
        f"Affected Customers: <b>"
        f"{summary['affected_customers']}</b>\n"
        f"Affected Transactions: <b>"
        f"{summary['affected_transactions']}</b>\n\n"

        "⚠️ <b>Severity</b>\n"
        f"{severity_lines}\n\n"

        "💰 <b>Fraud Score</b>\n"
        f"Average: <b>"
        f"{summary['average_fraud_score']:.2f}</b>\n"
        f"Maximum: <b>"
        f"{summary['max_fraud_score']:.2f}</b>\n\n"

        "🎯 <b>Alert Types</b>\n"
        f"{alert_type_lines}\n\n"

        "📋 <b>Triggered Rules</b>\n"
        f"{rule_lines}\n\n"

        "🚨 <b>Recent Alerts</b>\n"
        f"{recent_alerts_text or 'No alerts'}"
    )
    

# Send alert summary to telegram
def send_summary(logger, aggregator, telegram, consumer, alert_config, period_start, period_end):
    """
    Build and send the aggregated fraud alert summary. 
    Kafka offset is committed only after the Telegram message has been sent successfully.
    Args:
        aggregator: AlertAggregator instance.
        telegram: TelegramClient instance.
        consumer: KafkaConsumer instance.
        period_start: Start timestamp of the summary period.
        period_end: End timestamp of the summary period.
    Returns:
        bool: True if the summary was successfully sent, otherwise False.
    """

    if aggregator.is_empty():
        logger.info("Summary interval reached, but no fraud alerts were received.")
        return True

    summary = aggregator.get_summary()
    alerts = aggregator.get_alerts()
    logger.info(f"Preparing fraud alert summary: total_alerts={summary['total_alerts']} | affected_customers={summary['affected_customers']} | affected_transactions={summary['affected_transactions']}")

    message = format_alert_summary(
        config=alert_config,
        summary=summary,
        alerts=alerts,
        period_start=period_start,
        period_end=period_end
    )

    try:
        logger.info(f"Sending Telegram summary: message_length={len(message)}")
        success = telegram.send_message(message)
        if not success:
            logger.error("Failed to send fraud alert summary to Telegram.")
            return False

        # Commit Kafka offsets only after Telegram successfully receives the summary.
        consumer.commit()
        logger.info(f"Fraud alert summary sent successfully: total_alerts={summary['total_alerts']} | period_start={period_start} | period_end={period_end}")

        # Clear alerts only after successful send and Kafka commit.
        aggregator.clear()
        return True

    except Exception as error:
        logger.error(f"Error while sending fraud alert summary: {error}")
        return False


    
    
