from collections import Counter

class AlertAggregator:
    def __init__(self):
        self.alerts = []

    def add(self, alert):
        """
        Add a fraud alert to the aggregator.
        Args:
            alert: Dictionary containing fraud alert data.
        """
        self.alerts.append(alert)

    def count(self):
        """
        Return the total number of stored fraud alerts.
        Returns:
            int: Number of fraud alerts currently stored.
        """
        return len(self.alerts)

    def is_empty(self):
        """
        Check whether the aggregator contains any fraud alerts.
        Returns:
            bool: True if no alerts are stored, otherwise False.
        """
        return len(self.alerts) == 0

    def get_summary(self):
        """
        Calculate aggregated statistics from all stored fraud alerts.
        Returns:
            dict: Summary containing total alerts, severity counts, alert type counts, triggered rule counts, fraud score statistics, and affected customers and transactions.
        """
        if self.is_empty():
            return {
                "total_alerts": 0,
                "severity": {},
                "alert_types": {},
                "triggered_rules": {},
                "average_fraud_score": 0,
                "max_fraud_score": 0,
                "affected_customers": 0,
                "affected_transactions": 0,
            }

        severity_counter = Counter()
        alert_type_counter = Counter()
        rule_counter = Counter()
        scores = []
        customers = set()
        transactions = set()

        for alert in self.alerts:
            severity = alert.get("severity", "UNKNOWN")
            alert_type = alert.get("alert_type", "UNKNOWN")

            severity_counter[severity] += 1
            alert_type_counter[alert_type] += 1

            fraud_score = alert.get("fraud_score", 0)

            try:
                scores.append(float(fraud_score))
            except (TypeError, ValueError):
                pass

            customer_id = alert.get("customer_id")

            if customer_id:
                customers.add(customer_id)

            transaction_id = alert.get("transaction_id")

            if transaction_id:
                transactions.add(transaction_id)

            triggered_rules = alert.get("triggered_rules", [])

            if isinstance(triggered_rules, list):
                for rule in triggered_rules:
                    rule_counter[str(rule)] += 1
            elif triggered_rules:
                rule_counter[str(triggered_rules)] += 1

        return {
            "total_alerts": len(self.alerts),
            "severity": dict(severity_counter),
            "alert_types": dict(alert_type_counter),
            "triggered_rules": dict(rule_counter),
            "average_fraud_score": sum(scores) / len(scores) if scores else 0,
            "max_fraud_score": max(scores) if scores else 0,
            "affected_customers": len(customers),
            "affected_transactions": len(transactions),
        }

    def get_alerts(self):
        """
        Return a copy of all stored fraud alerts.
        Returns:
            list: List containing the currently stored fraud alerts.
        """
        return list(self.alerts)

    def clear(self):
        """
        Remove all stored fraud alerts from the aggregator.
        """
        self.alerts.clear()
