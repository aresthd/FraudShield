import psycopg2
from psycopg2.extras import RealDictCursor
from utils.config import get_postgres_config

# Database Connection
def get_connection(config):
    """
    Establish a connection to the PostgreSQL database.
    The connection settings are provided through the configuration dictionary, including host, port, database, user, and password.
    Args:
        config: Dictionary containing PostgreSQL connection settings.
    Returns: An active PostgreSQL database connection.
    Raises:
        RuntimeError: If the PostgreSQL connection fails.
    """
    try:
        connection = psycopg2.connect(
            host=config["host"],
            port=int(config["port"]),
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        return connection
    except psycopg2.Error as e:
        raise RuntimeError(f"Failed to connect to PostgreSQL: {e}")



# Fraud Rules
def load_fraud_rules():
    """
    Load fraud rules from PostgreSQL.
    Returns:
        list[dict]: Fraud rules retrieved from fraud_rules table.
    """
    query = """
        SELECT 
            rule_id, rule_name, rule_type, description, threshold, window_seconds, 
            max_transactions, score, severity, enabled, action, version, updated_by, operation
        FROM fraud_rules
        WHERE enabled = TRUE
        ORDER BY rule_id;
    """

    connection = None
    try:
        config = get_postgres_config()
        connection = get_connection(config)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            rules = []
            for row in rows:
                rule = dict(row)

                # psycopg2 returns NUMERIC as Decimal.
                # Convert it because Kafka JSON serializer
                # cannot serialize Decimal directly.
                if rule.get("threshold") is not None:
                    rule["threshold"] = float(
                        rule["threshold"]
                    )
                rules.append(rule)
            return rules

    except Exception as e:
        raise RuntimeError(f"Failed to load fraud rules: {e}")

    finally:
        if connection is not None:
            connection.close()
            