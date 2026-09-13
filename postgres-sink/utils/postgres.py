import json
import re
import psycopg2
from utils.kafka import validate_message


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


def validate_identifier(identifier):
    """
    Validate a SQL identifier such as a table or column name.
    Only letters, numbers, and underscores are allowed, and the identifier must not start with a number.
    Args:
        identifier: SQL table name or column name to validate.
    Returns: The validated identifier.
    Raises:
        ValueError: If the identifier contains invalid characters.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


def build_insert_query(table, columns):
    """
    Build a parameterized PostgreSQL INSERT query.
    The table and column names are validated before being included in the SQL statement. Values are represented using parameter placeholders to prevent SQL injection.
    The query automatically adds the current timestamp to the load_ts column and ignores conflicting records.
    Args:
        table: Target PostgreSQL table name.
        columns: List of target column names.
    Returns: A parameterized SQL INSERT query string.
    Raises:
        ValueError: If the table or any column name is invalid.
    """
    table = validate_identifier(table)
    columns = [validate_identifier(column) for column in columns]

    column_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    return f"""
        INSERT INTO {table} ({column_sql}, load_ts)
        VALUES ({placeholders}, NOW())
        ON CONFLICT DO NOTHING
    """


def normalize_value(value):
    """
    Normalize complex Python values before inserting them into PostgreSQL.
    Dictionary and list values are serialized into JSON strings so they can be stored in PostgreSQL columns that support JSON or text data. Other values are returned unchanged.
    Args:
        value: Value extracted from the Kafka message payload.
    Returns: A JSON string for dictionaries or lists, otherwise the original value.
    """
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def insert_record(connection, topic_config, payload):
    """
    Insert a Kafka message payload into the configured PostgreSQL table.
    The target table and columns are obtained from the topic configuration. 
    Payload values are normalized before being inserted. The database transaction is committed after a successful insert.
    Args:
        connection: Active PostgreSQL database connection.
        topic_config: Configuration containing the target table and columns.
        payload: Kafka message payload as a dictionary.
    Raises:
        psycopg2.Error: If the database operation fails.
    """
    table = topic_config["table"]
    columns = topic_config["columns"]
    values = [normalize_value(payload.get(column)) for column in columns]
    query = build_insert_query(table, columns)
    with connection.cursor() as cursor:
        cursor.execute(query, values)

    connection.commit()
    
    
def process_message(connection, message, catalog):
    """
    Process a Kafka message and store its payload in PostgreSQL.
    The Kafka topic is used to retrieve the corresponding table and column configuration from the catalog. 
    The message is validated before its payload is inserted into the configured PostgreSQL table.
    Args:
        connection: Active PostgreSQL database connection.
        message: Kafka message containing the topic and payload.
        catalog: Mapping of Kafka topics to database configurations.
    Raises:
        ValueError: If the message or topic configuration is invalid.
        psycopg2.Error: If the database insertion fails.
    """
    topic_config = catalog.get(message.topic)
    validate_message(message, topic_config)
    insert_record(connection=connection, topic_config=topic_config, payload=message.value,)