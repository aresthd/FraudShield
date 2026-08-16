import json
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import yaml
from faker import Faker

logger = logging.getLogger("fraudshield-data-producer")
fake = Faker("id_ID") 

# Static Data Loader
def load_static_data(file_path="static-data.yml"):
    """
    Load static/master data from YAML file.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Static data file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    if not data:
        raise ValueError(f"Static data file is empty: {path}")

    return data

# Transaction ID
def generate_transaction_id():
    """
    Generate unique transaction ID.
    Example:
    TX-20260816-A82C19F3
    """
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    random_part = uuid.uuid4().hex[:8].upper()
    return f"TX-{date_part}-{random_part}"

# IP Address
def generate_ip_address():
    """
    Generate synthetic public IPv4 address.
    """
    return fake.ipv4_public()

# Transaction Amount
def generate_amount(transaction_type, suspicious=False):
    """
    Generate transaction amount based on transaction type.
    Suspicious transactions have a higher probability of
    generating large transaction amounts.
    """
    if suspicious:
        amount = random.choice(
            [
                random.uniform(
                    20_000_000,
                    50_000_000,
                ),
                random.uniform(
                    50_000_000,
                    100_000_000,
                ),
                random.uniform(
                    100_000_000,
                    250_000_000,
                ),
            ]
        )
    else:
        ranges = {
            "TRANSFER": (
                100_000,
                20_000_000,
            ),
            "QRIS_PAYMENT": (
                10_000,
                2_000_000,
            ),
            "DEBIT": (
                20_000,
                5_000_000,
            ),
            "CREDIT": (
                100_000,
                30_000_000,
            ),
            "BILL_PAYMENT": (
                50_000,
                5_000_000,
            ),
            "ONLINE_PURCHASE": (
                50_000,
                10_000_000,
            ),
        }
        if transaction_type not in ranges:
            raise ValueError(
                f"Unsupported transaction type: "
                f"{transaction_type}"
            )
        minimum, maximum = ranges[transaction_type]
        amount = random.uniform(minimum, maximum)

    return float(
        Decimal(str(amount)).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )
    )


# Timestamp
def generate_timestamp():
    """
    Generate event timestamp in GMT+7 (WIB).
    This timestamp will later be used as Event Time
    by Flink TransactionEnrichment.
    """
    wib = timezone(timedelta(hours=7))
    return datetime.now(wib).isoformat()


# City Selection
def select_city(customer, cities, suspicious=False):
    """
    Select transaction city.
    Normal:
        Mostly customer's home city.
    Suspicious:
        Higher probability of using another city,
        which can later be used for impossible travel
        detection.
    """
    home_city = customer["home_city"]
    if suspicious and random.random() < 0.70:
        possible_cities = [
            city
            for city in cities.keys()
            if city != home_city
        ]
        return random.choice(possible_cities)
    return random.choice(
        [
            home_city,
            home_city,
            home_city,
            random.choice(list(cities.keys())),
        ]
    )


# Device Selection
def select_device(customer, suspicious=False):
    """
    Select transaction device.
    Normal: Customer's usual device.
    Suspicious: Possible new device.
    """
    if suspicious and random.random() < 0.60:
        return (
            f"DEV-"
            f"{random.randint(100000, 999999)}"
        )
    return customer["device_id"]