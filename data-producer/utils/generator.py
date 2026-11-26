import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from faker import Faker

fake = Faker("id_ID") 

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


# Static Data Validation
def validate_static_data(static_data):
    """
    Validate required static data sections.
    """
    required_sections = [
        "transaction_types",
        "channels",
        "payment_methods",
        "cities",
        "merchants",
    ]

    for section in required_sections:
        if section not in static_data:
            raise ValueError(f"Missing static data section: {section}")

    if not static_data["transaction_types"]:
        raise ValueError("transaction_types cannot be empty")

    if not static_data["cities"]:
        raise ValueError("cities cannot be empty")

    if not static_data["merchants"]:
        raise ValueError("merchants cannot be empty")

    for transaction_type in static_data["transaction_types"]:
        if transaction_type not in static_data["channels"]:
            raise ValueError(f"Missing channel configuration for transaction type: {transaction_type}")

        if transaction_type not in static_data["payment_methods"]:
            raise ValueError(f"Missing payment method configuration for transaction type: {transaction_type}")


# Customer Generator
def create_customer(customers, static_data):
    """
    Create a synthetic customer.
    Customer state is maintained by the producer so that
    multiple transactions can belong to the same customer.
    """
    customer_number = len(customers) + 1
    customer_id = f"CUS-{customer_number:06d}"
    account_id = f"ACC-{customer_number:06d}"
    device_id = f"DEV-{customer_number:06d}"
    home_cities = [
        city
        for city in static_data["cities"].keys()
        if static_data["cities"][city]["country"] == "Indonesia"
    ]

    home_city = random.choice(home_cities)
    customer = {
        "customer_id": customer_id,
        "account_id": account_id,
        "home_city": home_city,
        "device_id": device_id,
    }

    customers[customer_id] = customer

    return customer

def get_customer(customers, static_data, transaction_config):
    """
    Reuse existing customers most of the time.
    This is important because the downstream Flink job
    will perform stateful processing based on customer_id.
    """
    if (not customers or random.random() > transaction_config["existing_customer_probability"]):
        return create_customer(customers, static_data)

    customer_id = random.choice(list(customers.keys()))
    return customers[customer_id]


# Transaction Generator
def generate_transaction(customers, static_data, transaction_config):
    """
    Generate one transaction according to
    transactions_raw schema.
    """
    customer = get_customer(customers, static_data, transaction_config)
    suspicious = random.random() < transaction_config["suspicious_transaction_probability"]
    transaction_type = random.choice(static_data["transaction_types"])
    merchant = random.choice(static_data["merchants"])
    city = select_city(customer=customer, cities=static_data["cities"], suspicious=suspicious)
    city_data = static_data["cities"][city]

    transaction = {
        "transaction_id":generate_transaction_id(),
        "customer_id":customer["customer_id"],
        "account_id":customer["account_id"],
        "transaction_type":transaction_type,
        "amount": generate_amount(transaction_type=transaction_type, suspicious=suspicious),
        "currency": "IDR",
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "merchant_category": merchant["merchant_category"],
        "payment_method": random.choice(static_data["payment_methods"][transaction_type]),
        "device_id": select_device(customer=customer, suspicious=suspicious),
        "ip_address": generate_ip_address(),
        "city": city,
        "country": city_data["country"],
        "latitude": city_data["latitude"],
        "longitude": city_data["longitude"],
        "timestamp": generate_timestamp(),
        "channel": random.choice(static_data["channels"][transaction_type]),
    }

    return transaction


