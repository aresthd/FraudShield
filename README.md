# FraudShield
> Real-Time Banking Fraud Detection Platform

## Overview

FraudShield is a real-time banking fraud detection platform built to demonstrate an event-driven streaming architecture for detecting suspicious banking transactions with low latency.

The platform uses **Apache Kafka** as the event streaming backbone and **Apache Flink** as the real-time processing engine. Transactions are continuously generated, enriched, analyzed using stateful fraud detection rules, persisted into PostgreSQL, and suspicious activities are delivered as alerts through Telegram.

The main objective is to demonstrate how a streaming data platform can:

1. Generate synthetic banking transactions.
2. Stream transactions through Apache Kafka.
3. Validate and enrich transactions using Apache Flink.
4. Maintain customer behavioral state.
5. Apply fraud detection rules in real time.
6. Calculate fraud scores.
7. Classify transactions as `NORMAL`, `SUSPICIOUS`, or `FRAUD`.
8. Publish fraud results and alerts to Kafka.
9. Persist results into PostgreSQL.
10. Send aggregated fraud alerts to a Telegram group.


## Table of Contents
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Key Features](#key-features)
- [End-to-End Data Flow](#end-to-end-data-flow)
	- [Data Source](#data-source)
	- [Data Preprocessing](#data-preprocessing)
	- [Data Processing](#data-preprocessing)
	- [Database Sink](#database-sink)
	- [Alerting](#alerting)
- [Project Components](#project-components)
- [Fraud Detection Rules](#fraud-detection-rules)
- [Fraud Alerts](#fraud-alerts)
- [Dead Letter Queue](#dead-letter-queue)
- [Kafka Topics](#kafka-topics)
- [Flink Processing](#flink-processing)
- [Checkpoint & State Management](#checkpoint--state-management)
- [Installation](#installation)
- [Monitoring](#monitoring)
- [Data Examples](#data-examples)
- [Future Improvements](#future-improvements)
- [Conclusion](#conclusion)
- [Disclaimer](#disclaimer)


## Architecture
FraudShield is composed of several independent services running inside Docker containers. The project also includes Kafka Connect and Kafka UI for supporting Kafka-related operations and monitoring. The high-level architecture is shown below.

```text
                    +---------------------+
                    |   Data Producer     |
                    |       Python        |
                    +---------+-----------+
                              |
                              |
                              v
                     +----------------+
                     |     Kafka      |
                     |transactions_raw|
                     +-------+--------+
                             |
                             v
                  +----------------------+
                  |     Flink Job #1     |
                  | TransactionEnrichment|
                  |                      |
                  | - Validation         |
                  | - Event Time         |
                  | - Watermark          |
                  | - Customer State     |
                  | - Feature Enrichment |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  |      Kafka           |
                  |transactions_enriched |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  |     Flink Job #2     |
                  | StatefulFraudEngine  |
                  |                      |
                  | - Keyed State        |
                  | - Velocity Detection |
                  | - Dynamic Rules      |
                  | - Fraud Scoring      |
                  +----------+-----------+
                             |
                    +--------+--------+
                    |                 |
                    v                 v
             fraud_results       fraud_alerts
                    |                 |
                    v                 v
               PostgreSQL         Alerting
                                      |
                                      v
                                Telegram Group
```


## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Transaction generator, PostgreSQL sink, Alerting service |
| Apache Kafka | Event streaming and decoupling between services |
| Apache Flink | Stateful real-time stream processing |
| Java | Flink application implementation |
| PostgreSQL | Persistent storage |
| Telegram Bot API | Fraud alert notification |
| Docker | Containerization |
| Docker Compose | Multi-container orchestration |
| Zookeeper | Kafka coordination |
| Kafka Connect | Kafka integration infrastructure |
| Kafka UI | Kafka monitoring and visualization |
| Maven | Flink project build |


## Key Features

### Real-Time Transaction Processing
Transactions are continuously generated and published to Kafka.

```text
Python Data Generator
       ↓
transactions_raw
```

The generator uses Faker and predefined static data to simulate banking transaction events.

### Transaction Validation
Incoming transactions are validated before entering the enrichment pipeline. The validation checks include:

- `transaction_id` must not be null
- `customer_id` must not be null
- `amount` must be greater than zero
- `timestamp` must be valid

Invalid transactions are redirected to the Dead Letter Queue.

### Event-Time Processing
FraudShield uses transaction timestamps as event time. Apache Flink applies watermarking to handle transactions that arrive out of order.

```text
Transaction Event
       │
       ▼
Event Timestamp
       │
       ▼
Watermark
       │
       ▼
Stateful Processing
```

### Stateful Processing
Customer transaction behavior is maintained using Flink state. Examples of customer-related state include:

- Last device
- Last city
- Last transaction timestamp
- Last transaction amount

This state is used to identify behavioral changes such as:

- New device
- New location
- Rapid transactions
- Impossible travel

### Rule-Based Fraud Detection
Fraud decisions are generated using configurable fraud rules. Examples include:

- High amount transaction
- New device
- New location
- Night transaction
- Transaction velocity
- Foreign transaction
- Impossible travel
- High-risk merchant

### PostgreSQL Persistence
Fraud results are consumed from Kafka and persisted into PostgreSQL using a dedicated Python service. The PostgreSQL Sink uses Kafka consumer groups and commits the Kafka offset after successful database insertion.

### Telegram Alerting
Fraud alerts are consumed from the `fraud_alerts` Kafka topic by a dedicated Python alerting service. The service aggregates fraud alerts and sends formatted notifications to a Telegram group.


## End-to-End Data Flow

### Data Source
The `data-producer` service generates synthetic banking transactions using Python and Faker. Generated transactions contain information such as:
- Transaction ID
- Customer ID
- Account ID
- Transaction type
- Amount
- Currency
- Merchant
- Payment method
- Device
- IP address
- City
- Country
- Latitude
- Longitude
- Timestamp
- Channel

The generated transaction is published to kafka topic `transactions_raw`. The producer can also load fraud rules from PostgreSQL and publish them to kafka topic `fraud_rules`

### Data Preprocessing
The `TransactionEnrichment` Flink job consumes from kafka topic `transactions_raw` using consumer group `fraud-enrichment`

This flink job performs:
1. JSON parsing
2. Transaction validation
3. Invalid event routing to DLQ
4. Event-time assignment
5. Watermark generation
6. Customer-based keyed state
7. Transaction enrichment
8. Basic fraud-related feature generation

Valid transactions are published to kafka topic `transactions_enriched`. The invalid transactions are published to kafka topic `fraud_dlq`

### Data Processing
The `StatefulFraudEngine` Flink job consumes from kafka topic `transactions_enriched` and `fraud_rules`. The transaction stream is keyed by `customer_id`. This allows Flink to maintain customer-specific behavioral state.

This flink job performs:
- Stateful processing
- Customer behavior tracking
- Transaction velocity detection
- Sliding-window analysis
- Dynamic rule evaluation
- Impossible-travel detection
- Fraud scoring
- Risk classification

The output is split into kafka topic `fraud_results` and `fraud_alerts`

### Database Sink
The `postgres-sink` service consumes Kafka events and writes them into PostgreSQL. The sink uses `fraud-postgres-sink` as its Kafka consumer group.

Topic-to-table mapping is controlled through [the catalog file](postgres-sink/config/catalog.yml). The sink uses manual Kafka offset commits and commits an offset after successful database processing.


### Alerting
The `alerting` service consumes from kafka topic `fraud_alerts` using consumer group `fraud-alert` Alerts are collected by an in-memory aggregator.

Instead of sending one Telegram message for every individual event, the service periodically generates an aggregated summary containing information such as:

- Total alerts
- Severity distribution
- Alert type distribution
- Triggered rules
- Average fraud score
- Maximum fraud score
- Affected customers
- Affected transactions
- Recent alerts

The summary is then sent to a Telegram group through the Telegram Bot API. Kafka offsets are committed only after the Telegram message has been successfully sent. Example alert:

```text
🚨 FraudShield Fraud Summary

⏱️ Period
Periode start: 2026-09-26T10:10:03.509042+00:00 - 
Periode end: 2026-09-26T10:20:04.748417+00:00 - 

📊 Summary
Total Alerts: 275
Affected Customers: 128
Affected Transactions: 275

⚠️ Severity
  • CRITICAL: 7
  • MEDIUM: 268

💰 Fraud Score
Average: 51.20
Maximum: 120.00

🎯 Alert Types
  • HIGH_AMOUNT: 2
  • MULTI_RULE_FRAUD: 273

📋 Triggered Rules
  • FOREIGN_TRANSACTION: 19
  • HIGH_AMOUNT: 20
  • IMPOSSIBLE_TRAVEL: 265
  • NEW_DEVICE: 60
  • NEW_LOCATION: 270

🚨 Recent Alerts
1. TX-20260926-3C065FA2
   Customer: CUS-000080
   Severity: MEDIUM | Score: 45 | Type: MULTI_RULE_FRAUD

2. TX-20260926-EE1BC28B
   Customer: CUS-000166
   Severity: MEDIUM | Score: 60 | Type: MULTI_RULE_FRAUD

3. TX-20260926-DC95C2B0
   Customer: CUS-000126
   Severity: MEDIUM | Score: 45 | Type: MULTI_RULE_FRAUD
```


## Project Components
FraudShield consists of several containerized services responsible
for different stages of the real-time fraud detection pipeline.

| Component | Technology | Responsibility |
|---|---|---|
| Data Producer | Python | Generate transaction events |
| Transaction Enrichment | Apache Flink / Java | Validate and enrich transactions |
| Stateful Fraud Engine | Apache Flink / Java | Detect fraudulent behavior |
| PostgreSQL Sink | Python | Persist fraud results |
| Alerting | Python | Send fraud alerts to Telegram |



## Fraud Detection Rules
The current rule configuration contains the following rules:

| Rule ID | Rule Type | Description | Score | Severity |
|---|---|---|---:|---|
| R001 | `HIGH_AMOUNT` | Transaction exceeds threshold | 40 | HIGH |
| R002 | `NEW_DEVICE` | Transaction uses a new device | 15 | MEDIUM |
| R003 | `NEW_LOCATION` | Transaction occurs from a new location | 15 | MEDIUM |
| R004 | `NIGHT_TRANSACTION` | Transaction occurs during unusual night hours | 10 | MEDIUM |
| R005 | `VELOCITY` | More than 5 transactions within 30 seconds | 40 | CRITICAL |
| R006 | `FOREIGN_TRANSACTION` | Transaction occurs outside customer's home country | 20 | HIGH |
| R007 | `IMPOSSIBLE_TRAVEL` | Customer moves between distant locations within unrealistic time | 30 | CRITICAL |
| R008 | `HIGH_MERCHANT_RISK` | Transaction occurs at a high-risk merchant | 20 | HIGH |

Example:

```text
Transaction
    │
    ├── NEW_DEVICE
    │      +15
    │
    ├── VELOCITY
    │      +40
    │
    ├── NIGHT_TRANSACTION
    │      +10
    │
    └── HIGH_MERCHANT_RISK
           +20

Total Fraud Score = 85
```

The final result contains information such as:

- Fraud score
- Status
- Risk level
- Triggered rules
- Rule count
- Velocity count
- Velocity amount
- Window start/end
- Previous/current city
- Distance
- Time since previous transaction
- Impossible travel indicator
- Detection method
- Processing timestamp


## Fraud Alerts
Not every fraud result becomes an alert. The current implementation follows:

```text
NORMAL
   |
   +----> No Alert

SUSPICIOUS
   |
   +----> MEDIUM Alert

FRAUD
   |
   +----> CRITICAL Alert
```

The alert contains:

```text
alert_id
result_id
transaction_id
customer_id
severity
fraud_score
status
alert_type
triggered_rules
message
created_at
```


## Dead Letter Queue
FraudShield provides a Dead Letter Queue through kafka topic `fraud_dlq`. Invalid transactions are redirected to this topic instead of stopping the Flink pipeline.

Example invalid transaction:

```json
{
  "transaction_id": "TX-000099",
  "customer_id": "CUS-001",
  "amount": -500000
}
```

will fail validation because:

```text
amount <= 0
```

The event is then transformed into a DLQ message:

```json
{
  "dlq_id": "DLQ-000001",
  "source_topic": "transactions_raw",
  "error_type": "VALIDATION_ERROR",
  "error_message": "Transaction amount must be greater than zero",
  "failed_at_stage": "ENRICHMENT",
  "retry_count": 0,
  "original_payload": {
    "transaction_id": "TX-000099",
    "customer_id": "CUS-001",
    "amount": -500000
  }
}
```

This allows invalid data to be investigated separately without interrupting the streaming pipeline.


## Kafka Topics
FraudShield uses several Kafka topics to separate different types of events.

| Topic | Producer | Consumer | Consumer Group | Purpose |
|---|---|---|---|---|
| `transactions_raw` | Transaction Generator | Flink Job #1 | `fraud-enrichment` | Raw transactions |
| `transactions_enriched` | Flink Job #1 | Flink Job #2 | `fraud-engine` | Enriched transactions |
| `fraud_results` | Flink Job #2 | PostgreSQL Sink | `fraud-sink` | Fraud analysis results |
| `fraud_alerts` | Flink Job #2 | Alerting Service | `fraud-alert` | Fraud/suspicious alerts |
| `fraud_rules` | Rule Management / Generator | Flink Job #2 | `fraud-rules` | Dynamic fraud rules |
| `fraud_dlq` | Flink Jobs | DLQ Consumer | `fraud-dlq` | Invalid or failed events |


## Flink Processing
FraudShield contains two main Flink jobs.

| Job | Input | Output | Responsibility |
|---|---|---|---|
| `TransactionEnrichment` | `transactions_raw` | `transactions_enriched` | Validation, event time, watermark, enrichment and basic features |
| `StatefulFraudEngine` | `transactions_enriched`, `fraud_rules` | `fraud_results`, `fraud_alerts` | Stateful fraud detection, windowing, rules and scoring |


## Checkpoint & State Management
Flink checkpointing is enabled for the streaming jobs. The project provides directories for:

```text
checkpoints/
├── engine/
└── enrichment/

savepoints/
```

These directories are mounted into the Flink JobManager container. Checkpointing is configured through the job property files.

```text
flink/config/
├── transaction-enrichment.properties
└── stateful-fraud-engine.properties
```


## Installation
How to running FraudShield:

1. Clone the repository

	```bash
	git clone https://github.com/aresthd/FraudShield.git
	cd FraudShield
	```

2. Configure Environment Variables

	Create a `.env` file:
	```bash
	cp .env.example .env
	```

	Then configure the required variables.

3. Build the Docker Images

	Build all services:

	```bash
	docker compose build
	```

4. Start the Platform

	Start the complete stack:

	```bash
	docker compose up -d
	```

	This starts:
	```text
	Zookeeper
	Kafka
	Kafka Connect
	Kafka UI
	PostgreSQL
	PostgreSQL Sink
	Alerting
	Flink JobManager
	Flink TaskManager
	Data Producer
	```

5. Check Running Containers

	```bash
	docker compose ps
	```

	All required services should eventually reach their expected running/healthy state.


## Monitoring

### Kafka UI
Kafka UI is exposed on: [http://localhost:8085](http://localhost:8085/).

Kafka UI can be used to inspect:
- Kafka topics
- Messages
- Consumer groups
- Partitions
- Consumer offsets
- Kafka Connect configuration

### Flink Dashboard
The Flink JobManager UI is exposed through: [http://localhost:8081](http://localhost:8081/).

The dashboard can be used to inspect:
- Running jobs
- TaskManagers
- Operators
- Parallelism
- Checkpoints
- Job status
- Processing metrics

The two main jobs should appear as:
```text
TransactionEnrichment
StatefulFraudEngine
```

### PostgreSQL
PostgreSQL is exposed on the configured host port. Default: `localhost:5438`

Example connection:

```text
Host: localhost
Port: 5438
Database: postgres
User: postgres
Password: <configured password>
```

## Data Examples
-  Raw Transaction
	```json
	{
	"transaction_id": "TX-20260907-B416BCEF",
	"customer_id": "CUS-000001",
	"account_id": "ACC-000001",
	"transaction_type": "CREDIT",
	"amount": 21394465.07,
	"currency": "IDR",
	"merchant_id": "MER-00004",
	"merchant_name": "Gojek",
	"merchant_category": "TRANSPORTATION",
	"payment_method": "BANK_TRANSFER",
	"device_id": "DEV-000001",
	"ip_address": "210.46.20.135",
	"city": "Malang",
	"country": "Indonesia",
	"latitude": -7.983908,
	"longitude": 112.621391,
	"timestamp": "2026-09-08T01:46:08.927941+07:00",
	"channel": "INTERNET_BANKING"
	}
	```

- Enriched Transaction
	```json
	{
	"transaction_id": "TX-20260907-B416BCEF",
	"customer_id": "CUS-000001",
	"account_id": "ACC-000001",
	"transaction_type": "CREDIT",
	"amount": 21394465.07,
	"currency": "IDR",
	"merchant_id": "MER-00004",
	"merchant_name": "Gojek",
	"merchant_category": "TRANSPORTATION",
	"payment_method": "BANK_TRANSFER",
	"device_id": "DEV-000001",
	"ip_address": "210.46.20.135",
	"city": "Malang",
	"country": "Indonesia",
	"channel": "INTERNET_BANKING",
	"account_age_days": 1395,
	"customer_risk_level": "LOW",
	"merchant_risk_level": "MEDIUM",
	"is_new_device": true,
	"is_new_location": true,
	"is_foreign_transaction": false,
	"is_night_transaction": false,
	"is_high_amount": false,
	"event_date": "2026-09-07",
	"event_hour": 18,
	"timestamp": "2026-09-07T18:46:08.927Z",
	"processing_time": "2026-09-07T18:47:53.947224098Z"
	}
	```

- Fraud Result
	```json
	{
	"result_id": "RES-e524f474-a10a-49a6-ad6c-395efb18618f",
	"transaction_id": "TX-20260907-B416BCEF",
	"customer_id": "CUS-000001",
	"amount": 21394465.07,
	"fraud_score": 0,
	"status": "NORMAL",
	"risk_level": "LOW",
	"triggered_rules": [],
	"rule_count": 0,
	"velocity_count": 1,
	"velocity_amount": 21394465.07,
	"window_start": "2026-09-07T18:45:38.927Z",
	"window_end": "2026-09-07T18:46:08.927Z",
	"previous_city": null,
	"current_city": "Malang",
	"distance_km": 0.0,
	"time_since_last_transaction_sec": 0,
	"is_impossible_travel": false,
	"detection_method": "RULE_ENGINE",
	"processed_at": "2026-09-07T18:48:07.906242769Z"
	}
	```

- Fraud Alert
	```json
	{
        "alert_id": "ALT-ec3aa6c5-5725-44a0-8ff3-06d5bf97676e",
        "result_id": "RES-bdc3692f-c0eb-49f8-8363-f8a27d86d256",
        "transaction_id": "TX-20260907-696A447D",
        "customer_id": "CUS-000001",
        "severity": "MEDIUM",
        "fraud_score": 40,
        "status": "SUSPICIOUS",
        "alert_type": "VELOCITY",
        "triggered_rules": [
            "VELOCITY"
        ],
        "message": "Multiple suspicious transaction patterns detected",
        "created_at": "2026-09-07T18:48:08.254121527Z"
    }
	```


## Future Improvements
Potential improvements for the next iteration include:

- Machine Learning-based fraud prediction
- More advanced CEP patterns
- Additional fraud detection rules
- More sophisticated customer behavioral profiles
- Grafana/Prometheus monitoring
- Real-time fraud dashboard
- Schema Registry
- Avro/Protobuf serialization
- Kafka partitioning optimization
- Multi-node Kafka deployment
- Flink high availability
- Externalized checkpoint storage
- CI/CD pipeline
- Kubernetes deployment
- Rule Management API
- Fraud investigation interface


## Conclusion
FraudShield demonstrates an end-to-end real-time streaming architecture for banking fraud detection.

The complete pipeline is:

```text
                 +-------------------+
                 | Python Generator  |
                 +---------+---------+
                           |
                           v
                  transactions_raw
                           |
                           v
                +---------------------+
                | Transaction         |
                | Enrichment          |
                |                     |
                | Flink               |
                +----------+----------+
                           |
                           v
                transactions_enriched
                           |
                           v
                +---------------------+
                | Stateful Fraud      |
                | Engine              |
                |                     |
                | Flink               |
                +----------+----------+
                           |
                  +--------+--------+
                  |                 |
                  v                 v
           fraud_results      fraud_alerts
                  |                 |
                  v                 v
             PostgreSQL         Alerting
                                    |
                                    v
                               Telegram
```

The project demonstrates several important Data Engineering concepts:

- Event-driven architecture
- Apache Kafka
- Apache Flink
- Stateful stream processing
- Event-time processing
- Watermarks
- Keyed State
- Broadcast State
- Dynamic rules
- Window-based processing
- Fraud scoring
- Dead Letter Queue
- Kafka consumer groups
- PostgreSQL sinks
- Docker containerization
- Service decoupling
- Real-time alerting

FraudShield is designed as a foundation that can be extended toward a production-grade real-time fraud detection platform.


## Disclaimer
FraudShield is an educational and portfolio project designed to demonstrate real-time data engineering and stream processing concepts. The transaction data is synthetic and generated for development/testing purposes.

This system is not intended to be used as a production banking fraud detection system without additional security, reliability, compliance, monitoring, scalability, and domain-specific validation.


## License
The project is licensed under the MIT license - see the [LICENSE](LICENSE) file for more details.
