
## Architecture (base)

                    +----------------+
                    | Transaction API|
                    +-------+--------+
                            |
                            |
                      Kafka Producer
                            |
                            |
                    +-------v-------+
                    | Kafka Topic   |
                    | transactions  |
                    +-------+-------+
                            |
                            |
                     Apache Flink
                            |
        +-------------------+-------------------+
        |                   |                   |
        |                   |                   |
 Rule Engine         Window Aggregation     ML Prediction
        |                   |                   |
        +-------------------+-------------------+
                            |
                     Fraud Decision
                            |
        +---------+----------+---------+
        |         |                    |
        |         |                    |
 PostgreSQL   Elasticsearch      Alert Service
        |              |              |
        |              |              |
 Dashboard       Kibana         Telegram/Email




## Architecture (v0)
                  +-------------------------+
                 | Transaction Generator   |
                 | (Python + Faker)        |
                 +-----------+-------------+
                             |
                             |
                    Kafka Producer
                             |
                             |
                 ==========================
                 ||       Kafka          ||
                 ==========================
                             |
                    Topic : transactions
                             |
                             |
                     Apache Flink Job
                             |
         +-------------------+--------------------+
         |                                        |
 Rule Engine                         Fraud Scoring
         |                                        |
         +-------------------+--------------------+
                             |
                     Topic : fraud_result
                             |
              +--------------+--------------+
              |                             |
      PostgreSQL Consumer          Alert Consumer
              |                             |
              |                             |
        PostgreSQL                  Telegram (Optional)
              |
              |
        Grafana Dashboard




## Arsitektur FraudShield v1

                         ┌──────────────────────┐
                         │ Transaction Generator│
                         │      Python          │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ Kafka                │
                         │                      │
                         │ transactions.raw     │
                         └──────────┬───────────┘
                                    │
                                    │
                         CG: fraud-detection
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │                              │
                    │       FLINK JOB #1           │
                    │   Transaction Enrichment      │
                    │                              │
                    │ • Event Time                 │
                    │ • Watermark                  │
                    │ • Validation                 │
                    │ • Customer State             │
                    │ • Basic Enrichment            │
                    │                              │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │ Kafka                        │
                    │ transactions.enriched        │
                    └──────────────┬───────────────┘
                                   │
                         CG: fraud-engine
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │                              │
                    │       FLINK JOB #2           │
                    │   Stateful Fraud Engine      │
                    │                              │
                    │ • Keyed State                │
                    │ • Sliding Window             │
                    │ • Velocity Detection         │
                    │ • Dynamic Rules              │
                    │ • Fraud Score                │
                    │                              │
                    └──────────────┬───────────────┘
                                   │
                     ┌─────────────┴──────────────┐
                     │                            │
                     ▼                            ▼
          transactions.analyzed          fraud.alerts
                     │                            │
                     │                            │
              CG: fraud-sink              CG: fraud-alert
                     │                            │
                     ▼                            ▼
                PostgreSQL                  Alert Service
                                                  │
                                           Telegram / Email

Kemudian ada jalur khusus untuk dynamic fraud rules:

             Rule Management Service
                       │
                       ▼
              Kafka: fraud.rules
                       │
                       │
               CG: fraud-rules
                       │
                       ▼
                FLINK JOB #2
                Broadcast State
                       │
                       ▼
                Updated Rules


## Full End-to-End Flow FruadShield v1

                    TRANSACTION
                         │
                         ▼
              ┌──────────────────┐
              │ Python Generator  │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Kafka            │
              │ transactions.raw │
              └────────┬─────────┘
                       │
                CG: fraud-enrichment
                       │
                       ▼
        ┌──────────────────────────────┐
        │ FLINK JOB #1                 │
        │ Transaction Enrichment       │
        │                              │
        │ • Validation                 │
        │ • Event Time                 │
        │ • Watermark                  │
        │ • Enrichment                 │
        │ • Basic Features             │
        └──────────────┬───────────────┘
                       │
                       ▼
             Kafka transactions.enriched
                       │
                  CG: fraud-engine
                       │
                       ▼
        ┌──────────────────────────────┐
        │ FLINK JOB #2                 │
        │ Stateful Fraud Engine        │
        │                              │
        │ • Keyed State                │
        │ • Sliding Window             │
        │ • Velocity Detection         │
        │ • Customer History           │
        │ • Broadcast State            │
        │ • Dynamic Rules              │
        │ • CEP                        │
        │ • Fraud Scoring              │
        └──────────────┬───────────────┘
                       │
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
       fraud.results       fraud.alerts
              │                 │
       CG: fraud-sink     CG: fraud-alert
              │                 │
              ▼                 ▼
         PostgreSQL        Alert Service
              │                 │
              ▼                 ▼
           Grafana        Telegram/Email


Dan ada control plane untuk rule:

                Rule Management
                    │
                    ▼
                fraud.rules
                    │
                CG: fraud-rules
                    │
                    ▼
            FLINK JOB #2
                    │
            Broadcast State
                    │
                    ▼
            Dynamic Fraud Rules




## Posisi FraudShield v1 dalam Roadmap Besar

```
FraudShield v0
│
├── Kafka
├── Flink
├── Rule Engine
├── PostgreSQL
└── Docker
        │
        ▼
FraudShield v1
│
├── Stateful Processing
├── Event Time
├── Watermark
├── Sliding Window
├── Velocity Detection
├── Customer Behavior
├── Dynamic Rules
├── Broadcast State
├── Flink CEP
├── Multiple Topics
├── Multiple Consumer Groups
└── Monitoring
        │
        ▼
FraudShield v3
│
├── Machine Learning
├── Feature Engineering
├── Model Training
├── Model Registry
├── Real-time Inference
├── Fraud Probability
└── Hybrid Rule + ML Decision
```

## Kafka Topics FraudShield v1

| Topic                   | Producer              | Consumer        | Consumer Group     | Fungsi                           |
| ----------------------- | --------------------- | --------------- | ------------------ | -------------------------------- |
| `transactions.raw`      | Transaction Generator | Flink Job #1    | `fraud-enrichment` | Raw transaction                  |
| `transactions.enriched` | Flink Job #1          | Flink Job #2    | `fraud-engine`     | Transaction yang sudah diperkaya |
| `fraud.results`         | Flink Job #2          | PostgreSQL Sink | `fraud-sink`       | Semua hasil analisis             |
| `fraud.alerts`          | Flink Job #2          | Alert Service   | `fraud-alert`      | Transaksi fraud/suspicious       |
| `fraud.rules`           | Rule Service          | Flink Job #2    | `fraud-rules`      | Dynamic fraud rules              |
| `fraud.dlq`             | Flink Job #1/#2       | DLQ Consumer    | `fraud-dlq`        | Data invalid/error               |


## Flink Job FraudShield v1

| Job    | Nama                     | Input                                  | Output                          | Tanggung Jawab                                                              |
| ------ | ------------------------ | -------------------------------------- | ------------------------------- | --------------------------------------------------------------------------- |
| Job #1 | `fraudshield-enrichment` | `transactions.raw`                     | `transactions.enriched`         | Validation, Event Time, Watermark, enrichment, basic features               |
| Job #2 | `fraudshield-engine`     | `transactions.enriched`, `fraud.rules` | `fraud.results`, `fraud.alerts` | Stateful processing, windowing, velocity, dynamic rules, CEP, fraud scoring |

