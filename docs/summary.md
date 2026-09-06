# FraudShield 

## Overview
Tagline: Real-Time Banking Fraud Detection Platform

FraudShield adalah platform real-time fraud detection yang memonitor seluruh transaksi perbankan secara streaming menggunakan Apache Kafka dan Apache Flink. Setiap transaksi yang dilakukan nasabah (transfer, pembayaran QRIS, debit, kredit, pembayaran tagihan, pembelian online, dan sebagainya) akan dikirim sebagai event ke Kafka.

Apache Flink kemudian membaca stream tersebut secara terus-menerus dan melakukan analisis secara real-time menggunakan:
- Rule-Based Detection
- Stateful Processing
- Window Aggregation
- Historical User Behavior
- (Opsional) Machine Learning Prediction

Jika transaksi dianggap mencurigakan, sistem akan:
- menyimpan hasilnya ke database
- mengirim notifikasi
- menghasilkan dashboard monitoring
- memberikan alasan mengapa transaksi dianggap fraud

Target utama project ini adalah menunjukkan bagaimana sebuah sistem streaming dapat memproses ribuan transaksi per detik dengan latency rendah.


## Konsep utama
```
            Transaction
                ↓
            Kafka
                ↓
            Apache Flink
                ↓
    ┌───────────────────────────────┐
    │ Stateful Fraud Detection      │
    │                               │
    │ • Transaction Velocity        │
    │ • Sliding Window              │
    │ • Event Time                  │
    │ • Customer State              │
    │ • CEP                         │
    │ • Dynamic Rules               │
    └───────────────────────────────┘
                ↓
            Fraud Decision
                ↓
            Kafka
                ↓
    ┌──────────────┬───────────────┐
    ↓              ↓               ↓
    PostgreSQL    Alert Service    Dashboard
```


## Arsitektur
```
                    ┌──────────────────────┐
                    │ Transaction Generator│
                    │      Python          │
                    └──────────┬───────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │ Kafka                │
                    │                      │
                    │ transactions_raw     │
                    └──────────┬───────────┘
                                │
                                │
                    CG: fraud-detection
                                │
                                ▼
                ┌──────────────────────────────┐
                │                              │
                │       FLINK JOB #1           │
                │   Transaction Enrichment     │
                │                              │
                │ • Event Time                 │
                │ • Watermark                  │
                │ • Validation                 │
                │ • Customer State             │
                │ • Basic Enrichment           │
                │                              │
                └──────────────┬───────────────┘
                                │
                                ▼
                ┌──────────────────────────────┐
                │ Kafka                        │
                │ transactions_enriched        │
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
                └─────────────────┬────────────┘
                                  │
                    ┌─────────────┴──────────────┐
                    │                            │
                    ▼                            ▼
        transactions.analyzed               fraud_alerts
                    │                            │
                    │                            │
            CG: fraud-sink              CG: fraud-alert
                    │                            │
                    ▼                            ▼
                PostgreSQL                  Alert Service
                                                │
                                        Telegram / Email
```

Kemudian ada jalur khusus untuk dynamic fraud rules:
```
             Rule Management Service
                       │
                       ▼
              Kafka: fraud_rules
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
```


## Kafka Topics

### List Kafka Topic

| Topic                   | Producer              | Consumer        | Consumer Group     | Fungsi                           |
| ----------------------- | --------------------- | --------------- | ------------------ | -------------------------------- |
| `transactions_raw`      | Transaction Generator | Flink Job #1    | `fraud-enrichment` | Raw transaction                  |
| `transactions_enriched` | Flink Job #1          | Flink Job #2    | `fraud-engine`     | Transaction yang sudah diperkaya |
| `fraud_results`         | Flink Job #2          | PostgreSQL Sink | `fraud-sink`       | Semua hasil analisis             |
| `fraud_alerts`          | Flink Job #2          | Alert Service   | `fraud-alert`      | Transaksi fraud/suspicious       |
| `fraud_rules`           | Rule Service          | Flink Job #2    | `fraud-rules`      | Dynamic fraud rules              |
| `fraud_dlq`             | Flink Job #1/#2       | DLQ Consumer    | `fraud-dlq`        | Data invalid/error               |


### Schema Data in Kafka Topic

1. transactions_raw
    - Fungsi: Topic ini berisi data transaksi mentah yang dihasilkan oleh Transaction Generator. Data di sini sebaiknya belum memiliki hasil analisis fraud dari Flink.
    - Producer: Transaction Generator
    - Consumer: Flink Job #1
    - Consumer Group: `fraud-enrichment`
    - Schema:
        | Kolom               | Tipe      | Contoh                 | Keterangan        |
        | ------------------- | --------- | ---------------------- | ----------------- |
        | `transaction_id`    | String    | `TX-20260815-000001`   | ID unik transaksi |
        | `customer_id`       | String    | `CUS-000123`           | ID customer       |
        | `account_id`        | String    | `ACC-000123`           | ID rekening       |
        | `transaction_type`  | String    | `TRANSFER`             | Jenis transaksi   |
        | `amount`            | Decimal   | `7500000.00`           | Nominal transaksi |
        | `currency`          | String    | `IDR`                  | Mata uang         |
        | `merchant_id`       | String    | `MER-00045`            | ID merchant       |
        | `merchant_name`     | String    | `Tokopedia`            | Nama merchant     |
        | `merchant_category` | String    | `E_COMMERCE`           | Kategori merchant |
        | `payment_method`    | String    | `MOBILE_BANKING`       | Metode pembayaran |
        | `device_id`         | String    | `DEV-98765`            | ID perangkat      |
        | `ip_address`        | String    | `103.25.11.20`         | IP transaksi      |
        | `city`              | String    | `Jakarta`              | Kota transaksi    |
        | `country`           | String    | `Indonesia`            | Negara transaksi  |
        | `latitude`          | Decimal   | `-6.200000`            | Latitude          |
        | `longitude`         | Decimal   | `106.816666`           | Longitude         |
        | `timestamp`         | Timestamp | `2026-08-15T01:15:23Z` | Waktu transaksi   |
        | `channel`           | String    | `MOBILE`               | Channel transaksi |

    - Contoh data:
        ```
        {
            "transaction_id": "TX-20260815-000001",
            "customer_id": "CUS-000123",
            "account_id": "ACC-000123",
            "transaction_type": "TRANSFER",
            "amount": 7500000.00,
            "currency": "IDR",
            "merchant_id": "MER-00045",
            "merchant_name": "Tokopedia",
            "merchant_category": "E_COMMERCE",
            "payment_method": "MOBILE_BANKING",
            "device_id": "DEV-98765",
            "ip_address": "103.25.11.20",
            "city": "Jakarta",
            "country": "Indonesia",
            "latitude": -6.200000,
            "longitude": 106.816666,
            "timestamp": "2026-08-15T01:15:23Z",
            "channel": "MOBILE"
        }
        ```

2. transactions_enriched
    - Topic ini adalah hasil dari Flink Job #1. 
    - Tujuannya bukan menentukan fraud final.
    - Tujuannya adalah mengubah raw transaction menjadi transaction yang sudah memiliki contextual information dan initial features.
    - Schema:  
        | Kolom                    | Tipe      | Contoh           | Keterangan            |
        | ------------------------ | --------- | ---------------- | --------------------- |
        | `transaction_id`         | String    | `TX-...`         | ID transaksi          |
        | `customer_id`            | String    | `CUS-000123`     | Customer              |
        | `account_id`             | String    | `ACC-000123`     | Rekening              |
        | `transaction_type`       | String    | `TRANSFER`       | Jenis transaksi       |
        | `amount`                 | Decimal   | `7500000`        | Nominal               |
        | `currency`               | String    | `IDR`            | Mata uang             |
        | `merchant_id`            | String    | `MER-00045`      | Merchant              |
        | `merchant_name`          | String    | `Tokopedia`      | Merchant              |
        | `merchant_category`      | String    | `E_COMMERCE`     | Kategori              |
        | `payment_method`         | String    | `MOBILE_BANKING` | Payment method        |
        | `device_id`              | String    | `DEV-98765`      | Device                |
        | `ip_address`             | String    | `103.25.11.20`   | IP                    |
        | `city`                   | String    | `Jakarta`        | Kota                  |
        | `country`                | String    | `Indonesia`      | Negara                |
        | `timestamp`              | Timestamp | `...`            | Event time            |
        | `channel`                | String    | `MOBILE`         | Channel               |
        | `account_age_days`       | Integer   | `920`            | Umur rekening         |
        | `customer_risk_level`    | String    | `LOW`            | Risk customer         |
        | `merchant_risk_level`    | String    | `HIGH`           | Risk merchant         |
        | `is_new_device`          | Boolean   | `true`           | Device baru           |
        | `is_new_location`        | Boolean   | `false`          | Lokasi baru           |
        | `is_foreign_transaction` | Boolean   | `false`          | Transaksi luar negeri |
        | `is_night_transaction`   | Boolean   | `true`           | Transaksi malam       |
        | `is_high_amount`         | Boolean   | `false`          | Nominal tinggi        |
        | `event_date`             | Date      | `2026-08-15`     | Tanggal event         |
        | `event_hour`             | Integer   | `1`              | Jam event             |
        | `processing_time`        | Timestamp | `...`            | Waktu diproses        |

    - Contoh:
        ```
        {
            "transaction_id": "TX-20260815-000001",
            "customer_id": "CUS-000123",
            "account_id": "ACC-000123",
            "transaction_type": "TRANSFER",
            "amount": 7500000.00,
            "currency": "IDR",
            "merchant_id": "MER-00045",
            "merchant_name": "Tokopedia",
            "merchant_category": "E_COMMERCE",
            "payment_method": "MOBILE_BANKING",
            "device_id": "DEV-98765",
            "ip_address": "103.25.11.20",
            "city": "Jakarta",
            "country": "Indonesia",
            "channel": "MOBILE",
            "account_age_days": 920,
            "customer_risk_level": "LOW",
            "merchant_risk_level": "HIGH",
            "is_new_device": true,
            "is_new_location": false,
            "is_foreign_transaction": false,
            "is_night_transaction": true,
            "is_high_amount": false,
            "event_date": "2026-08-15",
            "event_hour": 1,
            "timestamp": "2026-08-15T01:15:23Z",
            "processing_time": "2026-08-15T01:15:24Z"
        }
        ```

3. fraud_results
    - Ini adalah hasil utama FraudShield.
    - Producer: `Flink Job #2`
    - Consumer: `PostgreSQL Sink` & `Elasticsearch Sink`
    - Consumer Groups: `fraud-sink` & `fraud-search`
    - Schema:
        | Kolom                             | Tipe      | Contoh        | Keterangan             |
        | --------------------------------- | --------- | ------------- | ---------------------- |
        | `result_id`                       | String    | `RES-000001`  | ID hasil               |
        | `transaction_id`                  | String    | `TX-...`      | ID transaksi           |
        | `customer_id`                     | String    | `CUS-000123`  | Customer               |
        | `amount`                          | Decimal   | `7500000`     | Nominal                |
        | `fraud_score`                     | Integer   | `85`          | Score fraud            |
        | `status`                          | String    | `FRAUD`       | Hasil akhir            |
        | `risk_level`                      | String    | `HIGH`        | Level risiko           |
        | `triggered_rules`                 | Array     | `[...]`       | Rules yang terpenuhi   |
        | `rule_count`                      | Integer   | `3`           | Jumlah rules           |
        | `velocity_count`                  | Integer   | `7`           | Transaksi dalam window |
        | `velocity_amount`                 | Decimal   | `45000000`    | Total nominal window   |
        | `window_start`                    | Timestamp | `...`         | Awal window            |
        | `window_end`                      | Timestamp | `...`         | Akhir window           |
        | `previous_city`                   | String    | `Jakarta`     | Lokasi sebelumnya      |
        | `current_city`                    | String    | `Singapore`   | Lokasi sekarang        |
        | `distance_km`                     | Decimal   | `885.4`       | Jarak                  |
        | `time_since_last_transaction_sec` | Integer   | `120`         | Jeda transaksi         |
        | `is_impossible_travel`            | Boolean   | `true`        | Impossible travel      |
        | `detection_method`                | String    | `RULE_ENGINE` | Metode                 |
        | `processed_at`                    | Timestamp | `...`         | Waktu processing       |

    - Contoh data:
        ```
        {
            "result_id": "RES-000001",
            "transaction_id": "TX-20260815-000001",
            "customer_id": "CUS-000123",
            "amount": 7500000.00,
            "fraud_score": 85,
            "status": "FRAUD",
            "risk_level": "HIGH",
            "triggered_rules": [
                {
                    "rule_id": "R003",
                    "rule_type": "NEW_DEVICE",
                    "score": 15
                },
                {
                    "rule_id": "R005",
                    "rule_type": "VELOCITY",
                    "score": 40
                },
                {
                    "rule_id": "R007",
                    "rule_type": "NIGHT_TRANSACTION",
                    "score": 10
                },
                {
                    "rule_id": "R008",
                    "rule_type": "HIGH_MERCHANT_RISK",
                    "score": 20
                }
            ],
            "rule_count": 4,
            "velocity_count": 7,
            "velocity_amount": 45000000.00,
            "window_start": "2026-08-15T01:14:53Z",
            "window_end": "2026-08-15T01:15:23Z",
            "previous_city": "Jakarta",
            "current_city": "Jakarta",
            "distance_km": 0,
            "time_since_last_transaction_sec": 5,
            "is_impossible_travel": false,
            "detection_method": "RULE_ENGINE",
            "processed_at": "2026-08-15T01:15:24Z"
        }
        ```

4. fraud_alerts
    - Tidak semua fraud_results harus menjadi alert.
    - Misalnya: 
        - `NORMAL` → tidak perlu alert.
        - `SUSPICIOUS` → mungkin alert dengan severity MEDIUM.
        - `FRAUD` → alert CRITICAL.
    - Schema:
        | Kolom             | Tipe      | Contoh                                      |
        | ----------------- | --------- | ------------------------------------------- |
        | `alert_id`        | String    | `ALT-000001`                                |
        | `result_id`       | String    | `RES-000001`                                |
        | `transaction_id`  | String    | `TX-000001`                                 |
        | `customer_id`     | String    | `CUS-000123`                                |
        | `severity`        | String    | `CRITICAL`                                  |
        | `fraud_score`     | Integer   | `85`                                        |
        | `status`          | String    | `FRAUD`                                     |
        | `alert_type`      | String    | `VELOCITY_FRAUD`                            |
        | `triggered_rules` | Array     | `[...]`                                     |
        | `message`         | String    | `Multiple suspicious transactions detected` |
        | `created_at`      | Timestamp | `...`                                       |

    - Contoh data:
        ```
        {
            "alert_id": "ALT-000001",
            "result_id": "RES-000001",
            "transaction_id": "TX-20260815-000001",
            "customer_id": "CUS-000123",
            "severity": "CRITICAL",
            "fraud_score": 85,
            "status": "FRAUD",
            "alert_type": "MULTI_RULE_FRAUD",
            "triggered_rules": [
                "NEW_DEVICE",
                "VELOCITY",
                "NIGHT_TRANSACTION",
                "HIGH_MERCHANT_RISK"
            ],
            "message": "Multiple suspicious transaction patterns detected",
            "created_at": "2026-08-15T01:15:25Z"
        }
        ```

5. fraud_rules
    - Topic ini berbeda dengan topic sebelumnya.
    - Kalau topic lain berisi transaction events, fraud_rules berisi configuration/control events.
    - Producer: `Rule Management Service`
    - Consumer: `Flink Job #2`
    - Consumer Group: `fraud-rules`
    - Flink akan memasukkannya ke: Broadcast State
    - Schema:
        | Kolom              | Tipe      | Contoh                          |
        | ------------------ | --------- | ------------------------------- |
        | `rule_id`          | String    | `R001`                          |
        | `rule_name`        | String    | `High Amount Transaction`       |
        | `rule_type`        | String    | `HIGH_AMOUNT`                   |
        | `description`      | String    | `Transaction exceeds threshold` |
        | `threshold`        | Decimal   | `100000000`                     |
        | `window_seconds`   | Integer   | `30`                            |
        | `max_transactions` | Integer   | `5`                             |
        | `score`            | Integer   | `40`                            |
        | `severity`         | String    | `HIGH`                          |
        | `enabled`          | Boolean   | `true`                          |
        | `action`           | String    | `FLAG`                          |
        | `version`          | Integer   | `3`                             |
        | `updated_at`       | Timestamp | `...`                           |
        | `updated_by`       | String    | `admin`                         |
        | `operation`        | String    | `UPSERT`                        |

    - Contoh rule 1:
        ```
        {
            "rule_id": "R001",
            "rule_name": "High Amount Transaction",
            "rule_type": "HIGH_AMOUNT",
            "description": "Flag transaction with amount above threshold",
            "threshold": 100000000,
            "window_seconds": null,
            "max_transactions": null,
            "score": 40,
            "severity": "HIGH",
            "enabled": true,
            "action": "FLAG",
            "version": 1,
            "updated_at": "2026-08-15T00:00:00Z",
            "updated_by": "admin",
            "operation": "UPSERT"
        }
        ```

    - Contoh Rule Velocity:
        ```
        {
            "rule_id": "R005",
            "rule_name": "Transaction Velocity",
            "rule_type": "VELOCITY",
            "description": "More than 5 transactions within 30 seconds",
            "threshold": null,
            "window_seconds": 30,
            "max_transactions": 5,
            "score": 40,
            "severity": "CRITICAL",
            "enabled": true,
            "action": "FLAG",
            "version": 1,
            "updated_at": "2026-08-15T00:00:00Z",
            "updated_by": "admin",
            "operation": "UPSERT"
        }
        ```

6. fraud_dlq
    - DLQ = Dead Letter Queue.
    - Topic ini sangat penting.
    - Jangan sampai satu transaksi corrupt menyebabkan Flink job mati.
    - Misalnya producer mengirim:
        ```
        {
        "transaction_id": null,
        "amount": "abc"
        }
        ```
    - Flink tidak bisa memprosesnya. Daripada:
        ```
        Flink ERROR
            ↓
        Job mati

        kita lakukan:

        Invalid Event
            ↓
        fraud_dlq
        ``` 
    - Schema:
        ```
        | Kolom              | Tipe        | Contoh                    |
        | ------------------ | ----------- | ------------------------- |
        | `dlq_id`           | String      | `DLQ-000001`              |
        | `source_topic`     | String      | `transactions_raw`        |
        | `source_partition` | Integer     | `2`                       |
        | `source_offset`    | Long        | `18291`                   |
        | `event_key`        | String      | `CUS-000123`              |
        | `error_type`       | String      | `VALIDATION_ERROR`        |
        | `error_message`    | String      | `amount must be positive` |
        | `failed_at_stage`  | String      | `ENRICHMENT`              |
        | `retry_count`      | Integer     | `0`                       |
        | `timestamp`        | Timestamp   | `...`                     |
        | `original_payload` | JSON/Object | `{...}`                   |
        ```

    - Contoh
        - Misalnya:
            ```
            {
                "transaction_id": "TX-000099",
                "amount": -500000,
                "customer_id": "CUS-001"
            }
            ```

        - Flink mendeteksi: `amount <= 0` Maka:
            ```
            {
                "dlq_id": "DLQ-000001",
                "source_topic": "transactions_raw",
                "source_partition": 2,
                "source_offset": 18291,
                "event_key": "CUS-001",
                "error_type": "VALIDATION_ERROR",
                "error_message": "Transaction amount must be greater than zero",
                "failed_at_stage": "ENRICHMENT",
                "retry_count": 0,
                "timestamp": "2026-08-15T01:20:10Z",
                "original_payload": {
                    "transaction_id": "TX-000099",
                    "customer_id": "CUS-001",
                    "amount": -500000
                }
            }
            ```


## Data Flow
```
                    Python Generator
                          │
                          ▼
                 ┌─────────────────┐
                 │ transactions_raw│
                 └────────┬────────┘
                          │
                          ▼
                  FLINK JOB #1
                          │
              ┌───────────┴───────────┐
              │                       │
          Valid Event            Invalid Event
              │                       │
              ▼                       ▼
 transactions_enriched          fraud_dlq
              │
              ▼
                  FLINK JOB #2
              │
        ┌─────┴─────────────┐
        │                   │
        ▼                   ▼
 fraud_results         fraud_alerts
        │                   │
        ▼                   ▼
 PostgreSQL            Alert Service
```


## Sample Data

- Kafka topic transactions_raw
    ```
    {
        "transaction_id": "TX-20260901-71DFC13D",
        "customer_id": "CUS-000001",
        "account_id": "ACC-000001",
        "transaction_type": "ONLINE_PURCHASE",
        "amount": 2934870.44,
        "currency": "IDR",
        "merchant_id": "MER-00010",
        "merchant_name": "Netflix",
        "merchant_category": "DIGITAL_SERVICE",
        "payment_method": "VIRTUAL_ACCOUNT",
        "device_id": "DEV-000001",
        "ip_address": "190.23.102.92",
        "city": "Malang",
        "country": "Indonesia",
        "latitude": -7.983908,
        "longitude": 112.621391,
        "timestamp": "2026-09-01T22:56:22.609577+07:00",
        "channel": "INTERNET_BANKING"
    }
    ```

- Kafka topic transactions_enriched
    ```
    {
        "transaction_id": "TX-20260901-71DFC13D",
        "customer_id": "CUS-000001",
        "account_id": "ACC-000001",
        "transaction_type": "ONLINE_PURCHASE",
        "amount": 2934870.44,
        "currency": "IDR",
        "merchant_id": "MER-00010",
        "merchant_name": "Netflix",
        "merchant_category": "DIGITAL_SERVICE",
        "payment_method": "VIRTUAL_ACCOUNT",
        "device_id": "DEV-000001",
        "ip_address": "190.23.102.92",
        "city": "Malang",
        "country": "Indonesia",
        "latitude": -7.983908,
        "longitude": 112.621391,
        "channel": "INTERNET_BANKING",
        "account_age_days": 1395,
        "customer_risk_level": "LOW",
        "merchant_risk_level": "LOW",
        "is_new_device": true,
        "is_new_location": true,
        "is_foreign_transaction": false,
        "is_night_transaction": false,
        "is_high_amount": false,
        "event_date": "2026-09-01",
        "event_hour": 15,
        "timestamp": "2026-09-01T15:56:22.609Z",
        "processing_time": "2026-09-01T15:59:38.998048620Z"
    }
    ```





## Flink Job

| Job    | Nama                     | Input                                  | Output                          | Tanggung Jawab                                                              |
| ------ | ------------------------ | -------------------------------------- | ------------------------------- | --------------------------------------------------------------------------- |
| Job #1 | `fraudshield-enrichment` | `transactions_raw`                     | `transactions_enriched`         | Validation, Event Time, Watermark, enrichment, basic features               |
| Job #2 | `fraudshield-engine`     | `transactions_enriched`, `fraud_rules` | `fraud_results`, `fraud_alerts` | Stateful processing, windowing, velocity, dynamic rules, CEP, fraud scoring |


### Flink Job #1 — Transaction Enrichment
- Nama job flink: TransactionEnrichment
- Tujuan: mengubah Raw Transaction menjadi Enriched Transaction
- Input: transactions_raw (Consumer group: fraud-enrichment)
- Processing:
    1. Validation
        Memastikan:
        - transaction_id != null
        - customer_id != null
        - amount > 0
        - timestamp valid
    2. Event Time
        - Flink mengambil: `transaction.timestamp` sebagai event time.
        - Kemudian: `WatermarkStrategy` digunakan untuk menangani data yang datang terlambat.
        - Misalnya:
            ```
            10:01:01
            10:01:02
            10:01:05
            10:01:03
            ```
            Data 10:01:03 datang terlambat. Flink harus mampu menangani kondisi tersebut.

    3. Customer Enrichment

        Misalnya transaksi:
        ```
        {
        "customer_id": "C001",
        "amount": 5000000,
        "city": "Jakarta"
        }
        ```
        ditambahkan:
        ```
        {
        "customer_id": "C001",
        "amount": 5000000,
        "city": "Jakarta",
        "customer_home_city": "Semarang",
        "customer_age": 29,
        "account_age_days": 920,
        "usual_device": "DEVICE001"
        }
        ```
    4. Basic Feature

        Flink juga dapat membuat feature: `is_night_transaction`,  `is_new_device`,  `is_foreign_transaction`,  `is_high_amount`, `merchant_risk_level`

- Output: `transactions_enriched`

    Contoh:
    ```
    {
        "transaction_id": "TX10001",
        "customer_id": "C001",
        "amount": 7500000,
        "city": "Jakarta",
        "device_id": "D009",
        "is_new_device": true,
        "is_night": false,
        "merchant_risk": "HIGH",
        "event_time": "2026-08-15T01:02:03"
    }
    ```

### Flink Job #2 — Stateful Fraud Engine
- Nama job flink: StatefulFraudEngine
- Input: transactions_enriched (Consumer group: fraud-engine)
- Stateful Processing

    Misalnya: Customer C001

    melakukan:
    ```
    01:00:01 → Rp500.000
    01:00:05 → Rp700.000
    01:00:08 → Rp800.000
    01:00:11 → Rp900.000
    01:00:14 → Rp1.000.000
    ```

    Masing-masing transaksi mungkin terlihat normal. Tetapi jika dilihat bersama: `5 transactions within 15 seconds` maka menjadi suspicious.

    Flink menyimpan state berdasarkan: `customer_id`

    misalnya:

    `keyBy(transaction -> transaction.customerId)`

    Sehingga Flink dapat mengetahui histori customer tersebut.

- Velocity Detection

    Flink dapat menghitung: `transaction_count`, `total_amount`, `average_amount`, `max_amount` dalam window.

    Contoh:

    ```
    Customer C001
    Transactions = 8
    Total = Rp45.000.000
    Window = 30 seconds
    ```

    Rule: `transaction_count > 5` → suspicious.

- Customer Behavioral State

    Flink juga dapat mempertahankan state: `last_transaction_time`, `last_transaction_city`, `last_device`, `last_amount`, `transaction_count`, `total_transaction_amount`

    Misalnya:

    ```
    01:00
    Jakarta

    01:02
    Jakarta

    01:03
    Singapore
    ```

    Perubahan lokasi sangat cepat. Maka: `Impossible Travel` dapat terdeteksi.

- Dynamic Rules dengan Broadcast State
    ```
    Rule Service
        ↓
    Kafka
        ↓
    fraud_rules
        ↓
    Flink Broadcast State
    ```

    Contoh rule:
    ```
    {
    "rule_id": "R001",
    "rule_type": "HIGH_AMOUNT",
    "threshold": 100000000,
    "score": 40,
    "enabled": true
    }
    ```
    Kemudian rule berubah:
    ```
    {
    "rule_id": "R001",
    "rule_type": "HIGH_AMOUNT",
    "threshold": 150000000,
    "score": 35,
    "enabled": true
    }
    ```
    Flink dapat menerima perubahan tersebut tanpa perlu redeploy seluruh job.

- Complex Event Processing

    Pada versi ini juga dapat mulai menggunakan Flink CEP untuk pola transaksi.

    Contoh:
    ```
    LOGIN
    ↓
    LOGIN_FAILED
    ↓
    LOGIN_FAILED
    ↓
    LOGIN_SUCCESS
    ↓
    LARGE_TRANSFER
    ```
    Pola tersebut jauh lebih mencurigakan dibanding satu transaksi besar.

    Contoh lain
    ```
    Small Transaction
        ↓
    Small Transaction
        ↓
    Small Transaction
        ↓
    Large Transaction
    ```

    Pattern:
    ```
    3 small transactions
    followed by
    1 large transaction
    ```
    CEP dapat digunakan untuk mendeteksi pola tersebut.


- Fraud Scoring

    Misalnya:
    ```
    High Amount       +30
    New Device        +15
    Night Transaction +10
    Velocity           +25
    Impossible Travel  +30
    CEP Pattern        +40
    ```

    Total: 150

    Classification:
    ```
    Score	Result
    0–39	NORMAL
    40–90	SUSPICIOUS
    ≥90	FRAUD
    ```

- Output

    Flink Job #2 menghasilkan: `fraud_results`

    Contoh:
    ```
    {
        "transaction_id": "TX001",
        "customer_id": "C001",
        "amount": 25000000,
        "fraud_score": 95,
        "status": "FRAUD",
        "triggered_rules": [
            "HIGH_AMOUNT",
            "NEW_DEVICE",
            "VELOCITY"
        ],
        "processed_at": "2026-08-15T01:10:20"
    }
    ```


## Alert Service
- Consumer: `fraud-alert`
- membaca: `fraud_alerts`
- kemudian:
    ```
    Kafka
    ↓
    Alert Service
    ↓
    Telegram

    atau:

    Kafka
    ↓
    Alert Service
    ↓
    Email
    ```


## PostgreSQL
- Consumer: `fraud-sink`
- membaca: `fraud_results`
- dan menyimpan:
    ```
    transactions
    fraud_results
    fraud_rules
    fraud_alerts
    ```
    

## Full End-to-End Flow FruadShield
```
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
              │ transactions_raw │
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
             Kafka transactions_enriched
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
       fraud_results       fraud_alerts
              │                 │
       CG: fraud-sink     CG: fraud-alert
              │                 │
              ▼                 ▼
         PostgreSQL        Alert Service
              │                 │
              ▼                 ▼
           Grafana        Telegram/Email
```

