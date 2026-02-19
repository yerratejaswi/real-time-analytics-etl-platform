
---

# 📊 Real-Time Analytics & ETL Platform

## 🚀 Overview

This project simulates a **Business Platform-style real-time data pipeline**, inspired by large-scale consumer applications such as TikTok.

It demonstrates how event data can be:

* Ingested in real time
* Processed via distributed streaming
* Transformed into curated analytical datasets
* Orchestrated using workflow automation
* Queried for business insights

The platform uses **Kafka, Spark Structured Streaming, Airflow, and Parquet-based warehouse storage** to implement an end-to-end ETL pipeline.

---

## 🏗 Architecture

```
Event Generator → Kafka → Spark Streaming ETL → Parquet Warehouse → Airflow → SQL Analytics
```

### Components

| Layer         | Technology                 | Purpose                             |
| ------------- | -------------------------- | ----------------------------------- |
| Ingestion     | Kafka                      | Real-time event streaming           |
| Processing    | Spark Structured Streaming | Batch & micro-batch transformations |
| Storage       | Parquet (partitioned)      | Curated warehouse layer             |
| Orchestration | Apache Airflow             | Workflow scheduling & monitoring    |
| Analytics     | SQL                        | Multi-dimensional analysis          |

---

## 📂 Project Structure

```
real-time-analytics-etl-platform/
│
├── docker-compose.yml
├── README.md
│
├── data-generator/
│   └── producer.py
│
├── spark-etl/
│   └── spark_job.py
│
├── airflow/
│   └── dags/
│       └── etl_pipeline.py
│
└── analytics/
    └── queries.sql
```

---

## 🔄 Data Flow

### 1️⃣ Event Generation

A simulated producer generates user activity events:

```json
{
  "user_id": 101,
  "event_type": "video_view",
  "watch_time": 120,
  "timestamp": 1700000000
}
```

These events are pushed to Kafka in real time.

---

### 2️⃣ Streaming ETL (Spark)

Spark Structured Streaming:

* Consumes Kafka events
* Parses JSON payloads
* Applies transformations
* Writes curated data to Parquet
* Partitions by `event_type`
* Uses checkpointing for fault tolerance

Partitioning strategy:

* `partitionBy(event_type)`
* Improves query performance
* Reduces scan cost for filtered queries

Checkpointing ensures:

* Exactly-once semantics
* Idempotent reprocessing
* Safe recovery after failure

---

### 3️⃣ Orchestration (Airflow)

Airflow manages:

* Scheduled ETL runs
* Task dependency management
* Monitoring & retry logic
* Failure handling

DAG: `real_time_etl_pipeline`

---

### 4️⃣ Analytical Queries

Example business queries:

```sql
-- Daily Active Users
SELECT COUNT(DISTINCT user_id)
FROM curated_events;

-- Average watch time per event type
SELECT event_type, AVG(watch_time)
FROM curated_events
GROUP BY event_type;
```

---

## 🐳 Running the Project

### Step 1 — Start Kafka

```bash
docker-compose up -d
```

---

### Step 2 — Run Event Producer

```bash
cd data-generator
python producer.py
```

---

### Step 3 — Run Spark ETL

```bash
cd spark-etl
spark-submit spark_job.py
```

---

### Step 4 — Start Airflow (if configured)

```bash
airflow standalone
```

Access UI:

```
http://localhost:8080
```

---

## 🛠 Key Engineering Concepts Demonstrated

* Distributed event streaming
* Micro-batch processing
* Schema-based transformation
* Partitioned data warehousing
* Idempotent ETL design
* Checkpoint-based recovery
* Workflow orchestration
* Query optimization

---

## 📈 Scalability Considerations

* Kafka partitions enable horizontal scaling
* Spark executors process data in parallel
* Partitioned Parquet reduces query scan time
* Airflow supports distributed scheduling

This architecture can be extended to support:

* Window-based aggregations
* Data validation frameworks
* Dead-letter queues
* Schema evolution handling
* Backfill pipelines

---

## 🎯 Learning Outcomes

This project demonstrates how modern data platforms:

* Separate ingestion from transformation
* Decouple producers from consumers
* Optimize analytical storage layers
* Ensure data reliability and recoverability
* Support business analytics at scale

---

## 🔮 Future Enhancements

* Add PostgreSQL warehouse layer
* Add data quality validation checks
* Implement late-event handling
* Add monitoring metrics (Prometheus/Grafana)
* Convert Spark job to Java for production parity

---

## 👨‍💻 Author

Tejaswi Yerra
MS Computer Science
Distributed Systems & Data Engineering


