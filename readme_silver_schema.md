# GlobalMart Snowflake Data Pipeline Project 🚀

This repository contains the end-to-end data pipeline implementation for GlobalMart in Snowflake. The project is structured using the Medallion Architecture (Bronze and Silver layers) to ingest and transform raw multi-format data into clean, structured tables.

---

## 📂 1. Bronze Layer (Raw Ingestion)

The Bronze layer ingests raw datasets from our external cloud storage stage (`@External_stage`) into Snowflake, preserving original formats.

### Data Sources & Ingestion Methods
| Source | Table Name | Source File Format | Ingestion Method | Target Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Point of Sale (POS)** | `pos_transaction` | CSV | Snowpipe (`External_pipe`) | Real-time transactional data tracking |
| **IoT Devices** | `raw_IoT` | JSON | Snowpipe (`pipe_for_Iot`) | Nested semi-structured sensor payloads |
| **ERP Systems** | `raw_erp_data` | Parquet | Manual Copy (`COPY INTO`) | Enterprise resource orders |
| **ERP Inventory** | `raw_erp_Inventory_data` | Parquet | Manual Copy (`COPY INTO`) | Enterprise inventory snapshots |

* **CDC Setup:** A native Snowflake stream (`stream_for_Pos`) is mapped to the `pos_transaction` table to capture real-time CDC (inserts/deletes) for downstream processing.

---

## ⚙️ 2. Silver Layer (Transformation & Cleaning)

The Silver layer applies schema-on-write structuring, deduplicates incoming streams, and flattens nested JSON/Parquet attributes.

### Cleaned Data Models
| Source Table (Bronze) | Target Table (Silver) | Format Change | Processing Logic |
| :--- | :--- | :--- | :--- |
| `stream_for_pos` | `cleaned_pos_transaction` | Stream to Structured | Merged incrementally via Task (`start_merge_when_stream_updates`) |
| `raw_erp_data` | `erp_orders` | Variant to Structured | Explicit path casting of nested Parquet schemas |
| `raw_erp_Inventory_data` | `inventory_snapshots` | Variant to Structured | Explicit path casting of nested Parquet schemas |
| `iot` | `device_event_readings_from_iot` | Variant to Structured | Double `LATERAL FLATTEN` on nested JSON `readings` and `alerts` arrays |

### Key Features
* **Automated Incremental Merges:** A continuous Snowflake task runs every minute. It checks the POS stream, uses a `QUALIFY ROW_NUMBER()` window function to remove duplicate actions, and runs a transactional `MERGE` (Upsert/Delete) into Silver.
* **Semi-Structured Flattening:** Uses `LATERAL FLATTEN(..., outer => TRUE)` on the IoT JSON arrays. The outer join ensures sensor readings are preserved even when no active device alerts exist.
* **Standardized Timezone:** Session timezone is set to `'Asia/Kolkata'` to ensure uniform, localized timestamps.