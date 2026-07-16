# GlobalMart Snowflake Project: Bronze Layer 🚀

This repository contains the **Bronze Layer (Raw Ingestion)** SQL scripts for the GlobalMart data pipeline. It continuously ingests raw datasets from our cloud storage bucket into Snowflake, maintaining the original formats.

---

## 📂 Ingested Data Sources

The Bronze layer processes data across three distinct file formats:

| Source | Table Name | Source File Format | Ingestion Method | Target Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Point of Sale (POS)** | `pos_transaction` | CSV | Snowpipe (`External_pipe`) | Real-time transactional data tracking |
| **IoT Devices** | `raw_IoT` | JSON | Snowpipe (`pipe_for_Iot`) | Nested semi-structured sensor payloads |
| **ERP Systems** | `raw_erp_data` | Parquet | Manual Copy (`COPY INTO`) | Enterprise resource orders |
| **ERP Inventory** | `raw_erp_Inventory_data` | Parquet | Manual Copy (`COPY INTO`) | Enterprise inventory snapshots |

---

## 🛠️ Pipeline Features

* **Auto-Ingestion (Snowpipes):** Continuous ingestion is enabled for both POS CSV data and IoT JSON payloads via Snowpipes configured with `AUTO_INGEST = TRUE`.
* **Change Data Capture (CDC):** A native Snowflake stream (`stream_for_Pos`) is mapped to the `pos_transaction` table to capture new, modified, or deleted rows for subsequent deduplication in the Silver layer.
* **Semi-Structured Support:** Native `VARIANT` column types are utilized for both IoT JSON and ERP Parquet sources, ensuring schema-on-read flexibility.