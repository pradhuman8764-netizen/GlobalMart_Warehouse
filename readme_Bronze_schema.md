content = """# GlobalMart Snowflake Project: Bronze Layer 🚀

This repository contains the **Bronze Layer (Raw Ingestion)** SQL scripts for the GlobalMart data pipeline. It continuously ingests raw, unparsed datasets from an AWS S3 external stage (`External_stage`) into the `BRONZE_DB.BRONZE` schema.

---

## 📂 Data Ingestion Summary

| Source Dataset | Target Table | Format | Ingestion Method | Active CDC Stream | Data Retention |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Point of Sale** | `pos_transaction` | CSV | Snowpipe (`External_pipe`) | `stream_for_Pos` | 14 Days |
| **IoT Sensors** | `raw_IoT` | JSON | Snowpipe (`pipe_for_Iot`) | `stream_for_iot` | 14 Days |
| **ERP Orders** | `raw_erp_data` | Parquet | Snowpipe (`ERP_ORDERS_PIPE`) | `stream_raw_erp_data` | 14 Days |
| **ERP Inventory** | `raw_erp_Inventory_data` | Parquet | Manual (`COPY INTO`) | None | Default |

---

## 🛠️ Pipeline Architecture & Key Features

* **Automated Data Ingestion (Snowpipes):** 
  * Configured `AUTO_INGEST = TRUE` for **POS**, **IoT**, and **ERP Orders** using AWS S3 event notifications.
  * Handles JSON array stripping (`STRIP_OUTER_ARRAY = TRUE`) and CSV header suppression (`SKIP_HEADER = 1`).
* **Change Data Capture (CDC):**
  * Snowflake `STREAMS` created on `pos_transaction`, `raw_IoT`, and `raw_erp_data` to track delta changes (`INSERT`, `UPDATE`, `DELETE`) for automated downstream Silver Layer processing.
* **Semi-Structured VARIANT Storage:**
  * Uses native `VARIANT` columns for IoT JSON payloads and ERP Parquet files to maintain raw schema-on-read flexibility.
* **Data Governance & Recovery:**
  * Explicit 14-day Time Travel retention configured on critical raw tables (`pos_transaction`, `raw_IoT`, `raw_erp_data`) for point-in-time recovery.
"""

file_path = "README.md"
with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"Generated {file_path}")