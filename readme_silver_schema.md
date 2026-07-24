content = """# GlobalMart Snowflake Project: Silver Layer 🧹⚡

This repository contains the **Silver Layer (Cleaning, Transformation & CDC)** SQL scripts for the GlobalMart data pipeline. It processes raw data from the `BRONZE` schema into clean, structured, and deduplicated tables within the `BRONZE_DB.SILVER` schema using automated Snowflake Tasks, Streams, and Change Data Capture (CDC).

---

## 📂 Processed Silver Datasets

| Target Silver Table | Source Stream / Table | Data Format | Processing Method | Primary CDC / Flattening Logic |
| :--- | :--- | :--- | :--- | :--- |
| **`cleaned_pos_transaction`** | `bronze.stream_for_pos` | Tabular / CSV | Stream + Task (`MERGE`) | Deduplication via `QUALIFY row_number()`, audit timestamps (`created_at`, `updated_at`) |
| **`erp_orders`** | `bronze.stream_raw_erp_data` | Semi-Structured (Parquet) | Stream + Task (`MERGE`) | Schema extraction from `VARIANT`, CDC operations (`INSERT`, `UPDATE`, `DELETE`) |
| **`inventory_snapshots`** | `bronze.raw_erp_Inventory_data` | Semi-Structured (Parquet) | Batch Direct Insert | Type-casted conversion from Parquet `VARIANT` to relational schema |
| **`device_event_readings_from_IOT`** | `bronze.stream_for_iot` | Semi-Structured (JSON) | Stream + Task (`MERGE`) | Array expansion using `LATERAL FLATTEN` on `readings` and `alerts` |

---

## 🛠️ Pipeline Architecture & Key Features

* **Automated Stream-Triggered Tasks:**
  * Tasks (`start_merge_when_stream_updates`, `task_for_ERP`, `task_for_iot`) run on 1-minute schedules and trigger conditionally using `WHEN SYSTEM$STREAM_HAS_DATA()` to optimize warehouse compute usage.
* **Robust Change Data Capture (CDC) & Deduplication:**
  * Uses Snowflake `MERGE INTO` statements combined with `QUALIFY row_number()` partitioning to safely handle duplicate entries and process `INSERT`, `UPDATE`, and `DELETE` actions from Bronze Streams.
* **Semi-Structured JSON Array Flattening:**
  * Leverages `LATERAL FLATTEN` to unnest nested JSON arrays (`readings` and `alerts`) in IoT payloads, applying `OUTER => TRUE` to preserve records when alert arrays are null or empty.
* **Data Governance & Disaster Recovery:**
  * Implements time-travel capabilities for point-in-time recovery via:
    * **Relative Offset:** `BEFORE (OFFSET => -3600)`
    * **Statement Query ID:** `BEFORE (STATEMENT => '<query_id>')`
    * **Timestamp Functions:** `AT (TIMESTAMP => DATEADD(hour, -1, current_timestamp()))`
    * **Table Restoration & Dropped Table Recovery:** Full table restoration using `CREATE OR REPLACE TABLE ... BEFORE()` and table recovery via `UNDROP TABLE`.
"""

file_path = "README_SILVER.md"
with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"Generated {file_path}")