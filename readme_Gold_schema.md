# 📊 GlobalMart Data Warehouse Pipeline: Gold Layer

Documentation for the **Gold Layer (`BRONZE_DB.GOLD`)**, focusing on analytics-ready views, cross-domain data integration (POS, ERP, IoT), and key business performance metrics.

---

## 🖼️ Analytical Views & Data Marts

| View Name | Source Silver Tables | Core Business Purpose | Key Metrics |
| :--- | :--- | :--- | :--- |
| **`pos_erp_GrossMargin_view`** | `cleaned_pos_transaction`, `ERP_ORDERS` | **Profitability Analysis**: Joins POS sales revenue with ERP procurement costs per SKU and store date. | `total_revenue`, `total_procurement_cost`, `gross_margin` |
| **`pos_iot_sensor_affect_revenue_view`** | `cleaned_pos_transaction`, `device_event_readings_from_IOT` | **Operational Risk**: Measures sales performance on days with critical IoT telemetry alerts (`TEMP_BREACH` or `HIGH` severity). | `max_temp_reached`, `total_count_per_day`, `qty_sold`, `total_revenue` |
| **`pos_iot_sensor_data`** | `cleaned_pos_transaction`, `device_event_readings_from_IOT` | **Telemetry & Sales**: Combines store hardware sensor readings with daily revenue totals. | `total_revenue`, `sensor_value_total`, `average_sensor_value` |

---

## 📈 Key Metrics & Window Calculations

* **Daily Aggregations & Customer Footfall:**
  * Calculates daily store performance metrics: `unit_sold`, `Total_revenue`, and unique store traffic (`COUNT(DISTINCT customer_id)`).
* **30-Day Rolling Revenue:**
  * Computes 30-day moving sales per store using window frame logic:
    ```sql
    SUM(daily_amount) OVER (
        PARTITION BY store_id 
        ORDER BY transaction_date 
        RANGE BETWEEN INTERVAL '30 day' PRECEDING AND CURRENT ROW
    ) AS total_revenue_30_days
    ```
* **Gross Profit Margin Logic:**
  * Integrates POS and ERP data using `COALESCE` to account for cost gaps:
    $$\text{Gross Margin} = \text{Total Revenue} - \text{Procurement Cost}$$

---

## 🔍 Execution Script

```sql
-- Create Schema
CREATE SCHEMA IF NOT EXISTS bronze_db.gold;

-- Query Analytical Views
SELECT * FROM bronze_db.gold.pos_erp_GrossMargin_view;
SELECT * FROM bronze_db.gold.pos_iot_sensor_data;
SELECT * FROM bronze_db.gold.pos_iot_sensor_affect_revenue_view;