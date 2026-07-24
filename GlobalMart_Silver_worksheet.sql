use database BRONZE_DB;

--=============================================================================================================================================================================================
-- CREATING SCHEMA FOR SILVER LAYER FOR CLEANING AND STREAMING AND TO APPLY CDC 
CREATE OR REPLACE SCHEMA silver;
--=============================================================================================================================================================================================


--=============================================================================================================================================================================================
-- creating an new table cleaned_pos_transaction from bronze schema with TASK implemented 

CREATE OR REPLACE TABLE bronze_db.silver.cleaned_pos_transaction (
    transaction_id      VARCHAR(50),
    store_id            VARCHAR(20),
    store_name          VARCHAR(100),
    store_city          VARCHAR(100),
    store_region        VARCHAR(100),
    cashier_id          VARCHAR(20),
    customer_id         VARCHAR(20),
    transaction_date    DATE,
    transaction_time    TIME,
    product_sku         VARCHAR(50),
    product_name        VARCHAR(255),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    quantity            NUMBER(10,0),
    unit_price          NUMBER(10,2),
    discount_pct        NUMBER(9,2),
    total_amount        NUMBER(12,2),
    payment_method      VARCHAR(50),
    loyalty_points      NUMBER(10,0)
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
); 


--=============================================================================================================================================================================================
-- creating TASK of merge statement which will work whenever the POs_stream will get new data 

CREATE OR REPLACE TASK start_merge_when_stream_updates
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('bronze_db.bronze.stream_for_pos')
AS
MERGE INTO bronze_db.silver.cleaned_pos_transaction AS TARGET
USING (
SELECT 

TRANSACTION_ID,
STORE_ID,
STORE_NAME,
STORE_CITY,
STORE_REGION,
CASHIER_ID,
CUSTOMER_ID,
TRANSACTION_DATE,
TRANSACTION_TIME,
PRODUCT_SKU,
PRODUCT_NAME,
CATEGORY,
SUBCATEGORY,
QUANTITY,
UNIT_PRICE,
DISCOUNT_PCT,
TOTAL_AMOUNT,
PAYMENT_METHOD,
LOYALTY_POINTS,
metadata$action as stream_action

FROM bronze_db.bronze.stream_for_pos
QUALIFY row_number() OVER (PARTITION BY  TRANSACTION_ID ORDER BY CASE WHEN metadata$action = 'INSERT' THEN 0 ELSE 1 END) = 1

) AS source
on target.TRANSACTION_ID = source.TRANSACTION_ID 


--=============================================================================================================================================================================================
-- FOR UPDATION IN TABE 
--=============================================================================================================================================================================================
WHEN MATCHED AND stream_action = 'INSERT' THEN

UPDATE SET 
        target.STORE_ID         = source.STORE_ID,
        target.STORE_NAME       = source.STORE_NAME,
        target.STORE_CITY       = source.STORE_CITY,
        target.STORE_REGION     = source.STORE_REGION,
        target.CASHIER_ID       = source.CASHIER_ID,
        target.CUSTOMER_ID      = source.CUSTOMER_ID,
        target.TRANSACTION_DATE = source.TRANSACTION_DATE,
        target.TRANSACTION_TIME = source.TRANSACTION_TIME,
        target.PRODUCT_SKU      = source.PRODUCT_SKU,
        target.PRODUCT_NAME     = source.PRODUCT_NAME,
        target.CATEGORY         = source.CATEGORY,
        target.SUBCATEGORY      = source.SUBCATEGORY,
        target.QUANTITY         = source.QUANTITY,
        target.UNIT_PRICE       = source.UNIT_PRICE,
        target.DISCOUNT_PCT     = source.DISCOUNT_PCT,
        target.TOTAL_AMOUNT     = source.TOTAL_AMOUNT,
        target.PAYMENT_METHOD   = source.PAYMENT_METHOD,
        target.LOYALTY_POINTS   = source.LOYALTY_POINTS,
        target.updated_at       = current_timestamp()

--=============================================================================================================================================================================================
-- FOR DELETION IN TABLE 
--=============================================================================================================================================================================================
WHEN MATCHED AND  stream_action = 'DELETE' THEN
DELETE 

--=============================================================================================================================================================================================
-- FOR INSERTION IN TABLE 
--=============================================================================================================================================================================================
WHEN NOT MATCHED AND stream_action = 'INSERT' THEN

INSERT (
        TRANSACTION_ID, STORE_ID, STORE_NAME, STORE_CITY, STORE_REGION, 
        CASHIER_ID, CUSTOMER_ID, TRANSACTION_DATE, TRANSACTION_TIME, 
        PRODUCT_SKU, PRODUCT_NAME, CATEGORY, SUBCATEGORY, QUANTITY, 
        UNIT_PRICE, DISCOUNT_PCT, TOTAL_AMOUNT, PAYMENT_METHOD, LOYALTY_POINTS ,UPDATED_AT     
    ) 
    VALUES (
        source.TRANSACTION_ID, source.STORE_ID, source.STORE_NAME, source.STORE_CITY, source.STORE_REGION, 
        source.CASHIER_ID, source.CUSTOMER_ID, source.TRANSACTION_DATE, source.TRANSACTION_TIME, 
        source.PRODUCT_SKU, source.PRODUCT_NAME, source.CATEGORY, source.SUBCATEGORY, source.QUANTITY, 
        source.UNIT_PRICE, source.DISCOUNT_PCT, source.TOTAL_AMOUNT, source.PAYMENT_METHOD, source.LOYALTY_POINTS ,current_timestamp()
    );

--=============================================================================================================================================================================================
-- for first time when we create or replace an task we have to resume the task 
ALTER TASK start_merge_when_stream_updates resume ;
select * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) order by scheduled_time desc;
select * from cleaned_pos_transaction ;
--=============================================================================================================================================================================================



--=============================================================================================================================================================================================
// creating cleaned parquet data of ERP ORDERS from bronze schemaa to silver schema 
--=============================================================================================================================================================================================
CREATE OR REPLACE TABLE erp_orders (
    order_id            VARCHAR(50),
    order_date          TIMESTAMP,
    order_status        VARCHAR(50),
    expected_delivery   DATE,
    actual_delivery     DATE,
    is_late             BOOLEAN,
    lead_time_days      NUMBER(5,0),
    category            VARCHAR(100),
    product_sku         VARCHAR(50),
    quantity_ordered    NUMBER(10,0),
    quantity_received   NUMBER(10,0),
    unit_cost           NUMBER(10,2),
    total_cost          NUMBER(12,2),
    store_id            VARCHAR(20),
    store_city          VARCHAR(100),
    supplier_id         VARCHAR(20),
    supplier_name       VARCHAR(150),
    supplier_city       VARCHAR(100),
    warehouse_id        VARCHAR(20) ,
    created_at          TIMESTAMP_NTZ(),
    updated_at          TIMESTAMP_NTZ()
);



--=============================================================================================================================================================================================
-- CREATING CDC FOR ERP ORDERS TABLE 
--=============================================================================================================================================================================================
CREATE TASK task_for_ERP
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 minute'
WHEN SYSTEM$stream_has_data('bronze_db.bronze.stream_raw_erp_data')
as
MERGE INTO bronze_db.silver.erp_orders AS  target USING 
(
SELECT 
    parquet_raw:order_id::VARCHAR        as  order_id,
    parquet_raw:order_date::TIMESTAMP    as  order_date,
    parquet_raw:order_status::VARCHAR    as  order_status,
    parquet_raw:expected_delivery::DATE  as  expected_delivery,
    parquet_raw:actual_delivery::DATE    as  actual_delivery,
    parquet_raw:is_late::BOOLEAN         as  is_late,    
    parquet_raw:lead_time_days::INT      as  lead_time_days,
    parquet_raw:category::VARCHAR        as  category,
    parquet_raw:product_sku::VARCHAR     as  product_sku,
    parquet_raw:quantity_ordered::INT    as  quantity_ordered,
    parquet_raw:quantity_received::INT   as  quantity_received,
    parquet_raw:unit_cost::NUMBER(10,2)  as  unit_cost,
    parquet_raw:total_cost::NUMBER(12,2) as  total_cost,
    parquet_raw:store_id::VARCHAR        as  store_id,
    parquet_raw:store_city::VARCHAR      as  store_city,
    parquet_raw:supplier_id::VARCHAR     as  supplier_id,
    parquet_raw:supplier_name::VARCHAR   as  supplier_name,
    parquet_raw:supplier_city::VARCHAR   as  supplier_city,
    parquet_raw:warehouse_id::VARCHAR    as  warehouse_id ,
    metadata$action as ERP_stream_action 
    
FROM bronze_db.bronze.stream_raw_erp_data 
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY CASE WHEN METADATA$ACTION  ='INSERT' THEN 0 ELSE 1 END) = 1)
as source
ON target.order_id = source.order_id

--=============================================================================================================================================================================================
-- FOR DELETION IN TABLE 

WHEN MATCHED AND source.ERP_stream_action  ='DELETE' THEN 
delete
--=============================================================================================================================================================================================



--=============================================================================================================================================================================================
-- FOR UPDATION IN TABLE 
--=============================================================================================================================================================================================
WHEN MATCHED AND source.ERP_stream_action  ='INSERT' THEN 
UPDATE SET 

   target.order_date               = source.order_date,   
   target.order_status             = source.order_status,    
   target.expected_delivery        = source.expected_delivery,          
   target.actual_delivery          = source.actual_delivery,        
   target.is_late                  = source.is_late,
   target.lead_time_days           = source.lead_time_days,       
   target.category                 = source.category, 
   target.product_sku              = source.product_sku,    
   target.quantity_ordered         = source.quantity_ordered,         
   target.quantity_received        = source.quantity_received,          
   target.unit_cost                = source.unit_cost,  
   target.total_cost               = source.total_cost,   
   target.store_id                 = source.store_id, 
   target.store_city               = source.store_city,   
   target.supplier_id              = source.supplier_id,    
   target.supplier_name            = source.supplier_name,      
   target.supplier_city            = source.supplier_city,      
   target.warehouse_id             = source.warehouse_id ,    
   target.updated_at               = current_timestamp()

--=============================================================================================================================================================================================
-- FOR INSERTION IN TABLE 
--=============================================================================================================================================================================================
WHEN NOT MATCHED AND source.ERP_stream_action  ='INSERT' THEN 
INSERT 
(
    order_id,
    order_date,
    order_status,
    expected_delivery,
    actual_delivery,
    is_late,
    lead_time_days,
    category,
    product_sku,
    quantity_ordered,
    quantity_received,
    unit_cost,
    total_cost,
    store_id,
    store_city,
    supplier_id,
    supplier_name,
    supplier_city,
    warehouse_id ,
    updated_at
)
VALUES (
    source.order_id,
    source.order_date,
    source.order_status,
    source.expected_delivery,
    source.actual_delivery,
    source.is_late,
    source.lead_time_days,
    source.category,
    source.product_sku,
    source.quantity_ordered,
    source.quantity_received,
    source.unit_cost,
    source.total_cost,
    source.store_id,
    source.store_city,
    source.supplier_id,
    source.supplier_name,
    source.supplier_city,
    source.warehouse_id , 
    current_timestamp()
);
--=============================================================================================================================================================================================--=============================================================================================================================================================================================


--=============================================================================================================================================================================================
-- RESUMING THE TASK WHEN CREATED BY DEFAULT TASK ARE IN SUSPENDED STATE 

ALTER TASK task_for_ERP RESUME;
select * from erp_orders;
--=============================================================================================================================================================================================

--=============================================================================================================================================================================================
--creating table for Parquet file format table storage in structured format

CREATE OR REPLACE TABLE bronze_db.silver.inventory_snapshots (
    snapshot_date       DATE,
    store_id            VARCHAR(20),
    warehouse_id        VARCHAR(20),
    category            VARCHAR(100),
    product_sku         VARCHAR(50),
    quantity_on_hand    NUMBER(10,0),
    reorder_level       NUMBER(10,0),
    max_stock_level     NUMBER(10,0),
    last_received_date  DATE
);
--=============================================================================================================================================================================================

-- DIRECTLY INSERTING THE DATA FROM RAW INVENTORY DATA TABLE TO STRUCTURED INVENTORY_SNAPSHOT TABLE 
--=============================================================================================================================================================================================
INSERT INTO inventory_snapshots (
    snapshot_date,      
    store_id,            
    warehouse_id,        
    category,            
    product_sku,         
    quantity_on_hand,   
    reorder_level,       
    max_stock_level,     
    last_received_date
)

SELECT 
    parquet_Inventory_raw:snapshot_date::date,
    parquet_Inventory_raw:store_id::VARCHAR,  
    parquet_Inventory_raw:warehouse_id::VARCHAR,
    parquet_Inventory_raw:category::VARCHAR,            
    parquet_Inventory_raw:product_sku::VARCHAR,         
    parquet_Inventory_raw:quantity_on_hand::number,   
    parquet_Inventory_raw:reorder_level::number,       
    parquet_Inventory_raw:max_stock_level::number,     
    parquet_Inventory_raw:last_received_date::date,
    FROM bronze_db.bronze.raw_erp_Inventory_data;
--=============================================================================================================================================================================================






--=============================================================================================================================================================================================
-- creating the table for IOT structured data storage 

CREATE OR REPLACE TABLE bronze_db.silver.device_event_readings_from_IOT(

    alert_type          varchar(20),
    severity            varchar(20),
    triggere_at         timestamp ,
    store_id            VARCHAR(20),
    store_name          VARCHAR(100),
    device_id           VARCHAR(50),
    event_id            VARCHAR(50),
    event_type          VARCHAR(50),
    event_timestamp     TIMESTAMP,
    store_floor         NUMBER(3,0),
    battery_pct         NUMBER(3,0),
    signal_rssi         NUMBER(4,0),
    firmware_version    VARCHAR(20),
    sensor_name         VARCHAR(50),
    sensor_value        NUMBER(10,2),
    sensor_unit         VARCHAR(20) ,
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
--=============================================================================================================================================================================================



--=============================================================================================================================================================================================
-- CREATING TASK FOR IOT TABLE WITH CDC MERGE STATEMENT HANDLING DUPLICATED VALUES AND STATEMENTS LIKE UPDATE DELETE INSERT 
--=============================================================================================================================================================================================
 CREATE OR REPLACE TASK task_for_iot 
 WAREHOUSE = 'COMPUTE_WH'
 SCHEDULE = '1 MINUTES'
 WHEN SYSTEM$STREAM_HAS_DATA('bronze_db.bronze.stream_for_iot')
as
 MERGE INTO bronze_db.silver.device_event_readings_from_IOT as target
 USING
 (
    select 
    // accessing the nested arry of "alerts" column using lateral flatten 
    a.value:alert_type::varchar  as alert_type,
    a.value:severity::varchar  as severity,
    a.value:triggered_at::timestamp as triggered_at ,

    // normal columns accesse directly by varient column "row_col"
    row_col:store_id::VARCHAR  as store_id ,
    row_col:store_name::VARCHAR as store_name,      
    row_col:device_id::VARCHAR  as device_id ,   
    row_col:event_id::VARCHAR   as event_id  ,  
    row_col:event_type::VARCHAR  as event_type  , 
    row_col:timestamp::TIMESTAMP  as event_timestamp,

    // nested array columns of metadata are accessed using dot(.) no need for lateral flatten 
    row_col:metadata.store_floor::INT  as store_floor,       
    row_col:metadata.battery_pct::INT  as battery_pct,     
    row_col:metadata.signal_rssi::INT  as signal_rssi,    
    row_col:metadata.firmware::VARCHAR  as firmware_version,

    // The reading column contain multiple items so this needs to be LATERAL FLATTENED 

    r.value:sensor::varchar as sensor_name,
    r.value:value::number(10,2) as sensor_value,
    r.value:unit::varchar as sensor_unit ,
    metadata$action as stream_action_for_iot ,
    
    FROM bronze_db.bronze.stream_for_iot ,
    LATERAL FLATTEN (input => row_col:readings) as r,
    LATERAL FLATTEN(input => row_col:alerts , outer => TRUE)  as a  // used outer to describe snowfalke to use outer join instead on inner as alerts may have nulls value 

    QUALIFY row_number() OVER (PARTITION BY   event_id , sensor_name ORDER BY CASE WHEN metadata$action ='INSERT' THEN 0 ELSE 1 END) = 1 ) as source 

    on source.event_id = target.event_id and target.sensor_name = source.sensor_name
    --=============================================================================================================================================================================================
    -- FOR DELETION 
   WHEN MATCHED AND  stream_action_for_iot = 'DELETE'
   THEN DELETE
   --=============================================================================================================================================================================================

   --=============================================================================================================================================================================================
   -- FOR UPDATION IN TABLE 
   
   WHEN MATCHED AND stream_action_for_iot = 'INSERT'
   THEN UPDATE SET 
   
   TARGET.alert_type      = source.alert_type ,
   TARGET.severity        = source.severity  ,
   TARGET.triggere_at     = source.triggered_at ,
   TARGET.store_id        = source.store_id  ,
   TARGET.store_name      = source.store_name ,
   TARGET.device_id       = source.device_id  ,
   TARGET.event_id        = source.event_id   ,             
   TARGET.event_type      = source.event_type  ,             
   TARGET.event_timestamp = source.event_timestamp ,
   TARGET.store_floor     = source.store_floor       ,
   TARGET.battery_pct     = source.battery_pct       ,
   TARGET.signal_rssi     = source.signal_rssi       ,
   TARGET.firmware_version= source.firmware_version  ,
   TARGET.sensor_name     = source.sensor_name       ,
   TARGET.sensor_value    = source.sensor_value ,
   TARGET.sensor_unit     = source.sensor_unit ,
   TARGET.updated_at      = current_timestamp()
    --=============================================================================================================================================================================================

--=============================================================================================================================================================================================
--FOR INSERTION IN TABLE 

WHEN NOT MATCHED AND stream_action_for_iot = 'INSERT'
THEN 
INSERT 
(
   alert_type,
    severity ,
    triggere_at ,
    store_id   ,
    store_name  ,
    device_id  ,
    event_id   ,      
    event_type  ,     
    event_timestamp ,
    store_floor      ,
    battery_pct      ,
    signal_rssi      ,
    firmware_version ,
    sensor_name      ,
    sensor_value      ,
    sensor_unit  , 
    updated_at 
)

VALUES (
source.alert_type,
source.severity ,
source.triggered_at,
source.store_id ,
source.store_name,
source.device_id ,
source.event_id  ,
source.event_type ,
source.event_timestamp,
source.store_floor     , 
source.battery_pct      ,
source.signal_rssi      ,
source.firmware_version ,
source.sensor_name   ,   
source.sensor_value,
source.sensor_unit,
current_timestamp()
);
--=============================================================================================================================================================================================

--RESUMING THE TASK AS IT WAS SUSPENDED AT CREATION 
ALTER TASK task_for_iot resume;
select * from device_event_readings_from_IOT;
--=============================================================================================================================================================================================
    

select * from device_event_readings_from_IOT; // Edited JSON IOT data transformed into strructured format with Lateral flatten applied on column "readings" and "alerts"
select * from inventory_snapshots ; // raw ERP_inventory table data processed into structured format and stored into this table

select * from cleaned_pos_transaction;  

select * from ERP_ORDERS; // raw ERP_orders table data processed into structured format and stored into this table



