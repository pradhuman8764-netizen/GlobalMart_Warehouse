use database BRONZE_DB;

create or replace schema silver;

// creating an new table cleaned_pos_transaction from bronze schema with TASK implemented 

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
);

// -- creating TASK of merge statement which will work whenever the POs_stream will get new data 

create or replace task start_merge_when_stream_updates
warehouse = 'COMPUTE_WH'
schedule = '1 minute'
when system$stream_has_data('bronze_db.bronze.stream_for_pos')
as
Merge into bronze_db.silver.cleaned_pos_transaction as target 
using (
select 

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
from bronze_db.bronze.stream_for_pos
qualify row_number() over (partition by TRANSACTION_ID order by case when metadata$action = 'INSERT' then 0 else 1 end) = 1

) as source
on target.TRANSACTION_ID = source.TRANSACTION_ID 

when matched and stream_action = 'INSERT' then 

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
        target.LOYALTY_POINTS   = source.LOYALTY_POINTS


when matched and stream_action = 'DELETE' then 
delete 


when not matched and stream_action = 'INSERT' then 

INSERT (
        TRANSACTION_ID, STORE_ID, STORE_NAME, STORE_CITY, STORE_REGION, 
        CASHIER_ID, CUSTOMER_ID, TRANSACTION_DATE, TRANSACTION_TIME, 
        PRODUCT_SKU, PRODUCT_NAME, CATEGORY, SUBCATEGORY, QUANTITY, 
        UNIT_PRICE, DISCOUNT_PCT, TOTAL_AMOUNT, PAYMENT_METHOD, LOYALTY_POINTS
    ) 
    VALUES (
        source.TRANSACTION_ID, source.STORE_ID, source.STORE_NAME, source.STORE_CITY, source.STORE_REGION, 
        source.CASHIER_ID, source.CUSTOMER_ID, source.TRANSACTION_DATE, source.TRANSACTION_TIME, 
        source.PRODUCT_SKU, source.PRODUCT_NAME, source.CATEGORY, source.SUBCATEGORY, source.QUANTITY, 
        source.UNIT_PRICE, source.DISCOUNT_PCT, source.TOTAL_AMOUNT, source.PAYMENT_METHOD, source.LOYALTY_POINTS
    );

// for first time when we create or replace an task we have to resume the task 
alter task start_merge_when_stream_updates resume ;
select * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) order by scheduled_time desc;
select * from cleaned_pos_transaction ;








//--------------------------------------------------------------------------------------------------------------------------------------//


// creating cleaned parquet data of ERP ORDERS from bronze schemaa to silver schema 

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



//===================================================================================================================================
-- CREATING CDC FOR ERP ORDERS TABLE 
--===========================================================================================================================----==
CREATE TASK task_for_ERP
warehouse = 'COMPUTE_WH'
schedule = '1 minute'
when system$stream_has_data('bronze_db.bronze.stream_raw_erp_data')
as
Merge INTO bronze_db.silver.erp_orders as  target using 
(
select 
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
qualify row_number() over (partition by order_id order by case when metadata$action  ='INSERT' then 0 else 1) = 1)
as source
on target.order_id = source.order_id


when matched and source.ERP_stream_action  ='DELETE' then 
delete

when matched and source.ERP_stream_action  ='INSERT' then 
update set 

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

when not matched and source.ERP_stream_action  ='INSERT' then 
insert 
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
values (
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



ALTER TASK task_for_ERP RESUME;
select * from erp_orders;



// creating table for Parquet file format table storage in structured format

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


insert into inventory_snapshots (
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

select 
    parquet_Inventory_raw:snapshot_date::date,
    parquet_Inventory_raw:store_id::VARCHAR,  
    parquet_Inventory_raw:warehouse_id::VARCHAR,
    parquet_Inventory_raw:category::VARCHAR,            
    parquet_Inventory_raw:product_sku::VARCHAR,         
    parquet_Inventory_raw:quantity_on_hand::number,   
    parquet_Inventory_raw:reorder_level::number,       
    parquet_Inventory_raw:max_stock_level::number,     
    parquet_Inventory_raw:last_received_date::date,
    from bronze_db.bronze.raw_erp_Inventory_data;








// creating the table for IOT structured data storeage 
CREATE OR REPLACE TABLE bronze_db.silver.device_event_readings_from_IOT (

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
    sensor_unit         VARCHAR(20)
);


 insert into bronze_db.silver.device_event_readings_from_IOT(
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
    sensor_unit )

    select 
    // accessing the nested arry of "alerts" column using lateral flatten 
    a.value:alert_type::varchar ,
    a.value:severity::varchar ,
    a.value:triggered_at::timestamp ,

    // normal columns accesse directly by varient column "row_col"
    row_col:store_id::VARCHAR  ,
    row_col:store_name::VARCHAR ,      
    row_col:device_id::VARCHAR   ,   
    row_col:event_id::VARCHAR     ,  
    row_col:event_type::VARCHAR    , 
    row_col:timestamp::TIMESTAMP ,

    // nested array columns of metadata are accessed using dot(.) no need for lateral flatten 
    row_col:metadata.store_floor::INT ,      
    row_col:metadata.battery_pct::INT ,     
    row_col:metadata.signal_rssi::INT ,    
    row_col:metadata.firmware::VARCHAR ,

    // The reading column contain multiple items so this needs to be LATERAL FLATTENED 

    r.value:sensor::varchar,
    r.value:value::number(10,2),
    r.value:unit::varchar
    
    from bronze_db.bronze.iot,
    lateral flatten (input => row_col:readings) as r,
    lateral flatten(input => row_col:alerts , outer => TRUE)  as a; // used outer to describe snowfalke to use outer join instead on inner as alerts may have nulls value 

select * from device_event_readings_from_IOT; // Edited JSON IOT data transformed into strructured format with Lateral flatten applied on column "readings" and "alerts"
select * from inventory_snapshots ; // raw ERP_inventory table data processed into structured format and stored into this table

select * from cleaned_pos_transaction;  

select * from ERP_ORDERS; // raw ERP_orders table data processed into structured format and stored into this table



