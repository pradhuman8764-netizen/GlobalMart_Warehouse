
create database if not exists Bronze_db;
create or replace schema bronze;


// stage created for the Pos Transaction and IoT transaction and ERP ORDERS
create or replace stage External_stage
URL = 's3://globalmart-pos-transactions'
CREDENTIALS = (
AWS_KEY_ID = 'AKIASFAQG3U64JR46ELZ'
AWS_SECRET_KEY = '9KwIfo3/0B8T6kTfzvQ6Nr2U18NaDS1hI6O1a7sb'

);

// -----------------------------------------------------------------------------------------//

// initializing the raw file which will have data which can be duplicated 
CREATE OR REPLACE TABLE pos_transaction (
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


// creating SNOWPIPE on the POS_TRANSACTION TABLE

create or replace pipe External_pipe
AUTO_INGEST = TRUE
AS
copy into pos_transaction 
from @External_stage/pos_transaction_folder/
file_format =(
 SKIP_HEADER = 1
 TYPE = 'CSV'
 FIELD_DELIMITER = ','
 DATE_FORMAT = 'YYYY-MM-DD'
 );


// checking the pipe working or not

DESC PIPE EXTERNAL_PIPE;

select * from pos_transaction; 

SELECT SYSTEM$PIPE_STATUS('External_pipe');


// creating the stream for the pos_transaction table 

create or replace stream  stream_for_Pos 
on  table pos_transaction; 

// ========================================== // 
select * from pos_transaction;
select * from stream_for_pos;


//---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- //

// creating IOT TABLE  with varient column 
create or replace table raw_IoT (row_col variant);


// As per requirement created the Pipe for Iot for Auto Ingestion 
 create or replace pipe pipe_for_Iot 
 auto_ingest = TRUE
 as 
copy into IoT 
from '@External_stage/IoT folder/'
file_format=(
type = 'JSON'
strip_outer_array = TRUE
 );

// checking IOT pipe status 
describe pipe pipe_for_iot;
SELECT SYSTEM$PIPE_STATUS('pipe_for_iot');

// -------------------------------------------------------------------------------------------------------- // 

// creating table for ERP Praquet file format data 

CREATE OR REPLACE TABLE raw_erp_data (parquet_raw VARIANT);

-- 2. Copy the file into the table using inline file format
COPY INTO raw_erp_data
FROM @External_stage/Erp_parquet_data/erp_orders.parquet  // we also have to define the file name in the folder because 
                                                        // same folder contain different files // we will make different folder for each file later 
FILE_FORMAT = (TYPE = 'PARQUET');

-- 3. Now query it cleanly without any syntax hurdles
SELECT parquet_raw FROM raw_erp_data;


/// creating second ERP for for the parquet file FORMAT
CREATE OR REPLACE TABLE raw_erp_Inventory_data (parquet_Inventory_raw VARIANT);

COPY INTO raw_erp_Inventory_data 
FROM @External_stage/Erp_parquet_data/erp_inventory.parquet
FILE_FORMAT = (TYPE = 'PARQUET');


select parquet_Inventory_raw from  raw_erp_Inventory_data;



select * from iot;  // JSON format data stored into IOT table having pipe created 
select * from pos_transaction;  // pos_transaction (CSV) having stage and stream , 5000 row inserted 15000 reamining when we put file in s3 it fetch automatically
select * from raw_erp_data;  // raw erp table in bronze schema where raw data is stored having stage connected with s3 bucket 
select * from raw_erp_inventory_data; // raw erp_inventory table in bronze schema where raw data is stored having stage connected with s3 bucket 





