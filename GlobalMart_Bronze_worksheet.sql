
CREATE DATABASE IF NOT EXISTS Bronze_db;
CREATE OR REPLACE SCHEMA bronze;

--=============================================================================================================================================================================================
-- stage created for the Pos Transaction and IoT transaction and ERP ORDERS
--=============================================================================================================================================================================================
CREATE OR REPLACE STAGE External_stage
URL = 's3://globalmart-pos-transactions'
CREDENTIALS = (
AWS_KEY_ID = 'AKIASFAQG3U64JR46ELZ'
AWS_SECRET_KEY = '9KwIfo3/0B8T6kTfzvQ6Nr2U18NaDS1hI6O1a7sb'

);

--=============================================================================================================================================================================================
-- initializing the raw file which will have data which can be duplicated 
--=============================================================================================================================================================================================
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

--=============================================================================================================================================================================================
-- creating SNOWPIPE on the POS_TRANSACTION TABLE
--=============================================================================================================================================================================================
CREATE OR REPLACE PIPE External_pipe
AUTO_INGEST = TRUE
AS
COPY INTO pos_transaction 
FROM @External_stage/pos_transaction_folder/
file_format =(
 SKIP_HEADER = 1
 TYPE = 'CSV'
 FIELD_DELIMITER = ','
 );

--=============================================================================================================================================================================================
-- checking the pipe working or not
--=============================================================================================================================================================================================
DESC PIPE EXTERNAL_PIPE;
SELECT SYSTEM$PIPE_STATUS('External_pipe');

--=============================================================================================================================================================================================
-- creating the stream for the pos_transaction table 
--=============================================================================================================================================================================================
CREATE OR REPLACE STREAM stream_for_Pos 
ON TABLE pos_transaction; 

select * from pos_transaction;
select * from stream_for_pos;



--=============================================================================================================================================================================================
--ccreating IOT TABLE  with varient column 
--=============================================================================================================================================================================================
create or replace table raw_IoT (row_col variant);

--=============================================================================================================================================================================================
-- creating stream on IOT for running cdc in silver schema for cleaned iot table 
create or replace stream stream_for_iot on table  raw_Iot;


--=============================================================================================================================================================================================
-- As per requirement created the Pipe for Iot for Auto Ingestion 
--=============================================================================================================================================================================================
 CREATE OR REPLACE PIPE pipe_for_Iot 
 AUTO_INGEST = TRUE
 AS
 COPY INTO  raw_IoT
 FROM '@External_stage/IoT folder/'
 FILE_FORMAT=(
 TYPE = 'JSON'
 STRIP_OUTER_ARRAY = TRUE     
 );

--=============================================================================================================================================================================================
-- checking IOT pipe status 
DESCRIBE PIPE pipe_for_iot;
SELECT SYSTEM$PIPE_STATUS('pipe_for_iot');
--=============================================================================================================================================================================================

-- checking stream and table content 
select * from stream_for_iot;
select * from raw_iot;
--=============================================================================================================================================================================================




--=============================================================================================================================================================================================
-- creating table for ERP Praquet file format data 

CREATE OR REPLACE TABLE raw_erp_data (parquet_raw VARIANT);
--=============================================================================================================================================================================================


-- Creating stream for ERP ORDERS for tracking updates on raw table and to use CDC on Silver schema ERP_ORDERS table 
create or replace stream stream_raw_erp_data on table  raw_erp_data;
--=============================================================================================================================================================================================

--=============================================================================================================================================================================================
-- CREATING AUTO INGEST PIPE FOR AUTO DATA INSERTION 
create or replace pipe ERP_ORDERS_PIPE
auto_ingest = TRUE
as
COPY INTO raw_erp_data
FROM @External_stage/Erp_parquet_data/ 
                                              -- same folder contain different files // we will make different folder for each file later 
FILE_FORMAT = (TYPE = 'PARQUET');
--=============================================================================================================================================================================================

-- CHECKING THE PIPE STATUS AND STREAM DATA
SELECT SYSTEM$PIPE_STATUS('ERP_ORDERS_PIPE');
select * from stream_raw_erp_data ;
SELECT parquet_raw FROM raw_erp_data;
--=============================================================================================================================================================================================





--=============================================================================================================================================================================================
--creating second ERP for for the parquet file FORMAT
--=============================================================================================================================================================================================
CREATE OR REPLACE TABLE raw_erp_Inventory_data (parquet_Inventory_raw VARIANT);

COPY INTO raw_erp_Inventory_data 
FROM @External_stage/Erp_parquet_data/erp_inventory.parquet -- we also have to define the file name in the folder because 
FILE_FORMAT = (TYPE = 'PARQUET');

select parquet_Inventory_raw from  raw_erp_Inventory_data;
--=============================================================================================================================================================================================




--=============================================================================================================================================================================================
select * from raw_iot; // JSON format data stored into IOT table having pipe created 
select * from pos_transaction;  // pos_transaction (CSV) having stage and stream , 5000 row inserted 15000 reamining when we put file in s3 it fetch automatically
select * from raw_erp_data;  // raw erp table in bronze schema where raw data is stored having stage connected with s3 bucket 
select * from raw_erp_inventory_data; // raw erp_inventory table in bronze schema where raw data is stored having stage connected with s3 bucket 





