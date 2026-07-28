--====================================================================================================================================================
-- USED SAME DATABASE AND CREATED SCHEMA SILVER 
--====================================================================================================================================================
USE DATABASE transcargo_db;
CREATE OR REPLACE SCHEMA silver;


--====================================================================================================================================================
-- CRREATED TABLE FOR CLEANED TELEMETRY IOT DATA 
--====================================================================================================================================================
CREATE OR REPLACE TABLE telemetry_iot_reading(
coolant_ok BOOLEAN,
    dtc_codes ARRAY,
    engine_temp_c NUMBER(5,2),
    event_ts TIMESTAMP,
    fuel_pct NUMBER(5,2),
    latitude NUMBER(9,6),
    longitude NUMBER(9,6),
    harsh_braking BOOLEAN,
    rpm INT,
    speed_kmph NUMBER(5,2),
    vehicle_id VARCHAR(50) ,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


--====================================================================================================================================================
-- CREATING AN TASK WHICH WILL START WHEN THE STREAM HAVE DATA AND WILL PERFORM MERGE STATEMENT FROM STREAM 
-- ALSO IT WILL ADD THE IGNORED CORRUPT DATA TO UTIL SCHEMA TABEL 
--====================================================================================================================================================
CREATE OR REPLACE TASK task_for_telemetic 
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('transcargo_db.bronze.stream_for_telemetic_data')
AS
BEGIN  -- BEGING ... END TELLS THAT THE TWO INSERT INTO QUARENTINE AND MERGE STATEMENT ARE IN SINGLE BLOCK 
BEGIN TRANSACTION; -- CASE - IF INSERT FIRST RUNS THEN IT WILL CONSUME STREAM DATA THEN NO DATA FOR MERGE / SO WE USE THIS TO LET STREAM AT SAME STATE UNTIL COMMIT

--====================================================================================================================================================
--- TABLE FROM UTIL SCHEMA WHICH STORED THE BAD RECORDS 

INSERT INTO transcargo_db.quarintene.quarintine_for_telemetic(row_payload , reason , quarentin_at)
SELECT varient_col , 
CASE WHEN varient_col:"engine_temp_c" >= 150 THEN 'Impossible_temperature'
WHEN varient_col:"speed_kmph" >= 200 THEN 'Impossible_Speed'
WHEN varient_col:"fuel_pct" is null THEN 'Invalid_Fuel_Capct' END AS  qurantine_reason , current_timestamp()

FROM transcargo_db.bronze.stream_for_telemetic_data 
WHERE METADATA$ACTION = 'INSERT'
AND 
(varient_col:"engine_temp_c" >= 150  OR
varient_col:"speed_kmph" >= 200 OR 
varient_col:"fuel_pct" IS NULL);

-- DEFINING CONDITION ON WHICH BAD RECORD WILL BE INSERTED INTO QUARENTINE TABLE 
/*
CONDITION
1.engine_temp_c" >= 150 
2.speed_kmph" >= 200 
3."fuel_pct" is null
*/

--====================================================================================================================================================
-- MERGE STATEMENT FOR CDC FOR TELEMETRY DATA 
--====================================================================================================================================================
MERGE INTO transcargo_db.silver.telemetry_iot_reading AS TARGET
USING (
SELECT
        varient_col:vehicle_id::VARCHAR(50) AS vehicle_id,
        varient_col:event_ts::TIMESTAMP AS event_ts,
        varient_col:coolant_ok::BOOLEAN AS coolant_ok,
        varient_col:dtc_codes::ARRAY AS dtc_codes,
        varient_col:engine_temp_c::NUMBER(5,2) AS engine_temp_c,
        varient_col:fuel_pct::NUMBER(5,2) AS fuel_pct,
        varient_col:gps.lat::NUMBER(9,6) AS latitude,
        varient_col:gps.lon::NUMBER(9,6) AS longitude,
        varient_col:harsh_braking::BOOLEAN AS harsh_braking,
        varient_col:rpm::INT AS rpm,
        varient_col:speed_kmph::NUMBER(5,2) AS speed_kmph ,
        METADATA$ACTION as stream_action
       
        FROM transcargo_db.bronze.stream_for_telemetic_data
        WHERE varient_col:"engine_temp_c" <= 150 AND varient_col:"speed_kmph" < 200 AND varient_col:"fuel_pct" IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY  varient_col:vehicle_id , varient_col:event_ts  ORDER BY CASE WHEN stream_action='INSERT' THEN 0 ELSE 1 END ) =1
) 
AS SOURCE 
on target.vehicle_id = source.vehicle_id  and target.event_ts = source.event_ts

--====================================================================================================================================================
-- FOR UPDATION 

WHEN MATCHED AND stream_action = 'INSERT' 
THEN UPDATE SET 

target.dtc_codes        =  source.dtc_codes  ,
target.engine_temp_c    =  source.engine_temp_c  ,
target.event_ts         =  source.event_ts ,
target.fuel_pct         =  source.fuel_pct  ,
target.latitude         =  source.latitude  ,
target.longitude        =  source.longitude  ,
target.harsh_braking    =  source.harsh_braking ,
target.rpm              =  source.rpm ,
target.speed_kmph       =  source.speed_kmph ,
target.vehicle_id       =  source.vehicle_id ,
target.updated_at       =  current_timestamp()

--====================================================================================================================================================
-- FOR DELETION 

WHEN MATCHED AND stream_action = 'DELETE'
THEN DELETE

--====================================================================================================================================================
-- FOR INSERTION 

WHEN NOT MATCHED AND stream_action = 'INSERT'
THEN 
INSERT 
(dtc_codes  ,
engine_temp_c  ,
event_ts ,
fuel_pct  ,
latitude  ,
longitude ,
harsh_braking ,
rpm ,
speed_kmph ,
vehicle_id ,
updated_at )

VALUES(
source.dtc_codes  ,
source.engine_temp_c  ,
source.event_ts  ,
source.fuel_pct  ,
source.latitude  ,
source.longitude  ,
source.harsh_braking  ,
source.rpm ,
source.speed_kmph ,
source.vehicle_id ,
current_timestamp()
);
COMMIT;
END;
-- =======================================================================================================================================================



--====================================================================================================================================================
-- CREATION CLEANED TABLE MAINTAINANCE LOG TABLE 

CREATE OR REPLACE TABLE maintainance_log(
work_order_id VARCHAR(50),
vehicle_id VARCHAR(50) ,
service_date DATE ,
work_type VARCHAR(100) ,
technician VARCHAR(100),
labour_hours  NUMBER(5,2) ,
parts_cost_inr NUMBER(10,2),
downtime_hours NUMBER(5,2),
notes TEXT ,
created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() ,
updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() );


--============================================================================================================================================================
-- CREATING AN TASK WHICH WILL START WHEN THE STREAM HAVE DATA AND WILL PERFORM MERGE STATEMENT FROM STREAM 
-- ALSO IT WILL ADD THE IGNORED CORRUPT DATA TO UTIL SCHEMA TABEL 
--============================================================================================================================================================
CREATE OR REPLACE TASK task_for_maintainance
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('transcargo_db.bronze.stream_for_maintainance')
AS
BEGIN  -- BEGIN .... END 
BEGIN TRANSACTION; -- BEGIN TRANSACTIONS ... COMMIT

--============================================================================================================================================================
-- INSERTIG BAD RECORD TO BE INSERTED INTO QUARENTING TABLE 
INSERT INTO  transcargo_db.quarintene.quarentime_for_maintainance(
work_order_id, vehicle_id, service_date, work_type, 
      technician, labour_hours, parts_cost_inr, downtime_hours, notes , reason , quarentin_at
)
--============================================================================================================================================================
-- CREATED AND CTE TO FIND THE DUPLICATE WORK_ORDER_ID AND INSERT INTO QUARINTING THE OLD ONE 

WITH temp_rank AS (
SELECT
work_order_id, vehicle_id, try_to_date(service_date, 'DD-MM-YYYY') AS service_date, work_type, 
      technician, labour_hours, parts_cost_inr, downtime_hours, notes , METADATA$ACTION AS stream_act ,
     row_number() OVER (PARTITION BY work_order_id  ORDER BY try_to_date(service_date,'DD-MM-YYYY') DESC) AS rankk 
     from transcargo_db.bronze.stream_for_maintainance) -- USING TRY_TO_DATE() FUNCTION TO CONVERT THE VARCHAR STRING TO DATE AND RETURN NULL IF NOT 

SELECT 
work_order_id, vehicle_id, service_date, work_type, 
      technician, labour_hours, parts_cost_inr, downtime_hours, notes , CASE WHEN parts_cost_inr < 0 THEN 'Invalid_cost' 
      WHEN rankk > 1 THEN 'Duplicated_row_old' END AS reason,
      current_timestamp()
      FROM temp_rank WHERE 
      stream_act = 'INSERT' AND (rankk > 1 OR parts_cost_inr < 0 );

/* 
SELECTING DATA WHICH IS BAD RECORD FROM THE CTE WHICH WILL BE INSERTED INTO THE QUARENTING TABLE 
 CONDITION 
1. DUPLICATED WORK_ID , 
2.PARTS_COST < 0  */

--============================================================================================================================================================
-- MERGE STATEMENT WHICH WILL PERFORM CDC UPDATE , INSERT OR DELETE FROM THE STREAM TABLE 

MERGE INTO transcargo_db.silver.maintainance_log AS TARGET USING ( 
SELECT 
       work_order_id, vehicle_id, 
       service_date, 
       work_type, 
       technician, labour_hours, parts_cost_inr, downtime_hours, notes , metadata$action as stream_action

      FROM transcargo_db.bronze.stream_for_maintainance 
      WHERE parts_cost_inr >= 0
      QUALIFY row_number() OVER (PARTITION BY work_order_id  ORDER BY try_to_date(service_date,'DD-MM-YYYY') DESC , case when metadata$action = 'INSERT' then 0 else 1 end) =1
      ) AS SOURCE 

     ON source.work_order_id = target.work_order_id

   --============================================================================================================================================================
-- FOR UPDATION 
WHEN MATCHED AND stream_action = 'INSERT'
THEN UPDATE SET 
   
target.work_order_id        =  source.work_order_id,
target.vehicle_id           =  source.vehicle_id,
target.service_date         =  source.service_date,
target.work_type            =  source.work_type,
target.technician           =  source.technician, 
target.labour_hours         =  source.labour_hours, 
target.parts_cost_inr       =  source.parts_cost_inr, 
target.downtime_hours       =  source.downtime_hours, 
target.notes                =  source.notes,
target.updated_at            =  CURRENT_TIMESTAMP()
      
--============================================================================================================================================================
-- FOR DELETION 

WHEN MATCHED AND stream_action = 'DELETE'
THEN DELETE

--============================================================================================================================================================
-- FOR INSERTION 
WHEN  NOT MATCHED AND stream_action = 'INSERT'
THEN 
INSERT(
work_order_id, vehicle_id, service_date, work_type, 
      technician, labour_hours, parts_cost_inr, downtime_hours, notes ,updated_at
)
VALUES
(
source.work_order_id,
source.vehicle_id,
source.service_date,
source.work_type,
source.technician, 
source.labour_hours, 
source.parts_cost_inr, 
source.downtime_hours, 
source.notes,
CURRENT_TIMESTAMP()

);
COMMIT; -- FOR PERMANENT SAVE 
END;



-- ============================================================================================================================================================
-- CREATING VEHICLE LOG TABLE 

CREATE OR REPLACE TABLE vehicle_log (

    vehicle_id VARCHAR(50),
    vin VARCHAR(50),
    registration_no VARCHAR(50),
    make VARCHAR(50),
    model VARCHAR(50),
    year INT,
    depot VARCHAR(100),
    status VARCHAR(50),
    odometer_km INT,
    fuel_type VARCHAR(50),
    last_updated TIMESTAMP ,
    table_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
);

--============================================================================================================================================================
-- CREATING AN TASK WHICH WILL START WHEN THE STREAM HAVE DATA AND WILL PERFORM MERGE STATEMENT FROM STREAM 
-- ALSO IT WILL ADD THE IGNORED CORRUPT DATA TO UTIL SCHEMA TABEL 
--============================================================================================================================================================
CREATE OR REPLACE TASK task_for_vehicle
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('transcargo_db.bronze.stream_for_vehicle')
AS
BEGIN
BEGIN TRANSACTION;
--============================================================================================================================================================
-- INSERTING BAD RECORD INTO QUARENTINE TABLE 

INSERT INTO transcargo_db.quarintene.quarentine_for_vehicle(
vehicle_id,
vin            ,
registration_no,
make           ,
model          ,
year           ,
depot          ,
status         ,
odometer_km    ,
fuel_type      ,
reason         ,
last_updated    
)

SELECT 
vehicle_id,
vin            ,
registration_no,
make           ,
model          ,
year           ,
depot          ,
status         ,
odometer_km    ,
fuel_type      ,

--============================================================================================================================================================
-- ADDED CASE STATEMENT WHICH WILL MAKE AN COLUMN FOR REASON IN QUARENTINE TABLE 
CASE 
WHEN  registration_no IS NULL THEN 'No_reg_number'
WHEN  odometer_km < 0 THEN 'faulty_odo'
WHEN  year NOT BETWEEN 1000 AND year(current_date()) OR length(year::text) <> 4 THEN 'Malformed_year'
END AS reason  ,last_updated

FROM transcargo_db.bronze.stream_for_vehicle 
WHERE registration_no IS NULL OR odometer_km < 0  OR  year NOT BETWEEN 1000 AND year(current_date()) OR length(year::text) <> 4;

-- DEFINING CONDITION ON WHICH THE DATA SHOULD BE INSERTED 
/*
CONDITION
1.registration_no IS NULL
2.odometer_km < 0 
3. MALFORMED YEAR 
*/

--============================================================================================================================================================
-- MERGE STATEMENT FOR VEHICEL TABLE 

MERGE INTO vehicle_log AS TARGET USING(
SELECT 
vehicle_id,
vin            ,
registration_no,
make           ,
model          ,
year           ,
depot          ,
status         ,
odometer_km    ,
fuel_type      ,
last_updated   ,
METADATA$ACTION AS stream_action
FROM transcargo_db.bronze.stream_for_vehicle 
WHERE 
registration_no IS NOT NULL AND odometer_km >= 0  AND (year  BETWEEN 1000 AND year(current_date())) AND length(year::text) = 4  
QUALIFY row_number() OVER (PARTITION BY vehicle_id   ORDER BY last_updated DESC , case when metadata$action = 'INSERT' then 0 else 1 end)  =1

)
AS SOURCE 
ON source.vehicle_id = target.vehicle_id

--============================================================================================================================================================
-- FOR DELETION 

WHEN MATCHED AND stream_action ='DELETE'
THEN DELETE

--============================================================================================================================================================
-- FOR UPDATION 

WHEN MATCHED AND stream_action ='INSERT'
THEN 
UPDATE SET
target.vehicle_id        = source.vehicle_id     ,
target.vin               = source.vin            ,
target.registration_no   = source.registration_no,
target.make              = source.make           ,
target.model             = source.model          ,
target.year              = source.year           ,
target.depot             = source.depot          ,
target.status            = source.status         ,
target.odometer_km       = source.odometer_km    ,
target.fuel_type         = source.fuel_type      ,
target.last_updated      = source.last_updated   ,
target.table_updated_at   = current_timestamp()

--============================================================================================================================================================
-- FOR INSERTION 

WHEN NOT MATCHED AND stream_action='INSERT'
THEN

INSERT(
vehicle_id,
vin,
registration_no,
make,
model,
year,
depot,
status,
odometer_km,
fuel_type,
last_updated,
table_updated_at
)
VALUES(
source.vehicle_id,
source.vin,
source.registration_no,
source.make,
source.model,
source.year,
source.depot,
source.status,
source.odometer_km,
source.fuel_type,
source.last_updated,
current_timestamp());

COMMIT;
END;
--============================================================================================================================================================



-- ===============================================================================================================================
-- BY DEFAULT TASK ARE SUSPENDED WE NEED TO RESUME THEM 
alter task task_for_vehicle resume;
alter task task_for_maintainance resume;
alter task task_for_telemetic resume;




--============================================================================================================================================================
-- LOGS TABLE 
select * from telemetry_iot_reading;
select * from vehicle_log;
select * from maintainance_log;

