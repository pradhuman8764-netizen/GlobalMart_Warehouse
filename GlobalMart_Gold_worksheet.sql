use database BRONZE_DB;

create or replace schema bronze_db.gold;

--=============================================================================================================================================================================================
--Finding for each day transaction details ( store infomation , transaction Information , total revenue , total unit sold , unique customers) 
--=============================================================================================================================================================================================
select 
transaction_date , 
store_region , 
store_city , 
store_id , 
store_name , 
category , 
sum(quantity) as unit_sold ,
sum(total_amount)  as Total_revenue ,
COUNT(DISTINCT customer_id) AS unique_customers
from bronze_db.silver.cleaned_pos_transaction
group by transaction_date ,  store_region , store_city , store_name , store_id , category 
order by transaction_date;


--=============================================================================================================================================================================================
-- finding running total of last 30 day over store id and  transaction date 
--=============================================================================================================================================================================================

select  store_id  , store_name , transaction_date::date as trans_date , greatest(total_amount,0) as total_amount,

sum(total_amount) over ( partition by store_id  order by trans_date  range between interval '29 day' preceding and current row) as running_total
 from bronze_db.silver.cleaned_pos_transaction;

--=============================================================================================================================================================================================
-- finding average sensor value  , total sensor value and total revenue of each store for  each day and creatig their view 
--=============================================================================================================================================================================================

create or replace view bronze_db.gold.pos_iot_sensor_data 
as 
select iot.store_id , iot.store_name , iot.sensor_name , pos.transaction_date ,
sum(pos.total_amount) as total_revenue ,
sum(iot.sensor_value) as sensor_value_total  ,
avg(sensor_value) as average_sensor_value
from bronze_db.silver.device_event_readings_from_IOT as iot 
join bronze_db.silver.cleaned_pos_transaction as pos 

on pos.store_id = iot.store_id

group by iot.store_id , iot.store_name , pos.transaction_date ,iot.sensor_name
order by pos.transaction_date;
--=============================================================================================================================================================================================


--=============================================================================================================================================================================================
-- checking view
select * from pos_iot_sensor_data;

--=============================================================================================================================================================================================
-- finding 30 days rolling revenue based on each store id
--=============================================================================================================================================================================================
with pos_revenue_summary as (
select store_id , transaction_date::date as transaction_date , sum(greatest(TOTAL_AMOUNT,0)) AS daily_amount 
from bronze_db.silver.cleaned_pos_transaction 
group by store_id , transaction_date 
order by transaction_date ) 


select store_id  , transaction_date , sum(daily_amount)

over (partition by store_id order by transaction_date range between interval '29 day' preceding and current row) as total_revenue_30_days 

from pos_revenue_summary;
--=============================================================================================================================================================================================
-- REVENUE VS COST GAP JONED POS_TRANSACTION TABLE WITH ERP_ORDERS FOR FINDING GROSS MARGIN

CREATE OR REPLACE VIEW bronze_db.gold.pos_erp_GrossMargin_view 
as
with pos_summary
as
(select store_id ,product_sku ,transaction_date , sum(total_amount)  as total_revenue 
from bronze_db.silver.cleaned_pos_transaction group by   store_id ,product_sku ,transaction_date) ,

erp_summary 
as(
SELECT store_id , product_sku , order_date::date as order_date , sum(total_cost) as procurement_cost 
from bronze_db.silver.ERP_ORDERS group by store_id , product_sku , order_date )

select 
p.store_id , p.product_sku , p.transaction_date , p.total_revenue , coalesce(e.procurement_cost ,0) as total_procurement_cost ,

p.total_revenue - coalesce(e.procurement_cost ,0) as gross_margin 

from pos_summary as p 
left join  erp_summary  as e 
on 
p.store_id = e.store_id and p.product_sku = e.product_sku and p.transaction_date = e.order_date
order by total_procurement_cost desc;

select * from gold.pos_erp_GrossMargin_view; 
--=============================================================================================================================================================================================

-- FINDING THE IOT READING (TEMPERATURE BREACH ) THAT AFFECT THE SALES 
create or replace view pos_iot_sensor_affect_revenue_view
as
with IOT_event_summary as 
(
select store_id , event_timestamp::date as event_time , alert_type ,event_type ,
max(sensor_value) as max_temp , 
count(*) as count_per_day 
from bronze_db.silver.device_event_readings_from_IOT
where alert_type = 'TEMP_BREACH' or severity = 'HIGH'
group by store_id , event_time , alert_type ,event_type) , 

 pos_revenue_summary as
(
select store_id , transaction_date , sum(quantity) as qty_sold , sum(total_amount) as total_revenue
from 
bronze_db.silver.cleaned_pos_transaction 
group by store_id , transaction_date )


select p.store_id , p.transaction_date , e.alert_type , e.event_type ,
coalesce(e.max_temp ,0) as max_temp_reached, 
coalesce(e.count_per_day ,0) as total_count_per_day ,
p.qty_sold , p.total_revenue 

from  pos_revenue_summary  as p
left join IOT_event_summary  as e 

on e.store_id = p.store_id and e.event_time = p.transaction_date
order by p.transaction_date;



--=============================================================================================================================================================================================
--TABLES 
--=============================================================================================================================================================================================
select * from bronze_db.silver.device_event_readings_from_IOT; 
select * from bronze_db.silver.cleaned_pos_transaction; 
select * from bronze_db.silver.ERP_ORDERS; 


--=============================================================================================================================================================================================
--VIEWS 
--=============================================================================================================================================================================================
select * from gold.pos_erp_GrossMargin_view; -- VIEW CREATED TO SHOW POS_ERP SUMMARY IN TERMS OF GROSS MARGIN 
select * from pos_iot_sensor_data;  -- POS - IOT SENSOR DATA HAVING  DATA LIKE AVERAGE SENSOR VALUE , TOTAL REVENUE , TOTAL SENSOR VALUE 
select * from pos_iot_sensor_affect_revenue_view ---- FINDING THE IOT READING (TEMPERATURE BREACH ) THAT AFFECT THE SALES 





