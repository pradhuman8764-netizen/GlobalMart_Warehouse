create schema bronze_db.gold;


select * from bronze_db.silver.device_event_readings_from_IOT; 
select * from bronze_db.silver.cleaned_pos_transaction; 
select * from bronze_db.silver.ERP_ORDERS; 

--Finding for each day transaction detaails ( store infomation , transaction Information , total revenue , total unit sold , unique customers) 
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




// finding running total of last 30 day over store id and  transaction date 

select  store_id  , store_name , transaction_date::date as trans_date , total_amount ,

sum(total_amount) over ( partition by store_id  order by trans_date  rows between 29 preceding and current row) as running_total
 from bronze_db.silver.cleaned_pos_transaction;


// finding average sensor value  , total sensor value and total revenue of each store for  each day and creatig their view 


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



select * from pos_iot_sensor_data;



with cte as (
select store_id , transaction_date , sum(TOTAL_AMOUNT) AS daily_amount 
from bronze_db.silver.cleaned_pos_transaction 
group by store_id , transaction_date 
order by transaction_date ) 


select store_id  , transaction_date , sum(daily_amount)

over (partition by store_id order by transaction_date range between interval '30 day' preceding and current row) as total_revenue_30_days 

from cte;