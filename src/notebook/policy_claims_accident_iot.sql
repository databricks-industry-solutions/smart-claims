-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Add telematics and accident data to the claims & policy data
-- MAGIC * simulate iot streaming data join with claims

-- COMMAND ----------

-- MAGIC %run ../../setup/initialize

-- COMMAND ----------

-- create table if not exists silver_claim_policy_accident as 
-- (
-- select p_c.*, t.* except(t.claim_no, t.chassis_no)
-- from 
-- silver_claim_policy_location as p_c 
-- join 
-- silver_accident as t
-- on p_c.claim_no=t.claim_no
-- )

-- COMMAND ----------

create table if not exists silver_claim_policy_telematics_avg as 
(
select p_c.*, t.telematics_latitude, t.telematics_longitude, t.telematics_speed
from 
silver_claim_policy_location as p_c 
join
(select chassis_no, avg(speed) as telematics_speed,
avg(latitude) as telematics_latitude,
avg(longitude) as telematics_longitude
from
silver_telematics
group by chassis_no) t
on p_c.chassis_no=t.chassis_no
)

-- COMMAND ----------

create table if not exists silver_claim_policy_accident as 
(
select p_c.*, t.* except(t.claim_no, t.chassis_no)
from 
silver_claim_policy_telematics_avg as p_c 
join 
silver_accident as t
on p_c.claim_no=t.claim_no
)

-- COMMAND ----------

create table if not exists silver_claim_policy_telematics as 
(
select p_c.*, t.latitude as telematics_latitude, t.longitude as telematics_longitude, t.event_timestamp as telematics_timestamp, t.speed as telematics_speed
from 
silver_telematics as t
join 
silver_claim_policy_location as p_c 
on p_c.chassis_no=t.chassis_no
)
