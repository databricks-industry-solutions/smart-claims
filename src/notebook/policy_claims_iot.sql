-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Add iot data to the claims & policy data
-- MAGIC * simulate iot streaming data join with claims

-- COMMAND ----------

-- MAGIC %run ../../setup/initialize

-- COMMAND ----------

create table if not exists policy_claims_iot as 
(
select p_c.*, t.latitude, t.longitude, t.event_timestamp, t.speed
from 
silver_claims_policy as p_c 
left outer join 
telematics as t
on p_c.chassis_no=t.chassis_no
)

-- COMMAND ----------

create table if not exists policy_claims_iot_available as 
(
select p_c.*, t.latitude, t.longitude, t.event_timestamp, t.speed
from 
silver_claims_policy as p_c 
join 
telematics as t
on p_c.chassis_no=t.chassis_no
)
