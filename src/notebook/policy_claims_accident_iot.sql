-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Add telematics and accident data to the claims & policy data
-- MAGIC * simulate iot streaming data join with claims

-- COMMAND ----------

-- MAGIC %run ../../setup/initialize

-- COMMAND ----------

create table if not exists silver_claim_policy_telematics as 
(
select p_c.*, t.latitude, t.longitude, t.event_timestamp, t.speed
from 
silver_claim_policy as p_c 
left outer join 
silver_telematics as t
on p_c.chassis_no=t.chassis_no
)

-- COMMAND ----------

create table if not exists silver_claim_policy_accident as 
(
select p_c.*, t.* except(t.claim_no, t.chassis_no)
from 
silver_claim_policy_telematics as p_c 
join 
silver_accident as t
on p_c.claim_no=t.claim_no
)
