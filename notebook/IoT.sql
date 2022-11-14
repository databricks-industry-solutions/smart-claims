-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Bring in telematics data

-- COMMAND ----------

create table if not exists smart_claims_new.telematics
using delta location
"dbfs:/user/hive/warehouse/smart_claims.db/telematic";

-- COMMAND ----------

Select * from smart_claims_new.telematics

-- COMMAND ----------


