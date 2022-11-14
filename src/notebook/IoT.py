# Databricks notebook source
# MAGIC %run ../config/initialize

# COMMAND ----------

json_['database_schema_name']

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists smart_claims_new.telematics
# MAGIC using delta location
# MAGIC "dbfs:/user/hive/warehouse/smart_claims.db/telematic";

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from smart_claims_new.telematics

# COMMAND ----------


