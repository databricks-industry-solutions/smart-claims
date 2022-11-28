# Databricks notebook source
# MAGIC %run ../../setup/initialize

# COMMAND ----------

json_['database_schema_name']

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists smart_claims_new.telematics
# MAGIC using delta location
# MAGIC "../../resources/data/telematic";

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from smart_claims_new.telematics
