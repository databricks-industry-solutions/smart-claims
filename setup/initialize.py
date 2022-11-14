# Databricks notebook source
# MAGIC %pip install geopy

# COMMAND ----------

json_ = {
   "database_schema_name":"smart_claims",
   "sql_warehouse_id":"",
   "username_for_alerts":""
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaanup prior run state

# COMMAND ----------

# Drop Schema 
spark.sql("DROP " + json_["database_schema_name"] + " CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

sql_s = ''
CREATE DATABASE IF NOOT EXISTS
'''
spark.sql()
