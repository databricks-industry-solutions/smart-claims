# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data analysis (EDA)

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims

# COMMAND ----------

claims_df = spark.table("bronze_claims")
display(claims_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy

# COMMAND ----------

policy_df = spark.table("bronze_policies")
display(policy_df)

# COMMAND ----------

