# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims.git

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploratory Data analysis (EDA)

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims

# COMMAND ----------

claims_df = spark.table("bronze_claim")
display(claims_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy

# COMMAND ----------

policy_df = spark.table("bronze_policy")
display(policy_df)

# COMMAND ----------


