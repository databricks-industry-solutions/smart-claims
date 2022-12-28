# Databricks notebook source
# MAGIC %md
# MAGIC # Rule Engine 
# MAGIC * Checks coverage, severity and accident location and speed

# COMMAND ----------

#%run ../../setup/initialize

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists claims_policy_accident;
# MAGIC drop table if exists claims_policy_accident_insights_

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table claims_policy_accident as
# MAGIC select p.*, a.severity, a.content
# MAGIC from silver_claims_policy p
# MAGIC join accident a
# MAGIC on a.driver_id=p.driver_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_policy_accident

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

df_claims_policy_accident = spark.sql("select * from claims_policy_accident").withColumn("SUM_INSURED", col("SUM_INSURED").cast("int"))

# COMMAND ----------

display(df_claims_policy_accident)

# COMMAND ----------

from pyspark.ml.feature import Imputer

inputCol = ["SUM_INSURED"]

imputer = Imputer(
    inputCols= inputCol, 
    outputCols=["{}_imputed".format(c) for c in inputCol]
    ).setStrategy("median")

# Add imputation cols to df
df_claims_policy_accident = imputer.fit(df_claims_policy_accident).transform(df_claims_policy_accident)

# COMMAND ----------

display(df_claims_policy_accident)

# COMMAND ----------

df_claims_policy_accident.registerTempTable("temp_claims_policy_accident")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE temp2_claims_policy_accident AS
# MAGIC (SELECT *,
# MAGIC   CASE WHEN to_date(pol_eff_date, "dd-MM-yyyy") < to_date(claim_datetime) and to_date(pol_expiry_date, "dd-MM-yyyy") < to_date(claim_datetime)
# MAGIC     THEN 'VALID' 
# MAGIC     ELSE 'NOT VALID' 
# MAGIC   END 
# MAGIC   AS valid_date
# MAGIC FROM temp_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check coverage of policy and the accident amount
# MAGIC CREATE TABLE temp3_claims_policy_accident AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  sum_insured_imputed >= claim_amount_total 
# MAGIC     THEN 'calim value in the range of premium' 
# MAGIC     ELSE 'claim value more than premium' 
# MAGIC   END 
# MAGIC   AS valid_amount
# MAGIC FROM temp2_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the location and speed
# MAGIC -- If the speed is so high

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check coverage of policy and the accident amount
# MAGIC CREATE TABLE temp4_claims_policy_accident AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  incident_severity="Total Loss" AND severity > 0.9 THEN  "Severity matches the report"
# MAGIC        WHEN  incident_severity="Major Damage" AND severity > 0.8 THEN  "Severity matches the report"
# MAGIC        WHEN  incident_severity="Minor Damage" AND severity > 0.7 THEN  "Severity matches the report"
# MAGIC        WHEN  incident_severity="Trivial Damage" AND severity > 0.4 THEN  "Severity matches the report"
# MAGIC        ELSE "Severity doesn't match"
# MAGIC   END 
# MAGIC   AS reported_severity_check
# MAGIC FROM temp3_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql select * from claims_policy_accident

# COMMAND ----------

# MAGIC %sql
# MAGIC --Add a new column for release funds.... if all the other generated rules are ok
# MAGIC 
# MAGIC CREATE TABLE claims_policy_accident_insights AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  reported_severity_check="Severity matches the report" and valid_amount="calim value in the range of premium" and valid_date="VALID" then "release funds"
# MAGIC 
# MAGIC        ELSE "claim needs more investigation" 
# MAGIC   END 
# MAGIC   AS release_funds
# MAGIC FROM temp4_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists temp2_claims_policy_accident;
# MAGIC drop table if exists temp3_claims_policy_accident;
# MAGIC drop table if exists temp4_claims_policy_accident;
# MAGIC drop table if exists temp5_claims_policy_accident;

# COMMAND ----------

# MAGIC %sql CREATE TABLE policy_claims_iot_insights AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  speed <= 45 and speed > 0 THEN  "Normal Speed"
# MAGIC        WHEN speed > 45 THEN  "High Speed"
# MAGIC        ELSE "Invalid speed"
# MAGIC   END 
# MAGIC   AS speed_check
# MAGIC FROM policy_claims_iot)

# COMMAND ----------

# MAGIC %sql drop table policy_claims_iot_available_insights

# COMMAND ----------

# MAGIC %sql CREATE TABLE policy_claims_iot_available_insights AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  speed <= 45 and speed > 0 THEN  "Normal Speed"
# MAGIC        WHEN speed > 45 THEN  "High Speed"
# MAGIC        ELSE "Invalid speed"
# MAGIC   END 
# MAGIC   AS speed_check
# MAGIC FROM policy_claims_iot_available)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from policy_claims_iot_available
