# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims.git

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule Engine 
# MAGIC * These are pre-defined static checks that can be applied without requiring a human in the loop, thereby speeding up routine cases
# MAGIC * When the reported data does not comply with auto detected info, flags are raised to involve additional human investigation
# MAGIC   * Eg. Checks on policy coverage, assessed severity, accident location and speed limit violations
# MAGIC * <b>Input Table:</b> silver_claim_policy_accident
# MAGIC * <b>Rules Table:</b> claim_rules
# MAGIC * <b>Output Table:</b> gold_insights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Rules
# MAGIC * Ability to dynamically add/edit rules to meet bsiness requirements around claim processing 
# MAGIC * Rules are persisted in claim_rules and applied on new data in a generic pattern as prescribed in the rule definition
# MAGIC * Rule definition inludes a
# MAGIC   * Unique Rule name/id
# MAGIC   * Definition of acceptable and not aceptable data - written as code that can be directly applied
# MAGIC   * Severity (HIGH, MEDIUM, LOW)
# MAGIC   * Is_Activ (True/False)
# MAGIC * Some common checks include
# MAGIC   * Claim date should be within coverage period
# MAGIC   * Reported Severity should match ML predicted severity
# MAGIC   * Accident Location as reported by telematics data should match the location as reported in claim
# MAGIC   * Speed limit as reported by telematics should be within speed limits of that region if there is a dispute on who was on the offense 

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists claims_rules;
# MAGIC CREATE TABLE IF NOT EXISTS claims_rules (
# MAGIC   rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   rule STRING, 
# MAGIC   check_name STRING,
# MAGIC   check_code STRING,
# MAGIC   check_severity STRING,
# MAGIC   is_active Boolean
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Configure Rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invalid Policy Date

# COMMAND ----------

invalid_policy_date = '''
CASE WHEN to_date(pol_eff_date, "dd-MM-yyyy") < to_date(claim_datetime) and to_date(pol_expiry_date, "dd-MM-yyyy") < to_date(claim_datetime) THEN "VALID" 
ELSE "NOT VALID"  
END
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity, is_active) values('invalid policy date', 'valid_date', '" + invalid_policy_date + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exceeds Policy Amount

# COMMAND ----------

exceeds_policy_amount = '''
CASE WHEN  sum_insured >= claim_amount_total 
    THEN "calim value in the range of premium"
    ELSE "claim value more than premium"
END 
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('exceeds policy amount', 'valid_amount','" + exceeds_policy_amount + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Severity Mismatch

# COMMAND ----------

severity_mismatch = '''
CASE WHEN  incident_severity="Total Loss" AND severity > 0.9 THEN  "Severity matches the report"
       WHEN  incident_severity="Major Damage" AND severity > 0.8 THEN  "Severity matches the report"
       WHEN  incident_severity="Minor Damage" AND severity > 0.7 THEN  "Severity matches the report"
       WHEN  incident_severity="Trivial Damage" AND severity > 0.4 THEN  "Severity matches the report"
       ELSE "Severity does not match"
END 
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity, is_active) values('severity mismatch', 'reported_severity_check', '" + severity_mismatch + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exceeds Speed

# COMMAND ----------

exceeds_speed = '''
CASE WHEN  telematics_speed <= 45 and telematics_speed > 0 THEN  "Normal Speed"
       WHEN telematics_speed > 45 THEN  "High Speed"
       ELSE "Invalid speed"
END
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('exceeds speed', 'speed_check', '" + exceeds_speed + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

release_funds = '''
CASE WHEN  reported_severity_check="Severity matches the report" and valid_amount="calim value in the range of premium" and valid_date="VALID" then "release funds"
       ELSE "claim needs more investigation" 
END
'''
s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('release funds', 'release_funds', '" + release_funds + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC # Dynamic Application of Rules 

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.sql("SELECT * FROM silver_claim_policy_accident")

rules = spark.sql('SELECT * FROM claims_rules where is_active=True order by rule_id').collect()
for rule in rules:
  print(rule.rule, rule.check_code)
  df=df.withColumn(rule.check_name, expr(rule.check_code))
  
display(df)

# COMMAND ----------

#overwrite table with new insights
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("gold_insights")

# COMMAND ----------

#profile insights generated
df = spark.sql("SELECT valid_date, valid_amount,reported_severity_check, release_funds FROM gold_insights")
display(df)
