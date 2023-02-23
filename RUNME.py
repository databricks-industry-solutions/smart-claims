# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉
# MAGIC 
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC 
# MAGIC 2. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC 
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC 
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC 
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC 
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-specific. Keep in mind that rerunning this script again after modification resets them for other users too.
# MAGIC 
# MAGIC 2. If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install mlflow git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

pipeline_json = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 5,
                      "mode": "ENHANCED"
                  }
              }
          ],
          "development": True,
          "continuous": False,
          "edition": "advanced",
          "libraries": [
              {
                  "notebook": {
                      "path": f"01_policy_claims_accident"
                  }
              }
          ],
          "name": "SOLACC_smart_claims",
          "storage": f"/databricks_solacc/smart_claims/dlt",
          "target": f"smart_claims",
          "allow_duplicate_names": "true"
      }

# COMMAND ----------

# DBTITLE 1,We save some deployment data  for accelerators in DBFS
solacc_config_database = "databricks_solacc"
dlt_config_table = f"{solacc_config_database}.dlt"
dbsql_config_table = f"{solacc_config_database}.dbsql"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {solacc_config_database} LOCATION '/databricks_solacc/'")
spark.sql(f"CREATE TABLE IF NOT EXISTS {dlt_config_table} (path STRING, pipeline_id STRING, solacc STRING)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {dbsql_config_table} (path STRING, id STRING, solacc STRING)")

# COMMAND ----------

pipeline_id = NotebookSolutionCompanion().deploy_pipeline(pipeline_json, dlt_config_table, spark)

# COMMAND ----------

job_json = {
        "timeout_seconds": 28800,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "FSI"
        },
        "tasks": [
            {
                "job_cluster_key": "smart_claims_cluster",
                "notebook_task": {
                    "notebook_path": f"00_README"
                },
                "task_key": "00_README"
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"setup/setup"
                },
                "task_key": "setup",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "00_README"
                    }
                ]
            },
            {
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
                "task_key": "01_policy_claims_accident",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "setup"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"04a_policy_location"
                },
                "task_key": "04a_policy_location",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "01_policy_claims_accident"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"02_EDA"
                },
                "task_key": "02_EDA",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "01_policy_claims_accident"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"03_iot"
                },
                "task_key": "03_iot",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "02_EDA"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"05_severity_prediction"
                },
                "task_key": "05_severity_prediction",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "02_EDA"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"04b_policy_claims_accident_iot"
                },
                "task_key": "04b_policy_claims_accident_iot",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "04a_policy_location"
                    },
                    {
                        "task_key": "03_iot"
                    },
                    {
                        "task_key": "05_severity_prediction"
                    }
                ]
            },
            {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"06_rule"
                },
                "task_key": "06_rule",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "04b_policy_claims_accident_iot"
                    }
                ]
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "smart_claims_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"},
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
nsc = NotebookSolutionCompanion()
nsc.deploy_compute(job_json, run_job=run_job)
nsc.deploy_dbsql("./Smart Claims Report.dbdash", dbsql_config_table, spark)

# COMMAND ----------


