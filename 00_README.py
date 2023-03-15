# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims.git

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">
# MAGIC 
# MAGIC [![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC 
# MAGIC * <b>Domain </b>: Insurance 
# MAGIC * <b>Challenge </b>: 
# MAGIC   * Insurance companies have to constantly innovate to beat competition
# MAGIC   * Customer Retention & Loyalty can be a challenge as people are always shopping for more competitive rates leading to churn
# MAGIC   * Fraudulent transactions can erode profit margins 
# MAGIC   * Processing Claims can be very time consuming 
# MAGIC   * <i>How to improve the Claims Management process for faster claims settlement, lower claims processing costs and quicker identification of possible fraud.</i>
# MAGIC * <b><span style="color:#f03c15"> Solution: Smart Claims! </span></b>
# MAGIC   * A Databricks Solution Accelerator that uses the Lakehouse paradigm to automate certain components of this process that aids human investigation 
# MAGIC   * Please refer to the getting started doc in the docs
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/InsuranceReferenceArchitecture.png" width="70%" height="70%">
# MAGIC Every claim is different, the following steps capturess a typical workflow <br>
# MAGIC 1. The <b>Insured</b> contacts the broker who is the primary contact w.r.t. policy <br>
# MAGIC 2. The <b>Broker</b> examines the data to ensure that relevant details of the claim situation have been captured <br>
# MAGIC The <b>Adjuster</b> takes over the investigation and may collaborate with internal/external experts to determine the amount of loss or damages covered by the insurance policy.<br>
# MAGIC  3. The <b>Claims Investigator</b> will do due diligence on the paperwork<br>
# MAGIC  4. The <b>Compliance Officer</b> will check eligibility of coverage and ensure no foul play is involved<br>
# MAGIC  5. The <b>Appraiser</b> will conduct a damage evaluation to determine the severity of the claim<br>
# MAGIC 6. The <b>Adjuster</b> will ensure payment is approved and released and communicates back to the <b>Insured</b><br>
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC # Details
# MAGIC * <b>What</b>
# MAGIC   * How to manage operational costs so as to offer lower premiums, be competitive & yet remain profitable?
# MAGIC   * How can customer loyalty & retention be improved to reduce churn?
# MAGIC   * How to improve process efficiencies to reduce the response time to customers on the status/decision on their claims?
# MAGIC   * How can funds and resources be released in a timely manner to deserving parties?
# MAGIC   * How can suspicious activities be flagged for further investigation?
# MAGIC * <b>Why</b>
# MAGIC   * Faster approvals leads to Better Customer NPS scores and Lower Operating expenses
# MAGIC   * Detecting & Preventing fraudulent scenarios leaads to Lower Leakage ratio
# MAGIC   * Improving customer satisfaction leads to Lower Loss ratio
# MAGIC * <b>How: Claims Automation</b>
# MAGIC   * Automting certain aspects of the claims processing pipeline to reduce dependence of human personnel esspecially in mundane predictable tasks
# MAGIC   * Augmenting additional info/insights to existing claims data to aid/expedite human investigation, eg. Recommend Next Best Action
# MAGIC   * Providing greater explainability of the sitution/case for better decision making in the human workflow
# MAGIC   * Serving as a sounding board to avoid human error/bias as well as providing an audit trail for personel in Claims Roles 
# MAGIC 
# MAGIC # Emerging trends in Insurance 
# MAGIC * According to EY: 'It is given that the future of insurance will be <b>data-driven</b> and <b>analytics-enabled</b>. But tomorrow’s top-performing insurers will also excel at making <b>human connections</b> and applying the personal touch at the right time.'
# MAGIC * Deloitte in its '2023 Insurance outlook' states 'Technology infrastructure has improved, but focus needs to shift to <b>value realization</b>, and broaden  historical focus from risk and cost reduction to prioritize greater levels of experimentation and risk-taking that drives ongoing innovation, competitive differentiation, and profitable growth.' with increased focus on ESG as value differentiator & DEI to broaden offerings.
# MAGIC * Nationwide CTO, Jim Fowler in a podcast on 'Future of Insurance' summarized it aroud <b>Innovation</b>. 
# MAGIC * Each individual need is different. Hence personalization and delivering the relevant value to the concerned individual is an importaant ingredient to inovate. Personalization is not about bothering the customer with multiple touchpoints but wowing them with relevant insights that suit their need in a timely manner. 
# MAGIC * Apart from courage and conviction, Innovation requires patience because no worthy change is delivered overnight. Hence the need to be on a platform that enables fast paced innovation and an architecture that is open, extensible and pluggable so that technology is never a constraint nor a hindrance to execution of novel ideas. 
# MAGIC 
# MAGIC # Insurance Terminology
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/Insurance Technology.png" width="70%" height="70%">
# MAGIC 
# MAGIC # Insurance Reference Architecture
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/InsuranceReferenceArchitecture.png" width="70%" height="70%">
# MAGIC 
# MAGIC # Smart Claims Reference Architecture & Data Flow
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/smart_claims_process.png" width="70%" height="70%">
# MAGIC 
# MAGIC Claims flow typically involve some orchestration between an <b>operational</b> system such as Guidewire and an <b>analytic</b> system such as Databricks as shown in the diagram above. End users often use a smart app to file claims, look at the status of their case. Either via an app or an IoT dvice embedded in their vehicle, telematic data iss constantly streaming into one of these two systems which provides a lot of information regarding their driving patterns. Sometimes in the event of other credit scores, this data is used to assign a risk score for the driver which has a direct consequence on their premiums. In some ways, it can be argued that this type of <b>insurance risk score </b> is a better indicator of a person's safety track rather than a generic financial credit score which is determined primarily by their financial track record. 
# MAGIC 
# MAGIC 1. Policy data ingestion 
# MAGIC 2. Claims and telematics data ingestion 
# MAGIC 3. Ingest all data sources to the cloud storage
# MAGIC 4. Incrementally Load Raw data to Delta Bronze table
# MAGIC 5. Transform and Manipulate data
# MAGIC 6. Model scoring (and model training in the training pipeline)
# MAGIC 7. Load predictions to a gold table and perform aggregations
# MAGIC 8. Dashboard visualization
# MAGIC 9. Feed the results back to the operational system
# MAGIC 10. Claims routing based on decision
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC # Datasets
# MAGIC * All the data is synthetically generated data including the images and geo locations
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/datasets.png" width="60%" height="60%">
# MAGIC 
# MAGIC * Typical datasets include the above, some of these are slow moving while others are fast moving. 
# MAGIC * Some are strutured/semi-structured while others are un-structed. 
# MAGIC * Some of these are additive and are appended while others are inccremental updates and are treated as slowly changing dimensions.
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC # Domain Model
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/domain_model.png">
# MAGIC 
# MAGIC * There are several industry prescribed data domain models Eg. OMG (https://www.omg.org/) 
# MAGIC * The above diagram is a simplified domain model to capture some of the relevant data points for the use case.
# MAGIC * For more details, efer to the P&C etity definitions, terminology & logicaal model https://www.omg.org/spec/PC/1.0/PDF
# MAGIC 
# MAGIC # Insight Generation using ML & Rule Engine 
# MAGIC * A pre-trained <b>ML Model</b> is used to score the image attached in the claims record to assess the severity of damage.
# MAGIC * A <b>Rule Engine </b> is a flexible way to define known operational static checks that can be applied without requiring a human in the loop, thereby speeding up 'routine cases'. When the reported data does not comply with auto detected info, flags are raised to involve additional human investigation
# MAGIC * This additional info helps a claims investigator by narrowing down the number of cases that need intervention a well as by narrowing down the specific areas that need additional follow up and scrutiny
# MAGIC * Some common checks include
# MAGIC   * Claim date should be within coverage period
# MAGIC   * Reported Severity should match ML predicted severity
# MAGIC   * Accident Location as reported by telematics data should match the location as reported in claim
# MAGIC   * Speed limit as reported by telematics should be within speed limits of that region if there is a dispute on who was on the offense 
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/rule_engine.png" width="70%" height="70%">
# MAGIC 
# MAGIC # Workflow
# MAGIC * Different data sources flow in at their own pace, some independent, some with dependencies
# MAGIC * We will use Databricks multi-task Workflows to put the process in auto-pilot mode to demonstrate the Lakehouse paradigm.
# MAGIC * Some nodes are Delta Live Table nodes which employ the medallion architecture to refine and curate data, while others are notebooks which use a Model to score the data while still others are SQL workflows to refresh a dashboard with newly generated insights.
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/workflow.png" width="60%" height="60%">
# MAGIC 1. Setup involve all the work needed to setup the <br>
# MAGIC 2. Ingest claims, Policy & accident data ussing a DLT Pipeline <br>
# MAGIC 3. Ingest Telematic data <br>
# MAGIC 4. Augment claims data with latitude/longitude using zipcode <br>
# MAGIC 5. Apply ML model to incoming image data to auto infer severity <br>
# MAGIC 6. Join telematics data with claims data to recreaate scene of accident eg. location, speed. This is where other 3rd party dta can be layered ex. road conditions, weather data, etc. <br>
# MAGIC 7. Apply pre-determined rules dynamically to assess merit of the claim and if it is a 'normal' case, release of funds can be expedited <br>
# MAGIC 8. Claims Dashboard is refreshed to aid claim investigators with additional data insights inferenced through the data and AI pipeline <br>
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/medallion_architecture_dlt.png" width="80%" height="80%">
# MAGIC                                                                                    
# MAGIC Using DLT for ETL helps simplify and operationalize the pipeline with its support for autoloader, data quality via constraints, efficient auto-scaling for streaming workloads, resiliency via restart on failure, execution of administrative operations among others.
# MAGIC 
# MAGIC * Schema: smart_claims
# MAGIC * Tables:
# MAGIC   * <b>Bronze:</b> bronze_claim, bronze_policy, bronze_accident
# MAGIC   * <b>Silver:</b> silver_claim, silver_policy,  silver_claim_policy, silver_telematics, silver_accident, silver_claim_policy_accident, silver_claim_policy_telematics, silver_claim_policy_location
# MAGIC   * <b>Gold:</b> claim_rules, gold_insights
# MAGIC ___
# MAGIC 
# MAGIC # Insight visualization using Dashboards
# MAGIC A <b>Loss Summary</b> dashboard gives a birds eye view to overall business operations<br>
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/summary_dashboard.png" width="60%" height="60%">
# MAGIC 
# MAGIC * <b>Loss Ratio</b> is computed by insurance claims paid plus adjustment expenses divided by total earned premiums. 
# MAGIC   * For example, if a company pays $80 in claims for every $160 in collected premiums, the loss ratio would be 50%. 
# MAGIC   * The lower the ratio, the more profitable the insurance company. Each insurance company has its target loss ratio. A typical range is between 40%-60%. 
# MAGIC   * Damage is captured in 2 categories - property & liability - their loss ratios are tracked separately
# MAGIC   * The 80/20 Rule generally requires insurance companies to spend at least 80% of the money they take in from premiums on care costs and quality improvement activities. The other 20% can go to administrative, overhead, and marketing costs.
# MAGIC * <b>Summary</b> visualization captures count of incident type by severity
# MAGIC   * <b>Incident type</b> refers to damage on account of 
# MAGIC     * theft, collision (at rest, in motion (single/multiple vehicle collision)
# MAGIC   * <b>Damage Severity</b> is categorized as trivial, minor, major, total loss
# MAGIC * Analyzing recent trends helps to prepare for handling similar claims in the near future, for Eg.
# MAGIC   * What is the frequency of incident/damage amount by hour of day 
# MAGIC     * Are there certain times in a day such as peak hours that are more prone to incidents?
# MAGIC   * Is there a corelation to the age of the driver and the normalized age of the driver 
# MAGIC     * Note there are very few driver below or above a certain threshold
# MAGIC   * What about the number of incident coreelated to the age/make of the vehicle.
# MAGIC   * Which areas of the city have a higher incidence rate(construction, congestion, layout, density, etc)
# MAGIC 
# MAGIC A per claim <b>Investigation</b> dashboard gives additional where a claims officer picks a claim number and can drill into its various facets<br>
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/ClaimsInvestigation.png" width="80%" height="80%">
# MAGIC 
# MAGIC * The first panel uses <b>counter</b> widgets to provide statistics on rolling counts on number of 
# MAGIC   * Claims filed and of those how many were flagged as 
# MAGIC     * suspicious or 
# MAGIC     * had expired policies or
# MAGIC     * had a severity assessment mimatch or
# MAGIC     * claims amount exceeded the policy limits
# MAGIC * The next widget uses a <b>table</b> view to provide recent claims that are auto scored in the pipeline  using ML iferecing and rule engine
# MAGIC   * A green tick is used to denote auto-assessmentt matches claims description
# MAGIC   * A red cross indicates a mismatch that warrants further manual investigation
# MAGIC * Drill down to a specific claim to see
# MAGIC   * Images of the damaged vehicle 
# MAGIC   * Claim, Policy & Driver details 
# MAGIC   * Telematic data draws the path taken by the vehicle 
# MAGIC   * Reported data is contrasted with assessed data insights
# MAGIC ___
# MAGIC # Databricks value proposition in Smart Claims?
# MAGIC * Databricks features used
# MAGIC   * Delta, DLT, Multitask-workflows, ML & MLFlow, DBSQL Queries & Dashboards
# MAGIC * Unified Lakehouse architecture for
# MAGIC   * All data personas to work collaboratively on a single platform contributing to a single pipeline
# MAGIC   * All big data architecture paradigms including streaming, ML, BI, DE & Ops
# MAGIC * Workflow Pipelines are easier to create, monitor and maintain
# MAGIC   * Multi-task Workflows accommodate multiple node types (notebooks, DLT, ML tasks, QL dashboard and support repair&run & compute sharing)
# MAGIC   * DLT pipelines offer quality constraints and faster path to flip dev workloads to production
# MAGIC   * Robust, Scalable and fully automated via REST APIs thereby improving team agility and productivity
# MAGIC * BI & AI workloads 
# MAGIC   * Created, managed with MLFlow for easy reproducibility and auditability
# MAGIC   * Supports any model either created or ported 
# MAGIC   * Parameterized Dashboards that can access all data in the Laake and can be setup in minutes
# MAGIC ___
# MAGIC # How best to use this demo? 
# MAGIC * Ideal time: 1 hour (see recorded demo, deck, field-demo link)
# MAGIC * Ideal audience: Mix of tech and business folks (Basic Databricks knowhow is assumed)
# MAGIC * For optimum experience, reduce cluster startup times by having a running ML Runtime Interactive cluster, DBSQL Warehouse, DLT in dev mode
# MAGIC * Ideal Flow:
# MAGIC   * Explain need for claims automation via 'smart claims' & how Lakehouse aids the process 
# MAGIC   * Deck: based on this Readme, set the flow of the story (15 min)
# MAGIC   * Discovery of where they are (10 min)
# MAGIC   * Demo (25 min)
# MAGIC     * Data sources & EDA notebooks                                                                          
# MAGIC     * DE: Workflow & DLT Pipeline (5 min)
# MAGIC     * ML: Model management & inferencing (5 min)
# MAGIC     * BI: Loss summary & Claims Investigation (10 min)
# MAGIC  * Next steps (5 min)
# MAGIC ___
# MAGIC <anindita.mahapatra@databricks.com> <br>
# MAGIC <marzi.rasooli@databricks.com> <br>
# MAGIC <sara.slone@databricks.com> <br>
# MAGIC ___
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | geopy                                 | A Python client for geocoding   | MIT        | https://github.com/geopy/geopy                     |
# MAGIC 
# MAGIC 
# MAGIC ## Getting started
# MAGIC 
# MAGIC Although specific solutions can be downloaded as .dbc archives from our websites, we recommend cloning these repositories onto your databricks environment. Not only will you get access to latest code, but you will be part of a community of experts driving industry best practices and re-usable solutions, influencing our respective industries. 
# MAGIC 
# MAGIC <img width="500" alt="add_repo" src="https://user-images.githubusercontent.com/4445837/177207338-65135b10-8ccc-4d17-be21-09416c861a76.png">
# MAGIC 
# MAGIC To start using a solution accelerator in Databricks simply follow these steps: 
# MAGIC 
# MAGIC 1. Clone solution accelerator repository in Databricks using [Databricks Repos](https://www.databricks.com/product/repos)
# MAGIC 2. Attach the `RUNME` notebook to any cluster and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. The job configuration is written in the RUNME notebook in json format. 
# MAGIC 3. Execute the multi-step-job to see how the pipeline runs. 
# MAGIC 4. You might want to modify the samples in the solution accelerator to your need, collaborate with other users and run the code samples against your own data. To do so start by changing the Git remote of your repository  to your organization’s repository vs using our samples repository (learn more). You can now commit and push code, collaborate with other user’s via Git and follow your organization’s processes for code development.
# MAGIC 
# MAGIC The cost associated with running the accelerator is the user's responsibility.
# MAGIC 
# MAGIC 
# MAGIC ## Project support 
# MAGIC 
# MAGIC Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

# COMMAND ----------


