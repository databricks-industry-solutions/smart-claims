<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

* <b>Domain </b>: Insurance 
* <b>Challenge </b>: 
  * Insurance companies have to constantly innovate to beat competition
  * Customer Retention & Loyalty can be challenge as people are always shopping for more competitive rates leading to churn
  * Fraudulent transactions can erode profit margins 
  * Processing Claims which can be very time consuming at times
  * How to improve the Claims Management process for faster claims settlement, lower claims processing costs and quicker identification of possible fraud.
* <b><span style="color:#f03c15"> Smart Claims </span></b>: 
  * A Databricks Solution Accelerator that uses the Lakehouse paradigm to automate certain components of this process that aids human investigation 

<img src="./resource/images/ClaimsProcess.png" width="70%" height="70%">
Every claim is different, the following steps capturess a typical workflow <br>
1. The <b>Insured</b> contacts the broker who is the primary contact w.r.t. policy <br>
2. The <b>Broker</b> examines the data to ensure that relevant details of the claim situation have been captured <br>
The <b>Adjuster</b> takes over the investigation and may collaborate with internal/external experts to determine the amount of loss or damages covered by the insurance policy.<br>
 3. The <b>Claims Investigaton</b> will do due diligence on the paaperwork<br>
 4. The <b>Compliance Officer</b> will check eligibility of coverage and ensure no foul play is involved<br>
 5. The <b>Appraiser</b> will conduct a damage evaluation to determine the severity of the claim<br>
6. The <b>Adjuster</b> will ensure payment is approved and released and communicates back to the <b>Insured</b><br>

___

# Details
* <b>What</b>
  * How to manage operational costs so as to offer lower premiums, be competitive & yet remain profitable?
  * How can customer loyalty & retention be improved to reduce churn?
  * How to improve process efficiencies to reduce the response time to customers on the status/decision on their claims?
  * How can funds and resources be released in a timely manner to deserving parties?
  * How can suspicious activities be flagged for further investigation?
* <b>Why</b>
  * Faster approvals leads to Better Customer NPS scores and Lower Operating expenses
  * Detecting & Preventing fraudulent scenarios leaads to Lower Leakage ratio
  * Improving customer satisfaction leads to Lower Loss ratio
* <b>How: Claims Automation</b>
  * Automting certain aspects of the claims processing pipeline to reduce dependence of human personnel esspecially in mundane predictable tasks
  * Augmenting additional info/insights to existing claims data to aid/expedite human investigation, eg. Recommend Next Best Action
  * Providing greater explainability of the sitution/case for better decision making in the human workflow
  * Serving as a sounding board to avoid human error/bias as well as providing an audit trail for personel in Claims Roles 


# Smart Claims Reference Architecture & Data Flow
<img src="./resource/images/smart_claims_process.png" width="70%" height="70%">

Claims flow typically involve some orchestration between an <b>operational</b> system such as Guidewire and an <b>analytic</b> system such as Databricks as shown in the diagram above. End users often use a smart app to file claims, look at the status of their case. Either via an app or an IoT dvice embedded in their vehicle, telematic data iss constantly streaming into one of these two systems which provides a lot of information regarding their driving patterns. Sometimes in the event of other credit scores, this data is used to assign a risk score for the driver which has a direct consequence on their premiums. In some ways, it can be argued that this type of <b>insurance risk score </b> is a better indicator of a person's safety track rather than a generic financial credit score which is determined primarily by their financial track record. 

1. Policy data ingestion 
2. Claims and telematics data ingestion 
3. Ingest all data sources to the cloud storage
4. Incrementally Load Raw data to Delta Bronze table
5. Transform and Manipulate data
6. Model scoring (and model training in the training pipeline)
7. Load predictions to a gold table and perform aggregations
8. Dashboard visualization
9. Feed the results back to the operational system
10. Claims routing based on decision

___

# Datasets
<img src="./resource/images/datasets.png" width="60%" height="60%">

* Typical datasets include the above, some of these are slow moving while others are fast moving. 
* Some are strutured/semi-structured while others are un-structed. 
* Some of these are additive and are appended while others are inccremental updates and are treated as slowly changing dimensions.

___

# Domain Model
<img src="./resource/images/domain_model.png">

* There are several industry prescribed data domain models Eg. OMG (https://www.omg.org/) 
* The above diagram is a simplified domain model to capture some of the relevant data points for the use case.
* For more details, efer to the P&C etity definitions, terminology & logicaal model https://www.omg.org/spec/PC/1.0/PDF

# Insight Generation using ML & Rule Engine 
* A pre-trained <b>ML Model</b> is used to score the image attached in the claims record to assess the severity of damage.
* A <b>Rule Engine </b> is a flexible way to define known operational static checks that can be applied without requiring a human in the loop, thereby speeding up 'routine cases'. When the reported data does not comply with auto detected info, flags are raised to involve additional human investigation
* This additional info helps a claims investigator by arrowing down the number of casess that need intervention a well as by narrowing down the specific areas thatt need additional followup and scrutiny
* Some common checks include
  * Claim date should be within coverage period
  * Reported Severity should match ML predicted severity
  * Accident Location as reported by telematics data should match the location as reported in claim
  * Speed limit as reported by telematics should be within speed limits of that region if there is a dispute on who was on the offense 
* <img src="./resource/images/rule_engine.png" width="60%" height="60%">

# Workflow
* Different data sources flow in at their own pace, some independent, some with dependencies
* We will use Databricks multi-task Workflows to put the process in auto-pilot mode to demonstrate the Lakehouse paradigm.
* Some nodes are Delta Live Table nodes which employ the medallion architecture to refine and curate data, while others are notebooks which use a Model to score the data while still others are SQL workflows to refresh a dashboard with newly generated insights.
<img src="./resource/images/workflow.png" width="60%" height="60%">
1. Setup involve all the work needed to setup the <br>
2. Ingest claims, Policy & accident data ussing a DLT Pipeline <br>
3. Ingest Telematic data <br>
4. Augment claims data with latitude/longitude using zipcode <br>
5. Apply ML model to incoming image data to auto infer severity <br>
6. Join telematics data with claims data to recreaate scene of accident eg. location, speed. This is where other 3rd party dta can be layered ex. road conditions, weather data, etc. <br>
7. Apply pre-determined rules dynamically to assess merit of the claim and if it is a 'normal' case, release of funds can be expedited <br>
8. Claims Dashboard is refreshed to aid claim investigators with additional data inferenced through the data and AI pipeline <br>

<img src="./resource/images/medallion_architecture_dlt.png" width=80%" height=80%"> 
Using DLT for ETL helps simplify and operationalize the pipeline with its support for autoloader, data quality via constraints, efficient auto-scaling for streaming workloads, resiliency via restart on failure, execution of administrative operations among others.

___

# Insight visualization using Dashboards
<img src="./resource/images/summary_dashboard.png" width="60%" height="60%">

* A <b>Loss Summary</b> dashboard gives a birds eye view to overall business operations
* <b>Loss Ratio</b> is computed by insurance claims paid plus adjustment expenses divided by total earned premiums. 
 * For example, if a company pays $80 in claims for every $160 in collected premiums, the loss ratio would be 50%. 
 * The lower the ratio, the more profitable the insurance company. Each insurance company has its target loss ration
 * Damage is captured in 2 categories - property & liability - their loss ratios are tracked separately
 * A typical range is between 40%-60%. 
 * The 80/20 Rule generally requires insurance companies to spend at least 80% of the money they take in from premiums on care costs and quality improvement activities. The other 20% can go to administrative, overhead, and marketing costs.
* The <b>summary</b> visualization captures count of incident type by severity
 * <b>Incident type</b> refers to damage on account of theft, collision (at rest, in motion (single/multiple vehicle collision)
 * <b>Damage Severity</b> is categorized as trivial, minor, major, total loss
* Analyzing recent trends helps to prepare for similar occurances, for Eg.
 * What is the frequency of incident/damage amount by hour of day - are there certain times in a day such as peak hours that are more prone to incidents
 * Is there a corelation to the age of the driver and the normalized age of the driver (very few driver below or above a certain threshold)
 * What about the number of incident coreelated to the age/make of the vehicle.
 * Which areas of the city have a higher incidence rate(construction, congestion, layout, density, etc)
 
<img src="./resource/images/ClaimsInvestigation.png" width="80%" height="70%">

* A per claim <b>Investigation</b> dashboard gives additional where a claims officer picks a claim number and can drill into its various facets
* The first panel uses <b>counter</b> widgets to provide statistics on rolling counts on number of 
 * claims files and of those how many were flagged as 
  * suspicious or 
  * had expired policies or
  * had a severity assessment mimatch or
  * claims amount exceeded the policy limits
* The next widget uses a <b>table</b> view to provide recent claims that are auto scored in the pipeline  using ML iferecing and rule engine
 * A green tick is used to denote auto-assessmentt matches claims description
 * A red cross indicates a mismatch that warrants further manual investigation
* Drill dow to specific claim
 * Images of the damaged vehicle is shown
 * Claim, Policy & Driver details are pulled up
 * Trip detail from telematic data draws the path taken by the vehicle along with telematic data, reported data is contrasted with assessed data insights
___
# What is the Databricks value proposition in Smart Claims?
* Databricks features used
*  Delta, DLT, Multitask-workflows, ML & MLFlow, DBSQL Queries & Dashboards
* Unified Lakehouse architecture for
 * All data personas to work collaboratively on a single platform contributing to a singe pipeline
 * All big data architecture paradigms including streaming, ML, BI, DE & Ops
* Workflow Pipelines are easier to create, monitor and maintain
  * Multi-task Workflows accommodate multiple node types (notebooks, DLT, ML tasks, QL dasshboard and support repair&run & compute sharing)
  * DLT pipelines offer quality constraints and faster path to flip dev workloads to production
  * Robust, Scalable and fully automated via REST APIs thereby improving team agility and productivity
* BI & AI workloads 
 * Created, managed with MLFlow for easy reproducibility and auditablity
 * Supports any model either created or ported 
 * Parametrized Dashboards that can access all data in the Laake and can be setup in minutes
___
# How best to use this demo? 
* Ideal time: 1 hour (see recorded demo, deck, field-demo link)
* Ideal audience: Mix of tech and business folks (Basic Databricks knowhow is assumed)
* For optimum experience, reduce cluster startup times by having a running ML Runtime Interactive cluster, DBSQL Warehouse, DLT in dev mode
* Ideal Flow:
 * Explain need for claims automation via 'smart claims' & how Lakehouse aids the process 
 * Deck: (based on this readme & sets the flow of the story, 15 min)
 * Discovery of where they are ( 10 min)
 * Demo (25 min)
  * DE: Workflow & DLT (5 min)
  * ML: Model management & inferencing (5 min)
  * BI: Loss summary & Claims Investigation (10 min)
 * Next steps (5 min)
___
<anindita.mahapatra@databricks.com> <br>
<marzi.rasooli@databricks.com> <br>
<sara.slone@databricks.com> <br>
___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

