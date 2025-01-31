# Formula1 Data Pipeline Project using Azure Databricks, ADLS & ADF

This repository documents a Formula 1 racing data engineering project developed using various Azure services. The project’s objective is to efficiently process Formula 1 race data, establish an automated data pipeline, and make the processed information available for insightful analysis and reporting.

<ins>Overview of Formula 1 Racing Project</ins> <br>

<strong>Project Requirements<strong><br>
The main aim of the project is to determine the driver and constructor champions every year.

Data Source <br>
The data is obtained from the Ergast Developer API, which supplies essential data tables such as circuits, races, constructors, drivers, results, pitstops, lap times, and qualifying sessions.

Entity Relationship Diagram (ERD)
http://ergast.com/images/ergast_db.png

Project Resources & System Architecture<br>
Azure Databricks: Used for compute resources, utilizing both Python and SQL for data processing.<br>
Azure Data Lake Storage Gen2 : Implements hierarchical data storage, utilizing Delta tables.<br>
Azure Data Factory (ADF): Orchestrates data pipelines, integrating with Databricks notebooks for automated workflows.<br>
Azure Key Vault: Manages and secures credentials for accessing ADLS from Databricks notebooks.<br>


Step-by-Step Project Workflow
1. Storage Setup Using ADLS and Containers
Configured Azure Data Lake Storage with three distinct containers: raw , processed and presentation.

2. Compute Configuration with Databricks and ADLS Integration
Deployed an Azure Databricks workspace with a specified cluster setup. Mounted Azure Data Lake Storage securely using Service Principal authentication to ensure safe data access.

3. Ingestion of Raw Data
Loaded eight different types of files from the raw data container. Developed separate Databricks notebooks for data ingestion, transforming the raw data into a cleaner, processed format.

4. Processing and Transformation of Data
Utilized the ingested raw data to perform additional transformations. Created additional notebooks to calculate race results,  driver standings and  constructor standings.

5. Presentation of Processed Data for Analysis
The final processed datasets were saved in the presentation container. These datasets were analyzed to highlight top-performing drivers and teams.

6. Azure Data Factory Pipeline Automation<br>
Pipelines and Triggers <br>
Created three main pipelines: an ingestion pipeline, a transformation pipeline, and a processing pipeline, all automated with Azure Trigger functionality.<br>
Utilized tumbling window triggers to automate data processing based on pre-defined date ranges, ensuring timely and accurate updates.
