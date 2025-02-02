# Formula1 Data Pipeline Project using Azure Databricks, ADLS & ADF

This repository documents a Formula 1 racing data engineering project developed using various Azure services. The project’s objective is to efficiently process Formula 1 race data, establish an automated data pipeline, and make the processed information available for insightful analysis and reporting.

<ins>Overview of Formula 1 Racing Project</ins> <br>

<strong>Project Requirements<strong><br>
The main aim of the project is to determine the driver and constructor champions every year.

Data Source <br>
The data is obtained from the Ergast Developer API, which supplies essential data tables such as circuits, races, constructors, drivers, results, pitstops, lap times, and qualifying sessions.
| **File Name**   | **File Type**               |
|-----------------|-----------------------------|
| Circuits        | CSV                         |
| Races           | CSV                         |
| Constructors    | Single Line JSON            |
| Drivers         | Single Line Nested JSON     |
| Results         | Single Line JSON            |
| PitStops        | Multi Line JSON             |
| LapTimes        | Split CSV Files             |
| Qualifying      | Split Multi Line JSON Files |

Entity Relationship Diagram (ERD)
![image](https://github.com/user-attachments/assets/1bbdb925-3a8f-4dec-80f5-42d6d33a4529)


Project Resources & System Architecture<br>
Azure Databricks: Used for compute resources, utilizing both Python and SQL for data processing.<br>
Azure Data Lake Storage Gen2 : Implements hierarchical data storage, utilizing Delta tables.<br>
Azure Data Factory (ADF): Orchestrates data pipelines, integrating with Databricks notebooks for automated workflows.<br>
Azure Key Vault: Manages and secures credentials for accessing ADLS from Databricks notebooks.<br>

Solution Architecture <br>
![image](https://github.com/user-attachments/assets/d4feb335-3a49-4c38-9ada-362434eb5018)

Step-by-Step Project Workflow
1. Storage Setup Using ADLS and Containers <br>
Configured Azure Data Lake Storage with three distinct containers: raw , processed and presentation.

2. Compute Configuration with Databricks and ADLS Integration <br>
Deployed an Azure Databricks workspace with a specified cluster setup. Mounted Azure Data Lake Storage securely using Service Principal authentication to ensure safe data access.

3. Ingestion of Raw Data <br>
Loaded eight different types of files from the raw data container. Developed separate Databricks notebooks for data ingestion, transforming the raw data into a cleaner, processed format.

4. Processing and Transformation of Data <br>
Utilized the ingested raw data to perform additional transformations. Created additional notebooks to calculate race results,  driver standings and  constructor standings.

5. Presentation of Processed Data for Analysis <br>
The final processed datasets were saved in the presentation container. These datasets were analyzed to highlight top-performing drivers and teams.

6. Azure Data Factory Pipeline Automation<br>
Pipelines and Triggers <br>
Created three main pipelines: an ingestion pipeline, a transformation pipeline, and a processing pipeline, all automated with Azure Trigger functionality.
Utilized tumbling window triggers to run the pipeline once a week between pre-defined start & end dates,  .The ikage below shows the ADF pipeline configuration for data ingestion and transformation.

![image](https://github.com/user-attachments/assets/f6852d3f-cf77-49bf-bc0f-a18b794d800d)


## Visualisation
1. Area chart below shows the top 10 drivers of all years.
   
   ![visualization (2)](https://github.com/user-attachments/assets/35380b6c-dbd3-451a-8935-7beb62388308)

2. Area chart below shows the top 5 teams/constructors of all years

   ![visualization (3)](https://github.com/user-attachments/assets/a1b57ce9-08d2-468a-9188-79a119a38350)

