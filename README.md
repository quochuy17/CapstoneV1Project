- Project Discussion:
    + A music streaming startup - Sparkify, has grown the user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
    + Mission:  Design and build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team of Sparkify to continue finding insights in what songs their users are listening to.
    
- Project Description:
    + In this Capstone V1 project, the requirement is about to apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. 
    + Condition to complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3 and need to deploy this Spark process on a cluster using AWS.

- Project goals:
    + The core target of Capstone V1 project is to make a chance for those people or analytic groups such as data researchers, data analysts and other groups in order to analyze the large contexts of the US immigration data through gathering the data sources from four distinct datasets including:
    .Immigration.
    .Airport codes.
    .US Cities demographics. 
    .Global temperature. 
    + Through above dataset resources, the project built with a star schema which will enable data researchers, data analysts and other groups to get the insights of specific dataset. For instance, which city or area has more tourists visiting the United States, which states attract the most immigrant people, how long do they visit the US, what's the demography of the state where the immigrants see as the destination, and what's the average temparature of the US or other nations and the most common types of weather corresponding to each of temperature.

- The needed files:
    + "main.py" : implement the process of data sanity such as listout unique data, remove error data, parquet output files and run data quality check.
    + "project_configuration.py" : define needed function to load, transform and show the data.
    + "README.md" : explain the specific parts implemented on the project and other related ones.
    + "immigration_data_sample.csv" : This is the main dataset which including data on immigration to the United States.
    + "airport-codes_csv.csv" : The supplementary dataset will include data on airport codes of countries in some continents like NA, EU, AF, AS, OC.
    + "us-cities-demographics.csv" : This is also the supplementary dataset U.S. city demographics which provides the information about the demographics in some states, cities.
    + "I93_SAS_Labels_Description.SAS" : this is the temperature data which describes the general information of temperature in some areas.  
    + In addition, also welcoming to enrich the project with some sources of additional data in case to set the own project apart.

* Steps for implementation: 
    - The "Capstone V1 " project consists of the following main steps:
        + STEP 1: Scope the Project and Gather Data:
                . Project's goal : discover the insights of these datasets in the level of high-detail and make a project with a high level of applying to the real world in some cases. Besides, create a database that can be used as a resource for international immigrants and the US governement. 
                . This project results could help policy makers make informed decisions on immigration that the goverments can control carefully the immigrantion people and confirm thier status like name, age, health and other personal information. In addition, it also help these people obtain a chance to stay in the US by using the valuable insights of the data for permanent accommodation here.
                
        + STEP 2: Explore and Assess the Data:
                . For the criteria like data quality issues that the method is just to make some creative ways to enhance the quality of the dataset such as add extra columns, rows if needed or even look for the more distinct suitable datasets on other data resources.
                . For some case like missing value, duplicate data, de-dupe data,... The stategy is just to remove and limit the column or row happing errors like that. 
                
        + STEP 3: Define the Data Model (Appying the star schema)
                . The Fact table : applying a star schema for this context. The fact table does not follow the obvious structure that all dimension tables will connect directly to the fact table. 
                . The Dimension table : showing the main context for immigration patterns that can give the US goverments more better decisions. 
                . And these tables are shaped with each other and then truncated after finish a process. Lately, the core goal of helping the US government and International citizens address for the US immigration problems effectively.
                . List the steps necessary to pipeline the data into the chosen data model: 
                    . Create the high-level data pipeline flow for ETL process.
                    . Design a detailed data pipeline utilizing the stength of AWS ecosystem.
                    . Make a corresopnding for each steps in the data pipeline when implementing the ETL process.
                    . Optimize the data pipeline flow as well as the ETL process for later usage.
    
        + STEP 4: Run ETL to Model the Data
                . Create the Data Model : General architecture is using the Apache Airflow for creating the data pipeline and utilizing the Apache Spark to pull the data and store them in a data lake on an S3 bucket in Parquet format.
                . Data Quality check : 
                    .Integrity constraints on the relational database (e.g., unique key, data type, etc.)
                    * Unit tests for the scripts to ensure they are doing the right thing : create some realted unit tests for specific files in order to evaluate the data quality.  
 * Source/Count checks to ensure completeness : apply the Quality checks some situations like (duplicate, unique and no-null data) 
                
        + STEP 5: Complete Project Write Up
                .
                .
        
- Data cleaning process:
    + The necessary steps to clean the data including:
        . Step 1: Remove duplicate or irrelevant observations. Remove unwanted observations from your dataset, including duplicate observations or irrelevant observations. 
        . Step 2: Fix structural errors. 
        . Step 3: Filter unwanted outliers. 
        . Step 4: Handle missing data. 
        . Step 5: Validate and make the QA.

- Data sources : 
    + "I94 Immigration Data": This data is from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in.
    + "World Temperature Data": This dataset came from Kaggle. 
    + "U.S. City Demographic Data": This data comes from OpenSoft. 
    + "Airport Code Table": This is a simple table of airport codes and corresponding cities.

- Some specific cases and Future development plan:
    + The data was increased by 100x : Applying the AWS Redshift can handle big reads and writes so there wouldn't need to make some changes when allocating more nodes for the emr cluster to scale with the data quantity.
    + The data populates a dashboard that must be updated on a daily basis by 7am every day : needing to utilize AWS EventBrigde or maybe the Airflow and have a daily notification when implementing on 7am every day and then send the results via email about the status of the data process.
    + The database needed to be accessed by 100+ people : Utilizing the AWS redshift will handle effectively this senario with satisfying +100 people at the same time with the must-have condition that these people have to own the correct AWS credentials like IAM key and secret key.
    + In the future, need to apply more mordern technologies like Airflow for create the details ETL process and utilize some new visulization tool like Power BI, Google Data Studio for visualizing the insights after extending the scope of the project.