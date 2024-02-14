# PWC TECHNICAL CHALLENGE DATA ENGINEER
# Author: Juan Lozada Bonelli.
The challenge consisted of developing an ETL process using Python, as well as modeling and creating a Data Warehouse. Here we can find how the proposed solution has been implemented and considerations that have been taken into account during development.

------------------------------------------------------------------------------------------------------------------------------------------
TECHNOLOGIES:

For hosting and data storage, I have decided for the sake of simplicity  to create both, the source database (relational database) and the destination database (data warehouse) on the online platform 'SupaBase'. The ETL process will need to access this environment, so I will provide two sets of URLs and API keys to establish connections with both the source and the destination. This is because the free version used does not allow creating more than one database in the same project.
* Database project: PWC_DE_Juan_Lozada_challenge
* Datawarehouse project: PWC_DE_Juan_Lozada_challenge_DW
For the development of the ETL process, I have decided to use Python. The necessary libraries for execution are:
 -  os (access to variables)
 - dotenv (access to variables)
 - supabase (connection)
 - pandas
 - uuid
 - datetime
------------------------------------------------------------------------------------------------------------------------------------------

CODE STRUCTURE:

I decided to create four .py files:
- extraction.py: In this file, you'll find functions for connecting to the relational database and methods for extracting information from it.
- transformation.py: In this file, you'll find various functions for cleaning and transforming our data.
- loading.py: In this file, you'll find functions for connecting to the data warehouse and methods for loading records into it.
- main.py: This file establishes the execution order of the previously mentioned files (it's the executable).

------------------------------------------------------------------------------------------------------------------------------------------

SOURCE DATA:

I was provided with a DDL script to generate a relational database. Throughout the development, this schema was slightly modified to achieve greater functionality of the database. We can see the database model used in the following image:

![pwc_challenge_BD](https://github.com/juanlozadab/pwc_challenge_etl/assets/54322283/460eb30b-1c60-46ce-ab48-036b5a399009)

The biggest change we can observe compared to the provided DDL is the incorporation of the 'speciality' table, which contains the ID and the name of the medical specialty to which the doctor is dedicated.

------------------------------------------------------------------------------------------------------------------------------------------
DATA WAREHOUSE MODEL:

Due to the requirements outlined in the prompt, the relational database model provided to me, and certain assumptions on my part, I have designed the following Data Warehouse:

![DW_MODEL](https://github.com/juanlozadab/pwc_challenge_etl/assets/54322283/59774def-4782-4e86-a264-02e1bb0f9565)

We can observe that the model includes two fact tables.

* fact_patients_stay_cost:
Is focused on obtaining the costs incurred by a patient during a stay in a general manner. These costs come from the daily stay cost and the cost of each test conducted during the stay.Two assumptions were made when calculating costs. Firstly, the daily cost of a stay is assumed to be constant and corresponds to the price on the start date of the stay. In other words, if there is a price variation, it does not affect stays that were active at that time.Secondly, the test price is estimated based on the test admission date (admission_datetime), which is considered to be the moment when the payment is made, not the date when the test is conducted.
The value of this table lies in its ability to provide metrics about costs, the number of tests conducted, and patient stays.
* fact_tests_information
The table describing each test conducted by a patient is more detailed and allows for a wide variety of metrics. For example, we can analyze the number of tests per doctor, per medical specialty, determine which medical specialty generates the most revenue from tests, identify the most common type of test, among other analyses.


Regarding the dimensional tables, we have five: dim_patient containing patient information, dim_doctor containing doctor information, dim_speciality containing medical specialty information, dim_test containing test information, and dim_date enabling analyses based on time periods.

One clarification to note is that when the ETL runs, it adds two audit fields to the tables. These fields are record_id, which is a unique field, and etl_ts, which is a timestamp field. These fields allow us to identify the different loads made to the hub and filter out duplicate records in the fact tables. Duplicate fields in the dimension tables are not reloaded during ETL processing due to PK constraints.






 
