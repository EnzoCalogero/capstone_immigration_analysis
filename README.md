# US Immigration Arriving from air Warehouse
## Project Summary
The project aim is to create a warehouse, where information from immigration arriving in US by air are collect with information of the arriving persons, with links of information regading the country of provenience and the airports where they land.

## Business Scenario/ Project goals
In this project, we will be looking at the immigration data to the united states. More specifically, we're interested on only people arriving trough airports. We are looking at the following phenomena:

the reason of the arrival (type of visas and their justification);
From which country they are arriving and compare the original country charatheristic to better identify the reason(s) of the coming.
in which airport each nationality prefer to land.
To accomplish this study, we will be using the following datasets:

I94 Immigration Data: This data comes from the US National Tourism and Trade Office and includes the contents of the i94 form on entry to the united states. A data dictionary is included in the workspace. (https://travel.trade.gov/research/reports/i94/historical/2016.html) country_dict.csv: table containing country codes used in the dataset, extracted from the data dictionary. visa_dict.csv: table containing visa codes used in the dataset, extracted from the data dictionary.

Countries Dataset 2020. All about countries - demography, crime index, cost of living etc. Kaggle (https://www.kaggle.com/dumbgeek/countries-dataset-2020?select=Quality+of+life+index+by+countries+2020.csv)

airlines Database kaggle https://www.kaggle.com/open-flights/airline-database airport_dict.csv: table containing IATA codes for the airports used in the dataset, extracted from the data dictionary.

Airport Code Table: This is a simple table of airport codes and corresponding cities. https://datahub.io/core/airport-codes#data

U.S. City Demographic Data: This data comes from OpenSoft. (https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

## Solution overview

The whole solution is cloud based of Amazon Web Services (AWS) and based on the following steps.

* the raw data is saved in a S3 depository.
* the AWS-EMR/PySpark is processing the immigration dataset reading from s3 depository and saved the post cleanead dataset in a different folder on the s3 depository.
* the Apache Airflow pipeline DAG is doing the remain steps of the project:
* Loading cleaning and saving the needed dictionaries (country_dict.csv, airport_dict.csv,...)
* Loading from the s3 depository, cleaning and saving on a different folder the dataset Country, City, and Airport datasets
* All the processed and cleaned dataset are copied on the a pre-build Amazon Redshift cluster.
* Finally all the copied data is checked to ensure that all the previous steps were successful and correct.
