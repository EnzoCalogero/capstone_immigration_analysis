from datetime import datetime, timedelta
import os
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.operators import (StageToRedshiftOperator,  LoadFactOperator,
                                LoadDimensionOperator, StageToRedshiftOperatorcsv,  DataQualityOperator)
from airflow.operators.python_operator import PythonOperator
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

# Cleaning functions

def cleaning_cities(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'city.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    cites=pd.read_csv(LOCAL_FILE, sep=";")
    cites=cites[["City","State","Median Age","Total Population","Foreign-born","Average Household Size"]]
    cites=cites.drop_duplicates()
    airport=pd.read_csv('airports.csv')
    municipality=set(airport.municipality)
    cites=cites[cites.City.isin(municipality)]
    cites.rename(columns={'Median Age':'median_age',
                          'Total Population':'total_pupolation',
                          'Foreign_born':'Foreign-born',
                     'Average Household Size':'Average_household'}, 
                 inplace=True)

    cites.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)

def cleaning_airports(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'airports.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    airport=pd.read_csv(LOCAL_FILE)
 
    airport=airport[["type", "name", "municipality", "iata_code"]]
    airport_dict=pd.read_csv("airport_dict.csv")
    airport_set=set(airport_dict.iata_code)
    airport=airport[airport["iata_code"].isin(airport_set)]
    airport=airport.drop_duplicates()  
    airport['name']=airport['name'].str.replace(',','')
    airport['municipality']=airport['municipality'].str.replace(',','')

    airport.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)



def airport_dict(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'airport_dict.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    airport_dict=pd.read_csv(LOCAL_FILE, sep="=", names=["iata_code","full name"], skipinitialspace=True)
 
    airport_dict['iata_code']=airport_dict['iata_code'].str[1:].str[:3]
    airport_dict['full name']=airport_dict['full name'].str[2:].str[:-1]
    airport_dict.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)



def cleaning_airlines(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'airlines.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    airlines=pd.read_csv(LOCAL_FILE)
    airlines=airlines[airlines['Active']=='Y']
    airlines=airlines[airlines['IATA']>'00']
    airlines=airlines[airlines['Country'].notnull()]
    airlines=airlines[airlines['Country'] !='\\N']
    airlines=airlines[['Name', 'IATA', 'ICAO', 'Callsign', 'Country']]
    airlines.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)


def cleaning_life_quality(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'life_Quality.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    life_Quality=pd.read_csv(LOCAL_FILE)
    life_Quality['Country']=life_Quality['Country'].str.upper()
    country_dict=pd.read_csv('country_dict.csv')
    country_set=set(country_dict['country']) 
    
    life_Quality['Country']=life_Quality['Country'].str.upper()
    life_Quality=life_Quality[life_Quality.Country.isin(country_set)]
    life_Quality=life_Quality[["Country","Quality of Life Index","Health Care Index","Pollution Index","Climate Index"]]
    life_Quality.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)



def cleaning_property_cost(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'property_cost.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    property_cost=pd.read_csv(LOCAL_FILE)
    country_dict=pd.read_csv('country_dict.csv')
    country_set=set(country_dict['country']) 
    property_cost['Country']=property_cost['Country'].str.upper()
    property_cost=property_cost[["Country", "Price To Income Ratio", "Mortgage As A Percentage Of Income","Affordability Index" ]]
    property_cost=property_cost[property_cost.Country.isin(country_set)]
    property_cost.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)


def cleaning_health(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'health.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    health=pd.read_csv(LOCAL_FILE)
    health['Country']=health['Country'].str.upper()
    country_dict=pd.read_csv('country_dict.csv')
    country_set=set(country_dict['country']) 
    health=health[health.Country.isin(country_set)]
    health.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)


def cleaning_crime(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'crime.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    crime=pd.read_csv(LOCAL_FILE)
    crime['Country']=crime['Country'].str.upper()
    country_dict=pd.read_csv('country_dict.csv')
    country_set=set(country_dict['country']) 
             
    crime=crime[crime.Country.isin(country_set)]
    crime.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)

def cleaning_age(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'age.csv'
    
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    age=pd.read_csv(LOCAL_FILE)
    age['Country']=age['Country'].str.upper()
    
    country_dict=pd.read_csv('country_dict.csv')
    country_set=set(country_dict['country']) 
    age=age[age.Country.isin(country_set)]
    age.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)
 

def cleaning_cost(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'cost.csv'
    
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    cost=pd.read_csv(LOCAL_FILE)
    cost=cost[["Country", "Cost of Living Index","Rent Index","Restaurant Price Index"]]
    cost['Country']=cost['Country'].str.upper()
    country_dict=pd.read_csv('country_dict.csv')
    country_set=set(country_dict['country']) 
    
    cost=cost[cost.Country.isin(country_set)]
        
    cost.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)

    
def cleaning_population(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'population.csv'
    
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    population=pd.read_csv(LOCAL_FILE)#"country_dataset/Pupulation density by countries.csv")
    country_dict=pd.read_csv("country_dict.csv")
    country_set=set(country_dict['country'])
    population.rename(columns={'Country (or dependent territory)':'Country'}, inplace=True)
  
    population=population[["Country", "Area km2", "Population", "Density pop./km2" ]]
    population['Country']=population['Country'].str.upper()
    population=population[population.Country.isin(country_set)]
    
    population['Area km2']=population['Area km2'].str.replace(',','')
    population['Population']=population['Population'].str.replace(',','')
    population['Density pop./km2']=population['Density pop./km2'].str.replace(',','')
    
    population.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)

    
def clean_country_dict(bucket, filename_source, filename_dest):
    LOCAL_FILE = 'country_dict.csv'
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                        )
       
    s3.meta.client.download_file(bucket, filename_source, LOCAL_FILE)
    country_dict=pd.read_csv("country_dict.csv", sep="=", names=["code","country"], skipinitialspace=True)
    country_dict['country']=country_dict['country'].str[1:].str[:-1]
    country_dict['country']=country_dict['country'].str.upper()
    country_dict.to_csv('country_dict.csv', index=False)
    
   # population.to_csv(filename_dest, index=False)
    s3.Bucket(bucket).upload_file(filename_dest, LOCAL_FILE)


    
default_args = {
    'owner': 'udacity',
    'start_date':datetime(2020,5,31), 
    'depends_on_past': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=2),
    'catchup' : False,
    'email_on_retry': False
}

dag = DAG('immigration_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

cleaning_cities = PythonOperator(
    task_id='cleaning_cities',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_cities,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/us-cities-demographics.csv', 'filename_dest':'city.csv' }
   )

cleaning_airports= PythonOperator(
    task_id='cleaning_airports',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_airports,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/airport-codes_csv.csv', 'filename_dest':'airports.csv' }
   )

airport_dict = PythonOperator(
    task_id='airport_dict',
    dag=dag,
    provide_context=False,
    python_callable=airport_dict,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/airport_dict.csv', 'filename_dest':'airport_dict.csv' }
   )

cleaning_airlines = PythonOperator(
    task_id='cleaning_airlines',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_airlines,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/airlines.csv', 'filename_dest':'airlines.csv' }
   )

cleaning_property_cost = PythonOperator(
    task_id='cleaning_property_cost',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_property_cost,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Properties price index by countries 2020.csv', 'filename_dest':'property_cost.csv' }
   )

cleaning_life_quality = PythonOperator(
    task_id='cleaning_life_quality',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_life_quality,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Quality of life index by countries 2020.csv', 'filename_dest':'life_Quality.csv' }
   )

clear_population = PythonOperator(
    task_id='clear_population',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_population,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Pupulation density by countries.csv', 'filename_dest':'population.csv' }
   )

cleaning_age = PythonOperator(
    task_id='cleaning_age',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_age,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Coutries age structure.csv', 'filename_dest':'age.csv' }
   )

cleaning_health = PythonOperator(
    task_id='cleaning_health',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_health,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Health care index by countries 2020.csv', 'filename_dest':'health.csv' }
   )

cleaning_crime = PythonOperator(
    task_id='cleaning_crime',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_crime,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Crime index by countries 2020.csv', 'filename_dest':'crime.csv' }
   )

clear_cost = PythonOperator(
    task_id='clear_cost',
    dag=dag,
    provide_context=False,
    python_callable=cleaning_cost,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/Cost of living index by country 2020.csv', 'filename_dest':'cost.csv' }
    )

clean_country_dict = PythonOperator(
    task_id='clean_country_dict',
    dag=dag,
    provide_context=False,
    python_callable=clean_country_dict,
    op_kwargs={'bucket':'immigration-us-1','filename_source':'raw_data/country_dict.csv', 'filename_dest':'country_dict.csv' }
               #op_kwargs={'keyword_argument':'which will be passed to function'}
    )


airlines_to_redshift = StageToRedshiftOperatorcsv(
    task_id='airlines',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="airlines",
    delimiter=",",
     ignore_headers=0,
    s3_source="s3://immigration-us-1/airlines.csv",
    )

country_age_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_age',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_age",
    delimiter=",",
     ignore_headers=1,
    s3_source="s3://immigration-us-1/age.csv",
    )

cities_to_redshift = StageToRedshiftOperatorcsv(
    task_id='cities',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="cities",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/city.csv",
    )

airports_to_redshift = StageToRedshiftOperatorcsv(
    task_id='airports',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="airports",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/airports.csv",
    )

country_cost_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_cost',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_cost",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/cost.csv",
    )

country_crime_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_crime',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_crime",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/crime.csv",
    )

country_health_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_health',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_health",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/health.csv",
    )

country_life_quality_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_life_quality',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_life_quality",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/life_Quality.csv",
    )

country_pupolationn_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_pupolation',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_pupolation",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/population.csv",
    )

country_property_cost_to_redshift = StageToRedshiftOperatorcsv(
    task_id='country_property_cost',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_paths="",
    table="country_property_cost",
    delimiter=",",
    ignore_headers=1,
    s3_source="s3://immigration-us-1/property_cost.csv",
    )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    # list of tests with the expected result
    list_tests={
    "SELECT COUNT(*) FROM country_life_quality":80,
    "SELECT COUNT(*) FROM country_cost":130,
    "SELECT COUNT(*) FROM country_age":172,
    "SELECT COUNT(*) FROM country_life_quality":80,
    "SELECT COUNT(*) FROM country_crime":127,
    "SELECT COUNT(*) FROM country_health":93,
    "SELECT COUNT(*) FROM country_property_cost":103,
    "select  count(*) from airlines":1008,
    "select  count(*) from airports":556,
    "select count(*) from cities":112,
    "SELECT COUNT(*) FROM  country_pupolation":182
    },
    tables=(
        'country_life_quality',
        'country_property_cost',
        'airlines',
        'airports',
        'cities',
        'country_cost',
        'country_age',
        'country_life_quality',
        'country_crime',
        'country_pupolation',
        'country_health'
    )
)


end_operator = DummyOperator(task_id='Stop_execution',
                             dag=dag
                            )


start_operator >> clean_country_dict
start_operator >> airport_dict

clean_country_dict >> clear_population
clean_country_dict >> clear_cost
clean_country_dict >> cleaning_age
clean_country_dict >> cleaning_crime
clean_country_dict >> cleaning_health
clean_country_dict >> cleaning_property_cost
clean_country_dict >> cleaning_life_quality


clear_population >> country_pupolationn_to_redshift
clear_cost >> country_cost_to_redshift
cleaning_age >> country_age_to_redshift
cleaning_crime >> country_crime_to_redshift
cleaning_health >> country_health_to_redshift
cleaning_property_cost >> country_property_cost_to_redshift
cleaning_life_quality >> country_life_quality_to_redshift

start_operator >> cleaning_airlines
cleaning_airlines >> airlines_to_redshift

cleaning_airports >> airports_to_redshift

airlines_to_redshift >> run_quality_checks
country_age_to_redshift >> run_quality_checks
cleaning_cities >> cities_to_redshift

cities_to_redshift >> run_quality_checks

airports_to_redshift >> run_quality_checks

country_cost_to_redshift >> run_quality_checks
country_crime_to_redshift >> run_quality_checks
country_health_to_redshift >> run_quality_checks
country_life_quality_to_redshift >> run_quality_checks
country_pupolationn_to_redshift  >> run_quality_checks
country_property_cost_to_redshift >> run_quality_checks

airport_dict >> cleaning_airports
cleaning_airports>> cleaning_cities
#cleaning_cities >> end_operator
run_quality_checks >> end_operator

