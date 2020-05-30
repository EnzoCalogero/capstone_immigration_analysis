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
    'start_date':datetime(2020,5,29), #datetime(2020, 1, 12),
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


end_operator = DummyOperator(task_id='Stop_execution',
                             dag=dag
                            )

start_operator >> clean_country_dict

clean_country_dict >> clear_population
clean_country_dict >> clear_cost
clean_country_dict >> cleaning_age

clear_population >> country_pupolationn_to_redshift
clear_cost >> country_cost_to_redshift
cleaning_age >> country_age_to_redshift


start_operator >> airlines_to_redshift
#start_operator >> country_age_to_redshift
start_operator >> cities_to_redshift

#clean_country_dict >> clear_age


start_operator >> country_crime_to_redshift
start_operator >> country_health_to_redshift
start_operator >> country_life_quality_to_redshift

start_operator >> country_property_cost_to_redshift

airlines_to_redshift >> end_operator
#clear_age >> country_age_to_redshift
country_age_to_redshift >> end_operator

cities_to_redshift >> end_operator
country_cost_to_redshift >> end_operator
country_crime_to_redshift >> end_operator
country_health_to_redshift >> end_operator
#country_age_to_redshift >> end_operator
country_life_quality_to_redshift >> end_operator
country_pupolationn_to_redshift  >> end_operator
country_property_cost_to_redshift >> end_operator