from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,  LoadFactOperator,
                                LoadDimensionOperator, StageToRedshiftOperatorcsv,  DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date':datetime(2020,5,27), #datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry': False
}

dag = DAG('immigration_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

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

start_operator >> airlines_to_redshift
start_operator >> country_age_to_redshift
start_operator >> cities_to_redshift
start_operator >> country_cost_to_redshift
start_operator >> country_crime_to_redshift
start_operator >> country_health_to_redshift
start_operator >> country_life_quality_to_redshift
start_operator >> country_pupolationn_to_redshift
start_operator >> country_property_cost_to_redshift

airlines_to_redshift >> end_operator
country_age_to_redshift >> end_operator
cities_to_redshift >> end_operator
country_cost_to_redshift >> end_operator
country_crime_to_redshift >> end_operator
country_health_to_redshift >> end_operator
country_life_quality_to_redshift >> end_operator
country_pupolationn_to_redshift  >> end_operator
country_property_cost_to_redshift >> end_operator