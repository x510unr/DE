import os, logging, re

import pandas as pd 
import pyarrow.parquet as pq

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from google.cloud import storage

PROJECT_ID=os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ny_taxi')

local_path = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')


urls = ['https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet','https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet','https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv','https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet']

def download_file(url, name):


	os.system(f"curl -sSL {url} > {local_path}/{name}")

def upload_to_gcs(bucket, name, src_path):

	client = storage.Client()
	bucket = client.bucket(bucket)

	blob = bucket.blob(name)
	blob.upload_from_filename(src_path)

def remove_file(src_path):
	
	os.system(f"rm -f {src_path}")




dag =  DAG (

	dag_id='dl_ingestion',
	description = 'downlaod the files and upload to bucket and delete locally',
	schedule_interval='00 10 1 * *',
	max_active_runs = 1,
	start_date = datetime(2024,7,1),

) 

def create_task_group(url, dag):

	file_name = re.sub('[\-]+','',url.rsplit('/',1)[-1])
	nnam = file_name.rsplit('.',1)[0]

	with TaskGroup (group_id = f'group_{nnam}', dag=dag) as tg:

		download_task = PythonOperator(

			task_id = f'download_{nnam}',
			python_callable = download_file,
			op_kwargs = {
			'url':url,
			'name': file_name
			},
			dag = dag,

			)

		upload_task = PythonOperator(

			task_id = f'upload_{nnam}',
			python_callable = upload_to_gcs,
			op_kwargs = {
			"bucket" : BUCKET,
			"name" : f"raw/{file_name}",
			"src_path": f"{local_path}/{file_name}"
			},
			dag = dag,
			)

		remove_task = PythonOperator(

			task_id = f'remove_{nnam}',
			python_callable = remove_file,
			op_kwargs = {"src_path": f"{local_path}/{file_name}"},
			dag = dag,

			)

		download_task >> upload_task >> remove_task

	return tg 

for url in urls:
	create_task_group(url,dag)

		
