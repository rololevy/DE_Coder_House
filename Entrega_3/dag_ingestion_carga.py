import os, pendulum , sys
from datetime import timedelta
from functools import partial
from airflow import DAG
from airflow.operators.python import PythonOperator
from desafio_2_v2 import main # Aqui se importan las funciones a ejecutar (las del desafio 2)

DAG_ID = 'ingesta_carga_redshift'
DAG_DESCRIPTION = 'Api_rick_and_morti'
DAG_SCHEDULE = '00 8 */1 * *'
DAG_CATCHUP = False
TAGS = ["Entrega_3"]

ARGS = {
    'owner' : 'orlando_aguilera',
    'start_date' : pendulum.datetime(2022, 8, 24, tz="America/Argentina/Buenos_Aires"),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}



with DAG(dag_id = DAG_ID,
         description = DAG_DESCRIPTION,
         default_args = ARGS,
         schedule_interval = DAG_SCHEDULE,
         catchup = DAG_CATCHUP,
         tags = TAGS,
         max_active_runs = 1) as dag:
    
    name_task_1 = PythonOperator(task_id = 'job_get_data',
                                         python_callable = main,
                                         retries = 1,
                                         retry_delay = timedelta(minutes=1))
    
    # name_task_2 = PythonOperator(task_id = 'job_clean_data',
    #                                     python_callable = partial(clean_character_data, data),
    #                                     retries = 1,
    #                                     dag= dag,
    #                                     retry_delay = timedelta(minutes=1))
                                        
    # name_task_3 = PythonOperator(task_id = 'job_insert_data',
    #                                     python_callable = insert_data_into_redshift,
    #                                     retries = 1,
    #                                     retry_delay = timedelta(minutes=1))
    
    name_task_1 # >> name_task_2 >> name_task_3
