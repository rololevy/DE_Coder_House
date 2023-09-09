from datetime import timedelta
import os, pendulum , sys
from functools import partial
from airflow import DAG
from airflow.operators.python import PythonOperator
from desafio_2_v2 import main # Aqui se importan las funciones a ejecutar (las del desafio 2)
from airflow.operators.email_operator import EmailOperator
import smtplib
from email.mime.text import MIMEText

def send_email():
    subject = "Email Subject"
    body = "This is the body of the text message"
    sender = "o.oaguilera@gmail.com"
    recipients = ["o.oaguilera@gmail.com", "o.oaguilera2@gmail.com"]
    password = "******" # aqui ingrese su clave segura de 16 digitos de gmail desde el menu clave para aplicaciones

    msg = MIMEText(body, 'plain')  # Usamos 'plain' como tipo de contenido
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    try:
        smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())
        smtp_server.quit()
        print("Message sent successfully!")
    except Exception as e:
        print(f"Error sending email: {str(e)}")


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

with DAG(dag_id=DAG_ID,
         description=DAG_DESCRIPTION,
         default_args=ARGS,
         schedule_interval=DAG_SCHEDULE,
         catchup=DAG_CATCHUP,
         tags=TAGS,
         max_active_runs=1) as dag:

    name_task_1 = PythonOperator(task_id='job_get_data',
                                 python_callable=main,
                                 retries=1,
                                 retry_delay=timedelta(minutes=1))

    name_task_2 = PythonOperator(task_id='send_email_task',
                                 python_callable=send_email,
                                 retries=1,
                                 retry_delay=timedelta(minutes=1))



    # name_task_2 = PythonOperator(task_id = 'job_clean_data',
    #                                     python_callable = partial(clean_character_data, data),
    #                                     retries = 1,
    #                                     dag= dag,
    #                                     retry_delay = timedelta(minutes=1))
                                        
    # name_task_3 = PythonOperator(task_id = 'job_insert_data',
    #                                     python_callable = insert_data_into_redshift,
    #                                     retries = 1,
    #                                     retry_delay = timedelta(minutes=1))
    
    name_task_1 >> name_task_2 #>> name_task_3
