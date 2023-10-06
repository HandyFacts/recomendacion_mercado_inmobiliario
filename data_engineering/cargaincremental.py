import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
import datetime
import base64
import json
import numpy as np
import requests

bucket_name = 'dataset_houses_for_sale'
file_name = 'dataset_houses_for_sale.csv'
nameDAG           = 'DAG-read-csv'
project           = 'My Fist Project'
owner             = 'Yuli'
email             = ('yuliedpro1993@gmail.com')
GBQ_CONNECTION_ID = 'bigquery_default'



def get_for_sale(State_code,city):
    url = "https://us-real-estate.p.rapidapi.com/v3/for-sale"

    querystring = {"state_code":f"{State_code}","city":f"{city}","sort":"newest","offset":"0","limit":"42"}

    headers = {
        "X-RapidAPI-Key": "de70a81e28mshad517e750361265p1dbe50jsnf09b29a47f96",
        "X-RapidAPI-Host": "us-real-estate.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        # Convierte la respuesta JSON en un diccionario de Python
        data = response.json()

        results = data["data"]["home_search"]["results"]
        results = pd.DataFrame(results)
        # Define el nombre del archivo donde se guardará la respuesta JSON
        
    else:
        results=f"the request was failed. Error code: {response.status_code}"


def modificadf():

    """Read a CSV file (dataset_houses_for_sale.csv) uploaded to a Google Cloud Storage bucket."""
    # Obtén el nombre del archivo y el bucket desde el evento.
    bucket_name = 'dataset_houses_for_sale'
    file_name = 'dataset_houses_for_sale.csv'  # Nombre del archivo CSV

    # Inicializa el cliente de almacenamiento de Google Cloud.
    storage_client = storage.Client()

    # Accede al bucket y al archivo.
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Descarga el archivo a una ubicación temporal.
    temp_file = '/tmp/{}'.format(file_name)
    blob.download_to_filename(temp_file)






    # Lee el archivo CSV y crea un DataFrame
    df = pd.read_csv(temp_file)

    #agrega una columna al archivo
    df['prueba']= 0 





    #crea el nombre del nuevo archivo  
    file_m= 'dataset_houses_for_sale2.csv'

    #crear la ubicacion temporal para el nuevo archivo
    modificado='/tmp/{}'.format(file_m) 

    #guarda el nuevo archivo en la ubicacion creada   
    df.to_csv(modificado, index=False)

    # Accede al archivo.
    blob2= bucket.blob(file_m)
    
    # carga el archivo en la ubicacion temporal
    blob2.upload_from_filename(modificado)

    # Elimina el archivo temporal.
    os.remove(temp_file)
    os.remove(modificado)



default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2020, 11, 5),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = None) as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    t_begin = DummyOperator(task_id="begin")
    
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=modificadf,
                                 op_kwargs={
                                    'numeric_input': np.pi,
                                    'var1': "Variable1"
                                    }
                                 )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_python >> t_end