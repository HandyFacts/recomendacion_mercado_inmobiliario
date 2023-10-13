import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import datetime
import  requests as r
import json
import csv

bucket_name       = 'dataset_houses_for_sale'
processing_file   = 'data_api.csv'
file              ='data_api.csv'
nameDAG           = 'DAG_POST'
project           = 'radiant-micron-400219'
owner             = 'Yuli'
email             = ('yuliedpro1993@gmail.com')




def download_data(file_name):
    # Inicializa el cliente de almacenamiento de Google Cloud.
    storage_client = storage.Client()
    # Accede al bucket y al archivo.
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Descarga el archivo a una ubicación temporal.
    destination_file = '/tmp/{}'.format(file_name)
    blob.download_to_filename(destination_file)
    #devuelve la ubicacion en la que dejo el archivo
    return destination_file


def upload_data(file_name, destination_file): # recibe el nombre del archivo que va a subir y la ruta  donde esta almacenado
 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Accede al archivo.
    blob2= bucket.blob(file_name)
    
    # reemplaza el archivo en el bucket con el archivo en la destination file
    blob2.upload_from_filename(destination_file)


def post():
    destination_file = download_data(processing_file)
    url = 'https://handy-facts-service-ctxwxsa3aa-uc.a.run.app/house_post/'
    obj_lists = pd.read_csv(destination_file).to_dict(orient='records')
    
    def envio_info(obj,lista):
        obj_json = json.dumps(obj).replace('NaN', 'None').replace('true', 'True').replace('false', 'False')
        
        # Realiza una solicitud POST al URL especificado con el objeto JSON y espera hasta que se complete la solicitud
        response = r.post(url, json=obj_json, timeout=5)  # Ajusta el tiempo de espera según tus necesidades
        
        # Obtiene el código de respuesta HTTP
        codigo_respuesta = response.status_code
        print(codigo_respuesta)
        
        if codigo_respuesta != 200:
            id_problema = obj['property_id']
            lista.append((id_problema,codigo_respuesta))

    ids_problemas =[]
    for obj in obj_lists:
        envio_info(obj,ids_problemas)

    return ids_problemas
    
def save_ids(**kwargs):
    ti = kwargs['ti']
    ids_problemas = ti.xcom_pull(task_ids = 'task_post')


    # Ruta del archivo CSV en el que deseas guardar los elementos
    archivo_csv = '/tmp/ids_problemas.csv'

    if len(ids_problemas) != 0:
        # Abre el archivo CSV en modo de escritura
        with open(archivo_csv, 'w', newline='') as archivo:
            writer = csv.writer(archivo)
            # Escribe cada elemento de la lista en una fila separada, aplanando las tuplas
            for tupla in ids_problemas:
                writer.writerow(tupla)

    upload_data('ids_problemas.csv',archivo_csv)













default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': True,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2020, 11, 5),
    'retries': 4,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=.5),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 1,
         schedule_interval = None) as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    t_begin = DummyOperator(task_id="begin")
    


  
    task_post = PythonOperator(task_id='task_post',
                                 provide_context=True,
                                 python_callable=post,
                                 depends_on_past=True,

                                    dag=dag

                                 )
    
    task_save_ids = PythonOperator(task_id='task_save_ids',
                                 provide_context=True,
                                 python_callable=save_ids,
                                 depends_on_past=True,

                                    dag=dag

                                 )
    




    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_post >> task_save_ids  >>  t_end