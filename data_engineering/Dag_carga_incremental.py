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
import pickle
import requests

bucket_name       = 'dataset_houses_for_sale'
file_name         = 'Houses_for_sale_processed.csv'
processing_file   = 'data_api_processing'
file              = 'data_api.csv'
nameDAG           = 'Carga_incremental'
project           = 'radiant-micron-400219'
owner             = 'Yuli'


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


#esta funcion crea una lista con todos los ids que ya tiene la base de datos
def ids_list():

    destination_file = download_data(file_name)
    # Lee el archivo CSV y crea un DataFrame usando solo la columna property_id
    list_ids = pd.read_csv(destination_file, usecols=['property_id'])

    #crea una lista a partir de todos los ids en el dataset
    list_ids = list_ids['property_id'].to_list()

    list_name = 'lista_ids'

    #crear la ubicacion temporal para la lista
    path_list_ids ='/tmp/{}'.format(list_name) 

    
    #guarda la lista en un archivo pickle
    with open(path_list_ids, 'wb') as archivo_pickle:
        pickle.dump(list_ids, archivo_pickle)
    
    upload_data(list_name, path_list_ids)

    return  list_name

#esta funcion llama a la api y obtiene nuevos datos 
def get_new_data():

    #esta funcion llama a la api
    def api_requests(State_code):

        url = "https://us-real-estate.p.rapidapi.com/v3/for-sale"

        #parametros para la llamada a la api
        querystring = {"state_code":f"{State_code}","sort":"newest","offset":"0","limit":"42"}

        headers = {
            "X-RapidAPI-Key": "f2d5ac3b00msh218fccabe383fd3p1b888fjsnf4e445be17eb",
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

        return results
    
    #la lista de codigos de estados que entraran a la api
    lista_codigos = ['AK', 'AL','AR','AZ','CA','CO','CT','DC','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA',
                 'MD','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC',
                 'SD','TN','TX','UT','VA','WA','WI']
    
    #creo una lista donde guardare los dataframes
    dfs=[]
    #recorro todos los codigos en la lista de codigos de estados
    for codigo in lista_codigos:
        
        #llamo a la api con cada codigo
        df=api_requests(codigo)
        #añado los dataframes a la lista de dataframes
        dfs.append(df)

    #creo un dataframe con todos los datos nuevos
    df_nueva_data = pd.concat(dfs,ignore_index=True)

    #defino una ruta para guardar el dataframe
    
    path_nueva_data = '/tmp/{}'.format(processing_file) 

    #guardo el dataframe en la ruta
    df_nueva_data.to_csv(path_nueva_data, index=False)

    upload_data(processing_file, path_nueva_data)
    #devuelvo la ruta
    



def compare_data(**kwargs):

    #traigo el contexto de la task_ids_list
    ti = kwargs['ti']
    list_name = ti.xcom_pull(task_ids='task_ids_list')
    path_list_ids = download_data(list_name)

    #abro el archivo pickle con la lista de ids
    with open(path_list_ids, 'rb') as archivo_pickle:
    # Utiliza pickle.load() para cargar la lista desde el archivo
        list_ids = pickle.load(archivo_pickle)

    #traigo los datos datos de contexto de la task get_new_data
    destination_file = download_data(processing_file)
    #leo el archivo con pandas
    df_nueva_data = pd.read_csv(destination_file)

    #creo una lista para guardar los dicts 
    dicts_nueva_data = []
    for id in df_nueva_data['property_id'].to_list():
        
        #si el id no esta en la lista de ids en la base de datos
        if id not in list_ids:

            dict = df_nueva_data[df_nueva_data['property_id']==id].to_dict()
            dicts_nueva_data.append(dict)

    
    #convierto los dics en dataframes 
    dfs = []
    for dict in dicts_nueva_data: 
        df = pd.DataFrame(dict)
        dfs.append(df)

    #concateno los dicts
    Data_to_add = pd.concat(dfs,ignore_index=True)

    #creo el nombre del archivo
    nombre_nueva_data =  'data_api.csv'

    #creo el path para el archivo temporal con el nombre y la fecha en la que se hizo el scraping
    path_data_to_add = '/tmp/{}'.format(file) 

    #guardo el dataframe en un csv temporal
    Data_to_add.to_csv(path_data_to_add, index = False)

    upload_data(file, path_data_to_add)

   
    




default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': True,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2020, 11, 5),
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 5,
         schedule_interval = '0 0 * * 0' ) as dag: 

    t_begin = DummyOperator(task_id="begin")
    
  
    task_ids_list = PythonOperator(task_id='task_ids_list',
                                 provide_context=True,
                                 python_callable=ids_list,
                                    dag=dag
                                 )

    task_get_new_data = PythonOperator(task_id='task_get_new_data',
                                 provide_context=True,
                                 python_callable=get_new_data,
                                 
                                    dag=dag
                                 )
    
    task_compare_data = PythonOperator(task_id='task_compare_data',
                                 provide_context=True,
                                 python_callable=compare_data,
                                 
                                    dag=dag
                                 )
    


    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_ids_list >> task_get_new_data >> task_compare_data  >> t_end