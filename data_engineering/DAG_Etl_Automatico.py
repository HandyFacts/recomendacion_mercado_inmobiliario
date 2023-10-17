import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
import datetime
import numpy as np
from functools import reduce
import re
import math
import time

bucket_name       = 'dataset_houses_for_sale'
processing_file   = 'data_api_processing'
file              ='data_api.csv'
nameDAG           = 'DAG_ETL_api'
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

def drop_columns_1():
    #descarga el archivo file_name desde el bucket
    
    destination_file = download_data(processing_file)

    #lee el archivo csv descargado
    print(f'leyendo el archivo {processing_file}')

    houses_for_sale = pd.read_csv(destination_file)

    print(houses_for_sale.columns)

    columns_to_drop=['source','permalink','other_listings','open_houses','branding','coming_soon_date','matterport','search_promotions',
                    'rent_to_own','products','virtual_tours','community','price_reduced_amount','last_update_date','lead_attributes', 'tax_record']

    houses_for_sale.drop(columns_to_drop,axis = 1,inplace=True)

    houses_for_sale.drop_duplicates(subset='property_id', inplace = True)

    print(f'Se guarda el archivo en {processing_file} con las columnas {houses_for_sale.columns}')

    houses_for_sale.to_csv(destination_file, index=False)

    upload_data(processing_file, destination_file)


def data_processor(df, columnas, columna_target):
    
    # Inicializa una lista vacía llamada 'lista_dics_datos' para almacenar los datos transformados.
    lista_dics_datos = []
    
    # Itera a través de las filas del DataFrame 'df'.
    for _, row in df.iterrows():
        
        # Extrae el 'property_id' pues es unico en el dataframe de la fila actual.
        property_id = row['property_id']
            
        # Itera a través de los elementos en la columna especificada por 'columna_target'.
        for elemento in row[columna_target]:
            
            if elemento is not None and not (isinstance(elemento, float) and math.isnan(elemento)):
    
                try:
                    # Crea un nuevo diccionario 'elemento_data' que contiene la información de la fila actual.
                    elemento_data = eval(elemento)
                except:
                    elemento_data = elemento
            else:
                elemento_data = {}
                
            # Agrega información adicional al diccionario 'elemento_data'.
            elemento_data['property_id'] = property_id       
                       
            # Agrega el diccionario 'review_data' a la lista 'listadatos'.
            lista_dics_datos.append(elemento_data)

    # Crea un nuevo DataFrame 'df_limpio' a partir de la lista 'lista_dics_datos'
    # utilizando las columnas especificadas en 'columnas'.
    df_limpio = pd.DataFrame(lista_dics_datos, columns=columnas)
    
    # Devuelve el nuevo DataFrame 'df_limpio'.
    df_limpio.to_csv('/tmp/data_processor', index=False)



def drop_description():

    destination_file = download_data(processing_file)
    print(f'leyendo el archivo {processing_file}')
    houses_for_sale = pd.read_csv(destination_file)

    print(houses_for_sale.columns)
    
    

    #separar la columna description para poder transformarla
    df = houses_for_sale.groupby('property_id')['description'].apply(list).reset_index()

    #definir una lista de columnas
    columns = ['property_id']

    #añadir las columnas que estan dentro de description a la base de datos como columnas
    columns +=[x for x in eval(df['description'][0][0]).keys()]

    #aplicar la funcion de desanidar 
    df_temporal = data_processor(df,columns,'description')
    df_temporal = pd.read_csv('/tmp/data_processor')
    #unir los datasets
    df_final = pd.merge(df_temporal,houses_for_sale, on='property_id')
    
    #eliminar la columna description pues ya se desanido
    dropeable_columns = ['description']
    df_final.drop(dropeable_columns,axis=1, inplace = True)
    
    #funcion para convretir las fechas a datetime 

    def convert_date(text):
        try:
            return pd.to_datetime(text, errors='raise')
        except ValueError:
            return np.nan

    #eliminar las columnas que ya no se usaran 
    df_final['sold_date']=df_final['sold_date'].apply(convert_date)

    columns_to_drop = ['sold_date', 'sold_price', 'name','sub_type']
    df_final.drop(columns_to_drop,axis=1, inplace= True)

    #Quitar las letras de la fecha en list_date
    df_final['list_date']=(df_final['list_date'].astype(str).str.split('T')).str[0]

    #convertir list_date eb datetime
    df_final['list_date'] = df_final['list_date'].apply(convert_date)
    print(df_final.columns)

    df_final.to_csv(destination_file, index=False)

    upload_data(processing_file, destination_file)



def unraveler(df, target_columns:list):
    #crear una lista vacia para almacenar los datasets
    dfs_temporales =[]
    
    #iterar sobre las columnas de target_columns
    for column in target_columns:
        
        #crear un dataset con agrupado por property id pero de acuerdo a cada columna de la lista de columnas
        df_grouped = df.groupby('property_id')[column].apply(list).reset_index()
        
        #crear una lista de nombres de columnas
        columns = ['property_id']
        
        #aumentar en cada lista los valores que estan dentro de los diccionarios anidados
        try:
            columns +=[x for x in eval(df_grouped[column][0][0]).keys()]
        except:
            columns +=[x for x in (df_grouped[column][0][0]).keys()]
        
        #aplicar la funcion para desanidar
        df_temporal = data_processor(df_grouped,columns,column)
        df_temporal = pd.read_csv('/tmp/data_processor')
        dfs_temporales.append(df_temporal)
    
    # combinar los dataframes en uno nuevo
    resultado = reduce(lambda left, right: pd.merge(left, right, on='property_id'), dfs_temporales)
    
    #combinar los dataframe por la columna property_id
    resultado = pd.merge(resultado,df, on='property_id')

    resultado.to_csv('/tmp/data_unraveler', index=False)


def unraveler_flagsLoc():

    destination_file = download_data(processing_file)

    print(f'leyendo el archivo {processing_file}')
    df_final = pd.read_csv(destination_file)
    print(df_final.columns)

    #columnas a convertir
    columns = ['flags','location']

    #aplicar la funcion para desanidar
    df_final = unraveler(df_final, columns)
    df_final = pd.read_csv('/tmp/data_unraveler')


    #eliminar columnas nuevas que salieron de los diccionarios que se desanidaron
    df_final.drop(["is_new_listing","is_pending","is_subdivision","flags","location"],axis=1, inplace = True)                      ##'flags','location',


    # Utiliza expresiones regulares para encontrar todos los números en la cadena.
    def extraer_numeros(cadena):

        # El patrón \d+\.\d+|\d+ busca números enteros o decimales en la cadena.
        numeros_encontrados = re.findall(r'\d+\.\d+|\d+', str(cadena))

        # Si se encuentran números en la cadena:
        if numeros_encontrados:
            # Une los números encontrados utilizando comas y devuelve la cadena resultante.
            return ','.join(numeros_encontrados)

        else:
            # Si no se encuentran números en la cadena, devuelve 0.
            return np.nan

    #la columa de baños se debe convertir a numeros
    print(df_final.columns)
    df_final["baths_consolidated"] = df_final["baths_consolidated"].apply(extraer_numeros)

    #la columa de baños se convierte en float
    df_final["baths_consolidated"] = df_final["baths_consolidated"].apply(float)

    #hacer una lista
    bath_columns= []
    #iterar en la columna para hacer la lista
    for column in df_final.columns:
        if 'bath' in column:
            bath_columns.append(column)

    bath_columns.remove("baths_consolidated")
    #Eliminar las columnas de que tengan bath
    df_final.drop(bath_columns,axis=1, inplace=True)

    #renombar la columna
    df_final=df_final.rename(columns={"baths_consolidated":"baths"})

    df_final.to_csv(destination_file, index=False)

    upload_data(processing_file, destination_file)
    

def safe_eval(expression):
        try:
            result = eval(expression)
            return result
        except Exception as e:

            if expression is np.nan:
                return expression
            else:
            
                print(f"Error en la evaluación: {e}, and was given {expression}")
                return expression
            
def tags():

    destination_file = download_data(processing_file)
    print(f'leyendo el archivo {processing_file}')
    df_final = pd.read_csv(destination_file)
    print(df_final.columns)

##funcion para evaluar los nan en la columna tags 
    #se aplica la funcion a la columna tags
    df_final['tags'] = df_final['tags'].apply(safe_eval)

    df_final.to_csv(destination_file, index=False)
    upload_data(processing_file, destination_file)


def unraveler_addressCountyLoc():

    destination_file = download_data(processing_file)
    
    print(f'leyendo el archivo {processing_file}')
    df_final = pd.read_csv(destination_file)
    print(df_final.columns)
    


    #columnas a convertir
    columns = ["address","county"]

    #aplicar la funcion para desanidar
    df_final = unraveler(df_final, columns)
    df_final = pd.read_csv('/tmp/data_unraveler')

    #aplicar a la columna que se desanido a partir de adress
    df_final = unraveler(df_final,['coordinate'])
    df_final = pd.read_csv('/tmp/data_unraveler')

    #eliminar las columnas que ya se trabajaron
    df_final.drop(['coordinate','county','address'],axis=1, inplace=True)


    #aplicar la funcion safe eval para preparar la columna photos para desanidarla
    df_final['photos'] = df_final['photos'].apply(safe_eval)

    #funcion para obtener de la columna photos el segundo link de la foto, le primero ya se tiene en la columna photo
    def get_second_photo(list):
        try:
            #toma el segundo link de foto
            list=list[1]
            #toma el link sin la palabra href previo al url
            list=list['href']
            return list
            #sino encuentra un segundo link, toma el primero
        except:
            try:
                list=list[0]
                list = list['href']
                return list 
            except:
            
                return list

    #aplicar la funcion para sacar la segunda foto    
    df_final['photos'] = df_final['photos'].apply(get_second_photo)
    
    def get_photo(list):
        try:
            list = list['href']
            return list 
        except:
            
            return list

    #aplicar la funcion a la columna primary photo
    df_final['primary_photo'] = df_final['primary_photo'].apply(safe_eval)
    df_final['primary_photo'] = df_final['primary_photo'].apply(get_photo)
    df_final.set_index('property_id',inplace=True)
    df_final.dropna(subset='lon', inplace=True)
    df_final.dropna(subset='fips_code', inplace=True)
    #guardo el dataframe en un csv temporal

        
    df_final.to_csv(destination_file)
    upload_data(file, destination_file)
    


    
    



default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': True,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2023, 10, 13),
    'retries': 4,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=.5),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 1,
         schedule_interval = '0 1 * * 0') as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    t_begin = DummyOperator(task_id="begin")
    


  
    task_drop_columns_1 = PythonOperator(task_id='task_drop_columns_1',
                                 provide_context=True,
                                 python_callable=drop_columns_1,
                                 depends_on_past=True,

                                    dag=dag

                                 )

    task_drop_description = PythonOperator(task_id='task_drop_description',
                                 provide_context=True,
                                 python_callable=drop_description,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )
    
    task_unraveler_flagsLoc = PythonOperator(task_id='task_unraveler_flagsLoc',
                                 provide_context=True,
                                 python_callable=unraveler_flagsLoc,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )
    task_tags = PythonOperator(task_id='task_tags',
                                 provide_context=True,
                                 python_callable=tags,
                                 depends_on_past=True,

                                    dag=dag
                                 )
    task_unraveler_addressCountyLoc = PythonOperator(task_id='task_unraveler_addressCountyLoc',
                                 provide_context=True,
                                 python_callable=unraveler_addressCountyLoc,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )


    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_drop_columns_1 >> task_drop_description >> task_unraveler_flagsLoc >>task_tags >> task_unraveler_addressCountyLoc >>  t_end