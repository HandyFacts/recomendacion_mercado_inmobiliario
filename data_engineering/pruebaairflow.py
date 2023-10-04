#importar las librerias
import pandas as pd
import numpy as np 
from functools import reduce
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt

#leer el archivo
houses_for_sale = pd.read_csv('Dataset_houses_for_sale.csv')

#columnas para eliminar con datos que no son relevantes
columns_to_drop=['source','permalink','other_listings','open_houses','branding','coming_soon_date','matterport','search_promotions',
                 'rent_to_own','products','virtual_tours','community','price_reduced_amount']

#eliminar las columas
houses_for_sale.drop(columns=columns_to_drop,inplace=True)

#buscar en la columna description por valores que esta duplicados

#crear una lista para guardas las filas de la columna description
descriptions = []
#crear lita de id que estan repetidoas
ids=[]
for index, row in houses_for_sale.groupby('property_id')['description'].apply(list).reset_index().iterrows():
    id=str(row['property_id'])
    
    if len(row['description']) > 1:

        descriptions.append(row['description'])
        ids.append(id)
        if len(ids) >2:
            break

#los id_producto que estan con filas duplicadas
ids

#ver que filas son las duplicadas
descriptions[0]


print("las ids duplicadas son", ids)
print("las filas duplicadas son",descriptions[0])

#ver que filas son duplicadas por id, son exactamente las mimas filas por eso hay que eliminar
houses_for_sale[houses_for_sale['property_id']== 1034050928]

#eliminar las columnas duplicadas
houses_for_sale.drop_duplicates(subset='property_id', inplace = True)
print("el tamaño del dataset es :",houses_for_sale.shape)

#definir una funcion para sacar los datos de las columnas que contiene cadenas que representan diccionarios

def data_processor(df, columnas, columna_target):
    
    # Inicializa una lista vacía llamada 'lista_dics_datos' para almacenar los datos transformados.
    lista_dics_datos = []
    
    # Itera a través de las filas del DataFrame 'df'.
    for _, row in df.iterrows():
        
        # Extrae el 'property_id' pues es unico en el dataframe de la fila actual.
        property_id = row['property_id']
            
        # Itera a través de los elementos en la columna especificada por 'columna_target'.
        for elemento in row[columna_target]:

            if elemento is not None:
                try:
                    # Crea un nuevo diccionario 'elemento_data' que contiene la información de la fila actual.
                    elemento_data = eval(elemento).copy()
                except:
                    elemento_data = elemento.copy()
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
    return df_limpio
# aplicar la funcion en la columna descripcion

#separar la columna description para poder transformarla
df = houses_for_sale.groupby('property_id')['description'].apply(list).reset_index()

#definir una lista de columnas
columns = ['property_id']

#añadir las columnas que estan dentro de description a la base de datos como columnas
columns +=[x for x in eval(df['description'][0][0]).keys()]

#aplicar la funcion de desanidar 
df_temporal = data_processor(df,columns,'description')

#unir los datasets
df_final = pd.merge(df_temporal,houses_for_sale, on='property_id')
df_final.head()

#eliminar las columnas que no sirven para nuestro analisis
dropeable_columns = ['last_update_date', 'description','lead_attributes', 'tax_record']
df_final.drop(columns=dropeable_columns, inplace = True)

# Utiliza expresiones regulares para encontrar todos los números en la cadena.
def extraer_numeros(cadena):
    
    # El patrón \d+\.\d+|\d+ busca números enteros o decimales en la cadena.
    numeros_encontrados = re.findall(r'\d+\.\d+|\d+', str(cadena))
    
    # Si se encuentran números en la cadena:
    if numeros_encontrados:
        # Une los números encontrados utilizando comas y devuelve la cadena resultante.
        return ','.join(numeros_encontrados)
    
    # Si no se encuentran números en la cadena, devuelve 0.
    return np.nan

#la columa de baños se debe convertir a numeros
df_final['baths_consolidated'] = df_final['baths_consolidated'].apply(extraer_numeros)

#la columa de baños se convierte en float
df_final['baths_consolidated'] = df_final['baths_consolidated'].apply(float)

#funcion para convretir las fechas a datetime
def convert_date(text):
    try:
        return pd.to_datetime(text, errors='raise')
    except ValueError:
        return np.nan
    
#aplicar la funciona a la columna sold_date
df_final['sold_date']=df_final['sold_date'].apply(convert_date)

#imprimir las columnas que son unicas
print(len(df_final['sold_date'].unique()))

#usar los datos de esta base de datos para entrenar el otro modelo de casas vendidas
df_final[df_final['sold_date'].notna()].to_csv('Df_sold_homes_to_train.csv')

#eliminar las columnas que ya no se usaran 
columns_to_drop = ['sold_date', 'sold_price', 'name','sub_type']
df_final.drop(columns=columns_to_drop, inplace= True)

#Quitar las letras de la fecha en list_date
df_final['list_date']=(df_final['list_date'].astype(str).str.split('T')).str[0]

#convertir list_date eb datetime
df_final['list_date'] = df_final['list_date'].apply(convert_date)


#desanidar columnas 

#mostar las columnas que son str
for column in df_final.columns:
    if type(df_final[column][2]) is str:
        print( column , df_final[column][2]+'\n')

#ver las columnas que tienen diccionarios por dentro
df_final[['primary_photo','tags','photos','flags','location']].head()

# la función unraveler toma un DataFrame y una lista de columnas. Para cada columna en la lista, realiza transformaciones y manipulaciones específicas, agrupa los resultados, desanida datos si es necesario y luego combina los resultados en un DataFrame final. Esta función proporciona una forma de estructurar y transformar los datos de acuerdo con las columnas especificadas en la lista target_columns

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
        dfs_temporales.append(df_temporal)
    
    # combinar los dataframes en uno nuevo
    resultado = reduce(lambda left, right: pd.merge(left, right, on='property_id'), dfs_temporales)
    
    #combinar los dataframe por la columna property_id
    resultado = pd.merge(resultado,df, on='property_id')
    return resultado

#columnas a convertir
columns = ['flags','location']
#aplicar la funcion para desanidar
df_final = unraveler(df_final, columns)

#limpieza de columnas, eliminar columnas 

#eliminar columnas nuevas que salieron de los diccionarios que se desanidaron
df_final.drop(columns=['is_new_listing','is_pending','flags','location',"is_subdivision"], inplace = True)

#se va a limpiar las columnas que contengan la palabra bath
#hacer una lista
bath_columns= []
#iterar en la columna para hacer la lista
for column in df_final.columns:
    if 'bath' in column:
        bath_columns.append(column)

#iterar en la lista creada para saber cuales tienen mas datos utiles en sus columnas
for column in bath_columns:
    #buscar en el dataframe tal que la columna baths_consolidated tenga datos nan para ver si se puede rellenar los faltantes
    for num in df_final[df_final['baths_consolidated'].isna()==True][column]:
        if num > 0:
            print(num)

#no existen datos validos por lo que se van a eliminar las columnas 

#Eliminar de la lista la columna baths_consolidated
bath_columns.remove('baths_consolidated')

#Eliminar las columnas de que tengan bath
df_final.drop(bath_columns, axis=1, inplace=True)

#renombar la columna
df_final=df_final.rename(columns={"baths_consolidated":"baths"})

#limpiar las columnas tags 

#ver que columnas son de tipo str
for column in df_final.columns:
    if type(df_final[column][2]) is str:
        print( f'{column}' , df_final[column][2]+'\n')

##funcion para evaluar los nan en la columna tags
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

#se aplica la funcion a la columna tags
df_final['tags'] = df_final['tags'].apply(safe_eval)

#poner en lista cada una de las filas de la columna tags
def get_tags(list):
    
    my_list = []
    try:
        for tag in list:
            my_list.append(tag)
        return my_list 
    except:
        return my_list
    
#Aplicar la funciona a cada fila de la columna tags
tags = []
#recorre cada fila del datafram para sacar los tags
for index, row in df_final.iterrows():
    
    tag=get_tags(row['tags'])

    tags+=tag

#imprimir los resultados
print("el total de tags es:",len(tags))
print("los tags unicos son:",len(set(tags)))

#definir los tags mas vendidos 



#se va a hacer un wordcloud para saber cuales son los tags mas usados
# Tu lista de palabras
lista_de_palabras = tags
# Convierte la lista en una cadena de texto
texto = " ".join(lista_de_palabras)

# Crea un objeto WordCloud
nube_de_palabras = WordCloud(width=800, height=400, background_color='white').generate(texto)

# Muestra la nube de palabras utilizando Matplotlib
plt.figure(figsize=(10, 5))
plt.imshow(nube_de_palabras, interpolation='bilinear')
plt.axis("off")
plt.show()

#para eliminar las tags que son las mas frecuentes 
Start_tags=0
current_tags=1
while Start_tags != current_tags:
    Start_tags=len(tags)
    for tag in tags:
        #eliminar las palabras garage y story pues ya son parte del dataframe
        if 'garage' in tag or 'story' in tag or 'stories' in tag:
            tags.remove(tag)
    current_tags=len(tags)

#definir los tags unicos que hay en tags
tags_unicos ={}
#recorrer los tags para sacar un diccionario con los nombres y cantidad de tags
for tag in tags:
    if tag not in tags_unicos.keys():
        tags_unicos[tag]=tags.count(tag)
    else:
        tags.remove(tag)

#poner el resultado del diccionario en un dataframe
top_50_tags = pd.DataFrame(tags_unicos.values(),tags_unicos.keys()).sort_values(by=0,ascending=False).head(50).rename(columns={0:'count'})

#desanidar las columnas que faltan 

#columnas a convertir
columns = ['address',"county"]

#aplicar la funcion para desanidar
df_final = unraveler(df_final, columns)

#aplicar a la columna que se desanido a partir de adress
df_final = unraveler(df_final,['coordinate'])

#eliminar las columnas que ya se trabajaron
df_final.drop(columns=['coordinate','county','address'], inplace=True)

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