import requests
import pandas as pd

def places_search_function(request):
    try:
        api_key = "AIzaSyDet8j-BV2xIK98IeLDN8vrEYAIwoAR-wA"
        query = "restaurant Miami"
        url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={api_key}"

        all_results = []  # Almacenará todos los resultados

        while True:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                # Extraer los resultados de la consulta actual
                results = data.get('results', [])
                all_results.extend(results)  # Agregar los resultados actuales a la lista

                # Verificar si hay más páginas de resultados
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    break  # No hay más páginas, terminar el bucle

                # Esperar un breve período de tiempo antes de hacer la siguiente solicitud (para cumplir con las políticas de uso)
                import time
                time.sleep(2)

                # Configurar la URL para la siguiente página
                url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?pagetoken={next_page_token}&key={api_key}"
            else:
                return "Error en la consulta", 500

        # Crear un DataFrame con todos los resultados
        df = pd.DataFrame(all_results)

        # Guardar el DataFrame en BigQuery
        dataset = 'API_Google_Place.'
        table_name = 'Metadata'

        df = df.astype(str)

        df.to_gbq(destination_table=dataset + table_name,
                  project_id='pg-yelp-gmaps2',
                  table_schema=None,
                  if_exists='append', progress_bar=False, auth_local_webserver=False, location='us')

        return 'Datos guardados en BigQuery con éxito', 200
    except Exception as e:
        return f"Error en la función: {str(e)}", 500

