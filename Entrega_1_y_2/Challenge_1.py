import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime

# Realizar la solicitud GET a la API para obtener la lista de personajes
url = 'https://rickandmortyapi.com/api/character'
response = requests.get(url)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    # Extraer los datos JSON de la respuesta
    datos_json = response.json()

    # Obtener la lista de personajes
    personajes = datos_json['results']

    # Crear una lista para almacenar la información detallada de cada personaje
    lista_detalles_personajes = []

    # Iterar sobre la lista de personajes y obtener información detallada de cada uno
    for personaje in personajes:
        url_personaje = personaje['url']
        response_personaje = requests.get(url_personaje)
        if response_personaje.status_code == 200:
            detalle_personaje = response_personaje.json()
            lista_detalles_personajes.append(detalle_personaje)
        else:
            print('Error en la solicitud del personaje:', personaje['name'])

    # Crear un DataFrame de Pandas a partir de la lista de diccionarios con los detalles de los personajes
    df = pd.DataFrame.from_records(lista_detalles_personajes)

    # Realizar la limpieza de datos y eliminar duplicados
    df_cleaned = df.drop_duplicates(subset=['name', 'species', 'type'], keep='last')

    # Convertir el campo 'created' a formato de fecha y hora
    df_cleaned['created'] = pd.to_datetime(df_cleaned['created'])

    # Identificar las columnas problemáticas que generan el error "standard_conforming_strings"
    problematic_columns = ['origin', 'location', 'episode','url','image']

    # Eliminar las columnas problemáticas del DataFrame df_cleaned
    df_cleaned = df_cleaned.drop(columns=problematic_columns)

    # Mostrar el DataFrame limpio
    print(df_cleaned)

    # Guardar el DataFrame limpio en un archivo CSV
    df_cleaned.to_csv('rick_and_morty_characters_cleaned.csv', index=False)


    connection_params = {
    'dbname': 'd,
    'user': 'o',
    'password': '',
    'host': 'd',
    'port': ''
}

    # Insertar los datos limpios en la base de datos (utilizando Pandas y SQLAlchemy)
    engine = create_engine(
        f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['dbname']}"
    )

    df_cleaned.to_sql('rick_and_morty_v3', engine, if_exists='append', index=False)


    print("Datos insertados exitosamente en la tabla.")
else:
    print('Error en la solicitud. Código de estado:', response.status_code)
