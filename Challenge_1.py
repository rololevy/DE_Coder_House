import pandas as pd
import requests
import time
import json
import psycopg2
from sqlalchemy import create_engine

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

    # Simular una limpieza de datos y agregar el timestamp a cada personaje
    current_timestamp = int(time.time())  # Obtener el timestamp actual en segundos
    for personaje in personajes:
        # Simulación de limpieza de datos
        personaje['species'] = personaje['species'].replace('Unknown', 'N/A')
        personaje['type'] = personaje['type'].replace('Unknown', 'N/A')
        # Convertir diccionarios a strings en las columnas 'origin' y 'location'
    
        # Agregar el timestamp al personaje
        personaje['timestamp'] = current_timestamp

        # Agregar el personaje a la lista
        lista_detalles_personajes.append(personaje)

    # Crear un DataFrame de Pandas a partir de la lista de diccionarios con los detalles de los personajes
    df = pd.DataFrame.from_records(lista_detalles_personajes)

    # Mostrar las columnas del DataFrame
    print(df.columns)

    df['origin'] = df['origin'].apply(json.dumps)
    df['location'] = df['location'].apply(json.dumps)
    df['origin'] = df['origin'].astype(str)
    df['location'] = df['location'].astype(str)

    # Mostrar el DataFrame
    print(df)

    # Guardar el DataFrame en un archivo CSV
    df.to_csv('rick_and_morty_characters.csv', index=False)
    
    # Insertar los datos en la base de datos (utilizando Pandas y SQLAlchemy)

    connection_params = {
        "dbname": " ",
        "user": "",
        "password": "",
        "host": "",
        "port": ""
    }

    engine = create_engine(
        f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['dbname']}",
        connect_args={"sslmode": "require"}
    )
    
    df.to_sql('rick_and_morty_v3', engine, if_exists='append', index=False)

    print("Datos insertados exitosamente en la tabla.")
else:
    print('Error en la solicitud. Código de estado:', response.status_code)
