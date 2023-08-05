import pandas as pd
import requests
import time
import json
import psycopg2
from sqlalchemy import create_engine, types
from datetime import datetime

def get_character_data(url, num_pages=1):
    all_results = []  # Lista para almacenar los resultados de todas las páginas

    for page in range(1, num_pages + 1):
        response = requests.get(url, params={'page': page})
        if response.status_code == 200:
            datos_json = response.json()
            results = datos_json['results']
            all_results.extend(results)  # Agregar los resultados de la página actual a la lista
        else:
            print('Error en la solicitud:', response.status_code)

    return all_results

def clean_character_data(data):
    df = pd.DataFrame.from_records(data)
    df_cleaned = df.drop_duplicates(subset=['name', 'species', 'type'], keep='last')
    df_cleaned['created'] = pd.to_datetime(df_cleaned['created'])
    problematic_columns = ['origin', 'location', 'episode', 'url', 'image']
    df_cleaned = df_cleaned.drop(columns=problematic_columns)
    current_timestamp = datetime.now()
    current_timestamp_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    df_cleaned['timestamp'] = current_timestamp_str
    return df_cleaned

def insert_data_into_redshift(df_cleaned, connection_params):
    engine = create_engine(
        f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['dbname']}"
    )
    df_cleaned.to_sql('rick_and_morty_v3', engine, if_exists='append', index=False)

def main():
    url = 'https://rickandmortyapi.com/api/character'
    data = get_character_data(url)
    if data is not None:
        df_cleaned = clean_character_data(data)
        print(df_cleaned)
        df_cleaned.to_csv('rick_and_morty_characters_cleaned.csv', index=False)

        with open('config.json') as f:
            config = json.load(f)

        # Usar las credenciales del archivo de configuración
        connection_params = {
            'dbname': config['dbname'],
            'user': config['user'],
            'password': config['password'],
            'host': config['host'],
            'port': config['port']
        }

        insert_data_into_redshift(df_cleaned, connection_params)

        print("Datos insertados exitosamente en la tabla.")
    else:
        print('Hubo un error en la solicitud.')

if __name__ == "__main__":
    main()
