import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime

def get_character_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        datos_json = response.json()
        return datos_json['results']
    else:
        print('Error en la solicitud:', response.status_code)
        return None

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

        connection_params = {
            'dbname': 'data-engineer-database',
            'user': 'o_oaguilera_coderhouse',
            'password': 'lVUM508vJ9',
            'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            'port': '5439'
        }
        insert_data_into_redshift(df_cleaned, connection_params)

        print("Datos insertados exitosamente en la tabla.")
    else:
        print('Hubo un error en la solicitud.')

if __name__ == "__main__":
    main()
