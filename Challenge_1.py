pip install pandas
pip install requests
import pandas as pd
import requests

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

    # Mostrar el DataFrame
    print(df)

    # Guardar el DataFrame en un archivo CSV
    df.to_csv('rick_and_morty_characters.csv', index=False)
else:
    print('Error en la solicitud. Código de estado:', response.status_code)
