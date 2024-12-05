import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import requests
import json
import time
import datetime

# ----------------------------------------------------------------------------------------------------------
# Configuración de la API de OpenWeather
# ----------------------------------------------------------------------------------------------------------
API_KEY = "576eb84d843ccd2c3c97408e61b5b0ce"  # Reemplaza con tu API Key
CITY = "Toledo"  # Cambia por la ciudad deseada
WEATHER_URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# ----------------------------------------------------------------------------------------------------------
# Configuración de Cosmos DB - Definición directa de las credenciales
# ----------------------------------------------------------------------------------------------------------
HOST = "https://cosmosdbsqlexamen4.documents.azure.com:443/"  # Reemplaza con tu URL de Cosmos DB
MASTER_KEY = "x1yyEXksUztP5xWHaD5Ocmh40V8aEFljYoEhTlnGbIB5Le1iWOUxW9ctyorDudaqVok4kcamW5yxACDb3bJdlA=="  # Reemplaza con tu clave maestra
DATABASE_ID = "cosmosdbsqlexamen4"  # Nombre de la base de datos
CONTAINER_ID = "container1"  # Nombre del contenedor

def get_weather_data():
    try:
        response = requests.get(WEATHER_URL)
        if response.status_code == 200:
            weather_data = response.json()
            # Formatear los datos para Cosmos DB
            formatted_data = {
                "id": str(time.time()),  # Usamos un timestamp como id único
                "city": weather_data["name"],
                "temperature": weather_data["main"]["temp"],
                "weather": weather_data["weather"][0]["description"],
                "humidity": weather_data["main"]["humidity"],
                "pressure": weather_data["main"]["pressure"],
                "wind_speed": weather_data["wind"]["speed"],
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }
            return formatted_data
        else:
            print(f"Error al obtener los datos: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error al conectar con la API: {e}")
        return None

def insert_weather_data(container, data):
    try:
        container.create_item(body=data)  # Insertar un documento en el contenedor
        print(f"Documento insertado con id: {data['id']}")
    except exceptions.CosmosHttpResponseError as e:
        print(f"Error al insertar datos en Cosmos DB: {e.message}")

def run_sample():
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
    try:
        # Conectar o crear la base de datos
        try:
            db = client.create_database(id=DATABASE_ID)
            print(f'Database with id \'{DATABASE_ID}\' created')
        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
            print(f'Database with id \'{DATABASE_ID}\' was found')

        # Conectar o crear el contenedor
        try:
            container = db.create_container(id=CONTAINER_ID, partition_key=PartitionKey(path='/city'))
            print(f'Container with id \'{CONTAINER_ID}\' created')
        except exceptions.CosmosResourceExistsError:
            container = db.get_container_client(CONTAINER_ID)
            print(f'Container with id \'{CONTAINER_ID}\' was found')

        # Bucle para obtener datos meteorológicos e insertarlos en Cosmos DB
        while True:
            weather_data = get_weather_data()
            if weather_data:
                print("Datos obtenidos del clima:")
                print(weather_data)
                try:
                    insert_weather_data(container, weather_data)
                except Exception as e:
                    print(f"Error al insertar datos en Cosmos DB: {e}")
                print("Datos insertados exitosamente en Cosmos DB")
            else:
                print("No se pudieron obtener los datos del clima.")

            # Pausa de 20 segundos entre las iteraciones
            time.sleep(20)

    except exceptions.CosmosHttpResponseError as e:
        print(f'\nrun_sample has caught an error: {e.message}')
    finally:
        print("\nrun_sample done")


if __name__ == '__main__':
    run_sample()
