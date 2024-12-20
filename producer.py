from kafka import KafkaProducer
import requests
import json, time

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Cambia localhost:9092 por tu servidor Kafka
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# URL de la API gratuita
API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

def fetch_data():
    """Obtiene datos de la API."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        print("Datos obtenidos de la API.")
        return response.json()['data']
    else:
        print(f"Error al obtener datos de la API: {response.status_code}")
        return None

def send_to_kafka(data):
    """Envía datos al tema de Kafka."""
    try:
        for record in data:
            producer.send('data_lake_topic', value=record)
            print(f"Enviado a Kafka: {record}")
        producer.flush()
    except Exception as e:
        print(f"Error al enviar a Kafka: {str(e)}")

if __name__ == "__main__":
    print("Iniciando producer en bucle continuo...")
    while True:
        data = fetch_data()
        if data:
            send_to_kafka(data)
        print("Esperando 10 segundos para la siguiente lectura...")
        time.sleep(10)
