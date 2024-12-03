from kafka import KafkaConsumer
import json
import os

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    'data_lake_topic',
    bootstrap_servers=['localhost:9092'],  # Cambia localhost:9092 por tu servidor Kafka
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Carpeta para almacenar los datos en el sistema de archivos local
DATA_LAKE_DIR = "./data_lake"

# Asegúrate de que la carpeta existe
os.makedirs(DATA_LAKE_DIR, exist_ok=True)

def save_to_local(data, file_name="population_data.json"):
    """Guarda los datos en un archivo local."""
    file_path = os.path.join(DATA_LAKE_DIR, file_name)
    with open(file_path, "a") as file:
        for record in data:
            file.write(json.dumps(record) + "\n")
    print(f"Datos guardados en {file_path}")

if __name__ == "__main__":
    buffer = []
    batch_size = 100  # Guarda los datos en lotes de 100
    for message in consumer:
        print(f"Recibido de Kafka: {message.value}")
        buffer.append(message.value)
        if len(buffer) >= batch_size:
            save_to_local(buffer)
            buffer = []
    # Guarda cualquier dato restante
    if buffer:
        save_to_local(buffer)
