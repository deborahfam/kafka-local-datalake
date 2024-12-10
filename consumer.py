from kafka import KafkaConsumer
import json
import os, time

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    'data_lake_topic',
    bootstrap_servers=['localhost:9092'],  # Cambia localhost:9092 por tu servidor Kafka
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Carpeta para almacenar los datos en el sistema de archivos local
DATA_LAKE_DIR = "./data_lake"

# Asegúrate de que la carpeta existe
os.makedirs(DATA_LAKE_DIR, exist_ok=True)

def save_to_local(data, file_name="population_data.json"):
    """Guarda los datos en un archivo local."""
    file_path = os.path.join(DATA_LAKE_DIR, file_name)
    try:
        with open(file_path, "a") as file:
            for record in data:
                json_str = json.dumps(record)
                print(f"Guardando registro: {json_str}")  # Debug log
                file.write(json_str + "\n")
        print(f"Datos guardados exitosamente en {file_path}")
    except Exception as e:
        print(f"Error al guardar datos: {str(e)}")

if __name__ == "__main__":
    print("Iniciando consumer en bucle continuo...")
    buffer = []
    last_save_time = time.time()

    while True:
        try:
            # Consumir mensajes con timeout
            messages = consumer.poll(timeout_ms=1000)
            
            for topic_partition, records in messages.items():
                for record in records:
                    print(f"Recibido de Kafka: {record.value}")
                    buffer.append(record.value)
            
            # Verificar si han pasado 10 segundos
            current_time = time.time()
            if current_time - last_save_time >= 10:
                if buffer:
                    save_to_local(buffer)
                    buffer = []
                last_save_time = current_time
            
            time.sleep(1)  # Pequeña pausa para no saturar CPU
            
        except Exception as e:
            print(f"Error en el consumer: {str(e)}")
