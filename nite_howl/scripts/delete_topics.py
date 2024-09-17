from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

def delete_all_topics(broker):
    # Crear un cliente de administración
    admin_client = AdminClient({'bootstrap.servers': broker})

    # Obtener la lista de topics
    try:
        # Listar todos los topics
        metadata = admin_client.list_topics(timeout=10)
        topics = list(metadata.topics.keys())  # Convertir dict_keys a lista

        if not topics:
            print("No hay topics para eliminar.")
            return

        # Eliminar todos los topics
        fs = admin_client.delete_topics(topics, operation_timeout=30)
        for topic, f in fs.items():
            try:
                f.result()  # Espera a que se complete la operación
                print(f"El topic '{topic}' ha sido eliminado.")
            except KafkaException as e:
                print(f"Error al eliminar el topic '{topic}': {e}")
    except KafkaException as e:
        print(f"Error al listar los topics: {e}")

if __name__ == "__main__":
    # Reemplaza 'localhost:9092' con la dirección de tu broker Kafka
    delete_all_topics('localhost:9092')
