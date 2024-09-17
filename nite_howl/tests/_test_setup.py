import asyncio
import time
import pytest
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from nite_howl import NiteHowl
import pandas as pd

@pytest.fixture
def kafka_admin():
    broker = "localhost:9092"
    return AdminClient({'bootstrap.servers': broker})
 
@pytest.fixture
def kafka_producer():
    broker = "localhost:9092"
    kafka = NiteHowl(broker=broker)
    yield kafka

@pytest.fixture
def kafka_consumer():
    broker = "localhost:9092"
    group_id = "test_group"
    topic = "molina"
    key = "mask"
    headers = {"subregistry": "BS"}
    
    # Instancia del consumidor
    kafka = NiteHowl(broker=broker, group=group_id, topics=topic, key=key, headers=headers)
    yield kafka

def test_broker_exists(kafka_admin):
    try:
        cluster_metadata = kafka_admin.list_topics(timeout=5)
        assert cluster_metadata.brokers, "No se pudo conectar al broker de Kafka"
        print(f"Brokers encontrados: {cluster_metadata.brokers}")
        print(f"Topics disponibles: {list(cluster_metadata.topics)}")
    except KafkaException as e:
        pytest.fail(f"Error al conectarse al broker de Kafka: {e}")

@pytest.mark.asyncio
async def test_send_message(kafka_producer, kafka_consumer):
    topic = "molina"
    key = "mask"
    test_message = "Mas"
    headers = {"subregistry": "BS"}
    df = pd.DataFrame({'column1': [1], 'column2': [test_message]})
    timeout = 10

    async def test_receive_message():
        received_message = None
        received_topic = None
        received_key = None
        received_headers = None

        try:
            # Consumir mensaje usando NiteHowl
            radar_gen = kafka_consumer.radar(timeout)

            # Iterar sobre el generador para obtener el mensaje
            while True:
                try:
                    table, topic, key, headers = next(radar_gen)
                    received_message = table.to_pandas().iloc[0]['column2']
                    received_topic = topic
                    received_key = key
                    received_headers = headers
                    break  # Salir del bucle después de recibir el primer mensaje
                except StopIteration:
                    break  # Salir si no hay más elementos

        except KafkaException as e:
            pytest.fail(f"Error en el consumidor de Kafka: {e}")

        assert received_message == test_message, f"Se esperaba el mensaje '{test_message}', pero se recibió '{received_message}'"
        assert received_topic == topic, f"Se esperaba el topic '{topic}', pero se recibió '{received_topic}'"
        assert received_key == key, f"Se esperaba el key '{key}', pero se recibió '{received_key}'"
        assert received_headers == headers, f"Se esperaba el header '{headers}', pero se recibió '{received_headers}'"
        print(f"Mensaje recibido: {received_message}")
        print(f"Topic recibido: {received_topic}")
        print(f"Key recibido: {received_key}")
        print(f"Headers recibido: {received_headers}")

    # Crear una tarea para `test_receive_message` y comenzar a ejecutarla
    receive_task = asyncio.create_task(test_receive_message())
    
    # Enviar el mensaje
    kafka_producer.send(topic=topic, df=df, key=key, headers=headers)
    
    # Esperar a que `test_receive_message` termine
    await receive_task