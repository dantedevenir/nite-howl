import pytest
from nite_howl import NiteHowl
from confluent_kafka import KafkaException
import pandas as pd
import sys
sys.stdout = sys.stderr

@pytest.fixture
def kafka_consumer():
    broker = "localhost:9092"
    group_id = "test_group"
    topic = ["molina"]
    key = "mask"
    headers = {"subregistry": "BS"}
    
    # Instancia del consumidor
    kafka = NiteHowl(broker=broker, group=group_id, topics=topic, key=key, headers=headers)
    yield kafka

def test_receive_message(kafka_consumer):
        received_message = None
        received_topic = None
        received_key = None
        received_headers = None
        timeout = 10
        test_message = "Mas"

        try:
            # Consumir mensaje usando NiteHowl
            radar_gen = kafka_consumer.radar(timeout)

            # Iterar sobre el generador para obtener el mensaje
            while True:
                try:
                    table, topic, key, headers, type = next(radar_gen)
                    if type == pd.DataFrame:
                        received_message = table.iloc[0]['column2']
                    elif type == str:
                        received_message = table
                    else:
                        pytest.fail(f"Error en el consumidor de Kafka: {e}")
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