import pytest
from nite_howl import NiteHowl
from confluent_kafka import KafkaException


@pytest.fixture
def setup_kafka():
    broker = "localhost:9092"
    group_id = "test_group"
    topic = "test_topic"
    key = "test_key"
    headers = {"test_header": "value"}
    
    # Instancia del consumidor
    kafka = NiteHowl(broker=broker, group=group_id, topics=topic, key=key, headers=headers)
    yield kafka
    
    # Cleanup if necessary

def test_consume_message(setup_kafka):
    kafka = setup_kafka
    timeout = 1.0
    topic = "test_topic"
    key = "test_key"
    headers = {"test_header": "value"}
    test_message = "Hello!!!!!!!!!"
    
    received_message = None
    received_topic = None
    received_key = None
    received_headers = None

    try:
        # Consumir mensaje usando NiteHowl
        radar_gen = kafka.radar(timeout)

        # Iterar sobre el generador para obtener el mensaje
        for table, received_topic, received_key, received_headers in radar_gen:
            received_message = table.to_pandas().iloc[0]['column2']
            break  # Salir del bucle después de recibir el primer mensaje

    except KafkaException as e:
        pytest.fail(f"Error en el consumidor de Kafka: {e}")

    # Validar el mensaje recibido
    assert received_message == test_message, f"Se esperaba el mensaje '{test_message}', pero se recibió '{received_message}'"
    assert received_topic == topic, f"Se esperaba el topic '{topic}', pero se recibió '{received_topic}'"
    assert received_key == key, f"Se esperaba el key '{key}', pero se recibió '{received_key}'"
    assert received_headers == headers, f"Se esperaba el header '{headers}', pero se recibió '{received_headers}'"