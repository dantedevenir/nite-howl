import pytest
import time
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

@pytest.fixture
def kafka_admin():
    broker = "localhost:9092"
    return AdminClient({'bootstrap.servers': broker})

@pytest.fixture
def kafka_producer():
    broker = "localhost:9092"
    producer = Producer({'bootstrap.servers': broker})
    yield producer
    producer.flush()

@pytest.fixture
def kafka_consumer():
    broker = "localhost:9092"
    group_id = "test-group"
    topic = "test-topic"
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    yield consumer
    consumer.close()

def test_send_message(kafka_producer):
    topic = "test-topic"
    test_message = "Hello, Kafka!"
    
    # Enviar un mensaje
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    kafka_producer.produce(topic, test_message, callback=delivery_report)
    kafka_producer.flush()

    # Agregar un pequeño retraso para asegurar que el mensaje se procese
    time.sleep(5)

def test_broker_exists(kafka_admin):
    try:
        cluster_metadata = kafka_admin.list_topics(timeout=5)
        assert cluster_metadata.brokers, "No se pudo conectar al broker de Kafka"
        print(f"Brokers encontrados: {cluster_metadata.brokers}")
        print(f"Topics disponibles: {list(cluster_metadata.topics)}")
    except KafkaException as e:
        pytest.fail(f"Error al conectarse al broker de Kafka: {e}")


def test_receive_message(kafka_consumer):
    topic = "test-topic"
    test_message = "Hello, Kafka!"
    timeout = 30
    received_message = None
    try:
        while True:
            msg = kafka_consumer.poll(timeout)
            if msg is None:
                pytest.fail(f"No se encontraron mensajes en el topic '{topic}' en {timeout} segundos")
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                received_message = msg.value().decode('utf-8')  # Decodifica el mensaje a string
                break
    except KafkaException as e:
        pytest.fail(f"Error en el consumidor de Kafka: {e}")

    assert received_message == test_message, f"Se esperaba '{test_message}', pero se recibió '{received_message}'"
    print(f"Mensaje recibido: {received_message}")
