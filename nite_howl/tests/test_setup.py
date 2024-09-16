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
    group_id = "tmp_3"
    topic = "molina"
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([topic])
    yield consumer
    consumer.close()

def test_send_message(kafka_producer):
    topic = "molina"
    key = "mask"
    test_message = "Mas"
    headers = {"subregistry": "BS"}
    
    # Enviar un mensaje
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    kafka_producer.produce(topic, test_message, key=key, headers=headers, callback=delivery_report)
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
    topic = "molina"
    test_message = "Mas"
    key = "mask"
    headers = {"subregistry": "BS"}
    timeout = 10
    received_message = None
    received_topic = None
    received_key = None
    received_headers = None
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
            
            received_key = msg.key().decode('utf-8')
            if  key != msg.key().decode('utf-8'):
                pytest.fail(f"Se esperaba el key '{key}', pero se recibió '{received_key}'")
            else:
                received_message = msg.value().decode('utf-8')  # Decodifica el mensaje a string
                received_topic = msg.topic()
                received_headers = {k: v.decode('utf-8') for k, v in msg.headers()}
                break
            
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