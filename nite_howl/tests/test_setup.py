import logging
import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
import sys
sys.stdout = sys.stderr
#pytest -vvv -s -n auto --dist=loadscope --tb=short --maxfail=1 --disable-warnings -v
logging.basicConfig(format='%(message)s')

@pytest.fixture
def kafka_admin():
    broker = "localhost:9092"
    return AdminClient({'bootstrap.servers': broker})

def test_broker_exists(kafka_admin):
    try:
        cluster_metadata = kafka_admin.list_topics(timeout=5)
        assert cluster_metadata.brokers, "No se pudo conectar al broker de Kafka"
        print(f"Brokers encontrados: {cluster_metadata.brokers}")
        print(f"Topics disponibles: {list(cluster_metadata.topics)}")
    except KafkaException as e:
        pytest.fail(f"Error al conectarse al broker de Kafka: {e}")
