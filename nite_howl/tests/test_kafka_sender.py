import pytest
import pandas as pd
from nite_howl import NiteHowl
import time
import sys
sys.stdout = sys.stderr

@pytest.fixture
def kafka_producer():
    broker = "localhost:9092"
    kafka = NiteHowl(broker=broker)
    yield kafka
    
def test_send_message(kafka_producer):
    topic = "molina"
    key = "mask"
    test_message = "Mas"
    headers = {"subregistry": "BS"}
    df = pd.DataFrame({'column1': [1], 'column2': [test_message]})
    
    
    time.sleep(5)
    kafka_producer.send(topic=topic, msg=df, key=key, headers=headers)
    print(f"Mensaje enviado: {test_message}")
    print(f"Topic enviado: {topic}")
    print(f"Key enviado: {key}")
    print(f"Headers enviado: {headers}")