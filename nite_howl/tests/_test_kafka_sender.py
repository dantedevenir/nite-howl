import pytest
import pandas as pd
from nite_howl import NiteHowl
import time

@pytest.fixture
def setup_kafka():
    broker = "localhost:9092"
    group_id = "test_group"
    topic = "test_topic"
    key = "test_key"
    headers = {"test_header": "value"}
    
    kafka = NiteHowl(broker=broker, group=group_id, topics=topic, key=key, headers=headers)
    yield kafka

def test_produce_message(setup_kafka):
    kafka = setup_kafka
    topic = "test_topic"
    key = "test_key"
    headers = {"test_header": "value"}
    test_message = "Hello!!!!!!!!!"
    df = pd.DataFrame({'column1': [1], 'column2': [test_message]})
      
    time.sleep(5)

    kafka.send(topic=topic, df=df, key=key, headers=headers)
    