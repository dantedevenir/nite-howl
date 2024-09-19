import pyarrow.csv as csv
from .journal import minute
import pickle as pkl
import pandas as pd
from pathlib import Path

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

class NiteHowl:
    
    def __init__(self, broker, group = None, topics = [], key = None, headers = None) -> None:
        self.broker = broker
        self.producer = Producer({
            'bootstrap.servers': self.broker
        })
        self.topics = topics
        self.key = key
        self.headers = headers
        if topics and group:
            self.consumer = Consumer({
                'bootstrap.servers': broker,
                'group.id': group,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 5000
            })
            self.consumer.subscribe(topics)
            minute.register("info", f"Consumer conf: {broker}, group.id: {group} and topics: {topics}")
        minute.register("info", f"Producer conf: {broker}, key: {key} and headers: {headers}")
    
    def create_topics_if_not_exists(self):
        try:
            # Verificar los topics existentes
            existing_topics = self.admin.list_topics(timeout=10).topics
            topics_to_create = [
                NewTopic(topic, num_partitions=1, replication_factor=1)
                for topic in self.topics if topic not in existing_topics
            ]

            if topics_to_create:
                minute.register("info", f"The topics [{', '.join(self.topics)}] doesn't exist.")
                minute.register("info", "Try creating...")
                futures = self.admin.create_topics(topics_to_create)
                for topic, future in futures.items():
                    try:
                        future.result()  # Espera a que se complete la operación
                        minute.register("info", f"The topic '{topic}' has been created.")
                    except KafkaException as e:
                        minute.register("error", f"Error creating topic '{topic}': {e}")
            else:
                minute.register("info", "All topics exist.")
        except KafkaException as e:
            minute.register("error", f"Error checking topics: {e}")
        
    def send(self, topic, msg, key = None, headers = None):
        self.producer.poll(0)
        self.producer.produce(topic=topic, value=pkl.dumps(msg), key=key, headers=headers)
        self.producer.flush()
        minute.register("info", f"Send message to the topic '{topic}', key '{key}' and headers '{headers}'")
        minute.register("debug", "Esto sale en debug")
        
    def radar(self, timeout=1.0):
        if not self.topics:
            minute.register("error", f"The user didn't define topics: {self.topics}")
            yield None, None, None, None, None

        try:
            minute.register("error", f"Listening the topics {[topic for topic in self.consumer.list_topics().topics]}")
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        self.admin = AdminClient({'bootstrap.servers': self.broker})
                        self.create_topics_if_not_exists()
                        continue
                    else:
                        raise KafkaException(msg.error())

                headers = {k: v.decode('utf-8') for k, v in msg.headers() or {}}
                key = msg.key().decode('utf-8') if msg.key() else None

                if (self.key and key != self.key) or (self.headers and headers != self.headers):
                    minute.register("warning", f"Message filtered: topic: {msg.topic()}, key: {key}, headers: {headers}")
                    continue
                
                data = pkl.loads(msg.value())
                
                if isinstance(data, str):
                    type = str
                    minute.register("info", f"I catch a string")
                elif isinstance(data, pd.DataFrame):
                    type = pd.DataFrame
                    minute.register("info", f"I catch a Pandas Dataframe")
                elif isinstance(data, dict):
                    type = dict
                    minute.register("info", f"I catch a json")
                elif isinstance(data, list):
                    type = list
                    minute.register("info", f"I catch an array")  
                else:
                    minute.register("info", f"I catch something inusual")
                    return                
                
                yield data, msg.topic(), key, headers, type
        finally:
            self.consumer.close()