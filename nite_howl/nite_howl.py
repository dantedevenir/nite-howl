from io import BytesIO
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa
from .journal import minute

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
#export ROOT_PATH=/samba-data;export ENV_PATH=/samba-data/.env;export BROKER=localhost:9092;export TOPIC=testing;export GROUP=tmp

class NiteHowl:
    
    def __init__(self, broker, group = None, topics = [], key = None, headers = None) -> None:
        self.broker = broker
        self.producer = Producer({'bootstrap.servers': self.broker})
        self.topics = topics
        self.key = key
        self.headers = headers
        if topics and group:
            self.consumer = Consumer({
                'bootstrap.servers': broker,
                'group.id': group,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 5000,
            })
            try:
                self.consumer.subscribe(topics)
            except KafkaException as e:
                if e.args[0].code() == KafkaError._UNKNOWN_TOPIC_OR_PART:
                    self.admin = AdminClient({'bootstrap.servers': self.broker})
                    minute.register(f"Error: {e}. los tópicos '{", ".join(self.topics)}' no están disponible. Intentando crear...")
                    self.create_topics_if_not_exists()
                else:
                    raise  KafkaException(e)
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
                minute.register("info", f"The topics {', '.join(self.topics)} don't exist. Try creating...")
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
        
    def package(self, table) -> BytesIO:
        parquet_buffer = BytesIO()
        with pq.ParquetWriter(parquet_buffer, table.schema) as writer:
            writer.write_table(table)
            
        parquet_buffer.seek(0)
        return parquet_buffer
    
    def unpackage(self, parquet_bytes):
        parquet_buffer = BytesIO(parquet_bytes)
        parquet_buffer.seek(0)
        table = pq.read_table(parquet_buffer)
        return table
        
    def send(self, topic, df = None, path = None, key = None, headers = None):
        if not (path or (df is not None and not df.empty)):
            return
        
        if path:
            table = csv.read_csv(path)
        else:
            table = pa.Table.from_pandas(df)
        self.producer.poll(0)
        parquet_buffer = self.package(table)
        self.producer.produce(topic=topic, value=parquet_buffer.getvalue(), key=key, headers=headers)
        self.producer.flush()
        minute.register("info", f"Send message to the topic '{self.topics}', key '{key}' and headers '{headers}'")
        
    def radar(self, timeout=1.0):
        if not self.topics:
            minute.register("error", f"The user didn't define topics: {self.topics}")
            yield None, None, None, None

        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        self.admin = AdminClient({'bootstrap.servers': self.broker})
                        minute.register("error", f"The topics '{", ".join(self.topics)}' doesn't exist. Try creating...")
                        self.create_topics_if_not_exists()
                    else:
                        raise KafkaException(msg.error())

                headers = {k: v.decode('utf-8') for k, v in msg.headers() or {}}
                key = msg.key().decode('utf-8') if msg.key() else None

                if (self.key and key != self.key) or (self.headers and headers != self.headers):
                    minute.register("warning", f"Message filtered: topic: {msg.topic()}, key: {key}, headers: {headers}")
                    continue

                table = self.unpackage(msg.value())
                
                yield table, msg.topic(), key, headers
        finally:
            self.consumer.close()
