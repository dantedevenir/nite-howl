from io import BytesIO
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa
from .journal import minute

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
#export ROOT_PATH=/samba-data;export ENV_PATH=/samba-data/.env;export BROKER=localhost:9092;export TOPIC=testing;export GROUP=tmp

class NiteHowl:
    
    def __init__(self, broker, group = None, topics = None, key = None, headers = None) -> None:
        self.producer = Producer({'bootstrap.servers': broker})
        self.topics = topics
        self.key = key
        self.headers = headers
        if topics and group:
            self.consumer = Consumer({
                'bootstrap.servers': broker,
                'group.id': group
            })
            self.consumer.subscribe(topics.split(","))
    
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
            buffer = BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)
            
        parquet_buffer = self.package(table)
        self.producer.produce(topic, parquet_buffer.getvalue(), key=key, headers=headers)
        self.producer.flush()
        minute.register("info", f"Send to broker the topic {topic}")
        
    def radar(self):
        if not self.topics:
            yield None, None, None, None
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            if msg.key().decode('utf-8') != self.key and self.key:
                continue
            
            if {k: v.decode('utf-8') for k, v in msg.headers()} != self.headers and self.headers:
                continue
            
            table = self.unpackage(msg.value())
            yield table, msg.topic(), msg.key().decode('utf-8'), {k: v.decode('utf-8') for k, v in msg.headers()}