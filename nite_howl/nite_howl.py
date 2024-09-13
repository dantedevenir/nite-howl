from io import BytesIO
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow as pa

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
#export ROOT_PATH=/samba-data;export ENV_PATH=/samba-data/.env;export BROKER=localhost:9092;export TOPIC=testing;export GROUP=tmp

class NiteHowl:
    
    def __init__(self, broker, group = None, topics = None) -> None:
        self.producer = Producer({'bootstrap.servers': broker})
        self.topics = topics
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
        
    def send(self, topic, df = None, path = None):
        if not (path or df):
            return
        
        if path:
            table = csv.read_csv(path)
        else:
            buffer = BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)
            
        parquet_buffer = self.package(table)
        self.producer.produce(topic, parquet_buffer.getvalue())
        self.producer.flush()
        print(f"Send to broker the topic {topic}")
        
    def radar(self):
        if not self.topics:
            yield None, None
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            table = self.unpackage(msg.value())
            yield table, msg.topic()