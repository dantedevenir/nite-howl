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
                'group.id': group,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False
            })
            self.consumer.subscribe(topics.split(","))
            minute.register("info", f"Consumer conf: {broker}, group.id: {group} and topics: {topics.split(",")}")
        minute.register("info", f"Producer conf: {broker}, key: {key} and headers: {headers}")
        
    
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

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result. """
        if err is not None:
            minute.register("error", f'Message delivery failed: {err}')
        else:
            minute.register("info", f'Message delivered to {msg.topic()} [{msg.partition()}]')

        
    def send(self, topic, df = None, path = None, key = None, headers = None):
        if not (path or (df is not None and not df.empty)):
            return
        
        if path:
            table = csv.read_csv(path)
        else:
            table = pa.Table.from_pandas(df)
        self.producer.poll(0)
        parquet_buffer = self.package(table)
        self.producer.produce(topic=topic, value=parquet_buffer.getvalue(), key=key, headers=headers, callback=self.delivery_report)
        self.producer.flush()
        
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
                    else:
                        raise KafkaException(msg.error())

                headers = {k: v.decode('utf-8') for k, v in msg.headers() or {}}
                key = msg.key().decode('utf-8') if msg.key() else None

                if (self.key and key != self.key) or (self.headers and headers != self.headers):
                    minute.register("warning", f"Message filtered: topic: {msg.topic()}, key: {key}, headers: {headers}")
                    continue

                table = self.unpackage(msg.value())
                
                self.consumer.commit(offsets=[msg], asynchronous=False)
                yield table, msg.topic(), key, headers
        finally:
            self.consumer.close()
