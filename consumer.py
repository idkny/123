import orjson
from moongo import MongoConnect
from kafka import KafkaConsumer
from running import *



class MessageConsumer:
    mongo_do = MongoConnect("mongodb://localhost:27017", "test_kfk", "event")

    def __init__(self, broker, topic, group_id):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id

    def activate_listener(self):
        consumer = KafkaConsumer(
            bootstrap_servers=self.broker,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: orjson.loads(m)
        )
        if self.mongo_do.connect():
            consumer.subscribe(self.topic)
            print("consumer is listening....")

            try:
                for message in consumer:
                    print("received message = ", message.value)
                    self.mongo_do.insert_document(message.value)
                    consumer.commit()
            except KeyboardInterrupt:
                print("Aborted by user...")
            finally:
                consumer.close()
        else:
            consumer.close()
            print("check mongo connection")


if __name__ == "__main__":
    while True:
        consumer1 = MessageConsumer("localhost:9092", "First_topic", "mongo_consume")
        consumer1.activate_listener()
