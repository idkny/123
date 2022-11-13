import time
import orjson
from kafka import KafkaProducer
from running import *
from create_events import EventGenerator


class MessageProducer:
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: orjson.dumps(v),
            acks="all",
        )
 
    def send_msg(self, msg):
        print("sending message...")
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()
            future.get(timeout=60)
            print("message sent successfully...")
            return {"status_code": 200, "error": None}
        except Exception as ex:
            return ex


broker = "localhost:9092"
topic = "First_topic"
message_producer = MessageProducer(broker, topic)


if __name__ == "__main__":
    CheckIsRunning().is_zoo_running()
    CheckIsRunning().is_kafka_running("First_topic")
    while True:
        new_event =EventGenerator().create_new_event()
        registered_user = new_event
        print(registered_user)
        resp = message_producer.send_msg(registered_user)
        print(resp)
        time.sleep(3)
