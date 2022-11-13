import os
import kafka


class CheckIsRunning:
    
    def __init__(self):
        self.check_running_zoo = "echo ruok | nc 127.0.0.1 2181"
  

    def is_zoo_running(self):
        
        if not os.system(self.check_running_zoo):
            return True
        else:
            print('zookeeper is not running... navigate to the kafka/bin directory and run the following command:')
            print('zookeeper-server-start.sh config/zookeeper.properties')
        

    def is_kafka_running(self, id_group):
        consumer = kafka.KafkaConsumer(
            group_id=id_group, bootstrap_servers=["localhost:9092"]
        )
        topics = consumer.topics()
        if not topics:
            print(
                "No topics found... navigate to the kafka/bin directory and run the following command:"
            )
            print("JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties")
           