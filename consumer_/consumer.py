from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
import sys

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "foo",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
running = True

# adding data to mysqldb
def msg_process(msg):
    consumed_data = {}
    consumed_data[msg.key().decode('utf-8')] = msg.value().decode('utf-8')
    return consumed_data
    
# consuming data from kafka 
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(1)
            if msg is None:
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                return msg_process(msg)
    except KeyboardInterrupt as e:
        print(e)
    except KafkaException:
        print("Unknown partition provided")


if __name__ == "__main__":
    basic_consume_loop(consumer, ["employee_data1"])