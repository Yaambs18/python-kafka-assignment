from confluent_kafka import Producer
import socket


json_data = open('data.json', 'r')
data = json_data.read()

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}
producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: {}".format((str(msg))))

producer.produce("employee", key="data", value=data, callback=acked(None, "Data sent successfully."))

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)