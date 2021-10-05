import unittest
from consumer import consumer
from producer import producer

class  TestIntegrate(unittest.TestCase):
    def test_integrate(self):
        json_data = open('data.json', 'r')
        data = json_data.read()
        producer.producer.produce("employee", key="data", value=data, callback=producer.acked(None, "Data sent successfully."))
        producer.producer.poll(1)
        
        consumer.basic_consume_loop(consumer.consumer, ["employee"])
        
        self.assertEqual(consumer.connection.execute("select firstName, lastName from employee order by emp_id desc").all()[0],('Yansh', 'Bhardwaj'))

if __name__ == "__main__":
    unittest.main()