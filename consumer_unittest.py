import unittest
from consumer import consumer

class TestConsumer(unittest.TestCase):
    
    def test_mysql_connection(self):
        self.assertIsNotNone(consumer.connection)
        
    def test_table_creation(self):
        self.assertIsNone(consumer.meta.create_all(consumer.engine))
    
    def test_topic_existence(self):
        self.assertRaises(TypeError,consumer.basic_consume_loop(consumer.consumer, ['employ']))
                
if __name__ =="__main__":
    unittest.main()