import unittest
from producer.producer import acked

class  TestProducer(unittest.TestCase):
    
    def test_acked(self):
        self.assertIsNone(acked(None,"testing message"))
        
    def test_type_compatibilty_acked(self):
        self.assertRaises(TypeError,acked(None,00000))
    
if __name__ == '__main__':
    unittest.main()