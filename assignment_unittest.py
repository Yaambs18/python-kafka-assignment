import unittest
from producer.producer import acked

class  TestProducer(unittest.TestCase):
    
    def test_acked(self):
        self.assertEqual(acked(None,"testing message"),None)
        
    def test_type_compatibilty_acked(self):
        self.assertRaises(TypeError,acked(None,00000))
    
if __name__ == '__main__':
    unittest.main()