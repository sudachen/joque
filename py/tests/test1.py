
import env
import topic1
import broker

from joque import Joque
from unittest import TestCase, main

class OneTopicTests(TestCase):
    
    def setUp(self):
        broker.start()
        Joque.set_broker(broker.ADDRESS)

    def tearDown(self):
        broker.stop()

    def test_topic1(self):
        topic1.serve()
        self.assertIs(topic1.hello("hello world!"),None)
        self.assertEqual(topic1.upper("this is uppercased").result(),"THIS IS UPPERCASED")
        self.assertRaises(Exception,lambda: topic1.rise_expt("").result())
        Joque.shutdown()

if __name__ == '__main__':
    main()
