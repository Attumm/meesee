import json

import unittest

from unittest.mock import patch
from meesee import Meesee


class TestWorkerProducerLineCoverage(unittest.TestCase):
    def setUp(self):
        self.box = Meesee(workers=10, namespace="test", timeout=2)

    @patch('meesee.RedisQueue')
    @patch('redis.Redis')
    def test_worker_producer_line_coverage(self, mock_redis, mock_redis_queue):

        @self.box.worker_producer(input_queue="foo", output_queue="foobar")
        def test_func_both_queues(input_data):
            return input_data

        @self.box.worker_producer(input_queue="bar")
        def test_func_input_queue(input_data):
            return input_data

        @self.box.worker_producer(output_queue="baz")
        def test_func_output_queue(input_data):
            return input_data

        @self.box.worker_producer()
        def produce_to_qux(input_data):
            return input_data

        @self.box.worker_producer()
        def test_func_list(input_data):
            return [input_data, {"key": "value"}]

        @self.box.worker_producer()
        def test_func_dict(input_data):
            return {"key": input_data}

        @self.box.worker_producer()
        def test_func_list_with_dict(input_data):
            return input_data

        @self.box.worker_producer()
        def test_func_none(input_data):
            return None

        test_func_both_queues("test_data")
        test_func_input_queue("test_data")
        test_func_output_queue("test_data")
        produce_to_qux("test_data")
        test_func_list("test_data")
        test_func_dict("test_data")
        test_func_none("test_data")

        test_func_list_with_dict([{"key1": "value1"}, {"key2": "value2"}])

        mock_redis_queue.assert_called()
        mock_redis_queue.return_value.send.assert_called()
        self.assertIn("foo", self.box.worker_funcs)
        self.assertIn("bar", self.box.worker_funcs)
        self.assertIn("produce_to_qux", self.box.worker_funcs)

        mock_redis_queue.return_value.send.assert_any_call(json.dumps({"key": "test_data"}))


if __name__ == '__main__':
    unittest.main()
