import unittest
from meesee import Meesee, config
import redis
import uuid


box = Meesee(workers=2, namespace="test1", timeout=2)


@box.worker_producer(input_queue="foo", output_queue="foobar")
def foo(item, worker_id):
    redis_client = redis.Redis(**config['redis_config'])
    key = f"test1:result_test:foo:{worker_id}:{uuid.uuid4()}"
    redis_client.set(key, f"foo_processed_{item}")
    return [f"foo_processed_{item}"]


@box.worker()
def foobar(item, worker_id):
    redis_client = redis.Redis(**config['redis_config'])
    key = f"test1:result_test:foobar:{worker_id}:{uuid.uuid4()}"
    redis_client.set(key, item)


class TestMeesee(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.box = box
        cls.redis_client = redis.Redis(**config['redis_config'])

    def setUp(self):
        self.clean_up_redis()

    def tearDown(self):
        self.clean_up_redis()

    def clean_up_redis(self):
        patterns = [
            "test1:result_test:foo:*",
            "test1:result_test:foobar:*"
        ]
        for pattern in patterns:
            for key in self.redis_client.scan_iter(pattern):
                self.redis_client.delete(key)

    def test_produce_to_functionality(self):
        expected = ["item1", "item2", "item3"]

        @self.box.produce()
        def produce_to_foobar(items):
            return items

        produce_to_foobar(expected)
        self.box.push_button(workers=5, wait=3)

        results = []
        for key in self.redis_client.scan_iter("test1:result_test:foobar:*"):
            value = self.redis_client.get(key).decode('utf-8')
            results.append(value)

        self.assertEqual(sorted(results), sorted(expected))

    def test_worker_producer_functionality(self):
        expected = ["item1", "item2", "item3"]

        @self.box.produce(queue="foo")
        def produce_to_foo(items):
            return items

        produce_to_foo(expected)
        self.box.push_button(workers=10, wait=3)

        foo_results = []
        for key in self.redis_client.scan_iter("test1:result_test:foo:*"):
            value = self.redis_client.get(key).decode('utf-8')
            foo_results.append(value)

        foobar_results = []
        for key in self.redis_client.scan_iter("test1:result_test:foobar:*"):
            value = self.redis_client.get(key).decode('utf-8')
            foobar_results.append(value)

        self.assertEqual(sorted(foo_results), sorted([f"foo_processed_{item}" for item in expected]))
        self.assertEqual(sorted(foobar_results), sorted([f"foo_processed_{item}" for item in expected]))


if __name__ == '__main__':
    unittest.main()
