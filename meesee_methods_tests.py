import unittest
from meesee import Meesee, config
import redis
import uuid


box = Meesee(workers=1, namespace="test1", timeout=2)


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

        # Collect results from Redis
        results = []
        for key in self.redis_client.scan_iter("test1:result_test:foobar:*"):
            value = self.redis_client.get(key).decode('utf-8')
            results.append(value)

        self.assertEqual(sorted(results), sorted(expected))


if __name__ == '__main__':
    unittest.main()
