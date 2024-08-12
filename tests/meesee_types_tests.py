import unittest
import redis
from meesee import Meesee, config

box = Meesee(workers=5, namespace="test", timeout=2)


@box.worker(queue="strings")
def consume_strings(item, worker_id):
    redis_client = redis.Redis(**config['redis_config'])
    key = f"result_test:consume_strings:{worker_id}:{redis_client.incr('result_test:consume_strings:counter')}"
    redis_client.set(key, item)


@box.worker(queue="integers")
def consume_integers(item, worker_id):
    redis_client = redis.Redis(**config['redis_config'])
    key = f"result_test:consume_integers:{worker_id}:{redis_client.incr('result_test:consume_integers:counter')}"
    redis_client.set(key, str(item))


@box.worker(queue="lists")
def consume_lists(item, worker_id):
    redis_client = redis.Redis(**config['redis_config'])
    key = f"result_test:consume_lists:{worker_id}:{redis_client.incr('result_test:consume_lists:counter')}"
    redis_client.set(key, str(item))


@box.worker(queue="dicts")
def consume_dicts(item, worker_id):
    redis_client = redis.Redis(**config['redis_config'])
    key = f"result_test:consume_dicts:{worker_id}:{redis_client.incr('result_test:consume_dicts:counter')}"
    redis_client.set(key, str(item))


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
            "result_test:consume_dicts:*",
            "result_test:consume_strings:*",
            "result_test:consume_integers:*",
            "result_test:consume_lists:*",
        ]
        for pattern in patterns:
            for key in self.redis_client.scan_iter(pattern):
                self.redis_client.delete(key)

    def test_produce_and_consume_strings(self):
        expected = ["apple", "banana", "cherry"]

        @self.box.produce(queue="strings")
        def produce_strings():
            return expected

        produce_strings()
        self.box.push_button(workers=5, wait=1)

        results = []
        for key in self.redis_client.scan_iter("result_test:consume_strings:*"):
            if key != b"result_test:consume_strings:counter":
                value = self.redis_client.get(key).decode('utf-8')
                results.append(value)

        self.assertEqual(sorted(results), sorted(expected))

    def test_produce_and_consume_integers(self):
        expected = [1, 2, 3, 4, 5]

        @self.box.produce(queue="integers")
        def produce_integers():
            return expected

        produce_integers()
        self.box.push_button(workers=5, wait=1)

        results = []
        for key in self.redis_client.scan_iter("result_test:consume_integers:*"):
            if key != b"result_test:consume_integers:counter":
                value = int(self.redis_client.get(key))
                results.append(value)

        self.assertEqual(sorted(results), sorted(expected))

    def test_produce_and_consume_lists(self):
        expected = [[1, 2], [3, 4], [5, 6]]

        @self.box.produce(queue="lists")
        def produce_lists():
            return expected

        produce_lists()
        self.box.push_button(workers=5, wait=1)

        results = []
        for key in self.redis_client.scan_iter("result_test:consume_lists:*"):
            if key != b"result_test:consume_lists:counter":
                value = eval(self.redis_client.get(key))
                results.append(value)

        sorted_results = sorted(results)
        sorted_expected = sorted(expected)
        self.assertEqual(sorted_results, sorted_expected)

    def test_produce_and_consume_dicts(self):
        expected = [{"a": 1}, {"b": 2}, {"c": 3}]

        @self.box.produce(queue="dicts")
        def produce_dicts():
            return expected

        produce_dicts()
        self.box.push_button(workers=5, wait=1)

        results = []
        for key in self.redis_client.scan_iter("result_test:consume_dicts:*"):
            if key != b"result_test:consume_dicts:counter":
                value = self.redis_client.get(key)
                results.append(eval(value))  # Convert string back to dict

        sorted_results = sorted(results, key=lambda x: list(x.keys())[0])
        sorted_expected = sorted(expected, key=lambda x: list(x.keys())[0])

        self.assertEqual(sorted_results, sorted_expected)


if __name__ == '__main__':
    unittest.main()
