import unittest
import time

import redis

from meesee import startapp, RedisQueue

example_config = {
    "namespace": "test",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 1000
}

redis_instance = redis.StrictRedis()

example_config['timeout'] = 1
example_config['maxsize'] = float('inf')


def produce(amount):
    r = RedisQueue(**example_config)
    for i in range(1, amount + 1):
        r.send(i)


def produce_items(items):
    r = RedisQueue(**example_config)
    for i in items:
        r.send(i)


def increment_by_one(item, worker_id, key, r):
    r.r.incr(key)


def append_worker_id(item, worker_id, key, r):
    r.r.lpush(key, worker_id)


def incr_and_append_worker_id(item, worker_id, key, r):
    key_amount = 'test:amount'
    key_worker_id = 'test:workerids'
    increment_by_one(item, worker_id, key_amount, r)
    append_worker_id(item, worker_id, key_worker_id, r)


class TestAmounts(unittest.TestCase):

    def tearDown(self):
        keys_to_remove = ['test:amount', 'test:workerids']
        redis_instance.delete(*keys_to_remove)

    def test_incr_key_equals_produces_single_worker(self):
        expected = 100
        produce(expected)

        key = 'test:amount'
        kwargs = {'key': key, 'r': RedisQueue}
        init_kwargs = {'r': example_config}

        startapp(increment_by_one, workers=1, config=example_config, func_kwargs=kwargs, init_kwargs=init_kwargs)

        result = int(redis_instance.get(key))
        self.assertEqual(result, expected)

    def test_incr_key_equals_produces_five_workers(self):
        expected = 100
        produce(expected)

        key = 'test:amount'
        kwargs = {'key': key, 'r': RedisQueue}
        init_kwargs = {'r': example_config}

        startapp(increment_by_one, workers=5, config=example_config, func_kwargs=kwargs, init_kwargs=init_kwargs)

        result = int(redis_instance.get(key))
        self.assertEqual(result, expected)

    def test_incr_key_equals_produces_multiple_workers(self):
        expected = 123
        produce(expected)

        key = 'test:amount'
        kwargs = {'key': key, 'r': RedisQueue}
        init_kwargs = {'r': example_config}

        startapp(increment_by_one, workers=7, config=example_config, func_kwargs=kwargs, init_kwargs=init_kwargs)

        result = int(redis_instance.get(key))
        self.assertEqual(result, expected)

    def test_all_workers_are_present(self):
        expected = 268
        expected_workers_amount = 5
        expected_workers = {i for i in range(1, expected_workers_amount + 1)}

        produce(expected)

        key = 'test:amount'
        key_workerids = 'test:workerids'
        kwargs = {'key': key, 'r': RedisQueue}
        init_kwargs = {'r': example_config}

        startapp(incr_and_append_worker_id, workers=expected_workers_amount, config=example_config, func_kwargs=kwargs, init_kwargs=init_kwargs)

        result = int(redis_instance.get(key))
        self.assertEqual(result, expected)

        result_workers = redis_instance.lrange(key_workerids, 0, -1)
        result_workers_set = {int(i) for i in sorted(result_workers)}

        self.assertEqual(result_workers_set, expected_workers)


def append_item(item, worker_id, key, r, test_result_key):
    r.r.lpush(test_result_key, item)


class TestReceivedItemsEqualsSendItems(unittest.TestCase):

    def tearDown(self):
        keys_to_remove = ['test:items', 'test:result']
        redis_instance.delete(*keys_to_remove)

    def test_items_send_are_handled_single_worker(self):
        expected = ['1', '2', '3']
        produce_items(expected)

        key = 'test:items'
        result_key = 'test:result'
        kwargs = {'key': key, 'r': RedisQueue, 'test_result_key': result_key}
        init_kwargs = {'r': example_config}

        startapp(append_item, workers=5, config=example_config, func_kwargs=kwargs, init_kwargs=init_kwargs)

        result = [i.decode('utf-8') for i in redis_instance.lrange(result_key, 0, -1)]
        self.assertEqual(sorted(result), sorted(expected))

    def test_items_send_are_handled_multiple_worker(self):
        expected = ['1', '2', '3']
        produce_items(expected)

        key = 'test:items'
        result_key = 'test:result'
        kwargs = {'key': key, 'r': RedisQueue, 'test_result_key': result_key}
        init_kwargs = {'r': example_config}

        startapp(append_item, workers=5, config=example_config, func_kwargs=kwargs, init_kwargs=init_kwargs)

        result = [i.decode('utf-8') for i in redis_instance.lrange(result_key, 0, -1)]
        self.assertEqual(sorted(result), sorted(expected))


def sysexit(item, *args):
    if item == 'still_worked_on':
        time.sleep(0.1)
        raise SystemExit


class TestOSSignal(unittest.TestCase):

    def tearDown(self):
        list_key = '{}:{}'.format(example_config['namespace'], example_config['key'])
        keys_to_remove = ['test:items', 'test:result', list_key]
        redis_instance.delete(*keys_to_remove)

    def test_handle_sys(self):
        items = ['1', '2', 'still_worked_on']
        produce_items(items)
        list_key = '{}:{}'.format(example_config['namespace'], example_config['key'])

        startapp(sysexit, workers=5, config=example_config)

        result = [i.decode('utf-8') for i in redis_instance.lrange(list_key, 0, -1)]
        self.assertEqual(result, ['still_worked_on'])


if __name__ == '__main__':
    unittest.main()
