import json

import unittest

from unittest import mock
from unittest.mock import patch, MagicMock, call

from meesee import Meesee, config
from meesee import init_add, setup_init_items, InitFail
from meesee import startapp, run_worker, RedisQueue


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
        self.assertIn("foo", self.box._worker_funcs)
        self.assertIn("bar", self.box._worker_funcs)
        self.assertIn("produce_to_qux", self.box._worker_funcs)

        mock_redis_queue.return_value.send.assert_any_call(json.dumps({"key": "test_data"}))


class TestStartWorkers(unittest.TestCase):
    def setUp(self):
        self.box = Meesee(workers=2, namespace="test1", timeout=2)

    @patch('meesee.startapp')
    @patch('sys.stdout.write')
    def test_start_workers_no_workers(self, mock_stdout_write, mock_startapp):
        self.box._worker_funcs = {}
        self.box.start_workers()
        mock_stdout_write.assert_called_once_with("No workers have been assigned with a decorator\n")
        mock_startapp.assert_called_once_with(
            [],
            workers=10,
            config=config
        )

    @patch('meesee.startapp')
    @patch('sys.stdout.write')
    def test_start_workers_enough_workers(self, mock_stdout_write, mock_startapp):
        self.box._worker_funcs = {'worker1': MagicMock(), 'worker2': MagicMock()}
        self.box.start_workers(workers=3)
        mock_stdout_write.assert_not_called()
        mock_startapp.assert_called_once_with(
            list(self.box._worker_funcs.values()),
            workers=3,
            config=config,
        )

    @patch('meesee.startapp')
    @patch('sys.stdout.write')
    def test_start_workers_not_enough_workers(self, mock_stdout_write, mock_startapp):
        self.box._worker_funcs = {'worker1': MagicMock(), 'worker2': MagicMock(), 'worker3': MagicMock()}
        self.box.start_workers(workers=2)
        mock_stdout_write.assert_called_once_with(
            "Not enough workers, increasing the workers started with: 2 we need atleast: 3\n"
        )
        mock_startapp.assert_called_once_with(
            list(self.box._worker_funcs.values()),
            workers=3,
            config=config,
        )

    @patch('meesee.startapp')
    @patch('sys.stdout.write')
    def test_start_workers_custom_config(self, mock_stdout_write, mock_startapp):
        self.box._worker_funcs = {'worker1': MagicMock()}
        custom_config = {'custom': 'config'}
        self.box.start_workers(workers=1, config=custom_config)
        mock_stdout_write.assert_not_called()
        mock_startapp.assert_called_once_with(
            list(self.box._worker_funcs.values()),
            workers=1,
            config=custom_config
        )


class TestMeeseUtilityFunctions(unittest.TestCase):

    def test_init_add_success(self):
        func_kwargs = {'existing': 'value'}
        init_items = {'new_item': MagicMock(return_value='mocked_value')}
        init_kwargs = {'new_item': {'config': 'value'}}

        result = init_add(func_kwargs, init_items, init_kwargs)

        self.assertEqual(result, {'existing': 'value', 'new_item': 'mocked_value'})
        init_items['new_item'].assert_called_once_with(config='value')

    def test_init_add_type_error(self):
        func_kwargs = {'existing': 'value'}
        init_items = {'new_item': MagicMock(side_effect=TypeError)}
        init_kwargs = {'new_item': {'config': 'value'}}

        with self.assertRaises(InitFail):
            init_add(func_kwargs, init_items, init_kwargs)

    def test_setup_init_items(self):
        func_kwargs = {'item1': 'value1', 'item2': 'value2', 'item3': 'value3'}
        init_kwargs = {'item1': {}, 'item3': {}}

        expected_result = {'item1': 'value1', 'item3': 'value3'}
        result = setup_init_items(func_kwargs, init_kwargs)

        self.assertEqual(result, expected_result)


class TestStartAppExitScenarios(unittest.TestCase):

    @patch('meesee.Pool')
    @patch('sys.stdout.write')
    def test_keyboard_interrupt_exit(self, mock_stdout_write, mock_pool):
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.starmap.side_effect = KeyboardInterrupt()

        startapp(MagicMock())

        mock_stdout_write.assert_any_call('Starting Graceful exit\n')
        mock_pool_instance.close.assert_called_once()
        mock_pool_instance.join.assert_called_once()
        mock_stdout_write.assert_any_call('Clean shut down\n')

    @patch('meesee.Pool')
    @patch('sys.stdout.write')
    def test_system_exit(self, mock_stdout_write, mock_pool):
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.starmap.side_effect = SystemExit()

        startapp(MagicMock())

        mock_stdout_write.assert_any_call('Starting Graceful exit\n')
        mock_pool_instance.close.assert_called_once()
        mock_pool_instance.join.assert_called_once()
        mock_stdout_write.assert_any_call('Clean shut down\n')

    @patch('meesee.Pool')
    @patch('sys.stdout.write')
    def test_normal_execution(self, mock_stdout_write, mock_pool):
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        startapp(MagicMock())

        mock_pool_instance.starmap.assert_called_once()
        mock_stdout_write.assert_called_once_with('Clean shut down\n')
        mock_pool_instance.close.assert_not_called()
        mock_pool_instance.join.assert_not_called()


class TestRunWorker(unittest.TestCase):

    def setUp(self):
        self.timeout = 0.1  # Short timeout for tests

    @patch('meesee.setup_init_items')
    @patch('meesee.init_add')
    @patch('sys.stdout.write')
    @patch('traceback.print_exc')
    def test_run_worker_init_fail(self, mock_print_exc, mock_stdout_write, mock_init_add, mock_setup_init_items):
        mock_init_add.side_effect = InitFail()

        run_worker(MagicMock(), {}, None, {'timeout': self.timeout}, 1, {})

        mock_stdout_write.assert_called_with('worker 1 initialization failed\n')
        mock_print_exc.assert_called_once()

    @patch('meesee.setup_init_items')
    @patch('meesee.init_add')
    @patch('meesee.RedisQueue')
    @patch('sys.stdout.write')
    def test_run_worker_keyboard_interrupt(self, mock_stdout_write, mock_redis_queue, mock_init_add, mock_setup_init_items):
        mock_redis_queue.return_value.__iter__.side_effect = KeyboardInterrupt()

        run_worker(MagicMock(), {}, None, {'key': 'test_queue', 'timeout': self.timeout}, 1, {})

        mock_stdout_write.assert_called_with('timeout reached worker 1 stopped\n')

    @patch('meesee.setup_init_items', return_value={})
    @patch('meesee.init_add', return_value={})
    @patch('meesee.RedisQueue')
    @patch('sys.stdout.write')
    def test_run_worker_normal_execution(self, mock_stdout_write, mock_redis_queue, mock_init_add, mock_setup_init_items):
        mock_func = MagicMock(return_value=(None, None))
        mock_func.__name__ = 'test_func'
        mock_redis_queue.return_value.__iter__.return_value = [('key', b'test_item')]

        config = {'key': 'test_queue', 'timeout': 0.1}
        run_worker(mock_func, {}, None, config, 1, {})

        mock_stdout_write.assert_any_call('worker 1 started. test_func listening to test_queue \n')
        mock_func.assert_called_once_with('test_item', 1)

    @patch('meesee.setup_init_items', return_value={})
    @patch('meesee.init_add', return_value={})
    @patch('meesee.RedisQueue')
    @patch('sys.stdout.write')
    @patch('time.sleep')
    def test_run_worker_on_failure_func(self, mock_sleep, mock_stdout_write, mock_redis_queue, mock_init_add, mock_setup_init_items):
        def mock_worker_func(item, worker_id, **kwargs):
            if item == 'fail':
                raise Exception("Test exception")
            return None, None

        mock_on_failure_func = MagicMock()
        mock_redis_queue.return_value.__iter__.return_value = iter([
            ('key1', b'success'),
            ('key2', b'fail'),
            ('key3', b'success_after_fail')
        ])

        config = {'key': 'test_queue', 'timeout': 5}  # Set a longer timeout
        run_worker(mock_worker_func, {}, mock_on_failure_func, config, 1, {})

        mock_on_failure_func.assert_called_once_with(b'fail', mock.ANY, mock.ANY, 1)
        mock_stdout_write.assert_any_call('worker 1 failed reason Test exception\n')
        mock_stdout_write.assert_any_call('worker 1 running failure handler Test exception\n')
        assert mock_stdout_write.call_args_list[-1] == call('timeout reached worker 1 stopped\n')

    @patch('meesee.setup_init_items', return_value={})
    @patch('meesee.init_add', return_value={})
    @patch('meesee.RedisQueue')
    @patch('sys.stdout.write')
    def test_run_worker_system_exit(self, mock_stdout_write, mock_redis_queue, mock_init_add, mock_setup_init_items):
        mock_func = MagicMock(__name__='test_func')

        def side_effect(item, worker_id, **kwargs):
            if item == 'test_item1':
                return "", None
            raise SystemExit()

        mock_func.side_effect = side_effect
        mock_redis_queue.return_value.__iter__.return_value = iter([
            ('key1', b'test_item1'),
            ('key2', b'test_item2')
        ])

        config = {'key': 'test_queue'}

        run_worker(mock_func, {}, None, config, 1, {})
        mock_stdout_write.assert_any_call('worker 1 stopped\n')
        mock_redis_queue.return_value.first_inline_send.assert_called_once_with(b'test_item2')


class TestRedisQueueCoverage(unittest.TestCase):

    @patch('meesee.redis.Redis')
    def setUp(self, mock_redis):
        self.mock_redis = mock_redis.return_value
        self.queue = RedisQueue('test_namespace', 'test_key', {}, maxsize=10, timeout=5)

    def test_init(self):
        self.assertEqual(self.queue.namespace, 'test_namespace')
        self.assertEqual(self.queue.key, 'test_key')
        self.assertEqual(self.queue.maxsize, 10)
        self.assertEqual(self.queue.timeout, 5)

    def test_format_list_key(self):
        self.assertEqual(self.queue.format_list_key('ns', 'key'), 'ns:key')

    def test_set_list_key(self):
        self.queue.set_list_key('new_key', 'new_namespace')
        self.assertEqual(self.queue.list_key, 'new_namespace:new_key')

    def test_first_inline_send(self):
        self.queue.first_inline_send('item')
        self.mock_redis.lpush.assert_called_once_with(self.queue.list_key, 'item')

    def test_send_to(self):
        self.queue.send_to('other_key', 'item')
        self.mock_redis.rpush.assert_called_once_with('test_namespace:other_key', 'item')

    def test_send(self):
        self.mock_redis.llen.return_value = 5
        self.queue.send('item')
        self.mock_redis.rpush.assert_called_once_with(self.queue.list_key, 'item')

    def test_send_maxsize_reached(self):
        self.mock_redis.llen.return_value = 10
        self.queue.send('item')
        self.mock_redis.lpop.assert_called_once_with(self.queue.list_key)
        self.mock_redis.rpush.assert_called_once_with(self.queue.list_key, 'item')

    def test_send_unsafe(self):
        self.queue.send_unsafe('item')
        self.mock_redis.rpush.assert_called_once_with(self.queue.list_key, 'item')

    @patch('time.sleep')
    def test_send_wait(self, mock_sleep):
        self.mock_redis.llen.side_effect = [10, 10, 9]
        self.queue.send_wait('item')
        self.assertEqual(mock_sleep.call_count, 2)
        self.mock_redis.rpush.assert_called_once_with(self.queue.list_key, 'item')

    def test_send_dict(self):
        self.queue.send_dict({'key': 'value'})
        self.mock_redis.rpush.assert_called_once_with(self.queue.list_key, json.dumps({'key': 'value'}))

    def test_iter(self):
        self.assertIsInstance(iter(self.queue), RedisQueue)

    def test_next(self):
        self.mock_redis.blpop.return_value = ('key', 'value')
        self.assertEqual(next(self.queue), ('key', 'value'))

    def test_next_stop_iteration(self):
        self.mock_redis.blpop.return_value = None
        with self.assertRaises(StopIteration):
            next(self.queue)

    def test_len(self):
        self.mock_redis.llen.return_value = 5
        self.assertEqual(len(self.queue), 5)


if __name__ == '__main__':
    unittest.main()
