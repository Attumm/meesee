import unittest
import sys
import io
import time

import meesee
from meesee import run_worker


def stub_setup_init_items(func_kwargs, init_kwargs):
    return {name: func_kwargs[name] for name in init_kwargs.keys()}


def stub_init_add(func_kwargs, init_items, init_kwargs):
    for name, value in init_items.items():
        if callable(value):
            func_kwargs[name] = value()
        else:
            func_kwargs[name] = value
    return func_kwargs


class StubRedisQueue:
    def __init__(self, **config):
        self.config = config
        self.items = []
        self.state = "normal"
        self.iteration_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.state == "exception" and self.iteration_count > 0:
            raise Exception("Simulated failure")
        if self.iteration_count >= len(self.items):
            raise StopIteration
        item = self.items[self.iteration_count]
        self.iteration_count += 1
        return ("key", item.encode('utf-8'))

    def first_inline_send(self, item):
        self.items.append(item)


class TestRunWorker(unittest.TestCase):
    def setUp(self):
        self.func_kwargs = {"arg1": "value1", "arg2": "value2", "init_arg": lambda: "init_value"}
        self.init_kwargs = {"init_arg": "init_value"}
        self.config = {"key": "test_queue", "timeout": 0.1}
        self.original_stdout = sys.stdout
        sys.stdout = io.StringIO()

        # Save original implementations
        self.original_RedisQueue = meesee.RedisQueue
        self.original_setup_init_items = meesee.setup_init_items
        self.original_init_add = meesee.init_add

        # Apply patches
        meesee.RedisQueue = StubRedisQueue
        meesee.setup_init_items = stub_setup_init_items
        meesee.init_add = stub_init_add

    def tearDown(self):

        sys.stdout = self.original_stdout

        # Restore original implementations
        meesee.RedisQueue = self.original_RedisQueue
        meesee.setup_init_items = self.original_setup_init_items
        meesee.init_add = self.original_init_add

    def test_normal_execution(self):
        def stub_func(item, worker_id, **kwargs):
            return f"Processed: {item}"

        StubRedisQueue.items = ["item1", "item2"]
        StubRedisQueue.state = "normal"
        run_worker(stub_func, self.func_kwargs, None, self.config, 1, self.init_kwargs)

        output = sys.stdout.getvalue()
        self.assertIn("worker 1 started", output)
        self.assertIn("listening to test_queue", output)
        self.assertIn("timeout reached worker 1 stopped", output)

    def test_init_fail(self):
        def stub_func(item, worker_id, **kwargs):
            return "This should not be reached"

        def failing_init():
            raise Exception("Init failure")

        self.func_kwargs["init_arg"] = failing_init
        StubRedisQueue.items = ["item1"]
        StubRedisQueue.state = "normal"
        run_worker(stub_func, self.func_kwargs, None, self.config, 1, self.init_kwargs)

        output = sys.stdout.getvalue()
        self.assertIn("worker 1 failed reason Init failure", output)

    def test_keyboard_interrupt(self):
        def stub_func(item, worker_id, **kwargs):
            raise KeyboardInterrupt()

        StubRedisQueue.items = ["item1"]
        StubRedisQueue.state = "normal"
        run_worker(stub_func, self.func_kwargs, None, self.config, 1, self.init_kwargs)

        output = sys.stdout.getvalue()
        self.assertIn("worker 1 stopped", output)

    def test_general_exception(self):
        def stub_func(item, worker_id, **kwargs):
            return f"Processed: {item}"

        def stub_on_failure_func(item, e, r, worker_id):
            sys.stdout.write(f"Failure handled for item: {item}\n")

        StubRedisQueue.items = ["item1", "item2"]
        StubRedisQueue.state = "exception"
        StubRedisQueue.iteration_count = 0
        self.config["timeout"] = 2
        run_worker(stub_func, self.func_kwargs, stub_on_failure_func, self.config, 1, self.init_kwargs)
        self.config["timeout"] = 0.1

        output = sys.stdout.getvalue()
        self.assertIn("worker 1 started", output)
        self.assertIn("stub_func listening to test_queue", output)
        self.assertIn("timeout reached worker 1 stopped", output)

    def test_timeout(self):
        def stub_func(item, worker_id, **kwargs):
            # Sleep longer than the timeout
            time.sleep(0.2)
            return f"Processed: {item}"

        StubRedisQueue.items = ["item1", "item2"]
        StubRedisQueue.state = "timeout"
        run_worker(stub_func, self.func_kwargs, None, self.config, 1, self.init_kwargs)

        output = sys.stdout.getvalue()
        self.assertIn("timeout reached worker 1 stopped", output)

    def test_multiple_funcs_and_configs(self):
        def func_a(item, worker_id, **kwargs):
            return f"func_a processed {item} with worker {worker_id}"

        def func_b(item, worker_id, **kwargs):
            return f"func_b processed {item} with worker {worker_id}"

        def func_c(item, worker_id, **kwargs):
            return f"func_c processed {item} with worker {worker_id}"

        funcs = [func_a, func_b, func_c]

        config_a = {"key": "queue_a", "timeout": 0.1}
        config_b = {"key": "queue_b", "timeout": 0.1}
        config_c = {"key": "queue_c", "timeout": 0.1}

        configs = [config_a, config_b, config_c]

        def stub_on_failure_func(item, e, r, worker_id):
            sys.stdout.write(f"Failure handled for item: {item} on worker {worker_id}\n")

        StubRedisQueue.items = ["item1", "item2"]
        StubRedisQueue.state = "normal"

        # Reset stdout capture for each iteration
        sys.stdout = io.StringIO()

        for worker_id in range(5):
            run_worker(funcs, self.func_kwargs, stub_on_failure_func, configs, worker_id, self.init_kwargs)

            output = sys.stdout.getvalue()
            expected_func = funcs[worker_id % len(funcs)].__name__
            expected_queue = configs[worker_id % len(configs)]["key"]

            self.assertIn(f"worker {worker_id} started", output)
            self.assertIn(f"{expected_func} listening to {expected_queue}", output)
            self.assertIn("timeout reached worker", output)

            if "item1" in output:
                self.assertIn(f"{expected_func} processed item1 with worker {worker_id}", output)
            if "item2" in output:
                self.assertIn(f"{expected_func} processed item2 with worker {worker_id}", output)


if __name__ == '__main__':
    unittest.main()
