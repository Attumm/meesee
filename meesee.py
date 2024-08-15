import sys
import time
import json
import traceback
import redis

from multiprocessing import Pool

from functools import wraps

config = {
    "namespace": "main",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 1000,
}


class RedisQueue:

    def __init__(self, namespace, key, redis_config, maxsize=None, timeout=None):
        # TCP check if connection is alive
        # redis_config.setdefault('socket_timeout', 30)
        # redis_config.setdefault('socket_keepalive', True)
        # Ping check if connection is alive
        # redis_config.setdefault('health_check_interval', 30)
        self.r = redis.Redis(**redis_config)
        self.key = key
        self.namespace = namespace
        self.maxsize = maxsize
        self.timeout = timeout
        self.list_key = self.format_list_key(namespace, key)

    def format_list_key(self, namespace, key):
        return '{}:{}'.format(namespace, key)

    def set_list_key(self, key=None, namespace=None):
        if key is not None:
            self.key = key
        if namespace is not None:
            self.namespace = namespace
        self.list_key = self.format_list_key(self.namespace, self.key)

    def first_inline_send(self, item):
        # TODO rename method
        self.r.lpush(self.list_key, item)

    def send_to(self, key, item):
        self.r.rpush('{}:{}'.format(self.namespace, key), item)

    def send(self, item):
        """Adds item to the end of the Redis List.

        Side-effects:
           If size is above max size, the operation will keep the size the same.
           Note that if does not resize the list to maxsize.
        """
        if self.maxsize is not None and len(self) >= self.maxsize:
            self.r.lpop(self.list_key)
        self.r.rpush(self.list_key, item)

    def send_unsafe(self, item):
        """Adds item to the end of the Redis List.
        Because there is no limit enforcement, this could completely fill the redis queue.
        Causing issues down the line.
        """
        self.r.rpush(self.list_key, item)

    def send_wait(self, item):
        """Adds item to the end of the Redis List.
        Side-effects:
           If size is above max size, wait for 1 second and retry send operation.
        """
        while self.maxsize is not None and self.r.llen(self.list_key) >= self.maxsize:
            time.sleep(1)
        self.r.rpush(self.list_key, item)

    def send_dict(self, item):
        self.send(json.dumps(item))

    def __iter__(self):
        return self

    def __next__(self):
        result = self.r.blpop(self.list_key, self.timeout)
        if result is None:
            raise StopIteration
        return result

    def __len__(self):
        return self.r.llen(self.list_key)


class Meesee:

    def __init__(self, workers=10, namespace="main", timeout=None, queue="main", redis_config={}):
        self.workers = workers
        self.namespace = namespace
        self.timeout = timeout
        self.queue = queue
        self.redis_config = redis_config
        self._worker_funcs = {}

    def create_produce_config(self):
        return {
            "key": self.queue,
            "namespace": self.namespace,
            "redis_config": self.redis_config,
        }

    def worker_producer(self, input_queue=None, output_queue=None):
        def decorator(func):

            @wraps(func)
            def wrapper(*args, **kwargs):

                config = self.create_produce_config()
                if output_queue:
                    config["key"] = output_queue
                elif "produce_to_" in func.__name__:
                    config["key"] = func.__name__[len("produce_to_"):]

                redis_queue = RedisQueue(**config)
                result = func(*args, **kwargs)

                if isinstance(result, (list, tuple)):
                    for item in result:
                        if isinstance(item, (list, dict)):
                            item = json.dumps(item)
                        redis_queue.send(item)
                elif result is not None:
                    if isinstance(result, (list, dict)):
                        result = json.dumps(result)
                    redis_queue.send(result)

                return result
            parsed_name = input_queue if input_queue is not None else self.parse_func_name(func)
            self._worker_funcs[parsed_name] = wrapper

            return wrapper
        return decorator

    def produce(self, queue=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                config = self.create_produce_config()
                if queue:
                    config["key"] = queue
                if "produce_to_" in func.__name__:
                    config["key"] = func.__name__[len("produce_to_"):]
                redis_queue = RedisQueue(**config)

                for item in func(*args, **kwargs):
                    if isinstance(item, (list, dict)):
                        item = json.dumps(item)
                    redis_queue.send(item)

            return wrapper
        return decorator

    def produce_to(self):
        """
        Produce items to be sent to specific queues.
        Send items to its corresponding queue using a RedisQueue.

        The decorated function should yield tuples of (queue_name, item_value).

        Example:
            @box.produce_to()
            def produce_multi(items):
                return items

            items = [
                ("foo1", "item1"),
                ("foo2", "item2"),
                ("foo3", "item3"),
                ("foo1", "item4"),
                ("foo2", "item5"),
                ("foo3", "item6"),
            ]
            produce_multi(items)

        In this example:
        - Each tuple in the `items` list represents a (queue, value) pair.
        - The first element of each tuple ("foo1", "foo2", "foo3") is the queue name.
        - The second element of each tuple ("item1", "item2", etc.) is the value to be sent to the queue.

        The decorator will process these items as follows:
        1. "item1" will be sent to the "foo1" queue
        2. "item2" will be sent to the "foo2" queue
        3. "item3" will be sent to the "foo3" queue
        4. "item4" will be sent to the "foo1" queue
        5. "item5" will be sent to the "foo2" queue
        6. "item6" will be sent to the "foo3" queue

        Notes:
        - If an item is a list or dict, it will be JSON-encoded before being sent to the queue.
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                config = self.create_produce_config()
                redis_queue = RedisQueue(**config)

                for queue, item in func(*args, **kwargs):
                    if isinstance(item, (list, dict)):
                        item = json.dumps(item)
                    redis_queue.send_to(queue, item)

            return wrapper
        return decorator

    def parse_func_name(self, func):
        return func.__name__

    def worker(self, queue=None):
        def decorator(func):
            parsed_name = queue if queue is not None else self.parse_func_name(func)
            self._worker_funcs[parsed_name] = func
            return func
        return decorator

    def start_workers(self, workers=10, config=config):
        n_workers = len(self._worker_funcs)
        if n_workers == 0:
            sys.stdout.write("No workers have been assigned with a decorator\n")
        if n_workers > workers:
            sys.stdout.write(f"Not enough workers, increasing the workers started with: {workers} we need atleast: {n_workers}\n")
            workers = n_workers

        startapp(list(self._worker_funcs.values()), workers=workers, config=config)

    def push_button(self, workers=None, wait=None):
        if workers is not None:
            self.workers = workers
        configs = [
            {
                "key": queue,
                "namespace": self.namespace,
                "redis_config": self.redis_config,
            } for queue in self._worker_funcs.keys()
        ]
        if self.timeout is not None or wait is not None:
            for config in configs:
                config["timeout"] = self.timeout or wait

        startapp(list(self._worker_funcs.values()), workers=self.workers, config=configs)


class InitFail(Exception):
    pass


def init_add(func_kwargs, init_items, init_kwargs):
    try:
        for name, config in init_kwargs.items():
            func_kwargs[name] = init_items[name](**config)
    except TypeError as e:
        raise InitFail from e
    return func_kwargs


def setup_init_items(func_kwargs, init_kwargs):
    return {name: func_kwargs[name] for name in init_kwargs.keys()}


def run_worker(func, func_kwargs, on_failure_func, config, worker_id, init_kwargs):  # noqa:C901
    if isinstance(func, list):
        func = func[worker_id % len(func)]
    if isinstance(config, list):
        config = config[worker_id % len(config)]

    item, r = None, None
    init_items = setup_init_items(func_kwargs, init_kwargs)
    while True:
        try:
            func_kwargs = init_add(func_kwargs, init_items, init_kwargs)
            r = RedisQueue(**config)  # TODO rename r
            sys.stdout.write('worker {worker_id} started. {func_name} listening to {queue} \n'.format(
                worker_id=worker_id, func_name=func.__name__, queue=config["key"]))
            for key_name, item in r:
                _, item = func(item.decode('utf-8'), worker_id, **func_kwargs), None
        except InitFail:
            sys.stdout.write('worker {worker_id} initialization failed\n'.format(worker_id=worker_id))
            traceback.print_exc()
            break
        except (KeyboardInterrupt, SystemExit):
            sys.stdout.write('worker {worker_id} stopped\n'.format(worker_id=worker_id))
            if item is not None:
                r.first_inline_send(item)
            break
        except Exception as e:
            sys.stdout.write('worker {worker_id} failed reason {e}\n'.format(worker_id=worker_id, e=e))
            if on_failure_func is not None:
                sys.stdout.write('worker {worker_id} running failure handler {e}\n'.format(worker_id=worker_id, e=e))
                on_failure_func(item, e, r, worker_id)
            item = None
            time.sleep(0.1)  # Throttle restarting

        if config.get('timeout') is not None:
            sys.stdout.write('timeout reached worker {worker_id} stopped\n'.format(worker_id=worker_id))
            break


def startapp(func, func_kwargs={}, workers=10, config=config, on_failure_func=None, init_kwargs={}):
    with Pool(workers) as p:
        args = ((func, func_kwargs, on_failure_func, config, worker_id, init_kwargs)
                for worker_id in range(1, workers + 1))
        try:
            p.starmap(run_worker, args)
        except (KeyboardInterrupt, SystemExit):
            sys.stdout.write('Starting Graceful exit\n')
            p.close()
            p.join()
    sys.stdout.write('Clean shut down\n')
