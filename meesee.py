import sys
import time
import json
import traceback
import redis

from multiprocessing import Pool

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 1000,
}


class RedisQueue:
    def __init__(self, namespace, key, redis_config, maxsize=None, timeout=None):
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


def run_worker(func, func_kwargs, on_failure_func, config, worker_id, init_kwargs):
    if isinstance(func, list):
        func = func[worker_id%len(func)]
    if isinstance(config, list):
        config = config[worker_id%len(config)]

    item, r = None, None
    init_items = setup_init_items(func_kwargs, init_kwargs)
    while True:
        try:
            func_kwargs = init_add(func_kwargs, init_items, init_kwargs)
            r = RedisQueue(**config)  # TODO rename r
            sys.stdout.write('worker {worker_id} started\n'.format(worker_id=worker_id))
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
            break


def startapp(func, func_kwargs={}, workers=10, config=config, on_failure_func=None, init_kwargs={}):
    p = Pool(workers)
    args = ((func, func_kwargs, on_failure_func, config, worker_id, init_kwargs) for worker_id in
            range(1, workers + 1))
    try:
        p.starmap(run_worker, args)
    except (KeyboardInterrupt, SystemExit):
        sys.stdout.write('Starting Graceful exit\n')
        p.close()
        p.join()
    finally:
        sys.stdout.write('Clean shut down\n')
