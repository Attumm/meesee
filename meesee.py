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
    def __init__(self, namespace, key, redis_config, maxsize=None, timeout=None,
                 retry_base_delay=0.1, retry_retries=30, retry_max_delay=32, self_healing=0):
        # TCP check if connection is alive, Sane defaults
        redis_config.setdefault('socket_keepalive', True)
        self.r = redis.Redis(**redis_config)
        self.key = key
        self.namespace = namespace
        self.maxsize = maxsize
        self.timeout = timeout
        self.list_key = self.format_list_key(namespace, key)
        self.retry_base_delay = retry_base_delay
        self.retry_retries = rertry_retries
        self.retry_max_delay = retry_max_delay

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

    def iter_simple(self):
        """Simple is better than complex; this version operates within stable network environments.
        There are versions with this flow that have been running continuously for years without restarts.
        It incurs almost nonexistent CPU overhead, can handle Redis restarting,
        but be cautious—networking issues, such as new routing or interface changes,
        could render it unable to retrieve any data. Tradeoff, between low overhead.
        """
        result = self.r.blpop(self.list_key, self.timeout)
        if result is None:
            raise StopIteration
        return result

    def self_repairing_wait(self):
        """No longer simple, and there could be network chatter,
        but it is able to handle the case of new routing and changes in the network interface.
        Unfortunately, we will experience a timeout twice before we can detect a network issue and restart the connection.
        Thus, the rule of thumb will be for two scenarios:

        1. Missing one or two batches is acceptable in the extreme situation of network changes.
        Additional benefits include keeping network chatter low and CPU usage almost nonexistent.
        Set the socket_timeout to be 110% of your tick.
        For example, if each batch is 10 seconds, set the socket_timeout to 11 seconds.

        2. In the context of mission-critical data or an unstable network.
        Set socket_timeout to 40% of the time of each tick in your pipeline.
        For instance, if we send data every 10 seconds, set the timeout to 4 seconds.
        The time to timeout is 4 seconds, and the time for ping to timeout is also 4 seconds.
        Thus it takes 8 seconds before we restart the connection."
        """
        while True:
            try:
                result = self.r.blpop(self.list_key, self.timeout)
            except redis.TimeoutError:
                result = None
                print("redis timeout")
                # ping tries ping pong with server
                # the redis lib will hang, if no connection with redis is present.
                # thus the tcp timeout will be triggered again. which will be catched by the caller
                # indicating the connection has gone stale.
                self.r.ping()
                debug()
            if result is not None:
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


def exponential_backoff(retry_number, base_delay=0.1, max_delay=32):
    delay = min(base_delay * (2 ** retry_number), max_delay)
    time.sleep(delay)


def run_worker(func, func_kwargs, on_failure_func, config, worker_id, init_kwargs):
    if isinstance(func, list):
        func = func[worker_id % len(func)]
    if isinstance(config, list):
        config = config[worker_id % len(config)]

    item, r = None, None
    init_items = setup_init_items(func_kwargs, init_kwargs)
    tries, max_tries, keep_running = 0, 10, True
    while keep_running:
        try:
            func_kwargs = init_add(func_kwargs, init_items, init_kwargs)
            r = RedisQueue(**config)  # TODO rename r
            sys.stdout.write('worker {worker_id} started\n'.format(worker_id=worker_id))
            for key_name, item in r:
                _, item, tries = func(item.decode('utf-8'), worker_id, **func_kwargs), None, 0
        except InitFail:
            sys.stdout.write('worker {worker_id} initialization failed\n'.format(worker_id=worker_id))
            traceback.print_exc()
            break
        except (KeyboardInterrupt, SystemExit):
            sys.stdout.write('worker {worker_id} stopped\n'.format(worker_id=worker_id))
            if item is not None:
                r.first_inline_send(item)
            break
        except redis.exceptions.TimeoutError:
            sys.stdout.write('worker {worker_id} failed connection retry: {tries}\n'.format(worker_id=worker_id, tries=tries))

            print("okay let's retry to connect")
            tries += 1
            if tries > max_tries:
                keep_running = False

            exponential_backoff(tries, base_delay=0.1, max_delay=32)

        except redis.exceptions.ConnectionError:
            sys.stdout.write('worker {worker_id} failed setup connection retry: {tries}\n'.format(worker_id=worker_id, tries=tries))

            print("let's gooo")
            tries += 1
            if tries > max_tries:
                keep_running = False

            exponential_backoff(tries, base_delay=0.1, max_delay=32)
        except Exception as e:
            print(type(e).__name__)
            import traceback
            traceback.print_exc()
            sys.exit()

            sys.stdout.write('worker {worker_id} failed reason {e}\n'.format(worker_id=worker_id, e=e))
            if on_failure_func is not None:
                sys.stdout.write('worker {worker_id} running failure handler {e}\n'.format(worker_id=worker_id, e=e))
                on_failure_func(item, e, r, worker_id)
            item = None
            time.sleep(0.1)  # Throttle restarting

        if config.get('timeout') is not None:
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
