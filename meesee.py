import sys
import time
import syslog
import json
import redis

from multiprocessing import Pool

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 1000
}


class RedisQueue:
    def __init__(self, namespace, key, redis_config, maxsize=None, timeout=None):
        self.r = redis.Redis(**redis_config)
        self.list_key = '{}:{}'.format(namespace, key)
        self.namespace = namespace
        self.maxsize = maxsize
        self.timeout = timeout

    def first_inline_send(self, item):
        #TODO rename method
        self.r.lpush(self.list_key, item)
    
    def send_to(self, key, item):
        self.r.rpush('{}:{}'.format(self.namespace, key), item)
        
    def send(self, item):
        """Adds item to the end of the Redis List.

        Side-effects:
           If size is above max size, the operation will keep the size the same.
           Note that if does not resize the list to maxsize.
        """
        if self.maxsize is not None and self.r.llen(self.list_key) >= self.maxsize:
            self.r.lpop(self.list_key)
        self.r.rpush(self.list_key, item)

    def send_dict(self, item):
        self.send(json.dumps(item))

    def __iter__(self):
        return self

    def __next__(self):
        result = self.r.blpop(self.list_key, self.timeout)
        if result is None:
            raise StopIteration
        return result[0]


def run_worker(func, on_failure_func, config, worker_id):
    item, r = None, None
    while True:
        try:
            r = RedisQueue(**config)
            sys.stdout.write('worker {worker_id} started\n'.format(worker_id=worker_id))
            for item in r:
                func(item, worker_id)
        except (KeyboardInterrupt, SystemExit):
            sys.stdout.write('worker {worker_id} stopped\n'.format(worker_id=worker_id))
            r.first_inline_send(item)
            break
        except Exception as e:
            sys.stdout.write('worker {worker_id} failed reason {e}\n'.format(worker_id=worker_id, e=e))
            if on_failure_func is not None:
                sys.stdout.write('worker {worker_id} running failure handler {e}\n'.format(worker_id=worker_id, e=e))
                on_failure_func(item, e, r, worker_id)
            time.sleep(0.1)  # Throttle restarting

        if config.get('timeout') is not None:
            break


def startapp(func, workers=10, config=config, on_failure_func=None):
    p = Pool(workers)
    args = ((func, on_failure_func, config, worker_id) for worker_id in range(1, workers+1))
    try:
        p.starmap(run_worker, args)
    except (KeyboardInterrupt, SystemExit):
        sys.stdout.write('Starting Gracefull exit\n')
        p.close()
        p.join()
    finally:
        sys.stdout.write('Clean shut down\n')
