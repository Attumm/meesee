import time
from meesee import RedisQueue
from meesee import startapp

config = {
    "namespace": "remove_me",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}


def produce(items):
    r = RedisQueue(**config)
    for i in range(items):
        r.send(i)


def my_func(item, worker_id):
    if item is None or item == 'None':
        print('regression: item is None')
    print('got item {}'.format(locals()))

def raise_sys_exit(item, worker_id):
    print('raise sys_exit')
    raise SystemExit 


if __name__ == "__main__":
    produce(1)
    startapp(raise_sys_exit, workers=1, config=config)
    startapp(my_func, workers=1, config=config)

