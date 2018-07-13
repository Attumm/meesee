import time
from meesee import RedisQueue
from meesee import startapp

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100
}


def produce(items):
    r = RedisQueue(**config)
    for i in range(items):
        r.send(i)


def my_func(item, worker_id, name, number):
    print("hello, look at me")
    time.sleep(1)
    print('finished item', locals())


if __name__ == "__main__":
    # produce 10 items
    produce(10)
    # stop with keyboard interrupt
    startapp(my_func, workers=10, config=config, func_kwargs={'name': 'my_name', 'number': 9})
