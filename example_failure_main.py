import time
from meesee import RedisQueue
from meesee import startapp

config = {
    "namespace": "remove_me",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100
}

def produce(items):
    r = RedisQueue(**config)
    for i in range(items):
        r.send(i)

def my_func(item, worker_id):
    print('got item')
    time.sleep(1)
    if item > 5:
        raise ValueError
    print('finished item', locals())

def my_failure_func(item, exception, r_instance, worker_id):
    print(item, str(exception), worker_id)

if __name__ == "__main__":
    produce(10)
    startapp(myfunc, workers=10, config=config, on_failure_func=my_failure_func)

