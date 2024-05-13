import sys
from meesee import startapp

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100
}


def my_func(item, worker_id):
    print('{worker_id} hello, look at me'.format(worker_id=worker_id))


if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w') + 1]) if '-w' in sys.argv else 10
    startapp(my_func, workers=workers, config=config)
