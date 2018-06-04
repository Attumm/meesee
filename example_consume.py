import sys
from meesee import startapp

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100
}


def myfunc(item, worker_id):
    print('{worker_id} hello, look at me'.format(worker_id=worker_id))

if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w')+1]) if '-w' in sys.argv else 10
    startapp(myfunc, workers=workers, config=config)
