import sys
from meesee import startapp

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}


def myfunc(item, worker_id):
    print('worker: {worker_id} hello, look at me, msg: {item}'.format(worker_id=worker_id, item=item))

if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w')+1]) if '-w' in sys.argv else 10
    startapp(myfunc, workers=workers, config=config)
