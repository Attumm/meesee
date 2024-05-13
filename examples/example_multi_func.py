import sys
from meesee import startapp

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}


def func_a(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_a', worker_id, item))


def func_b(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_b', worker_id, item))


def func_c(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_c', worker_id, item))


funcs = [func_a, func_b, func_c]

if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w') + 1]) if '-w' in sys.argv else 10
    startapp(funcs, workers=workers, config=config)
