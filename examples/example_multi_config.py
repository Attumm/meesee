import sys
from meesee import startapp, RedisQueue

configs = [{
    "namespace": "removeme",
    "key": "tasks_a",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}, {
    "namespace": "removeme",
    "key": "tasks_b",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}, {
    "namespace": "removeme",
    "key": "tasks_c",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}]


def func_a(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_a', worker_id, item))


def func_b(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_b', worker_id, item))


def func_c(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_c', worker_id, item))


funcs = [func_a, func_b, func_c]


def produce(items, configs):
    r = RedisQueue(**configs[0])
    for config in configs:
        r.set_list_key(key=config['key'], namespace=config['namespace'])
        for _ in range(items):
            r.send(config['key'])


if __name__ == '__main__':
    produce(3, configs)
    workers = int(sys.argv[sys.argv.index('-w') + 1]) if '-w' in sys.argv else 10
    startapp(funcs, workers=workers, config=configs)
