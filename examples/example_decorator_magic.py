import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meesee import Meesee  # noqa: E402


config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100,
    "timeout": 1,
}

box = Meesee(config)


@box.worker()
def foobar(item, worker_id):
    print('func: foobar, worker_id: {}, item: {}'.format(worker_id, item))


@box.worker()
def name_of_the_function(item, worker_id):
    print('func: name_of_the_function, worker_id: {}, item: {}'.format(worker_id, item))


@box.worker(queue="passed_name")
def passed_name_not_this_one(item, worker_id):
    print('func: passed_name_not_this_one, worker_id: {}, item: {}'.format(worker_id, item))


@box.produce(queue="passed_name")
def produce_some_items(amount):
    yield from range(amount)


@box.produce()
def produce_to_foobar(items):
    return items


if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w') + 1]) if '-w' in sys.argv else 10
    produce_some_items(10)
    items = [{"name": f"name{i}"} for i in range(10)]
    produce_to_foobar(items)
    box.push_button(workers, wait=1)
