import time
import random

from meesee import RedisQueue
from meesee import startapp


config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 10000,
    "timeout": 1,
}


class SchroderCat:
    def __init__(self, name):
        self.name = name
        self.alive = None

    def __str__(self):
        status = str(self.alive) if self.alive is not None else 'Unknown'
        return 'name: {} is_alive: {}'.format(self.name, status)

    def __repr__(self):
        return str(self)

    def look_in_the_box(self):
        if self.alive is None:
            self.alive = bool(random.getrandbits(1))
        return self.alive


def produce(items):
    r = RedisQueue(**config)
    for i in range(items):
        r.send(i)


def myfunc(item, worker_id, ana, bob, citrus):
    time.sleep(1)
    ana.look_in_the_box()
    bob.look_in_the_box()
    citrus.look_in_the_box()
    print('finished item', locals())


if __name__ == "__main__":
    # produce 100 items
    produce(100)

    names = ['ana', 'bob', 'citrus']
    kwargs = {name: SchroderCat for name in names}
    init_kwargs = {name: {'name': name} for name in names}

    # stop with keyboard intrupped
    startapp(myfunc, workers=10, config=config, func_kwargs=kwargs, init_kwargs=init_kwargs)
