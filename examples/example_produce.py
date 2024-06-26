import sys
from meesee import RedisQueue

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


if __name__ == "__main__":
    amount = int(sys.argv[sys.argv.index('-p') + 1]) if '-p' in sys.argv else 10
    produce(amount)
