import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meesee import RedisQueue  # noqa: E402

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
