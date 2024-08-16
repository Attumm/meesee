import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meesee import Meesee  # noqa: E402

box = Meesee()


@box.produce_to()
def produce_multi(items):
    return items


@box.worker()
def foo1(item, worker_id):
    print(f"{worker_id} {item} foo1")
    return [item,]


@box.worker()
def foo2(item, worker_id):
    print(f"{worker_id} {item} foo2")
    return [item,]


@box.worker()
def foo3(item, worker_id):
    print(f"{worker_id} {item} foo3")
    return [item,]


if __name__ == '__main__':
    items = [
        ("foo1", "item1"),
        ("foo2", "item2"),
        ("foo3", "item3"),
        ("foo1", "item4"),
        ("foo2", "item5"),
        ("foo3", "item6"),
    ]
    produce_multi(items)
    box.push_button(wait=1)
