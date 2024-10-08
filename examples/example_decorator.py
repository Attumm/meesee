import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meesee import Meesee  # noqa: E402

box = Meesee()


@box.worker()
def func_a(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_a', worker_id, item))


@box.worker()
def func_b(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_b', worker_id, item))


@box.worker()
def func_c(item, worker_id):
    print('func: {}, worker_id: {}, item: {}'.format('func_c', worker_id, item))


if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w') + 1]) if '-w' in sys.argv else 10
    box.push_button(workers=workers)
