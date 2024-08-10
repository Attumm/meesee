import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meesee import Meesee  # noqa: E402


box = Meesee()


@box.worker()
def foobar(item, worker_id):
    print('func: foobar, worker_id: {}, item: {}'.format(worker_id, item))


@box.produce()
def produce_to_foobar(items):
    return items


if __name__ == '__main__':
    items = [{"name": f"name{i}"} for i in range(10)]
    produce_to_foobar(items)
    box.push_button(workers=5, wait=1)

# Example output
"""
worker 1 started. foobar listening to foobar
worker 2 started. foobar listening to foobar
worker 3 started. foobar listening to foobar
worker 4 started. foobar listening to foobar
func: foobar, worker_id: 1, item: {"name": "name0"}
func: foobar, worker_id: 1, item: {"name": "name1"}
worker 5 started. foobar listening to foobar
func: foobar, worker_id: 2, item: {"name": "name4"}
func: foobar, worker_id: 3, item: {"name": "name2"}
func: foobar, worker_id: 4, item: {"name": "name3"}
func: foobar, worker_id: 1, item: {"name": "name5"}
func: foobar, worker_id: 1, item: {"name": "name6"}
func: foobar, worker_id: 3, item: {"name": "name7"}
func: foobar, worker_id: 4, item: {"name": "name8"}
func: foobar, worker_id: 2, item: {"name": "name9"}
timeout reached worker 5 stopped
timeout reached worker 2 stopped
timeout reached worker 1 stopped
timeout reached worker 4 stopped
timeout reached worker 3 stopped
Clean shut down
"""
