import os
import sys
import json
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from meesee import Meesee  # noqa: E402


box = Meesee()


@box.produce()
def produce_to_process_data(items):
    return items


@box.worker_producer(output_queue="foobar")
def process_data(item, worker_id):
    item = json.loads(item)
    wait = item["wait"]
    print(f"{worker_id} processing: {item} for {wait} seconds and send it too foobar")
    item["name"] = f"{item['name']}_processed"
    time.sleep(wait)
    return [item,]


@box.collector(wait=1, until=5)
def foobar(items): 
    return items


if __name__ == '__main__':
    workers = int(sys.argv[sys.argv.index('-w') + 1]) if '-w' in sys.argv else 10
    wait = int(sys.argv[sys.argv.index('--wait') + 1]) if '--wait' in sys.argv else 5
    items = [{"name": f"name{i}", "wait": wait} for i in range(10)]
    print(f"sending {len(items)} tasks to {workers} workers")
    print(f"simulate processing with with a wait of {wait}")
    start = time.time()

    produce_to_process_data(items)
    box.push_button(workers, wait=0.1)

    result = foobar()
    print(result)
    print("-----")
    result = foobar()
    print(result)
    print(f"done with running took: {round(time.time()- start, 2)}")
