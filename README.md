# Meesee
[![CI](https://github.com/Attumm/meesee/actions/workflows/ci.yml/badge.svg)](https://github.com/Attumm/meesee/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Attumm/meesee/graph/badge.svg?token=upEkV8OYwI)](https://codecov.io/gh/Attumm/meesee)
[![Downloads](https://static.pepy.tech/badge/meesee)](https://pepy.tech/project/meesee)

Meesee is an task queue system featuring long-lived worker process parallelization through multiprocessing, with Redis as its backend. Engineered for reliability, efficiency, and ease of use, Meesee is tailored for distributed computing environments, particularly in big data and mission-critical software applications. By leveraging individual processes for each worker, Meesee circumvents Python's Global Interpreter Lock (GIL), making it ideal for compute intensive tasks.

## Production Proven

- In active production since 2018, powering the backends of at least three known companies
- Instances have demonstrated exceptional uptime, running for years without requiring maintenance
- The only identified scenario necessitating a restart is during network interface changes, which can leave workers connected to a non-functional network—an infrequent occurrence, but noteworthy for systems with multi-year uptime expectations
- Meesee workers are designed to restart without data loss, ensuring continuity even during rare restart events


## Core Design Principles

Meesee was specifically developed to address the following critical challenges in distributed computing:

1. **Long-Term Stability**: Ability to run for extended periods without maintenance or restarts
2. **Zero Message Loss**: Ensuring no messages are lost during service restarts for maintenance or deployments
3. **Optimized Performance**: Achieving surprising speed with minimal memory overhead for both client and Redis instances
4. **Deployment Flexibility**: Capability to schedule messages even when workers are offline during deployment
5. **Message Integrity Under Load**: Preventing message skips even during high-load scenarios
6. **Simplicity in Complexity**: Providing an intuitive interface to minimize the learning curve, acknowledging that distributed computing is challenging enough on its own


## Examples
How to [Examples](https://github.com/Attumm/meesee/tree/main/examples).

Create my_func that will 
1. print starting message.
2. Sleep 1 second.
3. print a ending message.

Let's start 10 of those.


```python
import time
from meesee import startapp

def my_func(item, worker_id):
    print("hello, look at me")
    time.sleep(1)
    print('finished item', locals())


startapp(my_func, workers=10)
```

Open another terminal, Let's produce some tasks
```python
from meesee import RedisQueue, config

def produce(items):
    r = RedisQueue(**config)
    for i in range(items):
        r.send(i)

produce(10)

```

Great, the placement of both scripts can be on any machine with connectivity to the redis instance.

## Install

```
$ pip install meesee
```

## Example Usage

Let's use Python to make writing workers and producers more fun.
Here's a simple [example](https://github.com/Attumm/meesee/tree/main/examples/example_decorator_magic_simple.py) demonstrating how to use Meesee the pythonic way.

```python
from meesee import Meesee 

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
```

This example demonstrates:
1. Creating a Meesee instance
2. Defining a worker function using the `@box.worker()` decorator
3. Defining a producer function using the `@box.produce()` decorator
4. Producing items to the queue
5. Starting workers to process the items



Example output
```bash
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
```

This output shows:
- Workers starting and listening to the 'foobar' queue
- Items being processed by different workers
- Workers shutting down after the timeout is reached

## Usage explained


Producers produce to workers, hence the name. They can either pass iterable values or iter themselves. For instance:

```python
@box.produce()
def produce():
    return [1, 2, 3]

# or 

@box.produce()
def produce_yield():
    yield from [1, 2, 3]
```

We can control which queue they will message to in two ways:

1. Specify the queue in the decorator:
```python
@box.produce(queue="foobar")
def produce_yield():
    yield from [1, 2, 3]
```
This will produce to the "foobar" queue.

2. Use magic naming:
```python
@box.produce()
def produce_to_foobar():
    yield from [1, 2, 3]
```
By naming the function `produce_to_foobar`, the function will also send the data to the "foobar" queue.

For workers, they are special in that they will start during multiprocessing. Here's an example to start 5 workers. Since we only set up one worker, all workers will be of that type:

```python
box.push_button(workers=5, wait=1)
```

This will start 5 worker processes, each listening to the queue specified in the worker function.

### Prerequisites

#### Redis instance

For Docker
```
$ docker run --name some-redis -d redis
```

## Support and Resources

- For feature requests, additional information or to report issues use github issues.
- Explore our comprehensive [examples](https://github.com/Attumm/meesee/tree/main/examples) for in-depth usage scenarios and best practices.
