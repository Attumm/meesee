# Meesee
[![Build Status](https://travis-ci.com/Attumm/meesee.svg?branch=main)](https://travis-ci.com/Attumm/meesee)

Task queue, Long lived workers process parallelization, with Redis as backend.
The project is used in production by three different companies.
There are Meesee instances that have been running without maintenance or restarts for more than one year.

Since the scope of the project is laser focussed on providing the following usecases.
There are no outstanding feature requests, the project is stable and no code are needed at the moment.
For feature request or additional information, an issue could be raised.
For examples on how to use Meesee there are [examples](https://github.com/Attumm/meesee/tree/main/examples) available.


1. Should be able to run for long periods, without maintenance or restarts.
2. Restarting the service for maintenance or deployments, should not lead to missing messages.
3. Should be reasonable fast and minimal amount of memory overhead for client and Redis instance.
4. Should be able to schedule messages when workers are offline during deployment.
5. Should not skip messages during certain scenario's such as heavy load.
6. Should try to be as simple as possible to use, without a big learning curve. Distributed computing is hard enough by itself.

## Examples

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


### Installing

Create a virtualenv for your project.
Install meesee:

```
$ . /path/to/virtualenv/bin/activate
$  pip install meesee
```

### Prerequisites

#### Redis instance

For Docker
```
$ docker run --name some-redis -d redis
```

For Debian, Ubuntu
```
$ sudo apt-get install redis-server
```
For Centos, Red Hat
```
$ sudo yum install redis
```

## Authors

* **Melvin Bijman** 
* **Mark Moes**

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

