# Meesee
[![Build Status](https://travis-ci.org/Attumm/meesee.svg?branch=master)](https://travis-ci.org/Attumm/meesee)

Task queue, Long lived workers process parallelization, with Redis as backend.
The project is still used in production and has to knowlegde been used in 3 companies in production setting.

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

### Prerequisites

#### Redis

For Debian, Ubuntu
```
sudo apt-get install redis-server
```
For Centos, Red Hat
```
sudo yum install redis
```

### Installing

Create a virtualenv for your project.
Install meesee:

```
$ . /path/to/virtualenv/bin/activate
$  pip install meesee
```

## Authors

* **Melvin Bijman** 
* **Mark Moes**

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

