
from meesee import RedisQueue, startapp

redis_config = {
    "host": '127.0.0.1',
    "port": 6380,
    "ssl": True,
    "ssl_keyfile":'test_redis_key.pem',
    "ssl_certfile":'test_redis_cert.pem',
    "ssl_cert_reqs":'required',
    "ssl_ca_certs":'test_redis_cert.pem'
}


config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": redis_config,
    "maxsize": 100,
    "timeout": 1,
}


def produce(items):
    r = RedisQueue(**config)
    for i in range(items):
        r.send(i)


def my_func(item, worker_id):
    print('worker: {worker_id} hello, look at me, msg: {item}'.format(worker_id=worker_id, item=item))

if __name__ == "__main__":
    # Create self-signed certs
    # openssl req -x509 -newkey rsa:4096 -keyout test_redis_key.pem -out test_redis_cert.pem -days 365 -nodes

    # Point redis to certs, add the following to your redis.conf
    # tls-port 6380
    # tls-cert-file /<your_path>/test_redis_cert.pem
    # tls-key-file /<your_path>/test_redis_key.pem
    # tls-ca-cert-file /your_path>/test_redis_cert.pem

    # Produce 10 tasks
    produce(10)
    # Start meesee and timeout after 1 second of no messages.
    startapp(my_func, workers=3, config=config)
