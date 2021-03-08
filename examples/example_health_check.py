import smtplib
from email.message import EmailMessage


def send_email(msg, to, from_, subject='default'):
    msg_email = EmailMessage()
    msg_email.set_content(msg)

    msg_email['Subject'] = subject
    msg_email['From'] = from_
    msg_email['To'] = to

    s = smtplib.SMTP('localhost')
    s.send_message(msg_email)
    s.quit()
    return True

config = {
    "namespace": "removeme",
    "key": "tasks",
    "redis_config": {},
    "maxsize": 100
}

from meesee import RedisQueue

# Note that max_allowed should be alteast one less then maxsize of the config
max_allowed = 10000
r = RedisQueue(**config)
if len(r) > max_allowed:
    send_email("items in quque {} is more then max allowed {}".format(len(r), max_allowed),
               "me@gmail.com",
               "to@gmail.com",
               "alarm title name"
               )
