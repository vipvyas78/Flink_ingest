# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import datetime
import logging
import logging.config
import random
import uuid
import logging.handlers
from json import dumps

from kafka import KafkaProducer


def generate_random_data():
    return random.randint(1, 10)


def connect_kafka(host, port):
    logging.info('Connecting to  Kafka[' + host + '] on port: ' + port)
    try:
        return KafkaProducer(bootstrap_servers=[host + ':' + port], client_id='producer1',
        value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as inst:
        logging.error(type(inst))
        logging.error('ERROR WHile connecting to Kafka broker at +[' + host + ':'+port + ']')



def topic_producer(names, page_uid, user_uuid):
    data = \
        {
            'user_id': page_uid,
            'page_id': user_uuid,
            'visit_timestamp': (datetime.datetime.now() - datetime.timedelta(days=generate_random_data())).strftime(
                "%Y-%m-%d %H:%M:%S").replace('"', '')
        }
    try:
        producer.send(names, value=data)
        logging.debug(data)
    except Exception as inst:
        logging.error(type(inst))
        logging.error('Error while producing data into teh topic')


if __name__ == '__main__':
    logging.config.fileConfig('conf/logging.conf')
    producer = connect_kafka('localhost', '9092')
    page_list = []
    user_list = []
    for page in range(1, 10):
        page_list.append(str(uuid.uuid4()))
    for user in range(1, 100):
        user_list.append(str(uuid.uuid4()))
    for i in range(1, 300):
        topic_producer('SourceEvents', random.choice(page_list), random.choice(user_list))
