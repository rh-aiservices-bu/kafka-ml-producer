import csv
import itertools
import json
from kafka import KafkaConsumer, KafkaProducer
import os
import time


KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL')
KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM','PLAIN')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
DELAY = os.getenv('PRODUCER_DELAY')
DATAFILE = 'network-data.csv'


def main():
    # Normally, we'd never want to lose a message,
    # but we want to ignore old messages for this demo, so we set
    # enable_auto_commit=False
    # auto_offset_reset='latest' (Default)
    # This has the effect of starting from the last message.

    print(KAFKA_BOOTSTRAP_SERVER)
    print(KAFKA_USERNAME)
    print(KAFKA_PASSWORD)
    print(KAFKA_TOPIC)

    with open(DATAFILE, 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        data = list(csv_reader)

    #remove column names before streaming    
    data.pop(0)

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             security_protocol=KAFKA_SECURITY_PROTOCOL,
                             sasl_mechanism=KAFKA_SASL_MECHANISM,
                             sasl_plain_username=KAFKA_USERNAME,
                             sasl_plain_password=KAFKA_PASSWORD,
                             api_version_auto_timeout_ms=30000,
                             max_block_ms=900000,
                             request_timeout_ms=450000,
                             acks='all')

    print(f'Subscribed to "{KAFKA_BOOTSTRAP_SERVER}" producing messages on topic "{KAFKA_TOPIC}"...')

    for item in data:
        time.sleep(float(DELAY))
        jsonpayload = json.dumps({'timestamp': item[0], 'value': item[1]})
        print(f'sending {jsonpayload}')
        producer.send(KAFKA_TOPIC, jsonpayload.encode('utf-8'))

    producer.flush()

if __name__ == '__main__':
    main()
