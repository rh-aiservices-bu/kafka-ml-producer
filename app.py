import os
import json
from kafka import KafkaConsumer, KafkaProducer
#from prediction import predict

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
#KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL')
KAFKA_SECURITY_PROTOCOL = 'SASL_PLAINTEXT'
#KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM')
KAFKA_SASL_MECHANISM = 'PLAIN'
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
#KAFKA_CONSUMER_GROUP = 'object-detection-consumer-group'
#KAFKA_CONSUMER_TOPIC = os.getenv('KAFKA_TOPIC_IMAGES')
KAFKA_PRODUCER_TOPIC = os.getenv('KAFKA_PRODUCER_TOPIC')


def main():
    # Normally, we'd never want to lose a message,
    # but we want to ignore old messages for this demo, so we set
    # enable_auto_commit=False
    # auto_offset_reset='latest' (Default)
    # This has the effect of starting from the last message.

    print(KAFKA_BOOTSTRAP_SERVER)
    print(KAFKA_USERNAME)
    print(KAFKA_PASSWORD)
    print(KAFKA_PRODUCER_TOPIC)
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             security_protocol=KAFKA_SECURITY_PROTOCOL,
                             sasl_mechanism=KAFKA_SASL_MECHANISM,
                             sasl_plain_username=KAFKA_USERNAME,
                             sasl_plain_password=KAFKA_PASSWORD,
                             api_version_auto_timeout_ms=30000,
                             max_block_ms=900000,
                             request_timeout_ms=450000,
                             acks='all')

    print(f'Subscribed to "{KAFKA_BOOTSTRAP_SERVER}" consuming topic "{KAFKA_CONSUMER_TOPIC}, producing messages on topic "{KAFKA_PRODUCER_TOPIC}"...')

    try:
        for record in consumer:
            msg = record.value.decode('utf-8')
            dict = json.loads(msg)
            #result = predict(dict)
            result = ['1','2']
            dict['prediction'] = result
            producer.send(KAFKA_PRODUCER_TOPIC, json.dumps(dict).encode('utf-8'))
            producer.flush()
    finally:
        print("Closing KafkaTransformer...")
        consumer.close()
    print("Kafka transformer stopped.")


if __name__ == '__main__':
    main()
