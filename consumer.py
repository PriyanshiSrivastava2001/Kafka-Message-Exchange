from confluent_kafka import Consumer

def consume_messages():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['test_topic'])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

if __name__ == '__main__':
    consume_messages()
