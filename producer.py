from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed:", err)
    else:
        print("Message delivered to", msg.topic())

def produce_messages():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for i in range(5):
        producer.produce('test_topic', f'Message {i}', callback=delivery_report)

    producer.flush()

if __name__ == '__main__':
    produce_messages()
