from kafka import KafkaProducer


def create_producer(broker_ids):
    producer = KafkaProducer(bootstrap_servers = broker_ids)
    return producer