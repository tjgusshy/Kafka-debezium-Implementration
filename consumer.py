from confluent_kafka import Consumer, KafkaError, KafkaException

# Adjusted consumer configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'new-test-group2',  # Consider using a new group ID for a fresh start
    'auto.offset.reset': 'earliest'  # Ensure we start from the beginning of the topic
}

consumer = Consumer(**config)
consumer.subscribe(['cdc.public.transaction'])


while True:
            msg = consumer.poll(timeout=1.0)  # Increase poll timeout if needed

            if msg is None:
                continue  # No message, normal behavior

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, normal behavior but can adjust if not desired
                    print(f'End of partition reached {msg.topic()}/{msg.partition()}')
                else:
                    print(f'Error: {msg.error()}')
            else:
                # Message is successfully received
                print(f"Received message: {msg.value().decode('utf-8')}")




                
    

if __name__ == "__main__":
    consume_messages()



    
