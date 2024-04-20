import json
import time
from google.cloud import pubsub_v1

project_id = "dataengineering-420401"
topic_id = "testpubsub"

# Initialize the Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Read and parse the JSON file
with open('bcsample.json', 'r') as file:
    records = json.load(file)

# Publish each record in the JSON file to the Pub/Sub topic
for record in records:
    # Convert the record to a JSON string and encode it to bytes
    data_str = json.dumps(record)
    data = data_str.encode('utf-8')
    
    # Publish the data and wait for the future to resolve
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

    time.sleep(0.25)

print(f"Published messages to {topic_path}.")