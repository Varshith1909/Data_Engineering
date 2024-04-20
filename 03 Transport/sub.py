import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO: Replace these placeholders with your actual project ID and subscription ID
project_id = "dataengineering-420401"
subscription_id = "my-sub"
# Number of seconds the subscriber should listen for messages
#timeout = 5.0

# Initialize the Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Decode the message data from bytes to a JSON string and then to a Python dictionary
    data = json.loads(message.data.decode('utf-8'))
    print(f"Received message with data: {data}")
    # Acknowledge the message so it won't be redelivered
    message.ack()

# Create a streaming pull future to manage background message streaming
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Wrap the subscriber in a 'with' block to automatically handle closing the client
with subscriber:
    try:
        # Block for the specified timeout and process messages as they arrive
        streaming_pull_future.result()
    except TimeoutError:
        # Handle timeout where no messages are received within the specified period
        print("Timed out waiting for messages.")
    except Exception as e:
        # Handle other exceptions such as issues with the Google Cloud Pub/Sub service
        print(f"An error occurred: {e}")
    finally:
        # Cancel the streaming pull operation to stop receiving messages
        streaming_pull_future.cancel()
        # Ensure all resources are freed properly
        streaming_pull_future.result()

    print("Finished listening for messages.")