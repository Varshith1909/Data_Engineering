import sys
from google.cloud import pubsub_v1

def receive_messages(project_id, subscription_id):
    # Initialize the Subscriber client
    subscriber = pubsub_v1.SubscriberClient()
    # Create a fully qualified subscriber path
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        print(f"Received message: {message.data.decode('utf-8')}")
        message.ack()  # Acknowledge the message to prevent redelivery

    # Subscribe to the specified subscription and listen for messages
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Use a 'with' block to automatically manage resources and handle exceptions
    with subscriber:
        try:
            # Block indefinitely to receive messages
            streaming_pull_future.result()
        except KeyboardInterrupt:
            # Allows the script to be stopped with CTRL+C
            streaming_pull_future.cancel()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python receiver.py <project_id> <subscription_id>")
        sys.exit(1)

    project_id = sys.argv[1]  # Project ID from command line argument
    subscription_id = sys.argv[2]  # Subscription ID from command line argument
    receive_messages(project_id, subscription_id)