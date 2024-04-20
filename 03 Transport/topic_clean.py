import signal
import sys
from google.cloud import pubsub_v1

# TODO: Replace 'your-project-id' and 'your-subscription-id' with your actual project and subscription IDs.
project_id = "dataengineering-420401"
subscription_id = "my-sub"

def callback(message):
    print(f"Received and discarded message: {message.data.decode('utf-8')}")
    # Acknowledge the message immediately to discard it
    message.ack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def signal_handler(sig, frame):
        print('Interrupt received, shutting down...')
        streaming_pull_future.cancel()
        subscriber.close()
        sys.exit(0)

    # Register signal handler for graceful shutdown.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Block the main thread and wait for the future to complete.
    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            streaming_pull_future.cancel()
            print(f"An error occurred: {e}")
        finally:
            subscriber.close()
            print("Subscription closed.")

if __name__ == '__main__':
    main()
