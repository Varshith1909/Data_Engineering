import os
import json
import base64
import zlib
from datetime import datetime
from google.cloud import pubsub_v1, storage
from google.oauth2 import service_account
from threading import Lock, Timer

key_path = "/home/vc8/dataengineering-420401-abb2c17261fc.json"
project_id = "dataengineering-420401"
subscription_id = "archivetest-sub"
bucket_name = "archivetest-vc"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

storage_client = storage.Client.from_service_account_json(key_path)
bucket = storage_client.bucket(bucket_name)

accumulated_data = []
lock = Lock()

def process_data(message_data):
    data = json.loads(base64.b64decode(message_data).decode('utf-8'))
    return data

def write_to_bucket():
    with lock:
        if accumulated_data:
            filename = datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".json.gz"
            file_path = os.path.join("/tmp", filename)
            
            with open(file_path, 'wb') as json_file:
                compressed_data = zlib.compress(json.dumps(accumulated_data, indent=2).encode('utf-8'))
                json_file.write(compressed_data)
            
            blob = bucket.blob(filename)
            blob.upload_from_filename(file_path, content_type='application/json')
            
            print(f"Uploaded {len(accumulated_data)} messages to {filename}")
            
            accumulated_data.clear()
            
            os.remove(file_path)

def schedule_periodic_write():
    write_to_bucket()
    Timer(60, schedule_periodic_write).start()

def callback(message):
    try:
        message_data = message.data
        data = process_data(message_data)
        with lock:
            accumulated_data.append(data)
        message.ack()
    except Exception as e:
        print(f"Failed to process message: {str(e)}")
        message.nack()

def main():
    print(f"Listening for messages on {subscription_path}...")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    schedule_periodic_write()
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()

if __name__ == '__main__':
    main()
