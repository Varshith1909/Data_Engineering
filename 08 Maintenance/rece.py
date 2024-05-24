import os
import json
import base64
import zlib
from datetime import datetime
from google.cloud import pubsub_v1, storage
from google.oauth2 import service_account
from threading import Lock, Timer
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding
import os

with open("private_key.pem", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

with open("public_key.pem", "rb") as key_file:
    public_key = serialization.load_pem_public_key(
        key_file.read(),
    )

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

def encrypt_data(data, public_key):
    symmetric_key = os.urandom(32)
    
    padder = sym_padding.PKCS7(128).padder()
    padded_data = padder.update(data) + padder.finalize()
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(symmetric_key), modes.CBC(iv))
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    
    encrypted_symmetric_key = public_key.encrypt(
        symmetric_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    
    return encrypted_symmetric_key, iv, encrypted_data

def write_to_bucket():
    with lock:
        if accumulated_data:
            filename = datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".json.gz.enc"
            file_path = os.path.join("/tmp", filename)
            
            compressed_data = zlib.compress(json.dumps(accumulated_data, indent=2).encode('utf-8'))
            
            encrypted_symmetric_key, iv, encrypted_data = encrypt_data(compressed_data, public_key)
            
            with open(file_path, 'wb') as encrypted_file:
                encrypted_file.write(encrypted_symmetric_key)
                encrypted_file.write(iv)
                encrypted_file.write(encrypted_data)
            
            blob = bucket.blob(filename)
            blob.upload_from_filename(file_path, content_type='application/octet-stream')
            
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
