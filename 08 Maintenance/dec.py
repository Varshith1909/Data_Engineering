import os
import zlib
import json
from google.cloud import storage
from google.oauth2 import service_account
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding

with open("private_key.pem", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

key_path = "/home/vc8/dataengineering-420401-abb2c17261fc.json"
bucket_name = "archivetest-vc"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
storage_client = storage.Client.from_service_account_json(key_path)
bucket = storage_client.bucket(bucket_name)

def decrypt_data(encrypted_symmetric_key, iv, encrypted_data, private_key):
    symmetric_key = private_key.decrypt(
        encrypted_symmetric_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    
    cipher = Cipher(algorithms.AES(symmetric_key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(encrypted_data) + decryptor.finalize()
    
    unpadder = sym_padding.PKCS7(128).unpadder()
    data = unpadder.update(padded_data) + unpadder.finalize()
    
    return data

def process_file(blob_name):
    blob = bucket.blob(blob_name)
    file_path = os.path.join("/tmp", blob_name)
    blob.download_to_filename(file_path)
    
    with open(file_path, 'rb') as encrypted_file:
        encrypted_symmetric_key = encrypted_file.read(256)
        iv = encrypted_file.read(16)
        encrypted_data = encrypted_file.read()
    
    decrypted_data = decrypt_data(encrypted_symmetric_key, iv, encrypted_data, private_key)
    
    decompressed_data = zlib.decompress(decrypted_data)
    
    json_data = json.loads(decompressed_data.decode('utf-8'))
    
    print(f"Decrypted and decompressed data from {blob_name}:")
    print(json.dumps(json_data, indent=2))
    
    os.remove(file_path)

def main():
    blobs = storage_client.list_blobs(bucket_name)
    
    for blob in blobs:
        if blob.name.endswith(".json.gz.enc"):
            process_file(blob.name)

if __name__ == '__main__':
    main()
