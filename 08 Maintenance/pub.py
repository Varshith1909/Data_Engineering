import requests
import json
import base64
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor

project_id = 'dataengineering-420401'
topic_name = 'archivetest'
key_path = '/home/vc8/dataengineering-420401-abb2c17261fc.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/pubsub"]
)

publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_name)

def publish_breadcrumb(breadcrumb):
    try:
        message_json = json.dumps(breadcrumb)
        message_bytes = base64.b64encode(message_json.encode('utf-8'))
        future = publisher.publish(topic_path, data=message_bytes)
        future.result()
    except Exception as e:
        print(f"An error occurred while publishing: {e}")

def fetch_and_publish_breadcrumbs_for_vehicle(vehicle_id):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)
    if response.status_code == 200:
        breadcrumbs = response.json()
        for breadcrumb in breadcrumbs:
            publish_breadcrumb(breadcrumb)

def main():
    vehicle_ids = [3633, 3130, 3804, 3504, 3913, 3405, 3147, 3313, 3903, 3619, 4022, 3721, 3961, 3915, 3314, 4027, 3053, 3743,
                   4068, 3732, 4237, 3156, 4047, 3328, 3953, 3530, 3206, 4019, 3102, 3541, 3642, 3635, 4062, 3224, 3621, 3571, 3920,
                   3028, 4001, 3507, 4238, 3909, 3557, 3714, 3572, 4030, 3744, 99222, 3151, 3029, 2932, 3951, 4055, 4529, 3733, 3305,
                   2930, 4017, 3508, 4046, 3957, 3317, 3144, 3945, 4071, 3554, 3755, 4206, 3935, 4050, 3568, 3553, 3942, 3724, 2908,
                   4527, 2916, 3727, 3745, 3122, 3749, 4203, 4015, 3216, 3613, 3422, 3937, 3021, 3562, 3165, 3623, 3210, 3204, 3520,
                   3707, 4214, 2940, 4043, 3648]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_and_publish_breadcrumbs_for_vehicle, vehicle_ids)

    print("Finished publishing data for all vehicles.")

if __name__ == '__main__':
    main()
