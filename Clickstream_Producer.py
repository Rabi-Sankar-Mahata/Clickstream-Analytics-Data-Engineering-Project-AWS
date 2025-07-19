import boto3
import json
import time
import random

# Initialize Kinesis client
kinesis = boto3.client(
    'kinesis',
    region_name='ap-south-1',
    aws_access_key_id='AKIASQR6V43DCZJVPDKI',
    aws_secret_access_key='jkFKi08HTxAf0aDY67cyc/0sqV1IM+megcftY4Km'
)

# Sample users and pages
user_ids = [1001, 1002, 1003]
pages = ['home', 'search', 'product', 'cart', 'checkout']

# Infinite loop to send data
while True:
    data = {
        "user_id": random.choice(user_ids),
        "event_time": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "page": random.choice(pages),
        "action": random.choice(["click", "view", "add_to_cart"])
    }

    print(f"Sending: {data}")

    try:
        response = kinesis.put_record(
            StreamName="clickstream-data",
            Data=json.dumps(data),
            PartitionKey=str(data['user_id'])
        )
        print("PutRecord succeeded:", response)
    except Exception as e:
        print("PutRecord failed:", e)

    time.sleep(1)  
