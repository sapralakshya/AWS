import json
import boto3
import datetime

def lambda_handler(event, context):
    # TODO implement
    print(event['responsePayload'])
    car_data=event['responsePayload']
    bucket_name="testdatapipelineproject"
    current_time=datetime.datetime.now().timestamp()
    
    print("Start sending data into S3 bucket")
    s3=boto3.resource('s3')
    s3Object=s3.Object(bucket_name,f"inbox/{str(current_time)}_car_data.json")
    
    s3Object.put (
        Body=(bytes(json.dumps(car_data).encode('UTF-8')))
        )
        
    print("Data write successful")
