from __future__ import print_function
import json
import boto3
from datetime import datetime, timedelta
import pytz

print('Loading function')


class S3Instance:
    """AWS Instance for Open """
    def __init__(self, resourceType):
        self.client = boto3.client(resourceType)
        self.resource = boto3.resource(resourceType)
    def list_objects(self, bucket, prefix=None):
        if prefix is None:
            response = self.client.list_objects(Bucket=bucket)
        else:
            response = self.client.list_objects(Bucket=bucket, Prefix=prefix)
        return response['Contents']
    def get_objects(self, bucket, key):
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response
    def convert_time_zone(self, date, zone):
        converted_date = date.replace(tzinfo=zone)
        return converted_date
    def delete_object(self, bucket, prefix):
        bucket_list = self.resource.Bucket(bucket)
        response = bucket_list.objects.filter(Prefix=prefix).delete()
        return response


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    current_object = event['Records'][0]['s3']['object']['key']
    object_parent = "{0}/".format(current_object.split('/')[0])
    print("parent object is {0}".format(object_parent))
    client = S3Instance('s3')
    current_object_get = client.get_objects(bucket, current_object)
    print(current_object_get)
    if bucket is not None:
        contents = client.list_objects(bucket)
        print(contents)
        contents_sorted = sorted(contents, key=lambda x: client.convert_time_zone(x['LastModified'], pytz.UTC))
        print(contents)
        expire_date = current_object_get['LastModified'].replace(tzinfo=pytz.UTC) - timedelta(minutes=30)
        print(expire_date)
        backup = {}
        for s3_object in contents_sorted:
            if s3_object['LastModified'] < expire_date:
                if not s3_object['Key'].startswith(object_parent):
                    parent_key = s3_object['Key'].split('/')[0]
                    if parent_key not in backup:
                        backup[parent_key] = s3_object['Key']
        if backup is not None:
            for prefix in backup:
                delete_list = client.delete_object(bucket, prefix)
                print(delete_list)
                if len(delete_list) > 0:
                    for delete_objects in delete_list[0]['Deleted']:
                        print("Deleted Key {0} from Prefix {1}".format(delete_objects['Key'], prefix))
                else:
                    print("nothing to delete")
    else:
        print("bucket {0} name is not found".format(bucket))
