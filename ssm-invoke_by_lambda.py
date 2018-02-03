from __future__ import print_function

import json

import boto3
import os
print('Loading function')


class SSMInstance:
    """AWS Instance for Open """
    def __init__(self, resource_type):
        self.client = boto3.client(resource_type)

    def run_command(self, target_instance, document_name, parameters=None, logoutput_s3bucket=None):
        if parameters and logoutput_s3bucket is None:
            response = self.client.send_command(Targets=[
                {
                    'Key': 'tag:Name',
                    "Values": [
                        target_instance
                    ]
                }
            ],
                DocumentName=document_name,
                MaxConcurrency='1',
                MaxErrors='1'
            )
            return response
        elif logoutput_s3bucket is None:
            response = self.client.send_command(Targets=[
                {
                    'Key': 'tag:Name',
                    "Values": [
                        target_instance
                    ]
                }
            ],
                Parameters=parameters,
                DocumentName=document_name,
                MaxConcurrency='1',
                MaxErrors='1'
            )
            return response
        else:
            print(logoutput_s3bucket)
            response = self.client.send_command(Targets=[
                {
                    'Key': 'tag:Name',
                    "Values": [
                        target_instance
                    ]
                }
            ],
                OutputS3Region=logoutput_s3bucket['region'],
                OutputS3BucketName=logoutput_s3bucket['bucket_name'],
                OutputS3KeyPrefix=logoutput_s3bucket['s3key_prefix'],
                Parameters=parameters,
                DocumentName=document_name,
                MaxConcurrency='1',
                MaxErrors='1'
            )
            return response


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    bucketName = event['Records'][0]['s3']['bucket']['name']
    objectName = event['Records'][0]['s3']['object']['key']
    ssmClient = SSMInstance('ssm')
    log_bucket_name = os.environ['LOG_BUCKET_NAME']
    log_s3key_prefix = os.environ['LOG_S3KEY_PREFIX']
    log_region = os.environ['LOG_REGION']
    document_name = os.environ['DOCUMENT_NAME']
    target_instance = os.environ['TARGET_INSTANCE']
    parameters = {
        "bucketName": [
            bucketName
        ],
        "objectName": [
            objectName
        ]
    }
    if target_instance and document_name is None:
        print("you have not set target_instance and document_name on environ")
        return "you have not set target_instance and document_name on environ"
    if log_bucket_name and log_s3key_prefix and log_region:
        log_dict={
            "region": log_region,
            "bucket_name": log_bucket_name,
            "s3key_prefix": log_s3key_prefix
        }
        response = ssmClient.run_command(target_instance=target_instance, document_name=document_name,parameters=parameters,logoutput_s3bucket=log_dict)
        print (response)
        return "CommandID: {0}".format(response['Command']['CommandId'])
    elif bucketName and objectName is not None:
        response = ssmClient.run_command(target_instance=target_instance, document_name=document_name,parameters=parameters)
        print (response)
        print("CommandID: {0}".format(response['Command']['CommandId']))
        return "CommandID: {0} ".format(response['Command']['CommandId'])
    else:
        print("BucketName and object id  is not valid")
