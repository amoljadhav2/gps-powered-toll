import boto3
from datetime import date
import logging
import logging.handlers
from configparser import ConfigParser
import os

def connectSqs():
    # Create SQS client
    sqs = boto3.client('sqs', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=sqs_endpoint_url)
    return sqs

def connectSns():
    # Create SNS client
    sns = boto3.client('sns', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    return sns

def connectEc2():
    # Create EC2 client
    ec2 = boto3.client('ec2', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    return ec2;

def connectIam():
    # Create an IAM client
    iam = boto3.client('iam', region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    return iam

dir_name = os.path.normpath(os.getcwd())
conf_file = [os.path.normpath(os.path.join(dir_name, "python", "gpt.ini").replace('python\\util', ''))]
config_read = ConfigParser()
config_read.read(conf_file)
aws_access_key_id = config_read.get('common', 'aws_access_key_id')
aws_secret_access_key = config_read.get('common', 'aws_secret_access_key')
region_name = config_read.get('common', 'region_name')
sqs_endpoint_url = config_read.get('aws', 'sqs_endpoint_url')
sns_topic_url = config_read.get('aws', 'sns_topic_url')

