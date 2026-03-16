import os
import boto3
from dotenv import load_dotenv

load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

def get_dynamodb_resource():
    session = boto3.resource("dynamodb", region_name=AWS_REGION)
    return session

def get_dynamodb_client():
    session = boto3.client("dynamodb", region_name=AWS_REGION)
    return session
