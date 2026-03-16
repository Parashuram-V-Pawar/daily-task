import boto3
from config.aws_clients import *

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table("notifications")

table.put_item(
    Item={
       "user_id": "USR0100",
        "notification_id": "notif_new_test_1",
        "message": "Trigger test again",
        "time_stamp": "2026-03-17T19:00:00"
    }
)