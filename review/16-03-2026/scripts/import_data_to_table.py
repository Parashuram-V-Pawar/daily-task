import json
import logging
from config.aws_clients import get_dynamodb_resource


dynamodb = get_dynamodb_resource()

table = dynamodb.Table("notifications")
logging.info("Loading json data to table.")
with open("data.json", "r") as f:
    data = json.load(f)
for item in data:
    table.put_item(Item = item)
logging.info("Loading complete")