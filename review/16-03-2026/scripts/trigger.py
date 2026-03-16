import json
import boto3
import boto3.dynamodb.conditions

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('notifications')

# function to trigger when data is inserted into the table in dynamodb
def lambda_handler(event, context):
    user_id = None
    if 'Records' in event and 'dynamodb' in event['Records'][0]:
        for record in event['Records']:
            if record['eventName'] == 'INSERT':
                new_image = record['dynamodb']['NewImage']
                user_id = new_image['user_id']['S']
                notification_id = new_image['notification_id']['S']
                message = new_image['message']['S']
                time_stamp = new_image['time_stamp']['S']
                print(f"New notification inserted: {user_id}, {notification_id}, {message}, {time_stamp}")

    # Query the table to get all notifications for a specific user_id
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('user_id').eq(user_id),
        ScanIndexForward=False,
        Limit=5
    )
    notifications = response.get('Items', [])
    return {
        'statusCode': 200,
        'body': json.dumps(notifications)
    }
