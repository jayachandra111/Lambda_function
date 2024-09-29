import boto3
import urllib.parse
import json
from PIL import Image
import io
from io import BytesIO
from datetime import datetime, timedelta

print("*" * 80)
print("Initializing..")
print("*" * 80)

# Initialize AWS clients
s3 = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ResizedImages')

# SNS Topic ARN
topic_arn = 'arn:aws:sns:ap-southeast-2:423623834268:Mytopic-1'
topic_arnu = 'arn:aws:sns:ap-southeast-2:423623834268:Mytopic-urgent-2mails'

def lambda_handler(event, context):
    # Extract source bucket name and object key from the S3 event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
   
    # Define target bucket
    target_bucket = 'resized-img-2'
   
    # Log some information for debugging purposes
    print("Source bucket:", source_bucket)
    print("Target bucket:", target_bucket)
    print("Object key:", object_key)
    print("Log Stream name:", context.log_stream_name)
    print("Log Group name:", context.log_group_name)
    print("Request ID:", context.aws_request_id)
    print("Mem. limits(MB):", context.memory_limit_in_mb)
   
    try:
        # Wait for the object to exist before proceeding
        print("Waiting for the object to persist in the S3 service...")
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=object_key)
       
        # Download the image from the S3 bucket
        response = s3.get_object(Bucket=source_bucket, Key=object_key)
        image_content = response['Body'].read()

        # Open the image using Pillow
        img = Image.open(BytesIO(image_content))

        # Get the format of the image (e.g., PNG, JPEG, etc.)
        image_format = img.format
        print(f"Original image format: {image_format}")
       
        # Resize the image (e.g., 100x100)
        resized_img = img.resize((100, 100))
        image_byte_arr = io.BytesIO()
       
        # Save the resized image in its original format
        resized_img.save(image_byte_arr, format=image_format)
        image_byte_arr = image_byte_arr.getvalue()

        # Set the appropriate content type for S3 upload
        content_type_map = {
            'JPEG': 'image/jpeg',
            'JPG': 'image/jpeg',
            'PNG': 'image/png',
            'GIF': 'image/gif',
            # Add more formats if needed
        }
        content_type = content_type_map.get(image_format.upper(), 'image/jpeg')  # Default to 'image/jpeg' if unknown
        print(f"Uploading resized object {object_key} with content type: {content_type}")
       
        # Upload the resized image to the target bucket
        s3.put_object(Bucket=target_bucket, Key=object_key, Body=image_byte_arr, ContentType=content_type)
       
        print("Upload completed successfully.")
       
        # Log the resize operation in DynamoDB
        current_timestamp = int(datetime.utcnow().timestamp())  # Current time in seconds
        table.put_item(
            Item={
                'ObjectKey': object_key,
                'Timestamp': current_timestamp
            }
        )
       
        # Query DynamoDB to count how many objects have been resized in the last 10 minutes
        ten_minutes_ago = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())
        response = table.scan(
            FilterExpression='#ts >= :ten_minutes_ago',
            ExpressionAttributeNames={'#ts': 'Timestamp'},
            ExpressionAttributeValues={':ten_minutes_ago': ten_minutes_ago}
        )
        resize_count = len(response['Items'])
        print(f"Number of objects resized in the last 10 minutes: {resize_count}")
       
        # If more than 5 objects resized in the last 10 minutes, send an SNS notification
        if resize_count > 5:
            subject = "Lambda SNS Email Notification - High Resize Volume"
            message = f"More than 5 objects have been resized in the last 10 minutes. Total: {resize_count}"
            sns_client.publish(TopicArn=topic_arnu, Message=message, Subject=subject)
            print("SNS notification sent due to high resize volume.")
        else:
            subject = "Lambda SNS Email Notification"
            message = "Resized Image"
            sns_client.publish(TopicArn=topic_arn, Message=message, Subject=subject)
       
        return {
            'statusCode': 200,
            'body': json.dumps(f"Resized image {object_key} uploaded successfully to {target_bucket}")
        }
   
    except Exception as err:
        # Log and return the error
        print(f"Error: {err}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing object {object_key}: {str(err)}")
        }
