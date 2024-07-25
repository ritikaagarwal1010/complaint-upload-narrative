import os
import json
import time
import uuid
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_resource = boto3.resource("s3")
dynamodb = boto3.client('dynamodb')

s3_bucket = os.environ["s3_bucket"]
table_name = os.environ["table_name"]

def lambda_handler(event, context):
    logger.info(event)
    uid4 = None
    exception_text = None
    statusCode = None
    try:
        if "invokeapi" in event['rawPath']:
            content = json.loads(event["body"])
            
            narrative_time = content["time"]
            narrative = content["narrative"]
            uid4 = uuid.uuid4()
            content["uuid"] = str(uid4)
            current_date = narrative_time[:10]
            print("content:",content)
            s3_path = write_json_to_s3(str(uid4), narrative, current_date, \
                                s3_bucket, narrative_time)
            s3_uri = 's3://'+s3_bucket+'/'+s3_path+''
            print(s3_uri)
    except Exception as exception:
            logger.error("Error %s in %s saving the raw data",exception)
            exception_text = str(exception)
            logger.info(exception_text)
            return {
            'statusCode': 503,
            'body': json.dumps(str(exception_text))
            }
            
    try:
        response = create_table(table_name)
    except Exception as table_exception:
        logger.error("Error %s in %s creating the table",table_exception)
        exception_text = str(table_exception)
        logger.info(exception_text)
        
    try:
        response = insert_data(table_name ,str(uid4), s3_uri, narrative_time)
        logger.info("item has been created")
        return {
            'statusCode': 200,
            'body': json.dumps(str(uid4))
        }
    except Exception as insert_exception:
        logger.error("Error %s in %s saving the raw data",insert_exception)
        exception_text = str(insert_exception)
        logger.info(exception_text)
        return {
            'statusCode': 503,
            'body': json.dumps(str(exception_text))
        }
        

def write_json_to_s3(uid4, narrative, current_date, s3_bucket, datetime):
    path = 'narrative/'+current_date+'/'+str(uid4)+'.json'
    try:
        obj = s3_resource.Object(s3_bucket, path)
        json_object = {
                        'narrative' : narrative,
                        'datetime' : current_date
                    }
        obj.put(Body=(bytes(json.dumps(json_object).encode('UTF-8'))))
    except Exception as exception:
        logger.error("Error in S3 Updation",exception)
        exception_text = str(exception)
    return path
    
def create_table(table_name):
    # Create DynamoDB table
    try:
        create_table_response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'uuid',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'uuid',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        logger.info(f"Table {table_name} created successfully.")
        return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB table created')
        }
    except dynamodb.exceptions.ResourceInUseException:
        logger.info(f"Table {table_name} already exists.")
    
def insert_data(table_name, item_uuid, s3_uri, dt):
    # Put item into table
    try:
        put_item_response = dynamodb.put_item(
            TableName= table_name,
            Item={
                'uuid': {'S': str(item_uuid)},
                's3_uri': {'S': s3_uri},
                'datetime': {'S': dt}
            })
        logger.info(f"Item with uuid {item_uuid} added successfully.")
    except Exception as e:
        logger.info(f"Error putting item: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB item setup completed.')
    }

    