import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

def addLabel(deviceid, datetime_value,label):
    try:
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table('iotapp')
        
        table.update_item(
            Key={'deviceid': deviceid, 'datetime_value': datetime_value},
            UpdateExpression='SET label = :val1',
            ExpressionAttributeValues={
                ':val1' : label
            }
        )

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_single_driver_from_dynamoAI(driverid):
    try:

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('driverTable')

        response = table.query(
            KeyConditionExpression = Key('driverid').eq(driverid)
        )

        items = response['Items']
        return items
    except:
  
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])


def addOffence(driverid, offence):
    try:
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table('driverTable')
        
        offence = Decimal(offence) + 1
        
        table.update_item(
            Key={'driverid': driverid},
            UpdateExpression='SET offence = :val1',
            ExpressionAttributeValues={
                ':val1' : offence 
            }
        )

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def addDemrit(driverid, offence):
    try:
        if(offence // 5) >= 1:
            demrit = (Decimal(offence) // 5) * 3
            dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
            table = dynamodb.Table('driverTable')
            
            table.update_item(
                Key={'driverid': driverid},
                UpdateExpression='SET demritpoint = :val1',
                ExpressionAttributeValues={
                    ':val1' : demrit
                }
            )

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def addOffenceRecord(driverid, datetime_value, longitude, latitude, speed):
    try:
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table('offenceTable')
        
 
        response = table.put_item(
            Item={
                'driverid': driverid,
                'occurence_time': datetime_value,
                'longitude': Decimal(str(longitude)),
                'latitude': Decimal(str(latitude)),
                'speed': Decimal(str(speed))
            }
        )

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

if __name__ == "__main__":
    addLabel()