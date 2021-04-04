import sys

def get_gps_data_from_dynamodb():
    try:
        import boto3
        import random
        from boto3.dynamodb.conditions import Key, Attr

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('gpsdata')

        randomNum = random.randint(1,30)
        response = table.query(
            KeyConditionExpression=Key('id').eq(randomNum),
            ScanIndexForward=False
        )

        items = response['Items']

        n=1 # limit to last 10 items
        data = items[:n]
        #data_reversed = data[::-1]
        
        return data

    except:
      
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_single_data_from_dynamodb(bookid):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response = table.query(
            IndexName="bookingid-datetime_value-index",
            KeyConditionExpression = Key('bookingid').eq(bookid+".0"),
            ScanIndexForward=True
        )

        items = response['Items']
        return items

    except:
      
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_single_driver_from_dynamodb(driverid):
    try:
        
        import boto3
        from boto3.dynamodb.conditions import Key, Attr

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

def get_offences_from_dynamodb(driverid):
    try:
        
        import boto3
        from boto3.dynamodb.conditions import Key, Attr

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('offenceTable')

        response = table.query(
            KeyConditionExpression = Key('driverid').eq(driverid)
        )

        items = response['Items']
        return items
    except:
  
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])


def get_all_data_from_dynamodb():
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response = table.query(
            IndexName = "vehicletype-datetime_value-index",
            KeyConditionExpression= Key('vehicletype').eq('GrabCar'),
            ScanIndexForward=True
        )

        items = response['Items']
        return items

    except:
    
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_all_driver_from_dynamodb():
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('driverTable')

        response = table.scan()
            
        data = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        
        return data
    except:
      
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_temperature_from_dynamodb(limit):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('temperature')

        response = table.query(
            IndexName = "location-datetime_value-index",
            KeyConditionExpression = Key('location').eq('Singapore'),
            ScanIndexForward=True
        )

        items = response['Items']
        n=limit
        data = items[:n]

        return data

    except:
   
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_maxdate_from_dynamodb():
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response = table.scan(ProjectionExpression='datetimestart_value')
        
        items = response['Items']

        data_reversed = items[::-1]
        return data_reversed

    except:

        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_dateiswhere_from_dynamodb(max_datetime):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response=table.query(
            IndexName = "vehicletype-datetime_value-index",
            KeyConditionExpression=Key('vehicletype').eq('GrabCar'),
            FilterExpression = Key('datetimestart_value').eq(max_datetime),
            ScanIndexForward=False
        )

        items = response['Items']
        
        data_reversed = items[::-1]

        return data_reversed

    except:
     
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_data_where_bookid_and_date_fromdynamodb(bookingid,max_datetime):
    try:

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response = table.query(
            IndexName = "bookingid-datetime_value-index",
            KeyConditionExpression=Key('bookingid').eq(bookingid),
            FilterExpression = Key('datetimestart_value').eq(max_datetime),
            ScanIndexForward=False
        )

        items = response['Items']

        data_reversed = items[::-1]
        
        return data_reversed


    except:
    
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_data_withlimit_from_dynamodb(limitnum):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response = table.query(
            IndexName = "vehicletype-datetime_value-index",
            KeyConditionExpression=Key('vehicletype').eq('GrabCar'),
            ScanIndexForward=False,
            Limit = limitnum
        )

        items = response['Items']
   
        return items


    except:
  
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_data_withbookingid_from_dynamodb(bookingid):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('iotapp')

        response = table.query(
            IndexName = "bookingid-datetime_value-index",
            KeyConditionExpression=Key('bookingid').eq(bookingid),
            ScanIndexForward=False
        )

        items = response['Items']

        return items

    except:
  
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def get_all_driverID():
    try:
        
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('driverTable')

        response = table.scan(ProjectionExpression='driverid')
        
        data = response['Items']
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        return data

    except:

        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def resetDemrit(driverid):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('driverTable')

        table.update_item(
            Key={'driverid': driverid},
            UpdateExpression='SET demritpoint = :val1',
            ExpressionAttributeValues ={
                ':val1' : 0
            }
        )

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def updateProfile(driverid, name, dob):
    try:
        import boto3
        from boto3.dynamodb.conditions import Key, Attr
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('driverTable')

        table.update_item(
            Key={'driverid': driverid},
            UpdateExpression='SET driverName = :val1, dob = :val2',
            ExpressionAttributeValues ={
                ':val1' : name,
                ':val2' : dob
            }
        )
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

if __name__ == "__main__":
    get_gps_data_from_dynamodb()