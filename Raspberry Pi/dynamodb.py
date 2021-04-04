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

if __name__ == "__main__":
    get_gps_data_from_dynamodb()