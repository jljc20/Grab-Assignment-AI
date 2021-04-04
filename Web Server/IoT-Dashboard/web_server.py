from flask import Flask, render_template,send_file, jsonify, request,Response

import argparse
import sys

import json
import jsonconverter as jsonc

import cv2
import base64
import dynamodb

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from time import sleep

import gevent
import gevent.monkey
from gevent.pywsgi import WSGIServer

from datetime import datetime

from IOTAssignmentUtilitiesdorachua.MySQLManager import MySQLManager

gevent.monkey.patch_all()

import boto3
import botocore


app = Flask(__name__)
app.config['ALLOWED_EXTENSIONS'] = set(['png','PNG','JPEG','JPG', 'jpg', 'jpeg'])

@app.route("/api/getdata",methods=['GET', 'POST'])
def apidata_getdata():
    try:
        bookingid = '51539607614.0'
        limit = 1000
        bookingid = "ALL"
        if 'bookingid' in request.form:        
            bookingid = request.form['bookingid']
        if 'datalimit' in request.form:
            limit = request.form['datalimit']       

    #    u='iotuser';pw='iotpassword';h='localhost';db='iotdatabase'
    #    mysqlm = MySQLManager(u,pw,h,db)
    #    mysqlm.connect()

    #    sql=f"SELECT MAX(datetimestart_value) FROM iotapp"
    #    datasql = {}            
    #    list_data = mysqlm.fetch_fromdb_as_list(sql,datasql)

    #    if len(list_data)>0:
    #        max_datetimestart_value = list_data[0]['MAX(datetimestart_value)']

    #        if bookingid == "ALL":
    #            sql=f"SELECT * FROM iotapp WHERE datetimestart_value=%(datetimestart_value)s ORDER BY datetime_value DESC"
    #            datasql = {"datetimestart_value": max_datetimestart_value}
    #        else:                
    #            sql=f"SELECT * FROM iotapp WHERE bookingid=%(bookingid)s AND datetimestart_value=%(datetimestart_value)s ORDER BY datetime_value DESC"
    #            datasql = {"bookingid": bookingid, "datetimestart_value": max_datetimestart_value}
    #    else:
    #        if bookingid == "ALL":
    #            sql=f"SELECT * FROM iotapp ORDER BY datetime_value DESC LIMIT {limit}"
    #            datasql = {}
    #        else:                
    #            sql=f"SELECT * FROM iotapp WHERE bookingid=%(bookingid)s ORDER BY datetime_value DESC"
    #            datasql = {"bookingid": bookingid}
                
     #   json_data = mysqlm.fetch_fromdb_as_json(sql,datasql)

        datetime = jsonc.data_to_json(dynamodb.get_maxdate_from_dynamodb())
    

        if len(datetime)>0:
            import pandas as pd
            pandadata = pd.read_json(datetime)
            pandadata['maxdatetime'] = pd.to_datetime(pandadata['datetimestart_value'])
            max_datetime = str(pandadata['maxdatetime'].max()) 

            if bookingid == "ALL":
                json_data = jsonc.data_to_json(dynamodb.get_dateiswhere_from_dynamodb(max_datetime))
            else:
                jsonc.data_to_json(dynamodb.get_data_where_bookid_and_date_fromdynamodb(str(bookingid)+".0",max_datetime))
        else:
            if bookingid == "ALL":
                json_data = jsonc.data_to_json(dynamodb.get_data_withlimit_from_dynamodb(limit))
            else:
                json_data = jsonc.data_to_json(dynamodb.get_data_withbookingid_from_dynamodb(bookingid))
        
        loaded_r = json.loads(json_data)
        data = {'chart_data': loaded_r, 'title': "IOT Data", 'chart_data_length': len(json_data)}

    #    mysqlm.disconnect()
        
        return jsonify(data)
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

@app.route("/api/getalldata",methods=['GET', 'POST'])
def apidata_getalldata():
    try:  

        #u='iotuser';pw='iotpassword';h='localhost';db='iotdatabase'
        #mysqlm = MySQLManager(u,pw,h,db)
        #mysqlm.connect()

        #sql=f"SELECT * from iotapp order by id desc"
        #datasql = {}            
                
        #json_data = mysqlm.fetch_fromdb_as_json(sql,datasql)
        json_data = jsonc.data_to_json(dynamodb.get_all_data_from_dynamodb())
        
        loaded_r = json.loads(json_data)
        data = {'chart_data': loaded_r, 'title': "IOT Data", 'chart_data_length': len(json_data)}

     #   mysqlm.disconnect()
        
        return jsonify(data)
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

@app.route("/api/getallDriver",methods=['GET','POST'])
def apidata_getallDriver():
    try:
        json_data = jsonc.data_to_json(dynamodb.get_all_driver_from_dynamodb())
        loaded_r = json.loads(json_data)
        data = {'chart_data': loaded_r, 'title': "IOT Data", 'chart_data_length': len(json_data)}
        
        return jsonify(data)
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

@app.route('/api/tempapi',methods=['GET','POST'])
def tempapi():
    try:
    #    u='iotuser';pw='iotpassword';h='localhost';db='iotdatabase'
    #    mysqlm = MySQLManager(u,pw,h,db)
    #    mysqlm.connect()
    
    #    sql = f"SELECT * FROM temperature ORDER BY datetime_value DESC LIMIT 100"
    #    datasql = {}
    #    json_data = mysqlm.fetch_fromdb_as_json(sql,datasql)
        limit = 100
        json_data = jsonc.data_to_json(dynamodb.get_temperature_from_dynamodb(limit))
        loader_r = json.loads(json_data)
        
        data = {'chart_data': loader_r, 'title': "Temperature & Humiditiy Data", 'chart_data_length': len(json_data)}

        #mysqlm.disconnect()

        return data
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])



@app.route("/api/getsingledata/<bookid>",methods=['GET', 'POST'])
def apidata_getsingledata(bookid):
    try: 

        #u='iotuser';pw='iotpassword';h='localhost';db='iotdatabase'
        #mysqlm = MySQLManager(u,pw,h,db)
        #mysqlm.connect()

        #sql=f"SELECT * from iotapp where bookingid = {bookid} order by id desc"   
        #datasql = {}
                
        #json_data = mysqlm.fetch_fromdb_as_json(sql,datasql)
        json_data = jsonc.data_to_json(dynamodb.get_single_data_from_dynamodb(bookid))
        loaded_r = json.loads(json_data)
        data = {'chart_data': loaded_r, 'title': "IOT Data", 'chart_data_length': len(json_data)}

       # mysqlm.disconnect()
        
        return jsonify(data)
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

@app.route("/api/getsingledriver/<driverid>",methods=['GET', 'POST'])
def apidata_getsingledriver(driverid):
    try: 

        json_data = jsonc.data_to_json(dynamodb.get_single_driver_from_dynamodb(driverid))
        loaded_r = json.loads(json_data)
        data = {'chart_data': loaded_r, 'title': "IOT Data", 'chart_data_length': len(json_data)}

        return jsonify(data)
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])


@app.route("/api/getdriverImage/<driverid>",methods=['GET', 'POST'])
def apidata_getdriverImage(driverid):
    try: 
                
        BUCKET='sp-jje-s3-bucket'
        location = {'LocationConstraint':'us-east-1'}
        filename = driverid
        
        stream = retrievefromS3(BUCKET, location,"Driver/"+filename,"profile")
        
        return jsonify(stream)
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

@app.route("/api/getdriverOffence/<driverid>",methods=['GET', 'POST'])
def apidata_getOffence(driverid):
    try: 

        json_data = jsonc.data_to_json(dynamodb.get_offences_from_dynamodb(driverid))
        loaded_r = json.loads(json_data)
        data = {'chart_data': loaded_r, 'title': "IOT Data", 'chart_data_length': len(json_data)}

        return jsonify(data)
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

@app.route("/api/updateProfile", methods=['POST'])
def apidata_updateProfile():
    try:
        if request.method == 'POST':
           
            files = request.files['file']
            name  = request.form['Name']
            dob   = request.form['BirthDate']
            driverid = request.form['driverid']
            
            if files and allowed_file(files.filename):
                files.filename = driverid
                filename = files.filename
                app.logger.info ('FileName: ' +filename)
                
                BUCKET='sp-jje-s3-bucket'
                collection_id='test'
                location = {'LocationConstraint':'us-east-1'}
                from PIL import Image, ImageDraw, ExifTags, ImageColor, ImageFont
                
                Image = Image.open(files)
                Image = Image.convert('RGB')
               
                from io import BytesIO
                buffered = BytesIO()
                Image.save(buffered,format="JPEG")    
                
                uploadToS3(buffered.getvalue(),"Driver/"+filename,BUCKET,location)
                add_faces_to_collection(BUCKET, "Driver/"+filename+".jpg",collection_id,driverid)            

            dynamodb.updateProfile(driverid, name, dob)
            return jsonify("test")
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def add_faces_to_collection(bucket, photo, collection_id,driverid):
    
    maxResults=10
    tokens=True

    client=boto3.client('rekognition')
    response=client.list_faces(CollectionId=collection_id,
                               MaxResults=maxResults)
    
    while tokens:
        faces = response['Faces']

        for face in faces:
            
            if face['ExternalImageId'] == driverid:
                
                faces=[]
                faces.append(face['FaceId'])
                delete_faces_from_collection(collection_id,faces)

        if 'NextToken' in response:
            nextToken=response['NextToken']
            response=client.list_faces(CollectionId=collection_id,
                                       NextToken=nextToken,MaxResults=maxResults)
        else:
            tokens=False

    response=client.index_faces(CollectionId=collection_id,
                                Image={'S3Object':{'Bucket':bucket,'Name':photo}},
                                ExternalImageId=driverid,
                                MaxFaces=1,
                                QualityFilter="AUTO",
                                DetectionAttributes=['ALL'])

def delete_faces_from_collection(collection_id, faces):
    client=boto3.client('rekognition')
    response=client.delete_faces(CollectionId=collection_id,
                               FaceIds=faces) 

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']


@app.route("/")
def home():
    return render_template('index.html')

@app.route("/list")
def list():
    return render_template('list.html')

@app.route("/data/<bookid>")
def data(bookid):
    return render_template('data.html',bookid=bookid)

@app.route("/driverlist")
def driverList():
    return render_template('driverList.html')

@app.route("/driver/<driverid>")
def driver(driverid):
    return render_template('driver.html',driverid=driverid)

@app.route("/api/resetDemrit", methods=['GET', 'POST'])
def resetDemrit():
    try:
        data = request.get_data().decode("utf-8")
        print(data)
        if data == "reset":
            json_data = jsonc.data_to_json(dynamodb.get_all_driverID())
            loaded = json.loads(json_data)
            for newdata in loaded:
                dynamodb.resetDemrit(newdata["driverid"])
            return jsonify("success")
        else:
           
            return jsonify("fail")
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])  


@app.route("/api/horn", methods=['GET','POST'])
def horn():
    try:
        data = request.get_json()
        hornData = data['horn']
        bookingID = data['bookingid']

        host="afm818vjfw2oi-ats.iot.us-east-1.amazonaws.com"
        rootCAPath="certs/AmazonRootCA1.pem"
        certificatePath="certs/a82890c8ad-certificate.pem.crt"
        privateKeyPath="certs/a82890c8ad-private.pem.key"

        my_rpi = AWSIoTMQTTClient("web_server_p1827806")
        my_rpi.configureEndpoint(host,8883)
        my_rpi.configureCredentials(rootCAPath,privateKeyPath,certificatePath)

        my_rpi.configureOfflinePublishQueueing(-1) # Infinite offline Publish queueing
        my_rpi.configureDrainingFrequency(2) # Draining: 2 Hz
        my_rpi.configureConnectDisconnectTimeout(10) # 10 sec
        my_rpi.configureMQTTOperationTimeout(5) # 5 sec
        my_rpi.connect()

        data = {"message":"honk"}
        topic = "sensors/"+bookingID+"/horn"
      
        success = my_rpi.publish(topic, json.dumps(data), 1)
        sleep(5)

        if success:
            print("success")
            return jsonify("horn successfully")
        else:
            print("fail")
            return jsonify("horn failed")
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])  

@app.route("/api/camera",methods=['GET', 'POST'])
def camera():
    try:
        
        data = request.get_json()
        capturetype = data['type']
   
     #   cam = cv2.VideoCapture("http://116.87.64.143:8081/video.mjpg")
        cam = cv2.VideoCapture("http://116.87.64.143:8081/")
        ret, frame = cam.read()
        dts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filename = dts.replace(":",".")

        enframe = cv2.imencode(".jpg",frame)[1].tostring()

        cam.release()
        
        BUCKET='sp-jje-s3-bucket'
        location = {'LocationConstraint':'us-east-1'}
        
        uploadToS3(enframe, filename, BUCKET, location)
        stream = retrievefromS3(BUCKET, location,filename,"camera")
        
        if capturetype == "object" :
            imstr, ItemList, NotCfmItemList = drawBoundingBox_Object(BUCKET, filename, stream)
            data = {'file': imstr,'datetime':dts, 'ItemObject': ItemList,'NotCfmObject': NotCfmItemList, 'Item_Length':len(ItemList), 'ntcfmItem_Length':len(NotCfmItemList)}

        elif capturetype == "facial":
            imstr, FaceList = drawBoundingBox_Facial(BUCKET, filename, stream)
            data = {'file': imstr, 'datetime': dts, 'FaceObject': FaceList, 'Face_Length':len(FaceList)}
       
        return jsonify(data)
    except:
        cam.release()
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

def drawBoundingBox_Object(BUCKET, filename, stream):

        from PIL import Image, ImageDraw, ExifTags, ImageColor, ImageFont
        image = Image.open(stream)
        
        ItemList = []
        NotCfmItemList = []
        
        index = 0
        notcfmIndex = 0
        imgWidth, imgHeight = image.size
        fnt = ImageFont.truetype('static/fonts/Roboto-Black.ttf',30)
        for label in detect_labels(BUCKET,filename+".jpg"):

            for instance in label['Instances']:

                box = instance['BoundingBox']
                left = imgWidth * box['Left']
                top = imgHeight * box['Top']
                width = imgWidth * box['Width']
                height = imgHeight * box['Height']

                points = (
                    (left,top),
                    (left + width, top),
                    (left + width, top + height),
                    (left, top + height),
                    (left, top)
                )

                draw = ImageDraw.Draw(image)
                draw.text((left+5,top),"Object: " + label["Name"]+"\nConfidence Level: "+ str(round(label["Confidence"],2))+"%",font =fnt, fill='#00d400')
                draw.line(points, fill='#00d400', width = 3)
              
            if len(label['Instances']) > 0:
                index = index + 1
                ItemIndex = "Object " + str(index)
                ItemObject = {
                                "ItemIndex": ItemIndex,
                                "NumberofItem": len(label['Instances']),
                                "parent": label['Parents'],
                                "ItemDetail":{ 
                                            "Name": label["Name"],
                                            "Confidence": round(label["Confidence"],2)}
                            }
                
                ItemList.append(ItemObject)

            elif (len(label['Instances']) is 0) and (len(label['Parents']) is not 0):
                notcfmIndex = notcfmIndex + 1
                ItemIndex = "Object " + str(notcfmIndex)
                ItemObject = {
                                "ItemIndex": ItemIndex,
                                "ItemDetail":{ 
                                            "Name": label["Name"],
                                            "Confidence": round(label["Confidence"],2)}
                            }
                NotCfmItemList.append(ItemObject)
                
        from io import BytesIO

        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        image.close()
        imstr = base64.b64encode(buffered.getvalue()).decode("utf-8")

        return imstr, ItemList, NotCfmItemList

def detect_labels(bucket, key, max_labels=10, min_confidence=90, region="us-east-1"):
    rekognition = boto3.client("rekognition", region)
    response = rekognition.detect_labels(
        Image={
                "S3Object": {
                    "Bucket": bucket,
                    "Name": key,
                }
        },
        MaxLabels=max_labels,
        MinConfidence=min_confidence,
    )
    return response['Labels']


def drawBoundingBox_Facial(BUCKET, filename, stream):

    from PIL import Image, ImageDraw, ExifTags, ImageColor, ImageFont
    image = Image.open(stream)

    index = 0
    FaceList = []

    collection_id = "test"
    imgWidth, imgHeight = image.size

    fnt = ImageFont.truetype('static/fonts/Roboto-Black.ttf',20)
    FaceMatches = detect_facial(BUCKET, filename+".jpg", collection_id)

    if len(FaceMatches) > 0:
        for match in FaceMatches:
            
            for face in match['Face']:
                box = match['Face']['BoundingBox']
                
                left = imgWidth * box['Left']
                top = imgHeight * box['Top']
                width = imgWidth * box['Width']
                height = imgHeight * box['Height']
                
                points = (
                    (left,top),
                    (left + width, top),
                    (left + width, top + height),
                    (left, top + height),
                    (left, top)
                )
                
                draw = ImageDraw.Draw(image)
                draw.text((left+5,top),"Face: "+ match['Face']['ExternalImageId'] +"\nSimilarity Level: "+ str(round(match['Similarity'],2))+"%",font =fnt, fill='#00d400')
                draw.line(points, fill='#00d400', width = 2)
        
        index = index + 1
        FaceIndex = "Face " + str(index)
        
        FaceObject = {
                                "FaceIndex": FaceIndex,
                                "FaceID": match['Face']['FaceId'],
                                "ExternalImageId": match['Face']['ExternalImageId'],
                                "Similarity": round(match['Similarity'],2)
                            }
                
        FaceList.append(FaceObject)

    from io import BytesIO

    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    image.close()
    imstr = base64.b64encode(buffered.getvalue()).decode("utf-8")

    return imstr, FaceList


def detect_facial(BUCKET, filename, collection_id):

    client=boto3.client("rekognition")

    if not collection_exist(collection_id, client):
        response = client.create_collection(CollectionId=collection_id)
        if response['StatusCode'] != 200:
             print ('Could not create collection,' +collection_id+ ',status code:' + str(response['StatusCode']))
        else:
            print('Collection ARN: '+ response['CollectionArn'] + ' ,status code: '+ str(response['StatusCode']))
    
    threshold = 70
    maxFaces = 2
    response = {}
    try:
        response=client.search_faces_by_image(CollectionId=collection_id,
                                    Image={'S3Object':{'Bucket': BUCKET,'Name': filename}},
                                    FaceMatchThreshold=threshold,
                                    MaxFaces=maxFaces)
        return response['FaceMatches']

    except:
        
        return response


def collection_exist(collection_id, client):

    return collection_id in list_collections(client)

def list_collections(client):

    response = client.list_collections()
    result = []

    while True:
        collections=response['CollectionIds']
        result.extend(collections)

        if 'NextToken' in response:
            nextToken = response['NextToken']
            response = client.list_collections(NextToken=nextToken)
        else:
            break

    return result

def uploadToS3(enframe,file_name, bucket_name, location):
    
    s3 = boto3.resource('s3') #Create an S3 resource
    exists = True

    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
    
    if exists == False:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

    # Upload a new file
    s3.Object(bucket_name, file_name+".jpg").put(Body=bytes(enframe))
    print("File uploaded")

def retrievefromS3(bucket_name, location, filename,retype):
    import io
    s3 = boto3.resource('s3') #Create an S3 resource
    exists = True

    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
    
    if exists == False:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    
    # Retrieve the file
    try:
        obj = s3.Object(bucket_name, filename+".jpg")
        response = obj.get()
        
        if retype == "profile":
            image = base64.b64encode(response['Body'].read()).decode('utf-8')
            return image
        elif retype == "camera":
            image = io.BytesIO(response['Body'].read())
            return image
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return False
        else:
            raise



@app.route("/temperature")
def temperature():
    return render_template('temperature.html')

if __name__ == '__main__':
   try:
        host = '0.0.0.0'
        port = 80
        parser = argparse.ArgumentParser()        
        parser.add_argument('port',type=int)
        
        args = parser.parse_args()
        if args.port:
            port = args.port
                
        http_server = WSGIServer((host, port), app)
        app.debug = True
        print('Web server waiting for requests')
        http_server.serve_forever()

       

   except:
        print("Exception while running web server")
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])
