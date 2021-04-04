from time import sleep
import sys
from gpiozero import Buzzer
import Adafruit_DHT
import json

from datetime import datetime  
from datetime import timedelta  
import argparse
import random
import dynamodb


from IOTAssignmentClientdorachua.GrabCarClient import GrabCarClient
from IOTAssignmentUtilitiesdorachua.MySQLManager import MySQLManager
from IOTAssignmentUtilitiesdorachua.MySQLManager import QUERYTYPE_DELETE, QUERYTYPE_INSERT
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from twilio.rest import Client

#def getData(gcc,sqlm,datetime_start):    
def getData(gcc,datetime_start,my_rpi):
    while True:
        try:
            reading = gcc.get_reading()            

            if reading is not None and len(reading)>0:
                readings = json.loads(reading)
                            
                for str_reading in readings:
                    r = json.loads(str_reading)
                 
                    gps = getGPS()
                   # if sqlm.isconnected:
                    #    gps = getGPS(mysqlm)
                        #sql = "INSERT INTO iotapp (bookingid,bookingidwithtime,accuracy,datetimestart_value,seconds,speed,speedkmhour,datetime_value,long_value,lat_value) VALUES (%(bookingid)s,%(bookingidwithtime)s,%(accuracy)s,%(datetimestart_value)s,%(seconds)s,%(speed)s,%(speedkmhour)s,%(datetime_value)s,%(longitude)s,%(latitude)s)"
                    
                    bid = r["bookingid"]                                                
                    seconds = r["seconds"]
                    speedkmhour = r["speedkmhour"]
              
                    longitude = gps[0]["long_value"] 
                    latitude = gps[0]["lat_value"]
                    datetime_value = datetime_start + timedelta(seconds=seconds)
                    datetimestart_value = str(datetime_start)
                    bidwithtime = f"{bid}_{datetimestart_value}"
                    import uuid
                    uuid = uuid.uuid1()
                    r['deviceid'] = str(uuid)
                    r['vehicletype'] = 'GrabCar'
                    r['driverid'] = 'p1827806'
                    r['datetime_value'] = datetime_value.strftime('%Y-%m-%d %H:%M:%S')
                    r['longitude'] = float(longitude)
                    r['latitude'] = float(latitude)
                    r['datetimestart_value'] = datetimestart_value
                    r['bookingidwithtime'] = bidwithtime
                    
                #    data = {'deviceid':"deviceid_JunYang",'bookingid': bid , 'bookingidwithtime':bidwithtime,'accuracy':accuracy,'datetimestart_value':datetimestart_value,'seconds': seconds,'speed': speed,'speedkmhour': speedkmhour, 'datetime_value':datetime_value.strftime('%Y-%m-%d %H:%M:%S'),'longitude':float(longitude),'latitude':float(latitude)}           
                    success = my_rpi.publish("sensors/speed", json.dumps(r), 1)
                    
                #     success = mysqlm.insertupdatedelete(QUERYTYPE_INSERT,sql,data)
                   
                    if success:
                        print(f"{str(r)} inserted")
                        if speedkmhour > 100.0:
                            '''
                            account_sid = "sid here"
                            auth_token = "auth token here"
                            client = Client(account_sid, auth_token)
                            message = f"Booking ID {bid} is driving at {speedkmhour:.2f} km/hr and it is above the speed limit"
                                
                            my_hp = "+65xxxxxxxx"
                            twilio_hp = "twilio phone number here"

                            client.api.account.messages.create(to=my_hp,
                            from_=twilio_hp,
                            body=message)
                            '''
                #    else:
                #        print("Not connected to database")
                    
            yield             

        except GeneratorExit:
            print("Generator Exit error")
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
            return

        except KeyboardInterrupt:
            exit(0)

        except:
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])       

def setTempData(my_rpi):    
    while True:
        try:
           # if sqlm.isconnected:
            pin = 13
            humidity, temperature = Adafruit_DHT.read_retry(11, pin)
            #temperature = random.randint(20,35)
            #humidity = random.randint(10,85) 
            #sql = "INSERT INTO temperature (temperature_value,humidity_value) VALUES (%(temperature)s,%(humidity)s)"
            datetime_start = datetime.now()
            import uuid
            uuid2 = uuid.uuid1()

            data = {'deviceid':str(uuid2),'location':'Singapore','temperature':temperature,'humidity':humidity,'datetime_value':datetime_start.strftime('%Y-%m-%d %H:%M:%S')} 
            success = my_rpi.publish("sensors/temperature", json.dumps(data), 1)

            if success:
                print(f"{data} inserted")
                   
            yield

        except GeneratorExit:
            print("Generator Exit error")
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
            return

        except KeyboardInterrupt:
            exit(0)

        except:
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])    

def getGPS():
    while True:
        try:
            #if sqlm.isconnected:
            #sql = "SELECT * from gpsdata ORDER BY RAND() LIMIT 1"
            #datasql = {}            
            #list_data = mysqlm.fetch_fromdb_as_list(sql,datasql)

            list_data = dynamodb.get_gps_data_from_dynamodb()
          
            return list_data

            #else:
             #   print("Not connected to database")
            
        except GeneratorExit:
            print("Generator Exit error")
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
            return

        except KeyboardInterrupt:
            exit(0)

        except:
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])  

def listenforHorn(client, userdata, message):
    try:
        pin = 21
        topic = message.topic
        driverid = topic.strip("sensors/horn")
        print("driverid: "+ driverid)
        print(message.payload)
        data = json.loads(message.payload)
        
        if(data['message'] == 'honk'):
            buzzer = Buzzer(pin)
            buzzer.on()
            sleep(2)
            buzzer.off()
             
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

if __name__ == "__main__":

    try:                 
        host,port = "127.0.0.1", 8889
        parser = argparse.ArgumentParser()
        parser.add_argument('host')
        parser.add_argument('port',type=int)
        
        args = parser.parse_args()
        if args.host:
            host = args.host
        if args.port:
            port = args.port

        mygcc = GrabCarClient(host,port)

       
        host="afm818vjfw2oi-ats.iot.us-east-1.amazonaws.com"
        rootCAPath="certs/AmazonRootCA1.pem"
        certificatePath="certs/a82890c8ad-certificate.pem.crt"
        privateKeyPath="certs/a82890c8ad-private.pem.key"

        my_rpi = AWSIoTMQTTClient("socket_client_p182806")
        my_rpi.configureEndpoint(host,8883)
        my_rpi.configureCredentials(rootCAPath,privateKeyPath,certificatePath)

        my_rpi.configureOfflinePublishQueueing(-1) # Infinite offline Publish queueing
        my_rpi.configureDrainingFrequency(2) # Draining: 2 Hz
        my_rpi.configureConnectDisconnectTimeout(10) # 10 sec
        my_rpi.configureMQTTOperationTimeout(5) # 5 sec
        my_rpi.connect()
        '''
        u='p1827806';pw="SUb3p3UZ2zG";h='iotdatabase.cqxpfrhzltqv.us-east-1.rds.amazonaws.com';db='iotdatabase'
       
        mysqlm =  MySQLManager(u,pw,h,db)
        mysqlm.connect()
        '''
        print("Streaming started")        
        datetime_start = datetime.now()

        #gen = getData(mygcc,mysqlm,datetime_start)
        gen = getData(mygcc,datetime_start,my_rpi)
        tem = setTempData(my_rpi)
        my_rpi.subscribe("sensors/+/horn",1, listenforHorn)
        while True:
            
            next(gen)
            next(tem)
            sleep(2)
        
    except KeyboardInterrupt:
        print('Interrupted')
     #   mysqlm.disconnect()
        sys.exit()

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])       




