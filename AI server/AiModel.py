import asyncio
import sys

import json
import jsonconverter as jsonc
import requests

import pandas as pd
import numpy as np
import dynamoAI
from time import sleep
from decimal import Decimal
from joblib import load
from copy import deepcopy

from IOTAssignmentServerdorachua.MyNewCarsFeeder import MyNewCarsFeeder
from IOTAssignmentServerdorachua.MySocketServer import MySocketServer
import argparse
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

async def main():

    try:
        
        #json_data = jsonc.data_to_json(dynamoAI.get_all_data())
        
        my_rpi = GetMQTTConnection()
        
        my_rpi.subscribe("sensors/speed",1, ListenForSpeed)
        while True:
            sleep(2)
     #   loaded = json.loads(json_data)
      #  for i in range(len(loaded)):
            
       #     print(loaded[i]["speed"])

    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit()

    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

class StackingRegressor:
    def __init__(self, models, second_model, features):
        self.models = models
        self.feature_models = []
        self.second_model = second_model
        self.features = features
    
    def _generate_f_features(self, X):
        f_features = np.zeros((X.shape[0], len(self.features) * len(self.models)))
        for num, features in enumerate(self.features * len(self.models)):
            model = self.feature_models[num]
            f_features[:, num] = model.predict(X.loc[:, features[1]])
        return f_features
    
    def fit(self, X, y):
        # generate multiple trained models with different features
        for model in self.models:
            for feature in self.features:
                model.fit(X.loc[:, feature[1]], y)
                self.feature_models.append(deepcopy(model))
        f_features = self._generate_f_features(X)
        self.second_model.fit(f_features, y)
    
    def predict(self, X):
        f_features = self._generate_f_features(X)
        return self.second_model.predict(f_features)

def ListenForSpeed(client, userdata, message):
    try:
        
        # Retrieving information about the practicular driver from iotapp
        loaded = json.loads(message.payload)
        loaded["bookingID"] = loaded.pop("bookingid")
        loaded["Accuracy"] = loaded.pop("accuracy")
        loaded["Speed"] = loaded.pop("speed")
        loaded["Bearing"] = loaded.pop("bearing")
        loaded["second"] = loaded.pop("seconds")
        from io import BytesIO
        buffered = BytesIO()
        buffered.write(json.dumps(loaded).encode())
    
        series = pd.Series(loaded)
        numericData = pd.to_numeric(series, errors='coerce')
        
        df = numericData.to_frame().T

        df.drop(columns=["deviceid", "speedkmhour", "vehicletype", "driverid", "datetime_value", "longitude", "latitude", "datetimestart_value", "bookingidwithtime"], inplace=True)
        # df = df.astype(np.float32)
        #test_data = pd.read_csv("C:/Users/Jeremy/Downloads/IOT/Assignment2/IoT-Dashboard/result.csv")
        test_X = pd.DataFrame()
        for col in df.columns:
            if col != "bookingID":
                temp = df.groupby("bookingID")[col].agg(["mean", "sum", "max", "min"])
                test_X[col + "_mean"] = temp["mean"]
                test_X[col + "_sum"] = temp["sum"]
                test_X[col + "_max"] = temp["max"]
                test_X[col + "_min"] = temp["min"]
        print(test_X.info())

        bookingID = test_X.index

        test_X = test_X.reset_index(drop=True)

        test_X.drop(columns=["second_min"], inplace=True)

        for col in test_X.columns:
            if col.startswith("second"):
                agg_method = col.split("_")[1]
                test_X["distance_" + agg_method] = test_X[col] * test_X["Speed_" + agg_method]
                test_X["velocity_x_" + agg_method] = test_X[col] * test_X["acceleration_x_" + agg_method]
                test_X["velocity_y_" + agg_method] = test_X[col] * test_X["acceleration_y_" + agg_method]
                test_X["velocity_z_" + agg_method] = test_X[col] * test_X["acceleration_z_" + agg_method]
                test_X["angle_x_" + agg_method] = test_X[col] * test_X["gyro_x_" + agg_method]
                test_X["angle_y_" + agg_method] = test_X[col] * test_X["gyro_y_" + agg_method]
                test_X["angle_z_" + agg_method] = test_X[col] * test_X["gyro_z_" + agg_method]

        print(test_X.info())
        print(test_X.head(10))
        #print(test_X.isnull().any())
        sr = load("./model/stackingRegressor.joblib")
        y_pred = sr.predict(test_X)

        # if y_pred is > 0.8, it is dangerous 
        if(y_pred > 0.8):
            results = 1

            # Retrieving information about the practicular driver from driverTable
            json_data1 = jsonc.data_to_json(dynamoAI.get_single_driver_from_dynamoAI(loaded["driverid"]))
            loaded_r1 = json.loads(json_data1)
            loaded_r1 = loaded_r1[0]

            bot_message = 'At ' + loaded["datetimestart_value"] + ', ' + loaded_r1["driverid"] + ' : ' + loaded_r1["driverName"] + ', you have been driving at the speed of ' + str(loaded["Speed"]) + 'km/hr ! Please Slow Down!'
            bot_token = ''#fill in the token
            bot_chatID = ''#fill in the ChatID
            send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

            response = requests.get(send_text)

            # Retrieving information about the practicular driver
            json_data = jsonc.data_to_json(dynamoAI.get_single_driver_from_dynamoAI(loaded["driverid"]))
            loaded_r = json.loads(json_data)
            loaded_r = loaded_r[0]

            # Updating the offence counts
            dynamoAI.addOffence(loaded["driverid"], loaded_r["offence"])
      
            # Every 5 offences, three demrit points. (12 points will ban account)
            dynamoAI.addDemrit(loaded["driverid"], loaded_r["offence"])

            # Adding the time of offence
            dynamoAI.addOffenceRecord(loaded["driverid"], loaded["datetime_value"], loaded["longitude"], loaded["latitude"], loaded["Speed"])

        else:
            results = 0

        # Label the trip to be dangerous or not dangerous with '0' and '1'
        dynamoAI.addLabel(loaded["deviceid"], loaded["datetime_value"],results)

        # Retrieving updated information about the practicular driver
        json_data_new = jsonc.data_to_json(dynamoAI.get_single_driver_from_dynamoAI(loaded["driverid"]))
        loaded_r_new = json.loads(json_data_new)
        loaded_r_new = loaded_r_new[0]
        demritpoint = Decimal(loaded_r_new["demritpoint"])
        
        
        if demritpoint > 0:
            bot_message = loaded_r_new["driverid"] + ' : ' + loaded_r_new["driverName"] + ', You have a total of ' + str(demritpoint) + ', Please drive more carefully. Note that if you have a total of 12 demrit points, your account will be banned' 
            bot_token = '1203266117:AAHCDMu7PtAW3gnLkubTE16FRtO-dYL6OLY'
            bot_chatID = '124122134'
            send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

            response = requests.get(send_text)

        print("done")
        
    except:
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])


def GetMQTTConnection():
    host="afm818vjfw2oi-ats.iot.us-east-1.amazonaws.com"
    rootCAPath="certs/AmazonRootCA1.pem"
    certificatePath="certs/a82890c8ad-certificate.pem.crt"
    privateKeyPath="certs/a82890c8ad-private.pem.key"

    my_rpi = AWSIoTMQTTClient("AI_Model")
    my_rpi.configureEndpoint(host,8883)
    my_rpi.configureCredentials(rootCAPath,privateKeyPath,certificatePath)

    my_rpi.configureOfflinePublishQueueing(-1) # Infinite offline Publish queueing
    my_rpi.configureDrainingFrequency(2) # Draining: 2 Hz
    my_rpi.configureConnectDisconnectTimeout(10) # 10 sec
    my_rpi.configureMQTTOperationTimeout(5) # 5 sec
    my_rpi.connect()
    return my_rpi

if __name__ == '__main__':
   try:        
        asyncio.run(main())
           
   except KeyboardInterrupt:
        print('Interrupted')
        sys.exit()

   except:
        print("Exception")
        import sys
        print(sys.exc_info()[0])
        print(sys.exc_info()[1])

