import requests
import json
import paho.mqtt.client as mqtt
import time
import sys
import dotenv
import sqlite3
from math import sqrt
from FlightRadar24.api import FlightRadar24API

class mqtt_flight:
    def __init__(self, mqtt_server, mqtt_client_id):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        self.mqtt_server = mqtt_server
        self.mqtt_client_id = mqtt_client_id
        self.client = mqtt.Client(client_id=self.mqtt_client_id)
        self.client.on_connect = on_connect
        self.client.connect(self.mqtt_server)
        self.client.loop_start()

    def publish_device(self, device_id, device_name, device_type, device_manufacturer, device_model):
        device_payload = {
            "name": device_name,
            "identifiers": "test123",
            #"type": device_type,
            "manufacturer": device_manufacturer,
            "model": device_model,
        }
        device_topic = f"homeassistant/sensor/{device_id}/config"
        self.device = device_payload
        self.client.publish(device_topic, json.dumps(device_payload),qos = 2, retain=True)


    def publish_sensor(self, device_id, sensor_id, sensor_name, sensor_type, sensor_unit):
        sensor_payload = {
            "dev": self.device,
            "name": sensor_name,
            #"stat_cla": sensor_type,
            "uniq_id": sensor_id,
            "stat_t": f"homeassistant/sensor/{sensor_id}/state",
        }
        sensor_topic = f"homeassistant/sensor/{sensor_id}/config"
        return self.client.publish(sensor_topic, json.dumps(sensor_payload),qos = 2,retain = True)

    def publish_data(self, device_id, sensor_id, value):
        data_topic = f"homeassistant/sensor/{sensor_id}/state"
        return self.client.publish(data_topic, str(value),qos = 0,retain = True)

class opensky:
    def __init__(self,LAMAX,LAMIN,LOMAX,LOMIN):
        self.plane_data = []
        self.lamax = LAMAX
        self.lamin = LAMIN
        self.lomax = LOMAX
        self.lomin = LOMIN
        self.coords = [LAMAX,LAMIN,LOMIN,LOMAX]
        self.fr_api = FlightRadar24API()
        self.airlines = self.fr_api.get_airlines()
        self.airports = self.fr_api.get_airports()

    def get_timestamp(self,offset):
        plane_time = time.time() - offset
        return str(int(plane_time))
    
    def get_planes_area(self):
        plane_data_all = self.fr_api.get_flights(bounds=",".join(self.coords))
        plane_data_temp = []
        if not plane_data_all:
            return None
        for planes in plane_data_all:
            plane_data_temp.append({"icao24": planes.icao_24bit, "longitude": planes.longitude, "latitude": planes.latitude, "dest": planes.destination_airport_iata, "origin": planes.origin_airport_iata, "id": planes.id, "airline":planes.airline_icao, "callsign": planes.callsign})
        self.plane_data = plane_data_temp
        return self.plane_data


    def get_airline(self,icao_code):
        result = search_dictionaries("ICAO", icao_code[:3], self.airlines)
        if result:
            return result[0].get("Name")
        else:
            return None
    
    def get_details(self, flight_id):
        details = self.fr_api.get_flight_details(flight_id)
        return details


class DBHandler:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS all_planes (icao24 text, longitude real, latitude real, dest text, origin text, id text PRIMARY KEY, airline text, callsign text);")
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS nearest_planes (id text PRIMARY KEY, target_coord_longitude real, target_coord_latitude real)''')

    def write_all_planes(self, planes):
        # Insert the data for each plane into the table
        for plane in planes:
            self.cursor.execute('''INSERT OR IGNORE INTO all_planes VALUES (?,?,?,?,?,?,?,?)''', (plane['icao24'], plane['longitude'], plane['latitude'], plane['dest'], plane['origin'], plane['id'], plane['airline'], plane['callsign']))

        # Save the changes to the database  
        self.conn.commit()

    def write_nearest_plane(self, icao24, target_coord):
        # Insert the data for the nearest plane into the table
        self.cursor.execute('''INSERT OR IGNORE INTO nearest_planes VALUES (?,?,?)''', (icao24, target_coord[1], target_coord[0]))

        # Save the changes to the database
        self.conn.commit()

    def close(self):
        self.conn.close()

#----------------Functions-----------------

def inital_env():
    return dotenv.dotenv_values(".env")

def search_dictionaries(key, value, list_of_dictionaries):
    return [element for element in list_of_dictionaries if element[key] == value]

def distance(long,long_target, lat, lat_target):
    # Calculate the Euclidean distance between two coordinates
    return sqrt((long - long_target)**2 + (lat_target - lat_target)**2)

def closest_coordinates(planes, target_coord):
    closest_coord = (planes[0].get("id"))
    min_distance = distance(planes[0].get("longitude"), target_coord[1], planes[0].get("latitude"), target_coord[0])
    
    for plane in planes:
        curr_distance = distance(plane.get("longitude"), target_coord[1], plane.get("latitude"), target_coord[0])
        
        if curr_distance < min_distance:
            min_distance = curr_distance
            closest_coord = plane.get("id")

    return closest_coord

#----------------Main-----------------

if __name__ == '__main__':
    config = inital_env()
    sky = opensky(config["LAMAX"],config["LAMIN"],config["LOMAX"],config["LOMIN"])
    db_handler = DBHandler('plane_data.db')
    mqtt_client = mqtt_flight(config["MQTT_SERVER"], "opensky_script")

    device_id = "nearest_plane"
    device_name = "Nearest Plane"
    device_type = "None"
    device_manufacturer = "OpenSky Network"
    device_model = "N/A"

    mqtt_client.publish_device(device_id, device_name, device_type, device_manufacturer, device_model)
    sensor = mqtt_client.publish_sensor(device_id, "test_id", "callsign", "None", "text")
    print(sensor)    

    while True:
        result = sky.get_planes_area()
        if result is not None:
            print("Plane :)")
            db_handler.write_all_planes(sky.plane_data)
            nearest_icao24 = closest_coordinates(sky.plane_data, [50.955322, 6.903259])
            print(nearest_icao24)
            test = sky.get_details(nearest_icao24)
            db_handler.write_nearest_plane(nearest_icao24, [50.955322, 6.903259])
            test2 = mqtt_client.publish_data(device_id, "test_id", nearest_icao24)
            print(test2)
        else:
            print("no Plane :(")
        time.sleep(60)