import json
import paho.mqtt.client as mqtt
import time
import dotenv
import sqlite3
import logging
from math import sqrt
from FlightRadar24.api import FlightRadar24API

class mqtt_flight:
    """
    A Class to handle an MQTT Connection and publish Home Assistant devices and Sensors.

    ...

    Attributes
    ----------
    mqtt_server : str
        adress of MQTT Broker
    mqtt_client_id : str
        MQTT CLient ID
    connected : bool
        MQTT Connection indicator
    disconnect : bool
        MQTT Connection indicator if disconnected
    client : Object
        MQTT client Object from paho.mqtt.client Paket


    Methods
    -------
    publish_sensor(device_id, device_manufacturer, device_name, device_model, sensors):
        publishes a List of Sensors to an MQTT Broker with a Home assistant discovery Payload
    publish_data(device_id, sensors, data):
        publishes a state for an Home Assistant Sensor over MQTT
    """

    def __init__(self, mqtt_server, mqtt_client_id):
        
        self.mqtt_server = mqtt_server
        self.mqtt_client_id = mqtt_client_id
        self.connected = False
        self.disconnect=False
        self.client = mqtt.Client(client_id=self.mqtt_client_id)
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logging.warning("Connected to MQTT Broker!")
                self.connected = True
                self.disconnect=False
            else:
                logging.warning("Failed to connect, return code %d\n", rc)
                self.connected = False

        def on_disconnect(client, userdata, rc):
            logging.warning("disconnecting reason  "  +str(rc))
            self.connected=False
            self.disconnect=True
        
        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect
        
        try:
            logging.warning(self.client.connect(self.mqtt_server))
            self.connected = True
            if self.connected:
                self.client.loop_start()
        except:
            self.connected = False
            logging.warning("MQTT Error run without MQTT Connection")
    

    def publish_sensor(self, device_id, device_manufacturer, device_name, device_model, sensors):
        """publishes a List of Sensors to an MQTT Broker with a Home assistant discovery Payload"""
        device_payload = {
            "name": device_name,
            "identifiers": device_id, 
            "manufacturer": device_manufacturer,
            "model": device_model,
        }
        for sensor in sensors:
            sensor_payload = {
                "dev": device_payload ,
                "name": sensor,
                "uniq_id": device_id + "_" + sensor,
                "stat_t": f"homeassistant/sensor/{sensor}/state",
            }
            sensor_topic = f"homeassistant/sensor/{sensor}/config"
            publish_check = self.client.publish(sensor_topic, json.dumps(sensor_payload),qos = 2,retain = True)
            if publish_check[0] == 0:
                logging.warning("Sensor: " + sensor + " wurde erfolgreich gepublished")
            else:
                logging.warning("Sensor: " + sensor + " publish fehlgeschlagen")


    def publish_data(self, device_id, sensors, data):
        """publishes a state for an Home Assistant Sensor over MQTT"""
        for i in range(0, len(sensors)  ):
            data_topic = f"homeassistant/sensor/{sensors[i]}/state"
            self.client.publish(data_topic, str(data[i]),qos = 0,retain = True)

class flightradar:
    """
    A Class to get flight Data

    ...

    Attributes
    ----------
    lamax: str
        Max latitude of bounds
    lamin: str
        Min latitude of bounds
    lomax: str
        Max longitude of bounds
    lomin: str
        Min longitude of bounds
    coords : Array
        Array of bounds Coords
    fr_api: Object
        Object for Flightdata query


    Methods
    -------
    get_planes_area():
        returns an Array of all Planes in bound
    get_details(ICAO COde):
        get flight information from ICAO Code
    """
    def __init__(self,LAMAX,LAMIN,LOMAX,LOMIN):
        self.plane_data = []
        self.lamax = LAMAX
        self.lamin = LAMIN
        self.lomax = LOMAX
        self.lomin = LOMIN
        self.coords = [LAMAX,LAMIN,LOMIN,LOMAX]
        self.fr_api = FlightRadar24API()

        if not self.lamax or not self.lamin or not self.lomax or not self.lomin:
            raise Exception("bounds not complete please check .env file")

    
    def get_planes_area(self):
        """returns an Array of all Planes in bound"""
        try:
            plane_data_all = self.fr_api.get_flights(bounds=",".join(self.coords))
        except:
            logging.warning("Connection not possible")
            plane_data_all = []
        plane_data_temp = []
        if not plane_data_all:
            return None
        for planes in plane_data_all:
            plane_data_temp.append({"icao24": planes.icao_24bit, "longitude": planes.longitude, "latitude": planes.latitude, "dest": planes.destination_airport_iata, "origin": planes.origin_airport_iata, "id": planes.id, "airline":planes.airline_icao, "callsign": planes.callsign})
        self.plane_data = plane_data_temp
        return self.plane_data
    
    def get_details(self, flight_id):
        """get flight information from ICAO Code"""
        try:
            details = self.fr_api.get_flight_details(flight_id)
        except:
            details = {}
        return details


class DBHandler:
    """
    A Class to handle SQLite Connection

    ...

    Attributes
    ----------
    db_name: str
        name of Database file

    Methods
    -------
    write_all_planes(planes):
        Insert the data for each plane into the table
    get_details(ICAO COde):
        get flight information from ICAO Code
    write_nearest_plane(self, plane):
        Insert the data for the nearest plane into the table
    """

    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS all_planes (icao24 text, longitude real, latitude real, dest text, origin text, id text PRIMARY KEY, airline text, callsign text);")
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS nearest_planes (airplane text, dest text, origin text, airline text, callsign text);''')

    def write_all_planes(self, planes):
        """Insert the data for each plane into the table"""
        for plane in planes:
            self.cursor.execute('''INSERT OR IGNORE INTO all_planes VALUES (?,?,?,?,?,?,?,?)''', (plane['icao24'], plane['longitude'], plane['latitude'], plane['dest'], plane['origin'], plane['id'], plane['airline'], plane['callsign']))

        # Save the changes to the database  
        self.conn.commit()

    def write_nearest_plane(self, plane):
        """Insert the data for the nearest plane into the table"""
        self.cursor.execute('''INSERT INTO nearest_planes VALUES (?,?,?,?,?)''', (plane[4], plane[2], plane[3], plane[1], plane[0]))

        # Save the changes to the database
        self.conn.commit()

#----------------Functions-----------------

def inital_env():
    return dotenv.dotenv_values(".env")

def distance(long,long_target, lat, lat_target):
    """berrechnet den Euclidean abstand zwischen zwei coordinaten"""
    return sqrt((long - long_target)**2 + (lat - lat_target)**2)

def closest_coordinates(planes, target_coord):
    """gets closest ID from a List list of Coordinates to a set Point"""
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

    if not config["COORDS_LO"] or not config["COORDS_LA"]:
        raise Exception("Coords for nearest Plane calculation not complete please check .env")

    sky = flightradar(config["LAMAX"],config["LAMIN"],config["LOMAX"],config["LOMIN"])
    db_handler = DBHandler('plane_data.db')
    mqtt_client = mqtt_flight(config["MQTT_SERVER"], "flightradar_script")

    device_id = "nearest_plane"
    device_name = "Nearest Plane"
    device_manufacturer = "Flightradar24"
    device_model = "N/A"
    sensors = ["callsign", "airline_name" , "airport_orgin" , "airport_dest" ,"aircraft" ]
    data = []

    if mqtt_client.connected:
        mqtt_client.publish_sensor(device_id, device_manufacturer, device_name, device_model, sensors)

    while True:
        result = sky.get_planes_area()

        if result is not None:
            db_handler.write_all_planes(sky.plane_data)

            nearest_icao24 = closest_coordinates(sky.plane_data, [float(config["COORDS_LA"]), float(config["COORDS_LO"])])
            closest = sky.get_details(nearest_icao24)

            data = [(closest.get("identification", {}) or {}).get("callsign", "N/A"), (closest.get("airline",{}) or {}).get("name", "N/A"), ((closest.get("airport",{})or {}).get("origin",{})or {}).get("name", "N/A"), ((closest.get("airport",{})or {}).get("destination",{})or {}).get("name", "N/A"), ((closest.get("aircraft",{})or {}).get("model",{})or {}).get("text", "N/A")]
            logging.warning(data[0] + " von " + data[1] + " fliegt von " + data[2] + " nach "+ data[3] + " mit einem Flugzeug der Klasse: " + data[4])

            db_handler.write_nearest_plane(data)
            if mqtt_client.connected and not mqtt_client.disconnect:
                mqtt_client.publish_data(device_id, sensors, data)
  
        else:
            logging.warning("no Plane found in Area")
        time.sleep(60)